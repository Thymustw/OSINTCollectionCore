//! 連接器失敗的指數退避。collector 排程與 ops 健康視圖共用同一套算法。
//!
//! 退避只延後、不提前：下次可執行時間是 cron 下一個時點與
//! `last_run + retry_backoff` 的較晚者。`backoff_base = 0` 表示關閉退避。

use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use core_model::Connector;
use croner::Cron;
use uuid::Uuid;

/// 與 `[collector].backoff_base_secs` 預設值對齊（5 分鐘）。
pub const DEFAULT_BASE_SECS: u64 = 300;
/// 與 `[collector].backoff_max_secs` 預設值對齊（12 小時）。
pub const DEFAULT_MAX_SECS: u64 = 43_200;

/// 指數退避上限與基準。jitter 不放在這裡：由 `connector_id` 決定，可重現。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffConfig {
    pub base: Duration,
    pub max: Duration,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self::from_secs(DEFAULT_BASE_SECS, DEFAULT_MAX_SECS)
    }
}

impl BackoffConfig {
    /// 以秒數建立。`max < base` 時 `retry_backoff` 仍會夾在 `max` 內。
    /// `base = 0` 表示關閉退避（恆回 0）。
    #[must_use]
    pub fn from_secs(base_secs: u64, max_secs: u64) -> Self {
        Self {
            base: Duration::from_secs(base_secs),
            max: Duration::from_secs(max_secs),
        }
    }
}

/// 連續失敗 `error_count` 次之後，距離 `last_run` 還要再等多久。
///
/// - `error_count <= 0` 或 `cfg.base == 0` → 0（不退避）
/// - 否則 `min(base × 2^(n−1), max)`，再套上由 `connector_id` 決定的 ±10% jitter
/// - `n` 大到 `2^(n−1)` 會溢位時直接回 `max`（jitter 後仍不得超過 `max`、不得為負）
#[must_use]
pub fn retry_backoff(error_count: i32, connector_id: Uuid, cfg: &BackoffConfig) -> Duration {
    if error_count <= 0 || cfg.base.is_zero() {
        return Duration::ZERO;
    }
    let delay = exponential_capped(error_count, cfg);
    apply_jitter(delay, connector_id, cfg.max)
}

/// 下次 collector 會嘗試執行的時間。
///
/// - 未排程（`schedule` 空／空白）→ `None`（不會自動跑）
/// - 從未跑過 → `Some(now)`（立刻到期；沒有 `last_run` 可加退避）
/// - 否則 → `max(cron 依 last_run 的下一個時點, last_run + retry_backoff)`
///
/// cron 解析失敗時仍回退避終點（或 `now`），讓 ops 至少看得到「還要等多久」。
#[must_use]
pub fn next_attempt_at(
    connector: &Connector,
    now: DateTime<Utc>,
    cfg: &BackoffConfig,
) -> Option<DateTime<Utc>> {
    let schedule = connector.schedule.as_deref().map(str::trim).unwrap_or("");
    if schedule.is_empty() {
        return None;
    }
    let backoff_until = match connector.last_run {
        Some(last) => add_duration(
            last,
            retry_backoff(connector.error_count, connector.id, cfg),
        ),
        None => now,
    };
    let cron_next = cron_next_after(schedule, connector.last_run, now).unwrap_or(now);
    Some(cron_next.max(backoff_until))
}

/// 現在是否仍在退避視窗內（與 cron 是否到期無關）。
///
/// `error_count <= 0` 或沒有 `last_run` 都不是退避中。
#[must_use]
pub fn is_backing_off(connector: &Connector, now: DateTime<Utc>, cfg: &BackoffConfig) -> bool {
    if connector.error_count <= 0 {
        return false;
    }
    let Some(last) = connector.last_run else {
        return false;
    };
    now < add_duration(
        last,
        retry_backoff(connector.error_count, connector.id, cfg),
    )
}

fn exponential_capped(error_count: i32, cfg: &BackoffConfig) -> Duration {
    let exp = (error_count as u32).saturating_sub(1);
    // 2^63 * 1 秒就已經遠超過任何合理的 max；直接回 max 避免位移溢位。
    if exp >= 63 {
        return cfg.max;
    }
    let factor = 1u64 << exp;
    let base_secs = cfg.base.as_secs();
    let max_secs = cfg.max.as_secs();
    let secs = base_secs.saturating_mul(factor).min(max_secs);
    Duration::from_secs(secs)
}

/// 由 UUID 決定 [-10%, +10%] 的可重現 jitter，不用亂數。
fn apply_jitter(delay: Duration, connector_id: Uuid, max: Duration) -> Duration {
    if delay.is_zero() {
        return Duration::ZERO;
    }
    // 0..=2000 → -1000..=+1000，單位是「萬分之 delay」（±10.00%）。
    let jitter_bp = (connector_id.as_u128() % 2001) as i64 - 1000;
    let delay_ms = delay.as_millis() as i128;
    let adjusted = delay_ms.saturating_mul(10_000 + i128::from(jitter_bp)) / 10_000;
    let max_ms = max.as_millis() as i128;
    let clamped = adjusted.clamp(0, max_ms);
    Duration::from_millis(clamped as u64)
}

fn add_duration(when: DateTime<Utc>, delay: Duration) -> DateTime<Utc> {
    let delta = chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::zero());
    when + delta
}

fn cron_next_after(
    schedule: &str,
    last_run: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Option<DateTime<Utc>> {
    let cron = Cron::from_str(schedule).ok()?;
    let Some(last) = last_run else {
        return Some(now);
    };
    cron.find_next_occurrence(&last, false).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    /// jitter = 0 的 id：`as_u128() % 2001 == 1000`。
    fn zero_jitter_id() -> Uuid {
        Uuid::from_u128(1000)
    }

    fn ts(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    fn cfg() -> BackoffConfig {
        BackoffConfig::from_secs(300, 43_200)
    }

    fn connector(
        id: Uuid,
        schedule: Option<&str>,
        last_run: Option<DateTime<Utc>>,
        error_count: i32,
    ) -> Connector {
        Connector {
            id,
            source_id: Uuid::nil(),
            name: "test".into(),
            connector_type: "rss".into(),
            version: "0.1.0".into(),
            enabled: true,
            configuration: json!({}),
            credential_reference: None,
            schedule: schedule.map(str::to_string),
            rate_limit: json!({}),
            timeout: json!({}),
            proxy_reference: None,
            checkpoint: json!({}),
            last_run,
            last_success: None,
            status: "idle".into(),
            error_count,
        }
    }

    #[test]
    fn sequence_is_base_times_power_of_two() {
        let id = zero_jitter_id();
        let cfg = cfg();
        assert_eq!(retry_backoff(1, id, &cfg), Duration::from_secs(300));
        assert_eq!(retry_backoff(2, id, &cfg), Duration::from_secs(600));
        assert_eq!(retry_backoff(3, id, &cfg), Duration::from_secs(1_200));
        assert_eq!(retry_backoff(4, id, &cfg), Duration::from_secs(2_400));
        assert_eq!(retry_backoff(5, id, &cfg), Duration::from_secs(4_800));
        assert_eq!(retry_backoff(6, id, &cfg), Duration::from_secs(9_600));
        assert_eq!(retry_backoff(7, id, &cfg), Duration::from_secs(19_200));
        assert_eq!(retry_backoff(8, id, &cfg), Duration::from_secs(38_400));
    }

    #[test]
    fn caps_at_max() {
        let id = zero_jitter_id();
        let cfg = cfg();
        assert_eq!(retry_backoff(9, id, &cfg), Duration::from_secs(43_200));
        assert_eq!(retry_backoff(10, id, &cfg), Duration::from_secs(43_200));
    }

    #[test]
    fn overflow_error_count_returns_max() {
        let id = zero_jitter_id();
        let cfg = cfg();
        assert_eq!(
            retry_backoff(i32::MAX, id, &cfg),
            Duration::from_secs(43_200)
        );
    }

    #[test]
    fn non_positive_error_count_is_zero() {
        let id = zero_jitter_id();
        let cfg = cfg();
        assert_eq!(retry_backoff(0, id, &cfg), Duration::ZERO);
        assert_eq!(retry_backoff(-3, id, &cfg), Duration::ZERO);
    }

    #[test]
    fn base_zero_disables_backoff() {
        let id = Uuid::from_u128(42);
        let cfg = BackoffConfig::from_secs(0, 43_200);
        assert_eq!(retry_backoff(1, id, &cfg), Duration::ZERO);
        assert_eq!(retry_backoff(99, id, &cfg), Duration::ZERO);
        assert_eq!(retry_backoff(i32::MAX, id, &cfg), Duration::ZERO);
    }

    #[test]
    fn jitter_stays_within_ten_percent_and_is_deterministic() {
        let cfg = cfg();
        let base = Duration::from_secs(300);
        let min = Duration::from_millis((300_000.0 * 0.9) as u64);
        let max = Duration::from_millis((300_000.0 * 1.1) as u64);
        let mut seen = Vec::new();
        for n in 0u128..50 {
            let id = Uuid::from_u128(n * 17 + 3);
            let d = retry_backoff(1, id, &cfg);
            assert!(
                d >= min && d <= max,
                "jitter 必須在 ±10% 內：id={id} delay={d:?} 範圍 {min:?}..={max:?}"
            );
            assert!(d <= cfg.max, "jitter 後不得超過 max");
            assert_eq!(retry_backoff(1, id, &cfg), d, "同一 id 必須得到同一結果");
            seen.push(d);
        }
        assert!(
            seen.iter().any(|d| *d != base),
            "抽 50 個 id 至少要看到非零 jitter，否則雜湊沒生效"
        );
    }

    #[test]
    fn jitter_does_not_exceed_max() {
        let cfg = BackoffConfig::from_secs(300, 300);
        for n in 0u128..30 {
            let id = Uuid::from_u128(n);
            let d = retry_backoff(5, id, &cfg);
            assert!(d <= cfg.max, "已達上限時 jitter 仍不得超過 max：{d:?}");
        }
    }

    #[test]
    fn next_attempt_at_takes_later_of_cron_and_backoff() {
        let id = zero_jitter_id();
        let cfg = cfg();
        let last = ts(2026, 1, 1, 12, 0);

        // n=1 → 退避 5 分鐘（12:05）；cron 下一個是 12:15 → 取 12:15
        let c = connector(id, Some("*/15 * * * *"), Some(last), 1);
        assert_eq!(
            next_attempt_at(&c, ts(2026, 1, 1, 12, 1), &cfg),
            Some(ts(2026, 1, 1, 12, 15))
        );

        // n=4 → 退避 40 分鐘（12:40）；cron 下一個是 12:15 → 取 12:40
        let c = connector(id, Some("*/15 * * * *"), Some(last), 4);
        assert_eq!(
            next_attempt_at(&c, ts(2026, 1, 1, 12, 1), &cfg),
            Some(ts(2026, 1, 1, 12, 40))
        );
    }

    #[test]
    fn next_attempt_at_never_run_is_now() {
        let now = ts(2026, 1, 1, 12, 0);
        let c = connector(zero_jitter_id(), Some("*/15 * * * *"), None, 0);
        assert_eq!(next_attempt_at(&c, now, &cfg()), Some(now));
    }

    #[test]
    fn next_attempt_at_unscheduled_is_none() {
        let now = ts(2026, 1, 1, 12, 0);
        let c = connector(zero_jitter_id(), None, Some(now), 3);
        assert_eq!(next_attempt_at(&c, now, &cfg()), None);
        let blank = connector(zero_jitter_id(), Some("  "), Some(now), 3);
        assert_eq!(next_attempt_at(&blank, now, &cfg()), None);
    }

    #[test]
    fn is_backing_off_only_inside_window() {
        let id = zero_jitter_id();
        let cfg = cfg();
        let last = ts(2026, 1, 1, 12, 0);
        let c = connector(id, Some("*/15 * * * *"), Some(last), 1);
        assert!(is_backing_off(&c, ts(2026, 1, 1, 12, 4), &cfg));
        assert!(!is_backing_off(&c, ts(2026, 1, 1, 12, 5), &cfg));
        let healthy = connector(id, Some("*/15 * * * *"), Some(last), 0);
        assert!(!is_backing_off(&healthy, ts(2026, 1, 1, 12, 4), &cfg));
    }
}
