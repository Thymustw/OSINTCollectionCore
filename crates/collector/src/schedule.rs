//! cron 字串解析與到期判斷。

use chrono::{DateTime, Utc};
use croner::Cron;
use std::str::FromStr;

use crate::error::CollectorError;

/// 解析 5 欄（或 6 欄含秒）cron。失敗不 panic。
pub fn parse_cron(schedule: &str) -> Result<Cron, CollectorError> {
    let trimmed = schedule.trim();
    Cron::from_str(trimmed).map_err(|err| CollectorError::InvalidSchedule {
        schedule: trimmed.to_string(),
        message: err.to_string(),
    })
}

/// `last_run` 為空 → 到期（尚未跑過，立刻跑一次）。
/// 否則看 `last_run` 之後的下一個 occurrence 是否 `<= now`。
pub fn is_due(
    schedule: &str,
    last_run: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> Result<bool, CollectorError> {
    let cron = parse_cron(schedule)?;
    let Some(last) = last_run else {
        return Ok(true);
    };
    if last >= now {
        return Ok(false);
    }
    match cron.find_next_occurrence(&last, false) {
        Ok(next) => Ok(next <= now),
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, m: u32, d: u32, h: u32, min: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    #[test]
    fn never_run_is_due() {
        assert!(is_due("*/15 * * * *", None, ts(2026, 9, 11, 12, 0)).unwrap());
    }

    #[test]
    fn every_15_minutes_after_slot() {
        let last = ts(2026, 9, 11, 12, 0);
        let now = ts(2026, 9, 11, 12, 15);
        assert!(is_due("*/15 * * * *", Some(last), now).unwrap());
    }

    #[test]
    fn not_due_before_next_slot() {
        let last = ts(2026, 9, 11, 12, 0);
        let now = ts(2026, 9, 11, 12, 10);
        assert!(!is_due("*/15 * * * *", Some(last), now).unwrap());
    }

    #[test]
    fn invalid_cron_explains_next_step() {
        let err = parse_cron("not a cron").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("5 欄"), "{msg}");
        assert!(msg.contains("*/15"), "{msg}");
    }
}
