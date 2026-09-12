//! 批次累積與 backpressure 的決策邏輯。
//!
//! 刻意做成**純函式**（不碰 Kafka、不碰 OpenSearch、不碰時鐘以外的東西）：
//! 「什麼時候該送出」「lag 多高要降速」這兩件事若寫在 consumer 迴圈裡，
//! 只能靠真的塞滿一個 partition 才驗得到。

use std::time::{Duration, Instant};

use crate::service::IndexBounds;

/// 為什麼現在要 flush。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// 累積到 `batch_size`。
    BatchFull,
    /// 距離第一筆進來已經超過 `batch_timeout`。
    Timeout,
    /// 正在關閉，把手上的送完。
    Shutdown,
}

/// 批次狀態機。
#[derive(Debug)]
pub struct BatchController {
    bounds: IndexBounds,
    /// 這一批第一筆進來的時間。空批次時是 `None`。
    opened_at: Option<Instant>,
}

impl BatchController {
    #[must_use]
    pub fn new(bounds: IndexBounds) -> Self {
        Self {
            bounds,
            opened_at: None,
        }
    }

    /// 有一筆進批次了。
    pub fn record_push(&mut self, now: Instant) {
        if self.opened_at.is_none() {
            self.opened_at = Some(now);
        }
    }

    /// flush 完成，重新開始計時。
    pub fn reset(&mut self) {
        self.opened_at = None;
    }

    /// 現在該不該送出。`len` 是目前批次大小。
    #[must_use]
    pub fn should_flush(&self, len: usize, now: Instant) -> Option<FlushReason> {
        if len == 0 {
            return None;
        }
        if len as u32 >= self.bounds.batch_size {
            return Some(FlushReason::BatchFull);
        }
        let opened = self.opened_at?;
        if now.duration_since(opened) >= self.bounds.batch_timeout {
            return Some(FlushReason::Timeout);
        }
        None
    }

    /// 下一次 poll 該等多久。
    ///
    /// 批次空的時候等久一點（沒事可做，不要空轉）；批次非空時**最多只能等到逾時為止**，
    /// 否則一筆孤單的訊息會被下一次 poll 的長逾時卡住整整 30 秒才進 index。
    #[must_use]
    pub fn poll_timeout(&self, len: usize, idle: Duration, now: Instant) -> Duration {
        if len == 0 {
            return idle;
        }
        let Some(opened) = self.opened_at else {
            return idle;
        };
        let elapsed = now.duration_since(opened);
        self.bounds
            .batch_timeout
            .saturating_sub(elapsed)
            // 0 會讓 poll 立刻回逾時並且完全不收訊息，變成忙碌迴圈。
            .max(Duration::from_millis(1))
    }
}

/// lag 對應的降速決策。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backpressure {
    pub active: bool,
    pub sleep: Duration,
}

/// consumer lag → 要不要在批次之間插入延遲。
///
/// # 這個延遲在做什麼（以及不在做什麼）
///
/// 它**不會**讓 lag 變小——indexer 本來就已經在全速消費。
/// 它的目的是 CLAUDE.md §7 的「Background work must yield under VM/host pressure」：
/// lag 高的時候瓶頸幾乎一定在 OpenSearch（bulk 佇列滿、CPU 吃滿），
/// 這時候更用力送只會換到更多 429 與重試，對這台共用工作站上的其他 VM 也不友善。
///
/// **真正往上游傳遞的 backpressure 是 `osint_queue_depth` 這個 gauge**
/// （indexer 把 lag 寫進去）。collector 依 `RESOURCE_BUDGET.md` 依它降低採集速率——
/// 那才是讓 lag 變小的那一端。這裡只負責不要把下游壓垮。
#[must_use]
pub fn backpressure(lag: u64, bounds: IndexBounds) -> Backpressure {
    if lag <= bounds.lag_threshold {
        return Backpressure {
            active: false,
            sleep: Duration::ZERO,
        };
    }
    // 超過門檻多少倍，就睡多少倍（上限 8 倍，免得一次卡住幾十秒讓 consumer
    // 被 broker 踢出 group——session.timeout.ms 是 10 秒）。
    let factor = (lag / bounds.lag_threshold.max(1)).clamp(1, 8);
    Backpressure {
        active: true,
        sleep: bounds.backpressure_sleep * u32::try_from(factor).unwrap_or(1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bounds() -> IndexBounds {
        IndexBounds {
            batch_size: 3,
            batch_timeout: Duration::from_millis(100),
            lag_threshold: 1_000,
            backpressure_sleep: Duration::from_millis(200),
            ..IndexBounds::default()
        }
    }

    #[test]
    fn empty_batch_never_flushes() {
        let controller = BatchController::new(bounds());
        assert_eq!(controller.should_flush(0, Instant::now()), None);
    }

    #[test]
    fn full_batch_flushes_immediately() {
        let mut controller = BatchController::new(bounds());
        let now = Instant::now();
        controller.record_push(now);
        assert_eq!(
            controller.should_flush(3, now),
            Some(FlushReason::BatchFull)
        );
    }

    #[test]
    fn partial_batch_flushes_on_timeout() {
        let mut controller = BatchController::new(bounds());
        let start = Instant::now();
        controller.record_push(start);
        assert_eq!(controller.should_flush(1, start), None);
        assert_eq!(
            controller.should_flush(1, start + Duration::from_millis(100)),
            Some(FlushReason::Timeout),
            "沒有這條路徑的話，低流量時最後幾筆永遠不進 index"
        );
    }

    #[test]
    fn reset_restarts_the_timer() {
        let mut controller = BatchController::new(bounds());
        let start = Instant::now();
        controller.record_push(start);
        controller.reset();
        assert_eq!(
            controller.should_flush(1, start + Duration::from_secs(10)),
            None,
            "reset 後還沒有新的 push，不該有逾時"
        );
    }

    #[test]
    fn poll_timeout_shrinks_as_the_batch_ages() {
        let mut controller = BatchController::new(bounds());
        let start = Instant::now();
        let idle = Duration::from_secs(30);
        assert_eq!(controller.poll_timeout(0, idle, start), idle);
        controller.record_push(start);
        assert_eq!(
            controller.poll_timeout(1, idle, start),
            Duration::from_millis(100)
        );
        assert_eq!(
            controller.poll_timeout(1, idle, start + Duration::from_millis(60)),
            Duration::from_millis(40)
        );
    }

    #[test]
    fn poll_timeout_never_hits_zero() {
        // 0 會讓 poll 立刻回逾時而完全不收訊息 → 忙碌迴圈把 CPU 吃滿。
        let mut controller = BatchController::new(bounds());
        let start = Instant::now();
        controller.record_push(start);
        let timeout =
            controller.poll_timeout(1, Duration::from_secs(30), start + Duration::from_secs(5));
        assert!(!timeout.is_zero());
    }

    #[test]
    fn backpressure_is_off_below_the_threshold() {
        let bp = backpressure(999, bounds());
        assert!(!bp.active);
        assert!(bp.sleep.is_zero());
    }

    #[test]
    fn backpressure_scales_with_lag_but_is_capped() {
        assert_eq!(
            backpressure(2_500, bounds()).sleep,
            Duration::from_millis(400)
        );
        assert_eq!(
            backpressure(1_000_000, bounds()).sleep,
            Duration::from_millis(1_600),
            "上限 8 倍：睡超過 session.timeout.ms（10 秒）會被 broker 踢出 consumer group"
        );
    }

    #[test]
    fn backpressure_sleep_stays_under_the_session_timeout() {
        // session.timeout.ms 在 core-events 是 10000。這裡的最大值必須遠低於它。
        let bp = backpressure(u64::MAX, IndexBounds::default());
        assert!(
            bp.sleep < Duration::from_secs(5),
            "實際 {:?}：睡太久會讓 consumer 被踢出 group，反而讓 lag 更糟",
            bp.sleep
        );
    }
}
