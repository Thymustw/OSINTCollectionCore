//! Admission Controller（SPEC_V0.3 §23/§24）。P0 走獨立保留名額，不排隊；
//! P1-P4 共用一個佇列，依資源壓力訊號動態放行/拒絕。
//!
//! 目前只有兩個真實呼叫端：`merge.rs`（P0，同步 HTTP 路徑）與 stix-worker
//! （P3，背景 job）。P1/P2/P4 型別上保留，等未來真實呼叫端出現才會被用到。

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// 呼叫端宣告的優先權。
///
/// 宣告順序就是 Ord：`P0 < P1 < P2 < P3 < P4`（數字越大優先權越低）。
/// `admit()` 的壓力降級判斷依賴這個順序，**不要改 variant 宣告順序**。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    P0,
    P1,
    P2,
    P3,
    P4,
}

/// 資源壓力訊號。對照 RESOURCE_BUDGET.md §12 的四段狀態機。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourcePressure {
    Normal,
    Elevated,
    High,
    Critical,
}

/// 讀取目前資源壓力的介面。這輪唯一實作是 [`StaticPressure`]（永遠回
/// Normal）——沒有 GPU 監控來源，不假裝有。未來要接真實 GPU/RAM 監控時
/// 實作這個 trait 即可，不用改 [`AdmissionController`] 本身的邏輯。
pub trait PressureSource: Send + Sync {
    fn current(&self) -> ResourcePressure;
}

/// 這輪唯一的 [`PressureSource`] 實作：永遠回報 Normal。
#[derive(Debug, Clone, Copy, Default)]
pub struct StaticPressure;

impl PressureSource for StaticPressure {
    fn current(&self) -> ResourcePressure {
        ResourcePressure::Normal
    }
}

/// Admission 被拒的原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    /// P0 保留名額用完（立即失敗，不等待）。
    P0Busy,
    /// 資源壓力擋下這個優先權（附帶當時的壓力等級）。
    RejectedByPressure(ResourcePressure),
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::P0Busy => write!(
                f,
                "P0 保留名額已滿。互動請求不會排隊，請稍後再試或降低同時進行的 resolve 次數"
            ),
            Self::RejectedByPressure(p) => write!(
                f,
                "資源壓力 {p:?} 擋下這個優先權。背景 AI 工作已暫停放行，請等壓力回落後再試"
            ),
        }
    }
}

impl std::error::Error for AdmissionError {}

/// Admission 通過後持有的 guard。drop 時釋放對應的 semaphore permit。
pub struct AdmitGuard {
    _permit: OwnedSemaphorePermit,
}

impl std::fmt::Debug for AdmitGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmitGuard").finish_non_exhaustive()
    }
}

/// Admission Controller：P0 保留名額 + P1-P4 共用名額，依資源壓力動態放行。
#[derive(Clone)]
pub struct AdmissionController<P: PressureSource = StaticPressure> {
    reserved_p0: Arc<Semaphore>,
    shared: Arc<Semaphore>,
    pressure: P,
}

impl<P: PressureSource> AdmissionController<P> {
    #[must_use]
    pub fn new(p0_capacity: usize, shared_capacity: usize, pressure: P) -> Self {
        Self {
            reserved_p0: Arc::new(Semaphore::new(p0_capacity.max(1))),
            shared: Arc::new(Semaphore::new(shared_capacity.max(1))),
            pressure,
        }
    }

    /// P0：立刻嘗試拿保留名額，拿不到直接回錯誤，不等待。
    pub fn try_admit_p0(&self) -> Result<AdmitGuard, AdmissionError> {
        self.reserved_p0
            .clone()
            .try_acquire_owned()
            .map(|permit| AdmitGuard { _permit: permit })
            .map_err(|_| AdmissionError::P0Busy)
    }

    /// P1-P4：依資源壓力訊號決定是否放行，放行後才等 shared semaphore 的
    /// permit（這一步會等待，不是立即失敗）。
    ///
    /// 降級判斷對照 RESOURCE_BUDGET.md §12：
    /// - Critical：P2-P4 直接拒絕（「stop background jobs」），連 P1 也不放行
    /// - High：P2/P3/P4 拒絕（「defer P2/P3/P4 AI work」）
    /// - Elevated：只擋 P4（「stop new P4 work」）
    /// - Normal：全部放行
    pub async fn admit(&self, priority: Priority) -> Result<AdmitGuard, AdmissionError> {
        let pressure = self.pressure.current();
        let blocked = match pressure {
            ResourcePressure::Normal => false,
            ResourcePressure::Elevated => priority == Priority::P4,
            ResourcePressure::High => priority >= Priority::P2,
            ResourcePressure::Critical => priority >= Priority::P1,
        };
        if blocked {
            return Err(AdmissionError::RejectedByPressure(pressure));
        }
        let permit = self
            .shared
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| AdmissionError::RejectedByPressure(pressure))?;
        Ok(AdmitGuard { _permit: permit })
    }

    #[must_use]
    pub fn p0_available(&self) -> usize {
        self.reserved_p0.available_permits()
    }

    #[must_use]
    pub fn shared_available(&self) -> usize {
        self.shared.available_permits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn p0_busy_returns_immediately_without_waiting() {
        let ctrl = AdmissionController::new(1, 1, StaticPressure);
        let _guard = ctrl.try_admit_p0().expect("第一個 P0 必須拿到名額");

        let started = Instant::now();
        let result =
            tokio::time::timeout(Duration::from_millis(20), async { ctrl.try_admit_p0() }).await;
        let elapsed = started.elapsed();

        let inner = result.expect("try_admit_p0 必須在時限內回來，不能卡住等待");
        assert!(
            matches!(inner, Err(AdmissionError::P0Busy)),
            "期望 P0Busy，實際 {inner:?}"
        );
        assert!(
            elapsed < Duration::from_millis(20),
            "P0 名額用完必須立刻失敗，實際耗時 {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn p1_to_p4_are_admitted_under_static_normal_pressure() {
        let ctrl = AdmissionController::new(1, 4, StaticPressure);
        for priority in [Priority::P1, Priority::P2, Priority::P3, Priority::P4] {
            let guard = ctrl
                .admit(priority)
                .await
                .unwrap_or_else(|err| panic!("{priority:?} 在 Normal 下必須放行，實際 {err:?}"));
            drop(guard);
        }
    }

    #[tokio::test]
    async fn shared_capacity_caps_concurrent_p1_p4_permits() {
        let capacity = 2usize;
        let ctrl = Arc::new(AdmissionController::new(1, capacity, StaticPressure));
        let peak = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..6 {
            let ctrl = Arc::clone(&ctrl);
            let peak = Arc::clone(&peak);
            let current = Arc::clone(&current);
            handles.push(tokio::spawn(async move {
                let _guard = ctrl
                    .admit(Priority::P3)
                    .await
                    .expect("Normal 下 P3 必須放行");
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                current.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for handle in handles {
            handle.await.expect("join");
        }
        let seen = peak.load(Ordering::SeqCst);
        assert!(
            seen <= capacity,
            "同時拿到的 shared permit 應 ≤ {capacity}，實際 {seen}"
        );
        assert!(seen >= 1, "應至少放行過一次");
        assert_eq!(
            seen, capacity,
            "6 個 task、容量 {capacity}，peak 應碰到上限才算真的在排隊"
        );
    }

    #[test]
    fn admit_guard_drop_returns_p0_permit() {
        let ctrl = AdmissionController::new(1, 1, StaticPressure);
        assert_eq!(ctrl.p0_available(), 1);
        {
            let _guard = ctrl.try_admit_p0().expect("應拿到唯一的 P0 名額");
            assert_eq!(ctrl.p0_available(), 0);
            assert!(
                matches!(ctrl.try_admit_p0(), Err(AdmissionError::P0Busy)),
                "名額用完必須立刻回 P0Busy"
            );
        }
        assert_eq!(ctrl.p0_available(), 1, "AdmitGuard drop 後名額必須歸還");
        assert!(
            ctrl.try_admit_p0().is_ok(),
            "歸還後下一次 try_admit_p0 必須成功"
        );
    }

    struct CriticalPressure;

    impl PressureSource for CriticalPressure {
        fn current(&self) -> ResourcePressure {
            ResourcePressure::Critical
        }
    }

    #[tokio::test]
    async fn critical_pressure_rejects_p1_with_matching_error() {
        let ctrl = AdmissionController::new(1, 4, CriticalPressure);
        let err = ctrl
            .admit(Priority::P1)
            .await
            .expect_err("Critical 必須擋 P1");
        assert_eq!(
            err,
            AdmissionError::RejectedByPressure(ResourcePressure::Critical)
        );
    }

    struct HighPressure;

    impl PressureSource for HighPressure {
        fn current(&self) -> ResourcePressure {
            ResourcePressure::High
        }
    }

    #[tokio::test]
    async fn high_pressure_admits_p1_but_rejects_p2() {
        let ctrl = AdmissionController::new(1, 4, HighPressure);
        ctrl.admit(Priority::P1).await.expect("High 仍應放行 P1");
        let err = ctrl.admit(Priority::P2).await.expect_err("High 必須擋 P2");
        assert_eq!(
            err,
            AdmissionError::RejectedByPressure(ResourcePressure::High)
        );
    }
}
