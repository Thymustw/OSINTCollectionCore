//! 閘道觀測到的資源壓力（[`ObservedPressure`]）。
//!
//! 這不是 GPU／主機 RAM 監控——那些訊號這台機器上沒有可驗證的來源，不假裝有。
//! 這裡看的是**櫃台自己感受到的忙碌程度**：近期延遲、429 比例、逾時比例、
//! 5xx 比例、admission 排隊中的請求數。
//!
//! 門檻是**推測值，未做容量測試**。沒流量或樣本太少時固定回 [`ResourcePressure::Normal`]，
//! 避免 1 筆慢請求就降級。狀態降級有冷卻，避免在門檻附近來回跳。
//!
//! P0 不看這個訊號（見 [`crate::AdmissionController::try_admit_p0`]）——那是刻意契約。

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::admission::{PressureSource, ResourcePressure};

/// 單次嘗試的分類。成功與失敗都記延遲；分類決定比例。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptClass {
    Success,
    /// HTTP 429。
    RateLimited,
    Timeout,
    /// HTTP 5xx（含 502／503；不含 429）。
    ServerError,
    /// 其他錯誤（4xx 永久性、解析失敗、連線失敗…）。不計入 429／逾時／5xx 比例。
    Other,
}

/// 可注入的時鐘。單元測試用 [`ManualClock`]，不要真的睡。
pub trait InstantSource: Send + Sync {
    fn now(&self) -> Instant;
}

/// 生產用：`Instant::now()`。
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl InstantSource for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// 測試用可前進的時鐘。
pub struct ManualClock {
    now: Mutex<Instant>,
}

impl ManualClock {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new(Instant::now()),
        })
    }

    pub fn advance(&self, duration: Duration) {
        let mut now = self.now.lock().unwrap_or_else(|err| err.into_inner());
        *now += duration;
    }
}

impl InstantSource for ManualClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap_or_else(|err| err.into_inner())
    }
}

/// [`ObservedPressure`] 的門檻。
///
/// **全部都是推測值，未做容量測試。** 換部署或真實流量來了之後應重測再改
/// `config/default.toml` 的 `[ai_gateway.pressure]`。
#[derive(Debug, Clone, PartialEq)]
pub struct PressureThresholds {
    /// `false` 時 [`ObservedPressure::current`] 永遠回 Normal（仍可記錄樣本與 metric）。
    pub enabled: bool,
    pub window: Duration,
    /// 少於此數視為樣本不足，強制 Normal。
    pub min_samples: usize,
    /// 降級（壓力往下走）最短間隔。升級（壓力往上走）立即生效。
    pub cooldown: Duration,
    pub elevated_p95: Duration,
    pub high_p95: Duration,
    pub critical_p95: Duration,
    pub elevated_429_ratio: f64,
    pub high_429_ratio: f64,
    pub critical_429_ratio: f64,
    pub elevated_timeout_ratio: f64,
    pub high_timeout_ratio: f64,
    pub critical_timeout_ratio: f64,
    pub elevated_5xx_ratio: f64,
    pub high_5xx_ratio: f64,
    pub critical_5xx_ratio: f64,
    pub elevated_queue: usize,
    pub high_queue: usize,
    pub critical_queue: usize,
}

impl PressureThresholds {
    /// 與 `config/default.toml` 的 `[ai_gateway.pressure]` 預設值對齊。
    ///
    /// 推測值，未做容量測試。
    #[must_use]
    pub fn speculative_defaults() -> Self {
        Self {
            enabled: true,
            window: Duration::from_secs(30),
            min_samples: 5,
            cooldown: Duration::from_secs(15),
            elevated_p95: Duration::from_millis(5_000),
            high_p95: Duration::from_millis(15_000),
            critical_p95: Duration::from_millis(30_000),
            elevated_429_ratio: 0.10,
            high_429_ratio: 0.25,
            critical_429_ratio: 0.50,
            elevated_timeout_ratio: 0.05,
            high_timeout_ratio: 0.15,
            critical_timeout_ratio: 0.30,
            elevated_5xx_ratio: 0.05,
            high_5xx_ratio: 0.15,
            critical_5xx_ratio: 0.30,
            elevated_queue: 2,
            high_queue: 8,
            critical_queue: 16,
        }
    }
}

const MAX_SAMPLES: usize = 1_024;

struct Sample {
    at: Instant,
    latency: Duration,
    class: AttemptClass,
}

struct Inner {
    samples: VecDeque<Sample>,
    level: ResourcePressure,
    last_change: Instant,
}

/// 滑動時間窗壓力訊號。
pub struct ObservedPressure {
    inner: Mutex<Inner>,
    queue_depth: AtomicUsize,
    thresholds: PressureThresholds,
    clock: Arc<dyn InstantSource>,
}

impl ObservedPressure {
    #[must_use]
    pub fn new(thresholds: PressureThresholds) -> Arc<Self> {
        Self::with_clock(thresholds, Arc::new(SystemClock))
    }

    #[must_use]
    pub fn with_clock(thresholds: PressureThresholds, clock: Arc<dyn InstantSource>) -> Arc<Self> {
        let now = clock.now();
        Arc::new(Self {
            inner: Mutex::new(Inner {
                samples: VecDeque::new(),
                level: ResourcePressure::Normal,
                last_change: now,
            }),
            queue_depth: AtomicUsize::new(0),
            thresholds,
            clock,
        })
    }

    /// `enabled=false` 的便利建構子：永遠 Normal，仍接受 `record`。
    #[must_use]
    pub fn disabled() -> Arc<Self> {
        let mut thresholds = PressureThresholds::speculative_defaults();
        thresholds.enabled = false;
        Self::new(thresholds)
    }

    pub fn set_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    pub fn record(&self, class: AttemptClass, latency: Duration) {
        let now = self.clock.now();
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        inner.samples.push_back(Sample {
            at: now,
            latency,
            class,
        });
        prune(&mut inner.samples, now, self.thresholds.window);
        while inner.samples.len() > MAX_SAMPLES {
            inner.samples.pop_front();
        }
    }

    fn evaluate(&self) -> ResourcePressure {
        if !self.thresholds.enabled {
            return ResourcePressure::Normal;
        }
        let now = self.clock.now();
        let queue = self.queue_depth.load(Ordering::Relaxed);
        let mut inner = self.inner.lock().unwrap_or_else(|err| err.into_inner());
        prune(&mut inner.samples, now, self.thresholds.window);
        let raw = raw_level(&inner.samples, queue, &self.thresholds);
        apply_hysteresis(&mut inner, raw, now, self.thresholds.cooldown);
        inner.level
    }
}

impl PressureSource for ObservedPressure {
    fn current(&self) -> ResourcePressure {
        self.evaluate()
    }
}

fn prune(samples: &mut VecDeque<Sample>, now: Instant, window: Duration) {
    while let Some(front) = samples.front() {
        if now.saturating_duration_since(front.at) > window {
            samples.pop_front();
        } else {
            break;
        }
    }
}

fn raw_level(samples: &VecDeque<Sample>, queue: usize, t: &PressureThresholds) -> ResourcePressure {
    if samples.len() < t.min_samples {
        // 樣本不足維持 Normal 是刻意的：避免 1 筆慢請求就降級。
        return ResourcePressure::Normal;
    }
    let n = samples.len() as f64;
    let p95 = percentile95(samples.iter().map(|s| s.latency));
    let ratio_429 = samples
        .iter()
        .filter(|s| s.class == AttemptClass::RateLimited)
        .count() as f64
        / n;
    let ratio_timeout = samples
        .iter()
        .filter(|s| s.class == AttemptClass::Timeout)
        .count() as f64
        / n;
    let ratio_5xx = samples
        .iter()
        .filter(|s| s.class == AttemptClass::ServerError)
        .count() as f64
        / n;

    let mut level = ResourcePressure::Normal;
    if p95 >= t.elevated_p95
        || ratio_429 >= t.elevated_429_ratio
        || ratio_timeout >= t.elevated_timeout_ratio
        || ratio_5xx >= t.elevated_5xx_ratio
        || queue >= t.elevated_queue
    {
        level = ResourcePressure::Elevated;
    }
    if p95 >= t.high_p95
        || ratio_429 >= t.high_429_ratio
        || ratio_timeout >= t.high_timeout_ratio
        || ratio_5xx >= t.high_5xx_ratio
        || queue >= t.high_queue
    {
        level = ResourcePressure::High;
    }
    if p95 >= t.critical_p95
        || ratio_429 >= t.critical_429_ratio
        || ratio_timeout >= t.critical_timeout_ratio
        || ratio_5xx >= t.critical_5xx_ratio
        || queue >= t.critical_queue
    {
        level = ResourcePressure::Critical;
    }
    level
}

/// 升級立即生效；降級一次只走一格，且必須過完冷卻。
///
/// 冷卻只約束降級：突發 429 必須立刻擋 P3，但恢復要等窗口與冷卻，
/// 避免在門檻附近抖動（例如 24%／26% 的 429 來回跳 Elevated／High）。
fn apply_hysteresis(inner: &mut Inner, raw: ResourcePressure, now: Instant, cooldown: Duration) {
    let current = inner.level.severity();
    let target = raw.severity();
    if target > current {
        inner.level = raw;
        inner.last_change = now;
        return;
    }
    if target < current && now.saturating_duration_since(inner.last_change) >= cooldown {
        inner.level = step_down(inner.level);
        inner.last_change = now;
    }
}

fn step_down(level: ResourcePressure) -> ResourcePressure {
    match level {
        ResourcePressure::Critical => ResourcePressure::High,
        ResourcePressure::High => ResourcePressure::Elevated,
        ResourcePressure::Elevated => ResourcePressure::Normal,
        ResourcePressure::Normal => ResourcePressure::Normal,
    }
}

fn percentile95<I>(latencies: I) -> Duration
where
    I: IntoIterator<Item = Duration>,
{
    let mut values: Vec<Duration> = latencies.into_iter().collect();
    if values.is_empty() {
        return Duration::ZERO;
    }
    values.sort_unstable();
    let idx = ((values.len() as f64) * 0.95).ceil() as usize;
    let idx = idx.saturating_sub(1).min(values.len() - 1);
    values[idx]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admission::{AdmissionController, AdmissionError, Priority};

    fn fast_thresholds() -> PressureThresholds {
        PressureThresholds {
            enabled: true,
            window: Duration::from_secs(60),
            min_samples: 5,
            cooldown: Duration::from_secs(10),
            elevated_p95: Duration::from_millis(100),
            high_p95: Duration::from_millis(200),
            critical_p95: Duration::from_millis(400),
            elevated_429_ratio: 0.20,
            high_429_ratio: 0.50,
            critical_429_ratio: 0.80,
            elevated_timeout_ratio: 1.1, // 測 429／延遲時不要被逾時門檻干擾
            high_timeout_ratio: 1.1,
            critical_timeout_ratio: 1.1,
            elevated_5xx_ratio: 1.1,
            high_5xx_ratio: 1.1,
            critical_5xx_ratio: 1.1,
            elevated_queue: 100,
            high_queue: 100,
            critical_queue: 100,
        }
    }

    fn record_n(p: &ObservedPressure, class: AttemptClass, latency: Duration, n: usize) {
        for _ in 0..n {
            p.record(class, latency);
        }
    }

    #[test]
    fn insufficient_samples_stay_normal() {
        let clock = ManualClock::new();
        let p = ObservedPressure::with_clock(fast_thresholds(), clock);
        record_n(&p, AttemptClass::RateLimited, Duration::from_millis(10), 4);
        assert_eq!(
            p.current(),
            ResourcePressure::Normal,
            "4 < min_samples=5，即使全是 429 也必須維持 Normal"
        );
    }

    #[test]
    fn disabled_stays_normal_even_with_many_429s() {
        let mut t = fast_thresholds();
        t.enabled = false;
        t.min_samples = 1;
        let p = ObservedPressure::new(t);
        record_n(&p, AttemptClass::RateLimited, Duration::from_millis(10), 20);
        assert_eq!(p.current(), ResourcePressure::Normal);
    }

    #[test]
    fn latency_sequence_raises_through_levels() {
        let clock = ManualClock::new();
        let p = ObservedPressure::with_clock(fast_thresholds(), clock);
        record_n(&p, AttemptClass::Success, Duration::from_millis(100), 5);
        assert_eq!(p.current(), ResourcePressure::Elevated);
        record_n(&p, AttemptClass::Success, Duration::from_millis(250), 5);
        assert_eq!(p.current(), ResourcePressure::High);
        record_n(&p, AttemptClass::Success, Duration::from_millis(500), 5);
        assert_eq!(p.current(), ResourcePressure::Critical);
    }

    #[test]
    fn four_two_nine_ratio_raises_to_high() {
        let clock = ManualClock::new();
        let p = ObservedPressure::with_clock(fast_thresholds(), clock);
        record_n(&p, AttemptClass::Success, Duration::from_millis(10), 2);
        record_n(&p, AttemptClass::RateLimited, Duration::from_millis(10), 3);
        // 3/5 = 0.6 → High（≥0.50 且 <0.80）
        assert_eq!(p.current(), ResourcePressure::High);
    }

    #[test]
    fn hysteresis_holds_high_until_cooldown_then_steps_down() {
        let clock = ManualClock::new();
        let p = ObservedPressure::with_clock(
            fast_thresholds(),
            Arc::clone(&clock) as Arc<dyn InstantSource>,
        );
        record_n(&p, AttemptClass::RateLimited, Duration::from_millis(10), 5);
        assert_eq!(p.current(), ResourcePressure::Critical); // 5/5=1.0 ≥ 0.80

        // 大量成功把比例打下去，但冷卻未過，必須留在 Critical。
        record_n(&p, AttemptClass::Success, Duration::from_millis(10), 20);
        assert_eq!(
            p.current(),
            ResourcePressure::Critical,
            "冷卻未過不該降級，否則會在門檻附近抖動"
        );

        clock.advance(Duration::from_secs(10));
        assert_eq!(p.current(), ResourcePressure::High, "過冷卻後一次只降一格");
        clock.advance(Duration::from_secs(10));
        assert_eq!(p.current(), ResourcePressure::Elevated);

        // 5/25 = 0.20 剛好等於 elevated_429_ratio，raw 仍是 Elevated。
        // 遲滯合約是「raw < current 才降一格」：target == current 時不得再降，
        // 否則會把仍達門檻的狀態硬壓到 Normal。
        clock.advance(Duration::from_secs(10));
        assert_eq!(
            p.current(),
            ResourcePressure::Elevated,
            "raw 仍是 Elevated 時，過冷卻也不得再降到 Normal"
        );

        // 再一筆成功把比例壓到 5/26 ≈ 0.192 < 0.20，raw 才變 Normal。
        record_n(&p, AttemptClass::Success, Duration::from_millis(10), 1);
        clock.advance(Duration::from_secs(10));
        assert_eq!(p.current(), ResourcePressure::Normal);
    }

    #[test]
    fn upgrade_is_immediate_even_during_cooldown() {
        let clock = ManualClock::new();
        let p = ObservedPressure::with_clock(
            fast_thresholds(),
            Arc::clone(&clock) as Arc<dyn InstantSource>,
        );
        record_n(&p, AttemptClass::Success, Duration::from_millis(100), 5);
        assert_eq!(p.current(), ResourcePressure::Elevated);
        record_n(&p, AttemptClass::RateLimited, Duration::from_millis(10), 20);
        assert_eq!(
            p.current(),
            ResourcePressure::Critical,
            "升級必須立即生效，不必等冷卻"
        );
    }

    #[tokio::test]
    async fn high_rejects_p3_but_p0_still_admits() {
        let clock = ManualClock::new();
        let p = ObservedPressure::with_clock(fast_thresholds(), clock);
        record_n(&p, AttemptClass::Success, Duration::from_millis(10), 2);
        record_n(&p, AttemptClass::RateLimited, Duration::from_millis(10), 3);
        assert_eq!(p.current(), ResourcePressure::High);

        let src: Arc<dyn PressureSource> = p;
        let ctrl = AdmissionController::new(2, 4, src);
        let err = ctrl.admit(Priority::P3).await.expect_err("High 必須擋 P3");
        assert_eq!(
            err,
            AdmissionError::RejectedByPressure(ResourcePressure::High)
        );
        ctrl.try_admit_p0()
            .expect("P0 刻意不看壓力，High 時仍應放行");
    }

    #[test]
    fn speculative_defaults_are_ordered() {
        let t = PressureThresholds::speculative_defaults();
        assert!(t.elevated_p95 < t.high_p95);
        assert!(t.high_p95 < t.critical_p95);
        assert!(t.elevated_429_ratio < t.high_429_ratio);
        assert!(t.high_429_ratio < t.critical_429_ratio);
        assert!(t.min_samples >= 1);
        assert!(t.enabled);
    }
}
