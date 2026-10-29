//! collector 下游 lag 背壓閘門（fail-open）。
//!
//! 查的是 **normalizer** 的 consumer group 在 `raw.collected` 上的 lag，
//! 不是 indexer、也不是 `osint_queue_depth`。group／topic 由呼叫端從設定注入，
//! 不要在這裡寫死。
//!
//! 查詢失敗或逾時 → 照常收集。狀態變化才寫 log。`enabled=false` 時完全不查。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use core_config::CollectorBackpressureConfig;
use core_events::GroupLagProbe;
use core_observability::MetricsRegistry;

/// 本輪背壓狀態。gauge `osint_collector_backpressure_state` 用 [`Self::metric_value`]。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureState {
    /// lag < throttle：照 `global_inflight` 派。
    Normal,
    /// throttle ≤ lag < pause：本輪併發降為 1。
    Throttle,
    /// lag ≥ pause：本輪不派新工作。
    Pause,
}

impl BackpressureState {
    /// 0 = normal、1 = throttle、2 = pause。
    #[must_use]
    pub fn metric_value(self) -> u64 {
        match self {
            Self::Normal => 0,
            Self::Throttle => 1,
            Self::Pause => 2,
        }
    }

    /// 本輪 JoinSet 的 spawn 上限。Pause 是 0（呼叫端應改走略過，不要進 JoinSet）。
    #[must_use]
    pub fn dispatch_cap(self, global_limit: u32) -> usize {
        match self {
            Self::Pause => 0,
            Self::Throttle => 1,
            Self::Normal => global_limit.max(1) as usize,
        }
    }

    #[must_use]
    pub fn from_lag(lag: u64, throttle_lag: u64, pause_lag: u64) -> Self {
        if lag >= pause_lag {
            Self::Pause
        } else if lag >= throttle_lag {
            Self::Throttle
        } else {
            Self::Normal
        }
    }
}

/// 一次 [`BackpressureGate::evaluate`] 的結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackpressureDecision {
    pub state: BackpressureState,
    /// 最近一次**成功**查到的 lag。fail-open 時可能是先前快取，或 `None`。
    pub lag: Option<u64>,
}

/// 下游 lag 來源。生產路徑是 [`BrokerLagSource`]；測試餵假實作。
#[async_trait]
pub trait LagSource: Send + Sync {
    /// 目前的 total lag。錯誤字串會進 fail-open log。
    async fn current_lag(&self) -> Result<u64, String>;
}

/// 可注入的時鐘，讓快取間隔測試不必真的睡 30 秒。
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// 生產用時鐘。
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// 用 [`GroupLagProbe`] 查一個 (group, topic)。同步阻塞，所以包在 `spawn_blocking`。
pub struct BrokerLagSource {
    probe: GroupLagProbe,
    group: String,
    topic: String,
}

impl BrokerLagSource {
    #[must_use]
    pub fn new(probe: GroupLagProbe, group: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            probe,
            group: group.into(),
            topic: topic.into(),
        }
    }
}

#[async_trait]
impl LagSource for BrokerLagSource {
    async fn current_lag(&self) -> Result<u64, String> {
        let probe = self.probe.clone();
        let group = self.group.clone();
        let topic = self.topic.clone();
        tokio::task::spawn_blocking(move || {
            probe
                .group_lag(&group, &topic)
                .map(|lag| lag.total_lag)
                .map_err(|err| err.to_string())
        })
        .await
        .map_err(|err| {
            format!("lag 探針 task 結束：{err}。請確認 Redpanda 在跑、[broker].brokers 正確")
        })?
    }
}

/// 每 tick 呼叫一次。間隔內回快取；失敗／逾時 fail-open。
#[derive(Clone)]
pub struct BackpressureGate {
    inner: Arc<Inner>,
}

struct Inner {
    config: CollectorBackpressureConfig,
    source: Option<Arc<dyn LagSource>>,
    clock: Arc<dyn Clock>,
    metrics: MetricsRegistry,
    cache: Mutex<ProbeCache>,
}

#[derive(Debug, Clone, Copy)]
struct ProbeCache {
    checked_at: Option<Instant>,
    lag: Option<u64>,
    state: BackpressureState,
    probe_ok: bool,
    logged: Option<(BackpressureState, bool)>,
}

impl Default for ProbeCache {
    fn default() -> Self {
        Self {
            checked_at: None,
            lag: None,
            state: BackpressureState::Normal,
            probe_ok: true,
            logged: None,
        }
    }
}

impl BackpressureGate {
    /// `enabled=false`：完全不查。測試與尚未接線的 runner 用這個。
    #[must_use]
    pub fn disabled(metrics: MetricsRegistry) -> Self {
        Self::new(
            CollectorBackpressureConfig {
                enabled: false,
                ..CollectorBackpressureConfig::default()
            },
            None,
            metrics,
        )
    }

    #[must_use]
    pub fn new(
        config: CollectorBackpressureConfig,
        source: Option<Arc<dyn LagSource>>,
        metrics: MetricsRegistry,
    ) -> Self {
        Self::new_with_clock(config, source, Arc::new(SystemClock), metrics)
    }

    #[must_use]
    pub fn new_with_clock(
        config: CollectorBackpressureConfig,
        source: Option<Arc<dyn LagSource>>,
        clock: Arc<dyn Clock>,
        metrics: MetricsRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                source,
                clock,
                metrics,
                cache: Mutex::new(ProbeCache::default()),
            }),
        }
    }

    /// 本輪該用哪個狀態。間隔內不重查。
    pub async fn evaluate(&self) -> BackpressureDecision {
        if !self.inner.config.enabled {
            self.inner
                .metrics
                .set_gauge("osint_collector_backpressure_state", 0);
            return BackpressureDecision {
                state: BackpressureState::Normal,
                lag: None,
            };
        }

        let now = self.inner.clock.now();
        let interval = Duration::from_secs(self.inner.config.lag_check_interval_secs.max(1));
        {
            let cache = self.lock_cache();
            if let Some(at) = cache.checked_at {
                if now.saturating_duration_since(at) < interval {
                    self.publish_gauges(cache.state, cache.lag);
                    return BackpressureDecision {
                        state: cache.state,
                        lag: cache.lag,
                    };
                }
            }
        }

        let timeout = Duration::from_millis(self.inner.config.probe_timeout_ms.max(1));
        let probed = match &self.inner.source {
            None => Err(
                "下游 lag 探針未建立。請確認 [broker].brokers 有值，或把 [collector.backpressure].enabled 設 false"
                    .into(),
            ),
            Some(source) => match tokio::time::timeout(timeout, source.current_lag()).await {
                Ok(Ok(lag)) => Ok(lag),
                Ok(Err(err)) => Err(err),
                Err(_) => Err(format!(
                    "查詢 downstream lag 逾時（{}ms）。請確認 Redpanda 在跑、[broker].brokers 與 [normalizer].consumer_group 正確",
                    self.inner.config.probe_timeout_ms
                )),
            },
        };

        match probed {
            Ok(lag) => self.finish_ok(now, lag),
            Err(err) => self.finish_fail_open(now, err),
        }
    }

    fn finish_ok(&self, now: Instant, lag: u64) -> BackpressureDecision {
        let state = BackpressureState::from_lag(
            lag,
            self.inner.config.throttle_lag,
            self.inner.config.pause_lag,
        );
        self.inner
            .metrics
            .set_gauge("osint_collector_downstream_lag", lag);
        self.publish_gauges(state, Some(lag));
        self.store_and_log(now, Some(lag), state, true, None);
        BackpressureDecision {
            state,
            lag: Some(lag),
        }
    }

    fn finish_fail_open(&self, now: Instant, err: String) -> BackpressureDecision {
        self.inner
            .metrics
            .inc("osint_collector_lag_probe_failures_total", 1);
        // fail-open：照常收集。lag gauge 保留上次成功值，不要寫成 0 假裝追上了。
        let cached_lag = self.lock_cache().lag;
        self.publish_gauges(BackpressureState::Normal, cached_lag);
        self.store_and_log(now, cached_lag, BackpressureState::Normal, false, Some(err));
        BackpressureDecision {
            state: BackpressureState::Normal,
            lag: cached_lag,
        }
    }

    fn publish_gauges(&self, state: BackpressureState, lag: Option<u64>) {
        self.inner
            .metrics
            .set_gauge("osint_collector_backpressure_state", state.metric_value());
        if let Some(lag) = lag {
            self.inner
                .metrics
                .set_gauge("osint_collector_downstream_lag", lag);
        }
    }

    fn store_and_log(
        &self,
        now: Instant,
        lag: Option<u64>,
        state: BackpressureState,
        probe_ok: bool,
        fail_reason: Option<String>,
    ) {
        let mut cache = self.lock_cache();
        let prev = cache.logged;
        cache.checked_at = Some(now);
        cache.lag = lag;
        cache.state = state;
        cache.probe_ok = probe_ok;
        if should_log_change(prev, state, probe_ok) {
            log_state_change(
                state,
                probe_ok,
                lag,
                self.inner.config.throttle_lag,
                self.inner.config.pause_lag,
                fail_reason.as_deref(),
            );
            cache.logged = Some((state, probe_ok));
        }
    }

    fn lock_cache(&self) -> std::sync::MutexGuard<'_, ProbeCache> {
        self.inner
            .cache
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }
}

fn should_log_change(
    prev: Option<(BackpressureState, bool)>,
    state: BackpressureState,
    probe_ok: bool,
) -> bool {
    match prev {
        None => !probe_ok || state != BackpressureState::Normal,
        Some((old_state, old_ok)) => old_state != state || old_ok != probe_ok,
    }
}

fn log_state_change(
    state: BackpressureState,
    probe_ok: bool,
    lag: Option<u64>,
    throttle_lag: u64,
    pause_lag: u64,
    fail_reason: Option<&str>,
) {
    if !probe_ok {
        tracing::warn!(
            lag,
            error = fail_reason.unwrap_or("未知"),
            "查詢 downstream lag 失敗，照常收集（fail-open）。\
             請確認 Redpanda 在跑、[broker].brokers 與 [normalizer].consumer_group 正確"
        );
        return;
    }
    match state {
        BackpressureState::Pause => tracing::warn!(
            lag,
            pause_lag,
            "下游 lag 背壓：normalizer 在 raw.collected 上的 lag 達到 pause 門檻，本輪不派新工作。\
             門檻是推測值，未做容量測試；請看 osint_collector_downstream_lag 再調 [collector.backpressure]"
        ),
        BackpressureState::Throttle => tracing::warn!(
            lag,
            throttle_lag,
            "下游 lag 背壓：本輪併發降為 1。門檻是推測值，未做容量測試；\
             請看 osint_collector_downstream_lag 再調 [collector.backpressure]"
        ),
        BackpressureState::Normal => tracing::info!(lag, "下游 lag 背壓解除，恢復正常派工"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct FixedLag(u64);

    #[async_trait]
    impl LagSource for FixedLag {
        async fn current_lag(&self) -> Result<u64, String> {
            Ok(self.0)
        }
    }

    struct CountingLag {
        lag: Mutex<Result<u64, String>>,
        calls: AtomicU64,
    }

    impl CountingLag {
        fn ok(lag: u64) -> Arc<Self> {
            Arc::new(Self {
                lag: Mutex::new(Ok(lag)),
                calls: AtomicU64::new(0),
            })
        }

        fn err(message: &str) -> Arc<Self> {
            Arc::new(Self {
                lag: Mutex::new(Err(message.into())),
                calls: AtomicU64::new(0),
            })
        }

        fn calls(&self) -> u64 {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl LagSource for CountingLag {
        async fn current_lag(&self) -> Result<u64, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match &*self.lag.lock().unwrap_or_else(|err| err.into_inner()) {
                Ok(lag) => Ok(*lag),
                Err(err) => Err(err.clone()),
            }
        }
    }

    struct SlowLag;

    #[async_trait]
    impl LagSource for SlowLag {
        async fn current_lag(&self) -> Result<u64, String> {
            tokio::time::sleep(Duration::from_secs(5)).await;
            Ok(0)
        }
    }

    struct FakeClock {
        origin: Instant,
        offset: Mutex<Duration>,
    }

    impl FakeClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                origin: Instant::now(),
                offset: Mutex::new(Duration::ZERO),
            })
        }

        fn advance(&self, d: Duration) {
            *self.offset.lock().unwrap_or_else(|err| err.into_inner()) += d;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            self.origin + *self.offset.lock().unwrap_or_else(|err| err.into_inner())
        }
    }

    fn cfg() -> CollectorBackpressureConfig {
        CollectorBackpressureConfig::default()
    }

    fn gate(
        source: Arc<dyn LagSource>,
        clock: Arc<dyn Clock>,
    ) -> (BackpressureGate, MetricsRegistry) {
        let metrics = MetricsRegistry::new();
        let gate = BackpressureGate::new_with_clock(cfg(), Some(source), clock, metrics.clone());
        (gate, metrics)
    }

    fn prometheus_has(metrics: &MetricsRegistry, needle: &str) -> bool {
        metrics.render_prometheus().contains(needle)
    }

    #[test]
    fn from_lag_three_states() {
        assert_eq!(
            BackpressureState::from_lag(0, 5_000, 50_000),
            BackpressureState::Normal
        );
        assert_eq!(
            BackpressureState::from_lag(4_999, 5_000, 50_000),
            BackpressureState::Normal
        );
        assert_eq!(
            BackpressureState::from_lag(5_000, 5_000, 50_000),
            BackpressureState::Throttle
        );
        assert_eq!(
            BackpressureState::from_lag(49_999, 5_000, 50_000),
            BackpressureState::Throttle
        );
        assert_eq!(
            BackpressureState::from_lag(50_000, 5_000, 50_000),
            BackpressureState::Pause
        );
    }

    #[test]
    fn dispatch_cap_matches_state() {
        assert_eq!(BackpressureState::Normal.dispatch_cap(4), 4);
        assert_eq!(BackpressureState::Throttle.dispatch_cap(4), 1);
        assert_eq!(BackpressureState::Pause.dispatch_cap(4), 0);
        assert_eq!(BackpressureState::Normal.dispatch_cap(0), 1);
    }

    #[tokio::test]
    async fn evaluate_three_states_and_metrics() {
        let clock = FakeClock::new();
        for (lag, want, state_gauge) in [
            (
                0_u64,
                BackpressureState::Normal,
                "osint_collector_backpressure_state 0",
            ),
            (
                5_000,
                BackpressureState::Throttle,
                "osint_collector_backpressure_state 1",
            ),
            (
                50_000,
                BackpressureState::Pause,
                "osint_collector_backpressure_state 2",
            ),
        ] {
            let (g, metrics) = gate(Arc::new(FixedLag(lag)), clock.clone());
            let decision = g.evaluate().await;
            assert_eq!(decision.state, want, "lag={lag}");
            assert_eq!(decision.lag, Some(lag));
            let text = metrics.render_prometheus();
            assert!(text.contains(state_gauge), "{text}");
            assert!(
                text.contains(&format!("osint_collector_downstream_lag {lag}")),
                "{text}"
            );
        }
    }

    #[tokio::test]
    async fn probe_error_is_fail_open() {
        let clock = FakeClock::new();
        let source = CountingLag::err("broker 連不上");
        let (g, metrics) = gate(source.clone(), clock);
        let decision = g.evaluate().await;
        assert_eq!(decision.state, BackpressureState::Normal);
        assert_eq!(decision.lag, None);
        assert_eq!(source.calls(), 1);
        assert!(prometheus_has(
            &metrics,
            "osint_collector_backpressure_state 0"
        ));
        assert!(prometheus_has(
            &metrics,
            "osint_collector_lag_probe_failures_total 1"
        ));
        assert!(
            !metrics
                .render_prometheus()
                .contains("osint_collector_downstream_lag "),
            "失敗時不該把 lag 寫成 0"
        );
    }

    #[tokio::test]
    async fn probe_timeout_is_fail_open() {
        let clock = FakeClock::new();
        let metrics = MetricsRegistry::new();
        let mut config = cfg();
        config.probe_timeout_ms = 50;
        let g = BackpressureGate::new_with_clock(
            config,
            Some(Arc::new(SlowLag)),
            clock,
            metrics.clone(),
        );
        let started = Instant::now();
        let decision = g.evaluate().await;
        let elapsed = started.elapsed();
        assert_eq!(decision.state, BackpressureState::Normal);
        assert!(
            elapsed < Duration::from_secs(2),
            "逾時應在 probe_timeout 附近返回，實際 {elapsed:?}"
        );
        assert!(prometheus_has(
            &metrics,
            "osint_collector_lag_probe_failures_total 1"
        ));
    }

    #[tokio::test]
    async fn cache_interval_skips_reprobe() {
        let clock = FakeClock::new();
        let source = CountingLag::ok(100);
        let (g, _) = gate(source.clone(), clock.clone());
        let first = g.evaluate().await;
        let second = g.evaluate().await;
        assert_eq!(first, second);
        assert_eq!(source.calls(), 1, "間隔內不該重查");
        clock.advance(Duration::from_secs(30));
        let third = g.evaluate().await;
        assert_eq!(third.lag, Some(100));
        assert_eq!(source.calls(), 2, "間隔過了要重查");
    }

    #[tokio::test]
    async fn enabled_false_does_not_probe() {
        let source = CountingLag::ok(50_000);
        let metrics = MetricsRegistry::new();
        let g = BackpressureGate::new(
            CollectorBackpressureConfig {
                enabled: false,
                ..CollectorBackpressureConfig::default()
            },
            Some(source.clone()),
            metrics.clone(),
        );
        let decision = g.evaluate().await;
        assert_eq!(decision.state, BackpressureState::Normal);
        assert_eq!(decision.lag, None);
        assert_eq!(source.calls(), 0);
        assert!(prometheus_has(
            &metrics,
            "osint_collector_backpressure_state 0"
        ));
    }

    #[tokio::test]
    async fn missing_source_is_fail_open() {
        let metrics = MetricsRegistry::new();
        let g = BackpressureGate::new(cfg(), None, metrics.clone());
        let decision = g.evaluate().await;
        assert_eq!(decision.state, BackpressureState::Normal);
        assert!(prometheus_has(
            &metrics,
            "osint_collector_lag_probe_failures_total 1"
        ));
    }

    #[tokio::test]
    async fn fail_open_then_recovery_keeps_last_lag_until_success() {
        let clock = FakeClock::new();
        let source = CountingLag::ok(50_000);
        let (g, metrics) = gate(source.clone(), clock.clone());
        let paused = g.evaluate().await;
        assert_eq!(paused.state, BackpressureState::Pause);

        *source.lag.lock().unwrap() = Err("暫時斷線".into());
        clock.advance(Duration::from_secs(30));
        let open = g.evaluate().await;
        assert_eq!(open.state, BackpressureState::Normal);
        assert_eq!(open.lag, Some(50_000), "fail-open 應保留上次成功的 lag");
        assert!(prometheus_has(
            &metrics,
            "osint_collector_downstream_lag 50000"
        ));
        assert!(prometheus_has(
            &metrics,
            "osint_collector_backpressure_state 0"
        ));

        *source.lag.lock().unwrap() = Ok(10);
        clock.advance(Duration::from_secs(30));
        let recovered = g.evaluate().await;
        assert_eq!(recovered.state, BackpressureState::Normal);
        assert_eq!(recovered.lag, Some(10));
    }

    #[test]
    fn should_log_only_on_change() {
        assert!(
            !should_log_change(None, BackpressureState::Normal, true),
            "啟動時正常不該打 log"
        );
        assert!(should_log_change(None, BackpressureState::Pause, true));
        assert!(should_log_change(None, BackpressureState::Normal, false));
        assert!(!should_log_change(
            Some((BackpressureState::Pause, true)),
            BackpressureState::Pause,
            true
        ));
        assert!(should_log_change(
            Some((BackpressureState::Pause, true)),
            BackpressureState::Normal,
            true
        ));
        assert!(should_log_change(
            Some((BackpressureState::Normal, true)),
            BackpressureState::Normal,
            false
        ));
    }
}
