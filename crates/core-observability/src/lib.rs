//! JSON tracing、簡易 metrics、health/ready 型別。
//!
//! 不在這裡接 OpenTelemetry exporter；V0.1 skeleton 先把結構化 log 與記憶體計數打通。

mod health;
mod metrics;
mod tracing_setup;

pub use health::{CheckResult, HealthStatus, ReadyStatus};
pub use metrics::MetricsRegistry;
pub use tracing_setup::{TracingInitError, init_tracing};
