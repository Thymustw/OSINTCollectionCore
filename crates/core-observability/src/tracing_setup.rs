//! JSON tracing subscriber。適合之後接到 log 收集。

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::prelude::*;

/// tracing 初始化失敗。
#[derive(Debug, thiserror::Error)]
pub enum TracingInitError {
    #[error("tracing subscriber 已設定過。每個行程只能初始化一次")]
    AlreadySet,
}

/// 初始化全域 tracing。`RUST_LOG` 可覆寫 filter；預設 `info`。
pub fn init_tracing(default_filter: &str) -> Result<(), TracingInitError> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(true)
        .with_span_list(true)
        .with_target(true)
        .with_span_events(FmtSpan::NONE);
    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init()
        .map_err(|_| TracingInitError::AlreadySet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_twice_is_already_set() {
        // 測試行程可能已有其他 subscriber；兩種結果都可接受。
        match init_tracing("error") {
            Ok(()) => {
                let err = init_tracing("error").unwrap_err();
                assert!(matches!(err, TracingInitError::AlreadySet));
            }
            Err(TracingInitError::AlreadySet) => {}
        }
    }
}
