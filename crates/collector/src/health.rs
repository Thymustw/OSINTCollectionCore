//! collector 的 `/health` `/ready` `/metrics`。
//!
//! `/health` 是 liveness：行程活著即可，drain 期間維持 200。若 liveness 也 503，
//! 編排器會 SIGKILL，跟要修的問題同類。
//! `/ready` 是 readiness：drain 開始後回 503，停止接新流量。

use std::net::SocketAddr;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use core_observability::{HealthStatus, MetricsRegistry, ReadyStatus};
use storage_core::HealthProvider;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::error::CollectorError;

#[derive(Clone)]
struct HealthState<S> {
    metrics: MetricsRegistry,
    store: S,
    shutdown: CancellationToken,
}

/// 在背景聽 health 埠。`shutdown` 取消後 `/ready` 回 503，伺服器 graceful shutdown。
pub async fn serve<S>(
    bind: &str,
    metrics: MetricsRegistry,
    store: S,
    shutdown: CancellationToken,
) -> Result<(), CollectorError>
where
    S: HealthProvider + Clone + Send + Sync + 'static,
{
    let addr: SocketAddr = bind.parse().map_err(|err| CollectorError::Configuration {
        message: format!("collector.bind `{bind}` 不是合法位址：{err}"),
    })?;
    let app = health_router(metrics, store, shutdown.clone());
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| CollectorError::Configuration {
            message: format!("綁定 {addr} 失敗：{err}。請改 config [collector].bind 或釋放該埠"),
        })?;
    tracing::info!(%addr, "collector health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
        .map_err(|err| CollectorError::Configuration {
            message: format!("health 伺服器結束：{err}"),
        })
}

/// 組 `/health` `/ready` `/metrics`。單測用 oneshot，不必綁埠。
pub fn health_router<S>(metrics: MetricsRegistry, store: S, shutdown: CancellationToken) -> Router
where
    S: HealthProvider + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready::<S>))
        .route("/metrics", get(render_metrics::<S>))
        .with_state(HealthState {
            metrics,
            store,
            shutdown,
        })
}

async fn health() -> Json<HealthStatus> {
    Json(HealthStatus::alive())
}

async fn ready<S: HealthProvider>(
    State(state): State<HealthState<S>>,
) -> Result<Json<ReadyStatus>, StatusCode> {
    if state.shutdown.is_cancelled() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    let check = match state.store.health().await {
        Ok(h) if h.healthy => core_observability::CheckResult::ok("postgres", h.message),
        Ok(h) => core_observability::CheckResult::down("postgres", h.message),
        Err(err) => core_observability::CheckResult::down("postgres", err.to_string()),
    };
    let status = ReadyStatus::from_checks(vec![check]);
    if status.ready {
        Ok(Json(status))
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

async fn render_metrics<S: HealthProvider>(
    State(state): State<HealthState<S>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics.render_prometheus(),
    )
}
