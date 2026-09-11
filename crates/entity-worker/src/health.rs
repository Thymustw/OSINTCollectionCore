//! entity-worker 的 `/health` `/ready` `/metrics`。與 collector／normalizer／deduplicator
//! 同一套慣例。

use std::net::SocketAddr;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use core_observability::{HealthStatus, MetricsRegistry, ReadyStatus};
use storage_core::HealthProvider;
use storage_postgres::PostgresCanonicalStore;
use tokio::net::TcpListener;

use crate::error::EntityWorkerError;

#[derive(Clone)]
struct HealthState {
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
}

pub async fn serve(
    bind: &str,
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
) -> Result<(), EntityWorkerError> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|err| EntityWorkerError::Configuration {
            message: format!("entity_worker.bind `{bind}` 不是合法位址：{err}"),
        })?;
    let app = router(HealthState { metrics, store });
    let listener =
        TcpListener::bind(addr)
            .await
            .map_err(|err| EntityWorkerError::Configuration {
                message: format!(
                    "綁定 {addr} 失敗：{err}。請改 config [entity_worker].bind 或釋放該埠"
                ),
            })?;
    tracing::info!(%addr, "entity-worker health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|err| EntityWorkerError::Configuration {
            message: format!("health 伺服器結束：{err}"),
        })
}

fn router(state: HealthState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn health() -> Json<HealthStatus> {
    Json(HealthStatus::alive())
}

async fn ready(State(state): State<HealthState>) -> Result<Json<ReadyStatus>, StatusCode> {
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

async fn metrics(
    State(state): State<HealthState>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics.render_prometheus(),
    )
}
