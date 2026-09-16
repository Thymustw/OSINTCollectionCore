//! stix-worker 的 `/health` `/ready` `/metrics`。與 embedding-worker 同一套慣例。
//!
//! `/ready` 檢查 Postgres + 物件儲存：沒有 canonical store 或讀不到 bundle blob
//! 就完全沒事可做。

use std::net::SocketAddr;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use core_observability::{CheckResult, HealthStatus, MetricsRegistry, ReadyStatus};
use storage_core::HealthProvider;
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;

use crate::error::StixWorkerError;

#[derive(Clone)]
struct HealthState {
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
    objects: S3ObjectStore,
}

pub async fn serve(
    bind: &str,
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
    objects: S3ObjectStore,
) -> Result<(), StixWorkerError> {
    let addr: SocketAddr = bind.parse().map_err(|err| StixWorkerError::Configuration {
        message: format!("[stix_worker].bind `{bind}` 不是合法位址：{err}"),
    })?;
    let app = router(HealthState {
        metrics,
        store,
        objects,
    });
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| StixWorkerError::Configuration {
            message: format!("綁定 {addr} 失敗：{err}。請改 config [stix_worker].bind 或釋放該埠"),
        })?;
    tracing::info!(%addr, "stix-worker health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|err| StixWorkerError::Configuration {
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
    let checks = vec![
        to_check("postgres", state.store.health().await),
        to_check("s3", state.objects.health().await),
    ];
    let status = ReadyStatus::from_checks(checks);
    if status.ready {
        Ok(Json(status))
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

fn to_check(
    name: &'static str,
    result: Result<storage_core::StorageHealth, storage_core::StorageError>,
) -> CheckResult {
    match result {
        Ok(h) if h.healthy => CheckResult::ok(name, h.message),
        Ok(h) => CheckResult::down(name, h.message),
        Err(err) => CheckResult::down(name, err.to_string()),
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
