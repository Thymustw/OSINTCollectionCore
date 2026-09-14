//! embedding-worker 的 `/health` `/ready` `/metrics`。與 indexer 同一套慣例。
//!
//! `/ready` 檢查 Postgres + OpenSearch：沒有搜尋後端就完全沒事可做。
//! 推論在 OpenSearch JVM（ml-commons）裡跑，本行程不把模型部署當獨立
//! ready check——模型沒部署會在第一次 `embed` 失敗，那是設定問題，
//! 不是「這個行程還沒準備好收流量」。

use std::net::SocketAddr;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use core_observability::{CheckResult, HealthStatus, MetricsRegistry, ReadyStatus};
use storage_core::HealthProvider;
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use tokio::net::TcpListener;

use crate::error::EmbeddingWorkerError;

#[derive(Clone)]
struct HealthState {
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
    search: OpenSearchStore,
}

pub async fn serve(
    bind: &str,
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
    search: OpenSearchStore,
) -> Result<(), EmbeddingWorkerError> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|err| EmbeddingWorkerError::Configuration {
            message: format!("embedding_worker.bind `{bind}` 不是合法位址：{err}"),
        })?;
    let app = router(HealthState {
        metrics,
        store,
        search,
    });
    let listener =
        TcpListener::bind(addr)
            .await
            .map_err(|err| EmbeddingWorkerError::Configuration {
                message: format!(
                    "綁定 {addr} 失敗：{err}。請改 config [embedding_worker].bind 或釋放該埠"
                ),
            })?;
    tracing::info!(%addr, "embedding-worker health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|err| EmbeddingWorkerError::Configuration {
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
        to_check("opensearch", state.search.health().await),
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
