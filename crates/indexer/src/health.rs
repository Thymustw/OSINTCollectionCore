//! indexer 的 `/health` `/ready` `/metrics`。與 collector／normalizer／deduplicator／
//! entity-worker 同一套慣例。
//!
//! `/ready` 比其他服務多檢查一項 OpenSearch：indexer 沒有 OpenSearch 就完全沒事可做，
//! 只檢查 Postgres 會讓它在「搜尋投影整個壞掉」的情況下回報 ready。

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

use crate::error::IndexerError;

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
) -> Result<(), IndexerError> {
    let addr: SocketAddr = bind.parse().map_err(|err| IndexerError::Configuration {
        message: format!("indexer.bind `{bind}` 不是合法位址：{err}"),
    })?;
    let app = router(HealthState {
        metrics,
        store,
        search,
    });
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| IndexerError::Configuration {
            message: format!("綁定 {addr} 失敗：{err}。請改 config [indexer].bind 或釋放該埠"),
        })?;
    tracing::info!(%addr, "indexer health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|err| IndexerError::Configuration {
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
