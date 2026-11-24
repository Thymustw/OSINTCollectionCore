//! chunk-embedder 的 `/health` `/ready` `/metrics`。`/ready` 檢查 Postgres、OpenSearch、推論服務三者。

use std::net::SocketAddr;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use core_observability::{CheckResult, HealthStatus, MetricsRegistry, ReadyStatus};
use storage_core::HealthProvider;
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use tokio::net::TcpListener;

use crate::error::ChunkEmbedderError;

#[derive(Clone)]
struct HealthState {
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
    search: OpenSearchStore,
    inference_ready_url: String,
    http: reqwest::Client,
}

pub async fn serve(
    bind: &str,
    metrics: MetricsRegistry,
    store: PostgresCanonicalStore,
    search: OpenSearchStore,
    inference_url: &str,
) -> Result<(), ChunkEmbedderError> {
    let addr: SocketAddr = bind
        .parse()
        .map_err(|err| ChunkEmbedderError::Configuration {
            message: format!("chunk_embedder.bind `{bind}` 不是合法位址：{err}"),
        })?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(|err| ChunkEmbedderError::Configuration {
            message: format!("建立 health HTTP client 失敗：{err}"),
        })?;
    let app = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(render_metrics))
        .with_state(HealthState {
            metrics,
            store,
            search,
            inference_ready_url: format!("{}/ready", inference_url.trim_end_matches('/')),
            http,
        });
    let listener =
        TcpListener::bind(addr)
            .await
            .map_err(|err| ChunkEmbedderError::Configuration {
                message: format!(
                    "綁定 {addr} 失敗：{err}。請改 config [chunk_embedder].bind 或釋放該埠"
                ),
            })?;
    tracing::info!(%addr, "chunk-embedder health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|err| ChunkEmbedderError::Configuration {
            message: format!("health 伺服器結束：{err}"),
        })
}

async fn health() -> Json<HealthStatus> {
    Json(HealthStatus::alive())
}

async fn ready(State(state): State<HealthState>) -> Result<Json<ReadyStatus>, StatusCode> {
    let inference = match state.http.get(&state.inference_ready_url).send().await {
        Ok(r) if r.status().is_success() => CheckResult::ok("inference", "ready"),
        Ok(r) => CheckResult::down("inference", format!("HTTP {}", r.status())),
        Err(err) => CheckResult::down("inference", err.to_string()),
    };
    let checks = vec![
        to_check("postgres", state.store.health().await),
        to_check("opensearch", state.search.health().await),
        inference,
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

async fn render_metrics(
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
