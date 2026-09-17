//! Worker 的 `/health` `/ready` `/metrics` 骨架，泛型化避免逐個 worker 複製
//! 貼上（`graph-worker`／`stix-worker`／`embedding-worker` 三份 `health.rs`
//! 逐字重複達 85-95%，`discovery-worker` 是第四份、最後值得抽的時機——
//! 但既有三個 worker 這次不動，只有新的 `discovery-worker` 用這個 crate）。
//!
//! 每個 worker 固定檢查兩個 `HealthProvider`：`primary`（一律是 Postgres，
//! canonical store 沒了整個系統就沒有意義）+ `secondary`（worker 專屬的
//! 第二後端，例如 Neo4j／S3／OpenSearch）。沒有第二後端需求的 worker
//! （目前是 Phase 3 Step A 的 `discovery-worker`——還沒有 Discovery method
//! 需要用到 Neo4j）用 [`NoSecondary`] 補這個泛型位置。

use std::net::SocketAddr;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;
use axum::{Json, Router};
use core_observability::{CheckResult, HealthStatus, MetricsRegistry, ReadyStatus};
use storage_core::HealthProvider;
use tokio::net::TcpListener;

/// 給沒有第二後端的 worker 用。`health()` 永遠回健康——這個型別本身不代表
/// 任何真實依賴，只是讓 [`HealthState`] 的泛型參數有東西可以填。
#[derive(Debug, Clone, Copy)]
pub struct NoSecondary;

#[async_trait]
impl HealthProvider for NoSecondary {
    async fn health(&self) -> Result<storage_core::StorageHealth, storage_core::StorageError> {
        Ok(storage_core::StorageHealth::ok("none", "未使用第二後端"))
    }
}

#[derive(Clone)]
pub struct HealthState<P1, P2>
where
    P1: HealthProvider + Clone + Send + Sync + 'static,
    P2: HealthProvider + Clone + Send + Sync + 'static,
{
    pub metrics: MetricsRegistry,
    pub primary: P1,
    pub primary_name: &'static str,
    pub secondary: P2,
    pub secondary_name: &'static str,
}

/// 綁定 `bind`、起 `/health` `/ready` `/metrics`，SIGINT 時優雅結束。
///
/// 回 `Result<(), String>`——這個 crate 不知道呼叫端的錯誤型別長怎樣
/// （`GraphWorkerError`／`DiscoveryWorkerError`／…各自不同），呼叫端自己用
/// `.map_err(|msg| YourError::Configuration { message: msg })?` 包一層，
/// 通常只需要幾行的薄 wrapper（見 `crates/discovery-worker/src/health.rs`）。
pub async fn serve<P1, P2>(bind: &str, state: HealthState<P1, P2>) -> Result<(), String>
where
    P1: HealthProvider + Clone + Send + Sync + 'static,
    P2: HealthProvider + Clone + Send + Sync + 'static,
{
    let addr: SocketAddr = bind
        .parse()
        .map_err(|err| format!("bind 位址 `{bind}` 不合法：{err}"))?;
    let app = router(state);
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| format!("綁定 {addr} 失敗：{err}"))?;
    tracing::info!(%addr, "worker health 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .map_err(|err| format!("health 伺服器結束：{err}"))
}

fn router<P1, P2>(state: HealthState<P1, P2>) -> Router
where
    P1: HealthProvider + Clone + Send + Sync + 'static,
    P2: HealthProvider + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready::<P1, P2>))
        .route("/metrics", get(metrics::<P1, P2>))
        .with_state(state)
}

async fn health() -> Json<HealthStatus> {
    Json(HealthStatus::alive())
}

async fn ready<P1, P2>(
    State(state): State<HealthState<P1, P2>>,
) -> Result<Json<ReadyStatus>, StatusCode>
where
    P1: HealthProvider + Clone + Send + Sync + 'static,
    P2: HealthProvider + Clone + Send + Sync + 'static,
{
    let checks = vec![
        to_check(state.primary_name, state.primary.health().await),
        to_check(state.secondary_name, state.secondary.health().await),
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

async fn metrics<P1, P2>(
    State(state): State<HealthState<P1, P2>>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String)
where
    P1: HealthProvider + Clone + Send + Sync + 'static,
    P2: HealthProvider + Clone + Send + Sync + 'static,
{
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics.render_prometheus(),
    )
}
