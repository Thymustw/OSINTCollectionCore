//! discovery-worker 的 `/health` `/ready` `/metrics`。轉呼叫
//! `worker_skeleton::serve`——見那個 crate 的說明。
//!
//! Step A 還沒有任何 Discovery method 需要 Neo4j，`secondary` 用
//! [`worker_skeleton::NoSecondary`] 佔位；Step B 接上 Graph Expansion 時
//! 這裡要換成真的 `Neo4jStore` 並更新 `ready()` 的檢查項。

use storage_postgres::PostgresCanonicalStore;
use worker_skeleton::{HealthState, NoSecondary};

use crate::error::DiscoveryWorkerError;

pub async fn serve(
    bind: &str,
    metrics: core_observability::MetricsRegistry,
    store: PostgresCanonicalStore,
) -> Result<(), DiscoveryWorkerError> {
    let state = HealthState {
        metrics,
        primary: store,
        primary_name: "postgres",
        secondary: NoSecondary,
        secondary_name: "none",
    };
    worker_skeleton::serve(bind, state)
        .await
        .map_err(|message| DiscoveryWorkerError::Configuration { message })
}
