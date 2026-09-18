//! discovery-worker 的 `/health` `/ready` `/metrics`。轉呼叫
//! `worker_skeleton::serve`——見那個 crate 的說明。
//!
//! Step B 接上 Graph Expansion 後 `secondary` 換成真的 `Neo4jStore`
//! （Step A 用 `NoSecondary` 佔位），`ready()` 會多一項 Neo4j 健康檢查。

use storage_neo4j::Neo4jStore;
use storage_postgres::PostgresCanonicalStore;
use worker_skeleton::HealthState;

use crate::error::DiscoveryWorkerError;

pub async fn serve(
    bind: &str,
    metrics: core_observability::MetricsRegistry,
    store: PostgresCanonicalStore,
    graph: Neo4jStore,
) -> Result<(), DiscoveryWorkerError> {
    let state = HealthState {
        metrics,
        primary: store,
        primary_name: "postgres",
        secondary: graph,
        secondary_name: "neo4j",
    };
    worker_skeleton::serve(bind, state)
        .await
        .map_err(|message| DiscoveryWorkerError::Configuration { message })
}
