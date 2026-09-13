//! 獨立的 graph-context resolution。
//!
//! 跟 [`crate::ResolverService::resolve_entity`] **刻意分開**：Neo4j 連不上
//! 只讓這條路徑失敗，不拖累另外四個只需要 Postgres 的方法。HTTP 入口是
//! `POST /api/v1/entities/{id}/resolve/graph-context`。
//!
//! 寫入走 [`crate::persist::persist_candidate`]，與 `ResolverService` 同一套
//! Conflict 語意。

use core_model::{EntityId, ResolutionCandidate};
use storage_core::{GraphStore, RelationalStore};

use crate::GRAPH_CONTEXT_THRESHOLD;
use crate::error::ResolverError;
use crate::graph_context::check_graph_context;
use crate::persist::persist_candidate;

/// 對一個 Entity 跑 graph-context 比對，把新候選寫進 canonical store。
pub struct GraphContextResolver<S: RelationalStore, G: GraphStore> {
    store: S,
    graph: G,
}

impl<S: RelationalStore, G: GraphStore> GraphContextResolver<S, G> {
    #[must_use]
    pub fn new(store: S, graph: G) -> Self {
        Self { store, graph }
    }

    /// 對 `entity_id` 跑 graph-context，回傳**這次新寫入**的 candidate。
    ///
    /// 找不到 Entity 回 [`ResolverError::EntityNotFound`]——呼叫端不該因為
    /// 抽成獨立 endpoint 就少了「entity 不存在」的檢查。
    pub async fn resolve(
        &self,
        entity_id: EntityId,
    ) -> Result<Vec<ResolutionCandidate>, ResolverError> {
        self.store
            .get_entity(entity_id)
            .await?
            .ok_or(ResolverError::EntityNotFound { entity_id })?;

        let candidates =
            check_graph_context(&self.graph, entity_id, GRAPH_CONTEXT_THRESHOLD).await?;

        let mut written = Vec::new();
        for candidate in candidates {
            if let Some(kept) = persist_candidate(&self.store, candidate).await? {
                written.push(kept);
            }
        }
        Ok(written)
    }
}
