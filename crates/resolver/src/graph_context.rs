//! SPEC §6「graph context」：用一跳鄰居集合的 Jaccard 相似度找合併候選。
//!
//! **純圖結構、不查 Entity 本體。** 不依賴 [`storage_core::RelationalStore`]，
//! 也不寫入 candidate 表——寫入由 [`crate::ResolverService::resolve_entity`]
//! 的 persist 負責。聚合門檻是 [`crate::GRAPH_CONTEXT_THRESHOLD`]。
//!
//! # 候選集合（2-hop）與效能上限
//!
//! 1. 取 A 的一跳鄰居 `N(A)`。空集合直接回空——沒鄰居就沒有 graph context 可比。
//! 2. 對每個 B ∈ `N(A)` 再取 `N(B)`，把「不是 A、也不是 A 的鄰居」的節點收成候選 C
//!    （A 的鄰居的鄰居）。
//! 3. **候選數量上限 [`GRAPH_CONTEXT_CANDIDATE_CAP`]（50）。**
//!    超過就截斷並 `warn`。這是**效能上限、不是精確窮舉**：hub 節點的 2-hop
//!    會爆炸，漏比是刻意的，不要把「沒產出候選」讀成「圖上沒有相似節點」。
//! 4. 對每個 C 算 Jaccard `|N(A) ∩ N(C)| / |N(A) ∪ N(C)|`，`>= threshold` 才組候選。
//!    `score = jaccard * 0.7`（圖結構是弱訊號，遠不到自動合併）。
//!
//! 兩邊鄰居集合都空時 Jaccard 定義為 `0.0`，避免除以零。

use std::collections::HashSet;

use chrono::Utc;
use core_model::{EntityId, RESOLUTION_METHODS, ResolutionCandidate, ResolutionStatus};
use serde_json::json;
use storage_core::{GraphStore, GraphTraversalOptions};
use tracing::warn;
use uuid::Uuid;

use crate::error::ResolverError;

/// 2-hop 候選的效能上限。超過就截斷，不是精確窮舉。
pub const GRAPH_CONTEXT_CANDIDATE_CAP: usize = 50;

/// Jaccard 轉 score 的權重。圖結構是弱訊號。
pub const GRAPH_CONTEXT_SCORE_WEIGHT: f64 = 0.7;

const METHOD_GRAPH_CONTEXT: &str = RESOLUTION_METHODS[9];

/// 對 `entity_id` 跑 graph-context 比對，回傳**尚未寫入 store** 的候選。
///
/// `threshold` 作用在 Jaccard 本身（0..=1），不是 score。`jaccard >= threshold`
/// 才會產出候選；score 再乘 [`GRAPH_CONTEXT_SCORE_WEIGHT`]。
pub async fn check_graph_context<G: GraphStore>(
    graph: &G,
    entity_id: EntityId,
    threshold: f64,
) -> Result<Vec<ResolutionCandidate>, ResolverError> {
    let options = GraphTraversalOptions {
        max_hops: 1,
        relationship_types: None,
        entity_types: None,
        min_confidence: None,
        time_range: None,
    };

    let n_a = neighbor_ids(graph, &entity_id, &options).await?;
    if n_a.is_empty() {
        return Ok(Vec::new());
    }

    let candidates = collect_two_hop_candidates(graph, entity_id, &n_a, &options).await?;

    let mut out = Vec::new();
    for c_id in candidates {
        if c_id == entity_id {
            continue;
        }
        let n_c = neighbor_ids(graph, &c_id, &options).await?;
        let similarity = jaccard(&n_a, &n_c);
        if similarity < threshold {
            continue;
        }
        out.push(graph_context_candidate(
            entity_id, c_id, &n_a, &n_c, similarity,
        ));
    }
    Ok(out)
}

/// `|a ∩ c| / |a ∪ c|`。兩個都空時回 `0.0`，不除以零。
fn jaccard(a: &HashSet<EntityId>, c: &HashSet<EntityId>) -> f64 {
    let union = a.union(c).count();
    if union == 0 {
        0.0
    } else {
        a.intersection(c).count() as f64 / union as f64
    }
}

async fn neighbor_ids<G: GraphStore>(
    graph: &G,
    entity_id: &EntityId,
    options: &GraphTraversalOptions,
) -> Result<HashSet<EntityId>, ResolverError> {
    let nodes = graph.neighbors(entity_id, options).await?;
    Ok(nodes.into_iter().map(|n| n.entity_id).collect())
}

/// 從 `N(A)` 長出 2-hop 候選。超過 [`GRAPH_CONTEXT_CANDIDATE_CAP`] 截斷並 warn。
async fn collect_two_hop_candidates<G: GraphStore>(
    graph: &G,
    entity_id: EntityId,
    n_a: &HashSet<EntityId>,
    options: &GraphTraversalOptions,
) -> Result<Vec<EntityId>, ResolverError> {
    let mut neighbors: Vec<EntityId> = n_a.iter().copied().collect();
    neighbors.sort();

    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    let mut truncated = false;

    for b_id in neighbors {
        let n_b = neighbor_ids(graph, &b_id, options).await?;
        let mut extras: Vec<EntityId> = n_b
            .into_iter()
            .filter(|id| *id != entity_id && !n_a.contains(id) && !seen.contains(id))
            .collect();
        extras.sort();
        for c_id in extras {
            if candidates.len() >= GRAPH_CONTEXT_CANDIDATE_CAP {
                truncated = true;
                break;
            }
            seen.insert(c_id);
            candidates.push(c_id);
        }
        if truncated {
            break;
        }
    }

    if truncated {
        warn!(
            entity_id = %entity_id,
            cap = GRAPH_CONTEXT_CANDIDATE_CAP,
            kept = candidates.len(),
            "graph_context 2-hop 候選超過效能上限，已截斷；\
             這不是精確窮舉，hub 節點會漏比"
        );
    }

    Ok(candidates)
}

fn graph_context_candidate(
    entity_id: EntityId,
    other_id: EntityId,
    n_a: &HashSet<EntityId>,
    n_c: &HashSet<EntityId>,
    similarity: f64,
) -> ResolutionCandidate {
    let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(entity_id, other_id);
    let mut shared_neighbors: Vec<String> =
        n_a.intersection(n_c).map(ToString::to_string).collect();
    shared_neighbors.sort();
    let shared_count = shared_neighbors.len();
    let union_count = n_a.union(n_c).count();

    ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id,
        entity_b_id,
        score: similarity * GRAPH_CONTEXT_SCORE_WEIGHT,
        method: METHOD_GRAPH_CONTEXT.to_string(),
        evidence: json!({
            "method": METHOD_GRAPH_CONTEXT,
            "jaccard_similarity": similarity,
            "shared_neighbors": shared_neighbors,
            "entity_a_neighbor_count": n_a.len(),
            "entity_b_neighbor_count": n_c.len(),
            "shared_count": shared_count,
            "union_count": union_count,
        }),
        status: ResolutionStatus::Pending,
        created_at: Utc::now(),
        reviewed_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use storage_core::mock::MockGraphStore;
    use storage_core::{GraphEdge, GraphNode};

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, 8, 0, 0).unwrap()
    }

    fn nid(n: u128) -> EntityId {
        Uuid::from_u128(n)
    }

    fn node(id: u128, name: &str) -> GraphNode {
        GraphNode {
            entity_id: nid(id),
            entity_type: "person".into(),
            display_name: name.into(),
            attributes: json!({}),
        }
    }

    fn edge(id: u128, src: u128, dst: u128) -> GraphEdge {
        GraphEdge {
            relationship_id: Uuid::from_u128(id + 1000),
            source: nid(src),
            target: nid(dst),
            relationship_type: "associated_with".into(),
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
        }
    }

    async fn seed(graph: &MockGraphStore, nodes: &[u128], edges: &[(u128, u128, u128)]) {
        for id in nodes {
            graph
                .upsert_node(&node(*id, &format!("n{id}")))
                .await
                .expect("upsert_node");
        }
        for (id, src, dst) in edges {
            graph
                .upsert_edge(&edge(*id, *src, *dst))
                .await
                .expect("upsert_edge");
        }
    }

    /// 菱形：A-B、A-C、D-B、D-C。A 與 D 沒有直接邊，但 N(A)=N(D)={B,C}。
    #[tokio::test]
    async fn diamond_finds_d_with_jaccard_one() {
        let graph = MockGraphStore::new();
        // 1=A, 2=B, 3=C, 4=D
        seed(
            &graph,
            &[1, 2, 3, 4],
            &[(1, 1, 2), (2, 1, 3), (3, 4, 2), (4, 4, 3)],
        )
        .await;

        let hits = check_graph_context(&graph, nid(1), 0.5)
            .await
            .expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        let (a, b) = ResolutionCandidate::ordered_pair(nid(1), nid(4));
        assert_eq!(c.entity_a_id, a);
        assert_eq!(c.entity_b_id, b);
        assert!(c.entity_a_id < c.entity_b_id);
        assert!(
            (c.score - GRAPH_CONTEXT_SCORE_WEIGHT).abs() < 1e-12,
            "jaccard=1.0 時 score 應接近 0.7，得到 {}",
            c.score
        );
        assert_eq!(c.method, "graph_context");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "graph_context");
        assert_eq!(c.evidence["jaccard_similarity"], 1.0);
        assert_eq!(c.evidence["entity_a_neighbor_count"], 2);
        assert_eq!(c.evidence["entity_b_neighbor_count"], 2);
        assert_eq!(c.evidence["shared_count"], 2);
        assert_eq!(c.evidence["union_count"], 2);
        let shared = c.evidence["shared_neighbors"]
            .as_array()
            .expect("shared_neighbors 應為陣列");
        assert_eq!(shared.len(), 2);
        let mut shared_ids: Vec<&str> = shared.iter().filter_map(|v| v.as_str()).collect();
        shared_ids.sort();
        let mut expected = [nid(2).to_string(), nid(3).to_string()];
        expected.sort();
        assert_eq!(
            shared_ids,
            expected.iter().map(String::as_str).collect::<Vec<_>>()
        );
    }

    /// A-B，B 沒有其他邊 → 沒有 2-hop 候選。
    #[tokio::test]
    async fn leaf_neighbor_yields_no_candidates() {
        let graph = MockGraphStore::new();
        seed(&graph, &[1, 2], &[(1, 1, 2)]).await;

        let hits = check_graph_context(&graph, nid(1), 0.0)
            .await
            .expect("check");
        assert!(hits.is_empty(), "不該有 2-hop 候選，得到 {hits:?}");
    }

    /// A 完全沒有邊 → 空 Vec，連候選集合都長不出來。
    #[tokio::test]
    async fn isolated_node_yields_empty() {
        let graph = MockGraphStore::new();
        seed(&graph, &[1], &[]).await;

        let hits = check_graph_context(&graph, nid(1), 0.0)
            .await
            .expect("check");
        assert!(hits.is_empty(), "{hits:?}");
    }

    #[test]
    fn jaccard_empty_vs_empty_is_zero() {
        assert_eq!(jaccard(&HashSet::new(), &HashSet::new()), 0.0);
    }

    #[test]
    fn jaccard_complete_overlap_is_one() {
        let a: HashSet<EntityId> = [nid(1), nid(2)].into_iter().collect();
        let c = a.clone();
        assert_eq!(jaccard(&a, &c), 1.0);
    }

    #[test]
    fn jaccard_disjoint_is_zero() {
        let a: HashSet<EntityId> = [nid(1), nid(2)].into_iter().collect();
        let c: HashSet<EntityId> = [nid(3), nid(4)].into_iter().collect();
        assert_eq!(jaccard(&a, &c), 0.0);
    }

    #[test]
    fn jaccard_partial_overlap() {
        // {1,2,3} ∩ {2,3,4} = 2，∪ = 4 → 0.5
        let a: HashSet<EntityId> = [nid(1), nid(2), nid(3)].into_iter().collect();
        let c: HashSet<EntityId> = [nid(2), nid(3), nid(4)].into_iter().collect();
        assert_eq!(jaccard(&a, &c), 0.5);
    }
}
