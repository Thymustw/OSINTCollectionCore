//! Graph Expansion（SPEC_V0.3 §8）：給定一個已知 Entity，透過 Neo4j 1-hop
//! 鄰居找出「可能與這個 collection 相關、但還沒被納入的既有 Entity」，
//! 產出 Candidate + CandidateEvidence（Acceptance B：一律先 Candidate，
//! 不直接建立確認過的關聯）。
//!
//! 手法跟 `resolver::graph_context::check_graph_context` 同一個技術底層
//! （`GraphStore::neighbors`），但語意不同：那邊是「這兩個 Entity 是不是
//! 同一個真實世界的東西」（resolution/去重），這裡是「這個既有 Entity
//! 跟目前這次調查有沒有關聯，值得納入嗎」（discovery/擴張範圍）——
//! 鄰居本身就是候選，不比對 2-hop Jaccard 相似度。
//!
//! **已知限制**：這一版沒有查重——同一個 Entity 對同一個 collection
//! 重複跑會產生重複的 Candidate 列。刻意先不做，不是沒發現。

use chrono::Utc;
use core_model::{
    Candidate, CandidateEvidence, CandidateId, CandidateStatus, CandidateType, CollectionBudget,
    CollectionId, Entity, EntityId,
};
use storage_core::{GraphStore, GraphTraversalOptions, RelationalStore};
use uuid::Uuid;

pub const DISCOVERY_METHOD_GRAPH_EXPANSION: &str = "graph_expansion";

/// SPEC_V0.3 §8 已列的參考名稱；這次補上真正的實作（關聯式 Source 查詢路徑）。
pub const DISCOVERY_METHOD_SOURCE_EXPANSION: &str = "source_expansion";

/// Graph Expansion 產出的 candidate score／confidence 固定值——弱訊號：
/// 「這個既有 Entity 跟來源 Entity 有圖上的直接連結」不代表跟這次調查
/// 相關，純粹是「值得人工看一眼」的訊號。比 resolver 的
/// `GRAPH_CONTEXT_SCORE_WEIGHT`（0.7，衡量兩者是否同一實體）更弱——
/// 這裡衡量的是完全不同的東西（相關性，不是同一性），不能援用那個數字。
pub const GRAPH_EXPANSION_SCORE: f64 = 0.4;

/// Source Expansion 的 score／confidence。數值跟 [`GRAPH_EXPANSION_SCORE`]
/// 同量級，但**必須獨立宣告**：這是關聯式查詢（entity_extractions →
/// provenance → raw_evidence）的訊號，不是圖遍歷，之後兩邊可能各自調整。
pub const SOURCE_EXPANSION_SCORE: f64 = 0.4;

/// Graph Expansion 只產出這四類鄰居（Acceptance A：product／vulnerability／
/// repo／source 裡，前三類走圖；`source` 走關聯式路徑，見
/// [`DISCOVERY_METHOD_SOURCE_EXPANSION`]）。`software` 與 `product` 都保留，
/// 因為目前無法斷定使用者輸入的是哪種語意。
///
/// **字串必須是 serde snake_case**（例如 `"vulnerability"` 不是
/// `"Vulnerability"`）。Neo4j 端是 `IN $entityTypes` 精確字串比對，傳錯
/// 格式會靜默回空結果，不會報錯。
const GRAPH_EXPANSION_ENTITY_TYPES: &[&str] =
    &["vulnerability", "repository", "software", "product"];

/// `list_sources_by_entity` 的查詢上限。比照
/// `list_entity_extractions_by_entity` 的 1..=100 慣例；真正寫入還要再被
/// `max_candidates_per_run` 截斷。
const SOURCE_EXPANSION_QUERY_LIMIT: u32 = 100;

/// 對 `source` 做一次 1-hop Graph Expansion，回傳**尚未寫入 store** 的
/// `(Candidate, CandidateEvidence)` 配對。
///
/// `depth` 是 `source` 目前的深度，回傳的 Candidate 深度是 `depth + 1`。
/// `max_depth` 檢查在這裡做（Acceptance D）：`depth + 1 > max_depth` 直接
/// 回空 Vec，**不呼叫 `GraphStore`**——超過限制不得擴張，也不消耗任何
/// 請求配額。
///
/// `entity_types` 交給 [`GraphTraversalOptions`]，過濾在 storage 層做完。
/// `None` = 不過濾（測試迴歸用）；生產路徑傳
/// [`GRAPH_EXPANSION_ENTITY_TYPES`] 的 snake_case 字串。
pub async fn graph_expansion<G: GraphStore>(
    graph: &G,
    source: &Entity,
    collection_id: Option<CollectionId>,
    depth: i32,
    max_depth: i32,
    entity_types: Option<Vec<String>>,
) -> Result<Vec<(Candidate, CandidateEvidence)>, storage_core::StorageError> {
    let next_depth = depth + 1;
    if next_depth > max_depth {
        return Ok(Vec::new());
    }

    let options = GraphTraversalOptions {
        max_hops: 1,
        relationship_types: None,
        entity_types,
        min_confidence: None,
        time_range: None,
    };
    let neighbors = graph.neighbors(&source.id, &options).await?;

    let now = Utc::now();
    let mut out = Vec::with_capacity(neighbors.len());
    for node in neighbors {
        if node.entity_id == source.id {
            continue;
        }
        let candidate_id: CandidateId = Uuid::now_v7();
        let candidate = Candidate {
            id: candidate_id,
            candidate_type: CandidateType::Entity,
            value: node.display_name.clone(),
            normalized_value: node.display_name.trim().to_lowercase(),
            collection_id,
            discovered_by: format!("entity:{}", source.id),
            discovery_method: DISCOVERY_METHOD_GRAPH_EXPANSION.to_string(),
            confidence: GRAPH_EXPANSION_SCORE,
            score: GRAPH_EXPANSION_SCORE,
            status: CandidateStatus::Pending,
            depth: next_depth,
            created_at: now,
            reviewed_at: None,
        };
        let evidence = CandidateEvidence {
            id: Uuid::now_v7(),
            candidate_id,
            object_id: None,
            entity_id: Some(node.entity_id as EntityId),
            relationship_id: None,
            raw_evidence_id: None,
            reason: format!(
                "Entity `{}`（{}）的 1-hop graph neighbor，透過 graph_expansion 發現",
                source.name, source.id
            ),
            weight: GRAPH_EXPANSION_SCORE,
            created_at: now,
        };
        out.push((candidate, evidence));
    }
    Ok(out)
}

/// 解析 `job.parameters`、讀 Entity、讀（或退回保守預設）`CollectionBudget`、
/// 檢查 depth、原子消耗每日請求配額、呼叫 [`graph_expansion`]、寫入結果。
///
/// 回傳給 Job 結果訊息的摘要字串（成功）或錯誤訊息（失敗，呼叫端據此
/// 標記 Job Failed）。**泛型化 `S: RelationalStore`**（不寫死
/// `PostgresCanonicalStore`）——這樣測試可以用 `SqliteEmbeddedStore` +
/// `MockGraphStore`，不需要真實 Postgres/Neo4j。
///
/// `parameters` 必須包含 `entity_id`／`collection_id`（合法 UUID 字串），
/// `depth` 選填、預設 `0`。**`collection_id` 是必要參數**——Discovery
/// Budget 是 per-collection 的，沒有 collection 就無法判斷配額，不像
/// 其他地方的 `Option<CollectionId>` 那樣可以留白。
pub async fn run_graph_expansion<S: RelationalStore, G: GraphStore>(
    store: &S,
    graph: &G,
    parameters: Option<&serde_json::Value>,
) -> Result<String, String> {
    let params = parameters.ok_or_else(|| "discovery_run job 缺少 parameters".to_string())?;
    let entity_id: Uuid = params
        .get("entity_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| "parameters.entity_id 缺少或不是合法 UUID".to_string())?;
    let collection_id: Uuid = params
        .get("collection_id")
        .and_then(|v| v.as_str())
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| {
            "parameters.collection_id 缺少或不是合法 UUID——Discovery Budget 是 \
             per-collection 的，沒有 collection 就無法判斷配額"
                .to_string()
        })?;
    let depth = params
        .get("depth")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0) as i32;

    let entity = store
        .get_entity(entity_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("Entity `{entity_id}` 不存在"))?;

    let budget = store
        .get_collection_budget(collection_id)
        .await
        .map_err(|e| e.to_string())?
        .unwrap_or_else(|| CollectionBudget::conservative_default(collection_id, Utc::now()));

    if depth + 1 > budget.max_depth {
        return Err(format!(
            "depth {depth} 已達 max_depth {}，不得擴張（Acceptance D）",
            budget.max_depth
        ));
    }

    let today = Utc::now().date_naive();
    let consumption = store
        .try_consume_daily_request_budget(collection_id, today, 1, budget.daily_request_budget)
        .await
        .map_err(|e| e.to_string())?;
    if !consumption.allowed {
        return Err(format!(
            "collection `{collection_id}` 今日 daily_request_budget 已用完（{}/{}）",
            consumption.used_after, budget.daily_request_budget
        ));
    }

    let pairs = graph_expansion(
        graph,
        &entity,
        Some(collection_id),
        depth,
        budget.max_depth,
        Some(
            GRAPH_EXPANSION_ENTITY_TYPES
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
        ),
    )
    .await
    .map_err(|e| e.to_string())?;

    let cap = budget.max_candidates_per_run as usize;
    let mut written = 0usize;
    for (candidate, evidence) in &pairs {
        if written >= cap {
            tracing::warn!(
                %entity_id,
                cap = budget.max_candidates_per_run,
                "graph_expansion 候選數超過 max_candidates_per_run，已截斷"
            );
            break;
        }
        store
            .put_candidate(candidate)
            .await
            .map_err(|e| e.to_string())?;
        store
            .put_candidate_evidence(evidence)
            .await
            .map_err(|e| e.to_string())?;
        written += 1;
    }

    // Source 不在 Neo4j 圖投影裡。這段關聯式路徑
    // （entity_extractions → provenance → raw_evidence.source_id）
    // 是唯一能回答「這個 entity 關聯哪些情報來源」的方法。
    // 與 Graph Expansion 共用 `written` 計數器，不額外消耗 daily_request_budget。
    let mut source_found = 0usize;
    if written < cap {
        let source_ids = store
            .list_sources_by_entity(entity_id, SOURCE_EXPANSION_QUERY_LIMIT)
            .await
            .map_err(|e| e.to_string())?;
        source_found = source_ids.len();
        let now = Utc::now();
        let next_depth = depth + 1;
        for source_id in source_ids {
            if written >= cap {
                tracing::warn!(
                    %entity_id,
                    cap = budget.max_candidates_per_run,
                    "graph_expansion 候選數超過 max_candidates_per_run，source_expansion 已截斷"
                );
                break;
            }
            let Some(source) = store
                .get_source(source_id)
                .await
                .map_err(|e| e.to_string())?
            else {
                tracing::warn!(
                    %entity_id,
                    %source_id,
                    "list_sources_by_entity 回了 source_id 但 get_source 讀不到，略過"
                );
                continue;
            };
            let candidate_id: CandidateId = Uuid::now_v7();
            let candidate = Candidate {
                id: candidate_id,
                candidate_type: CandidateType::Source,
                value: source.name.clone(),
                normalized_value: source.name.trim().to_lowercase(),
                collection_id: Some(collection_id),
                discovered_by: format!("entity:{}", entity.id),
                discovery_method: DISCOVERY_METHOD_SOURCE_EXPANSION.to_string(),
                confidence: SOURCE_EXPANSION_SCORE,
                score: SOURCE_EXPANSION_SCORE,
                status: CandidateStatus::Pending,
                depth: next_depth,
                created_at: now,
                reviewed_at: None,
            };
            let evidence = CandidateEvidence {
                id: Uuid::now_v7(),
                candidate_id,
                object_id: None,
                entity_id: Some(entity.id),
                relationship_id: None,
                raw_evidence_id: None,
                reason: format!(
                    "Entity `{}` 透過 raw evidence 關聯到 Source `{}`，經 entity_extractions → provenance → raw_evidence 三跳查詢發現",
                    entity.name, source.name
                ),
                weight: SOURCE_EXPANSION_SCORE,
                created_at: now,
            };
            store
                .put_candidate(&candidate)
                .await
                .map_err(|e| e.to_string())?;
            store
                .put_candidate_evidence(&evidence)
                .await
                .map_err(|e| e.to_string())?;
            written += 1;
        }
    }

    Ok(format!(
        "graph_expansion 完成：Entity `{entity_id}` 找到 {} 個鄰居、{source_found} 個關聯 Source，寫入 {written} 筆 Candidate",
        pairs.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    use chrono::{TimeZone, Utc};
    use core_model::{
        Collection, Connector, Document, DocumentType, EntityExtraction, EntityType, Provenance,
        RawEvidence, Source, SourceType,
    };
    use serde_json::json;
    use storage_core::conformance::find_workspace_root;
    use storage_core::mock::MockGraphStore;
    use storage_core::{GraphEdge, GraphNode};
    use storage_sqlite::SqliteEmbeddedStore;

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, 8, 0, 0).unwrap()
    }

    fn nid(n: u128) -> EntityId {
        Uuid::from_u128(n)
    }

    fn node(id: u128, name: &str) -> GraphNode {
        node_typed(id, name, "vulnerability")
    }

    fn node_typed(id: u128, name: &str, entity_type: &str) -> GraphNode {
        GraphNode {
            entity_id: nid(id),
            entity_type: entity_type.into(),
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

    fn entity(id: u128, name: &str) -> Entity {
        Entity {
            id: nid(id),
            entity_type: EntityType::Person,
            name: name.into(),
            normalized_name: name.to_lowercase(),
            description: None,
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
            merged_into: None,
            attributes: json!({}),
        }
    }

    // ------------------------------------------------------- graph_expansion (純函式)

    #[tokio::test]
    async fn one_hop_neighbor_becomes_candidate_with_evidence() {
        let graph = MockGraphStore::new();
        graph.upsert_node(&node(1, "A")).await.unwrap();
        graph.upsert_node(&node(2, "B")).await.unwrap();
        graph.upsert_edge(&edge(1, 1, 2)).await.unwrap();

        let source = entity(1, "A");
        let collection_id = Some(Uuid::now_v7());
        let pairs = graph_expansion(&graph, &source, collection_id, 0, 3, None)
            .await
            .expect("graph_expansion 應該成功");

        assert_eq!(pairs.len(), 1, "{pairs:?}");
        let (candidate, evidence) = &pairs[0];
        assert_eq!(candidate.candidate_type, CandidateType::Entity);
        assert_eq!(candidate.discovery_method, DISCOVERY_METHOD_GRAPH_EXPANSION);
        // Acceptance B：一律先 Candidate，不是直接確認過的身份。
        assert_eq!(candidate.status, CandidateStatus::Pending);
        assert_eq!(candidate.depth, 1);
        assert_eq!(candidate.collection_id, collection_id);
        assert_eq!(candidate.value, "B");
        // Acceptance C：Explainability——evidence 指得出「為什麼」。
        assert_eq!(evidence.candidate_id, candidate.id);
        assert_eq!(evidence.entity_id, Some(nid(2)));
        assert!(evidence.reason.contains("graph_expansion"));
    }

    #[tokio::test]
    async fn isolated_entity_yields_no_candidates() {
        let graph = MockGraphStore::new();
        graph.upsert_node(&node(1, "A")).await.unwrap();
        let source = entity(1, "A");
        let pairs = graph_expansion(&graph, &source, None, 0, 3, None)
            .await
            .expect("graph_expansion 應該成功");
        assert!(pairs.is_empty(), "{pairs:?}");
    }

    #[tokio::test]
    async fn depth_at_limit_yields_no_candidates_and_does_not_query_graph() {
        // Acceptance D：max_depth=2 時不得建立 depth=3 的 expansion。
        let graph = MockGraphStore::new();
        graph.upsert_node(&node(1, "A")).await.unwrap();
        graph.upsert_node(&node(2, "B")).await.unwrap();
        graph.upsert_edge(&edge(1, 1, 2)).await.unwrap();
        let source = entity(1, "A");
        // depth=2, max_depth=2 → next_depth=3 > 2，必須拒絕擴張。
        let pairs = graph_expansion(&graph, &source, None, 2, 2, None)
            .await
            .expect("即使拒絕擴張，函式本身不該回錯誤");
        assert!(pairs.is_empty(), "超過 max_depth 不該有任何候選，{pairs:?}");
    }

    #[test]
    fn graph_expansion_entity_type_filter_is_serde_snake_case() {
        // 過濾值必須是 serde snake_case。Neo4j `IN $entityTypes` 精確比對，
        // 傳 Debug 格式（"Vulnerability"）會靜默回空。
        for &wire in GRAPH_EXPANSION_ENTITY_TYPES {
            let parsed: EntityType = serde_json::from_value(json!(wire))
                .unwrap_or_else(|err| panic!("`{wire}` 不是合法的 EntityType snake_case：{err}"));
            let roundtrip = serde_json::to_value(parsed).expect("EntityType 可序列化");
            assert_eq!(roundtrip, json!(wire), "{wire}");
        }
    }

    #[tokio::test]
    async fn graph_expansion_filters_neighbors_by_entity_type() {
        let graph = MockGraphStore::new();
        graph
            .upsert_node(&node_typed(1, "Org", "organization"))
            .await
            .unwrap();
        graph
            .upsert_node(&node_typed(2, "PersonNeighbor", "person"))
            .await
            .unwrap();
        graph
            .upsert_node(&node_typed(3, "CVE-1", "vulnerability"))
            .await
            .unwrap();
        graph.upsert_edge(&edge(1, 1, 2)).await.unwrap();
        graph.upsert_edge(&edge(2, 1, 3)).await.unwrap();

        let source = entity(1, "Org");
        let types: Vec<String> = GRAPH_EXPANSION_ENTITY_TYPES
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let pairs = graph_expansion(&graph, &source, None, 0, 3, Some(types))
            .await
            .expect("graph_expansion 應該成功");

        assert_eq!(
            pairs.len(),
            1,
            "person 鄰居應被過濾，只留 vulnerability：{pairs:?}"
        );
        assert_eq!(pairs[0].0.value, "CVE-1");
        assert_eq!(pairs[0].1.entity_id, Some(nid(3)));
    }

    // ------------------------------------------------------- run_graph_expansion（整合）
    //
    // harness 建構方式（open_harness／臨時檔清理）比照
    // `crates/resolver/src/auto_approval.rs` 測試模組既有的寫法——保持整個
    // repo 對「怎麼在測試裡開一個乾淨的 SqliteEmbeddedStore」的做法一致。

    struct Harness {
        store: SqliteEmbeddedStore,
        path: PathBuf,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            cleanup(&self.path);
        }
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    async fn open_harness() -> Harness {
        let root = find_workspace_root().expect("workspace root");
        let path: PathBuf = root.join(format!(
            "var/osint-graph-expansion-{}.sqlite",
            Uuid::now_v7()
        ));
        let store = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite");
        store.migrate().await.expect("migrate");
        Harness { store, path }
    }

    /// 建一筆 Collection。
    fn collection(id: Uuid) -> Collection {
        Collection {
            id,
            workspace_id: None,
            name: "test-collection".into(),
            description: None,
            status: "active".into(),
            priority: 1,
            created_at: ts(),
            updated_at: ts(),
        }
    }

    fn budget(id: Uuid, daily_request_budget: i64, max_depth: i32) -> CollectionBudget {
        budget_with_cap(id, daily_request_budget, max_depth, 10)
    }

    fn budget_with_cap(
        id: Uuid,
        daily_request_budget: i64,
        max_depth: i32,
        max_candidates_per_run: i32,
    ) -> CollectionBudget {
        CollectionBudget {
            collection_id: id,
            max_candidates_per_run,
            max_requests_per_run: 10,
            max_ai_calls_per_run: 10,
            max_depth,
            daily_request_budget,
            daily_ai_budget: 10,
            created_at: ts(),
            updated_at: ts(),
        }
    }

    /// 組一份跑 Graph Expansion 的 parameters（entity_id／collection_id／depth）。
    fn parameters(entity_id: Uuid, collection_id: Uuid, depth: i32) -> serde_json::Value {
        json!({
            "entity_id": entity_id.to_string(),
            "collection_id": collection_id.to_string(),
            "depth": depth,
        })
    }

    /// 建好 pre-requisite：一筆 Collection、兩筆 Entity（A、B）、graph 上 A-B
    /// 的邊。因為 data presence 是共通的，每個整合測試再依需求決定要不要
    /// `put_collection_budget`（那是可選的設定）。回傳 `(entity_id, collection_id, graph)`。
    async fn seed_single_neighbor(store: &SqliteEmbeddedStore) -> (Uuid, Uuid, MockGraphStore) {
        let collection_id = Uuid::now_v7();
        store
            .put_collection(&collection(collection_id))
            .await
            .unwrap();
        let a = entity(1, "A");
        let b = entity(2, "B");
        store.put_entity(&a).await.unwrap();
        store.put_entity(&b).await.unwrap();

        let graph = MockGraphStore::new();
        graph.upsert_node(&node(1, "A")).await.unwrap();
        graph.upsert_node(&node(2, "B")).await.unwrap();
        graph.upsert_edge(&edge(1, 1, 2)).await.unwrap();
        (a.id, collection_id, graph)
    }

    #[tokio::test]
    async fn run_graph_expansion_writes_candidates_and_consumes_daily_budget() {
        let h = open_harness().await;
        let (entity_id, collection_id, graph) = seed_single_neighbor(&h.store).await;
        h.store
            .put_collection_budget(&budget(collection_id, 10, 3))
            .await
            .unwrap();

        let summary = run_graph_expansion(
            &h.store,
            &graph,
            Some(&parameters(entity_id, collection_id, 0)),
        )
        .await
        .expect("run_graph_expansion 應該成功");
        assert!(summary.contains("寫入 1 筆"), "summary：{summary}");

        let candidates = h
            .store
            .list_candidates_by_collection(collection_id, None, None, 10)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1, "{candidates:?}");

        let usage = h
            .store
            .get_daily_usage(collection_id, Utc::now().date_naive())
            .await
            .unwrap();
        assert_eq!(usage.requests_used, 1, "配額真的要消耗");
    }

    #[tokio::test]
    async fn run_graph_expansion_rejects_when_daily_budget_exhausted() {
        let h = open_harness().await;
        let (entity_id, collection_id, graph) = seed_single_neighbor(&h.store).await;
        // daily_request_budget=0 → 配額不足，必須拒絕。
        h.store
            .put_collection_budget(&budget(collection_id, 0, 3))
            .await
            .unwrap();

        let err = run_graph_expansion(
            &h.store,
            &graph,
            Some(&parameters(entity_id, collection_id, 0)),
        )
        .await
        .expect_err("daily_request_budget=0 時該拒絕");
        assert!(err.contains("daily_request_budget"), "err：{err}");

        // 配額不夠時完全不寫入，不是寫一部分。
        let candidates = h
            .store
            .list_candidates_by_collection(collection_id, None, None, 10)
            .await
            .unwrap();
        assert!(candidates.is_empty(), "{candidates:?}");
    }

    #[tokio::test]
    async fn run_graph_expansion_rejects_when_depth_exceeds_max() {
        let h = open_harness().await;
        let (entity_id, collection_id, graph) = seed_single_neighbor(&h.store).await;
        // max_depth=1，depth=1 → next_depth=2 > 1，必須拒絕且不消耗配額。
        h.store
            .put_collection_budget(&budget(collection_id, 10, 1))
            .await
            .unwrap();

        let err = run_graph_expansion(
            &h.store,
            &graph,
            Some(&parameters(entity_id, collection_id, 1)),
        )
        .await
        .expect_err("depth 超過 max_depth 時該拒絕");
        assert!(err.contains("max_depth"), "err：{err}");

        // 深度檢查發生在配額消耗之前，所以不消耗任何每日配額。
        let usage = h
            .store
            .get_daily_usage(collection_id, Utc::now().date_naive())
            .await
            .unwrap();
        assert_eq!(usage.requests_used, 0);
    }

    #[tokio::test]
    async fn run_graph_expansion_falls_back_to_conservative_default_budget() {
        let h = open_harness().await;
        let (entity_id, collection_id, graph) = seed_single_neighbor(&h.store).await;
        // 不呼叫 put_collection_budget（沒有明確設定）→ 退回 conservative_default。

        let summary = run_graph_expansion(
            &h.store,
            &graph,
            Some(&parameters(entity_id, collection_id, 0)),
        )
        .await
        .expect("沒有明確設定 budget 時應退回保守預設並成功");
        assert!(summary.contains("寫入 1 筆"), "summary：{summary}");
    }

    /// 建一條 entity_extractions → provenance(derived_from) → raw_evidence
    /// → source 的關聯式路徑。Entity／Collection 必須已經存在。
    async fn seed_source_path(
        store: &SqliteEmbeddedStore,
        entity_id: Uuid,
        source_name: &str,
    ) -> Uuid {
        let source_id = Uuid::now_v7();
        store
            .put_source(&Source {
                id: source_id,
                name: source_name.into(),
                source_type: SourceType::Rss,
                platform: Some("nvd".into()),
                base_url: Some("https://example.invalid/rss".into()),
                description: None,
                language: Some("en".into()),
                country: None,
                enabled: true,
                collection_policy: json!({}),
                created_at: ts(),
                updated_at: ts(),
                last_seen: None,
            })
            .await
            .unwrap();
        let connector_id = Uuid::now_v7();
        store
            .put_connector(&Connector {
                id: connector_id,
                source_id,
                name: format!("connector-{source_name}"),
                connector_type: "rss".into(),
                version: "0.1.0".into(),
                enabled: true,
                configuration: json!({}),
                credential_reference: None,
                schedule: None,
                rate_limit: json!({}),
                timeout: json!({}),
                proxy_reference: None,
                checkpoint: json!({}),
                last_run: None,
                last_success: None,
                status: "idle".into(),
                error_count: 0,
            })
            .await
            .unwrap();
        let evidence_id = Uuid::now_v7();
        store
            .insert_raw_evidence(
                &RawEvidence {
                    id: evidence_id,
                    source_id,
                    connector_id,
                    external_id: Some(source_name.into()),
                    source_url: format!("https://example.invalid/{source_name}"),
                    retrieved_at: ts(),
                    content_type: Some("text/plain".into()),
                    mime_type: Some("text/plain".into()),
                    content_length: Some(32),
                    sha256: format!("{:0<64}", evidence_id.simple()),
                    storage_path: format!("s3://raw-evidence/{evidence_id}"),
                    http_status: Some(200),
                    http_headers: json!({}),
                    metadata: json!({}),
                    collector_version: "0.1.0".into(),
                },
                &[],
            )
            .await
            .unwrap();
        let document_id = Uuid::now_v7();
        store
            .put_document(&Document {
                id: document_id,
                object_type: DocumentType::Advisory,
                schema_version: "1.0".into(),
                title: Some(source_name.into()),
                body: Some("body".into()),
                summary: None,
                language: Some("en".into()),
                author: None,
                published_at: None,
                modified_at: None,
                observed_at: ts(),
                collected_at: ts(),
                source_url: None,
                canonical_url: None,
                normalized_content_hash: None,
                confidence: 0.9,
                labels: vec![],
                attributes: json!({}),
                external_key: None,
                simhash: None,
                duplicate_of: None,
            })
            .await
            .unwrap();
        store
            .put_provenance(&Provenance {
                id: Uuid::now_v7(),
                subject_id: document_id,
                action: "derived_from".into(),
                parent_id: Some(evidence_id),
                raw_evidence_id: Some(evidence_id),
                processor: "normalizer".into(),
                processor_version: "0.1.0".into(),
                timestamp: ts(),
                metadata: json!({}),
            })
            .await
            .unwrap();
        store
            .put_entity_extraction(&EntityExtraction {
                id: Uuid::now_v7(),
                object_id: document_id,
                entity_id,
                extractor: "regex-cve".into(),
                extractor_version: "0.1.0".into(),
                confidence: 0.99,
                text_offset: Some(0),
                excerpt: Some(source_name.into()),
            })
            .await
            .unwrap();
        source_id
    }

    #[tokio::test]
    async fn run_graph_expansion_writes_source_candidate_from_raw_evidence_path() {
        let h = open_harness().await;
        let collection_id = Uuid::now_v7();
        h.store
            .put_collection(&collection(collection_id))
            .await
            .unwrap();
        let org = entity(1, "Acme");
        h.store.put_entity(&org).await.unwrap();
        seed_source_path(&h.store, org.id, "NVD Feed").await;
        h.store
            .put_collection_budget(&budget(collection_id, 10, 3))
            .await
            .unwrap();

        // 圖上沒有鄰居，產出必須完全來自 source_expansion。
        let graph = MockGraphStore::new();
        graph
            .upsert_node(&node_typed(1, "Acme", "organization"))
            .await
            .unwrap();

        let summary = run_graph_expansion(
            &h.store,
            &graph,
            Some(&parameters(org.id, collection_id, 0)),
        )
        .await
        .expect("run_graph_expansion 應該成功");
        assert!(summary.contains("寫入 1 筆"), "summary：{summary}");
        assert!(summary.contains("1 個關聯 Source"), "summary：{summary}");

        let candidates = h
            .store
            .list_candidates_by_collection(collection_id, None, None, 10)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1, "{candidates:?}");
        assert_eq!(candidates[0].candidate_type, CandidateType::Source);
        assert_eq!(
            candidates[0].discovery_method,
            DISCOVERY_METHOD_SOURCE_EXPANSION
        );
        assert_eq!(candidates[0].value, "NVD Feed");
        assert_eq!(candidates[0].status, CandidateStatus::Pending);
    }

    #[tokio::test]
    async fn run_graph_expansion_shares_candidate_cap_with_source_expansion() {
        let h = open_harness().await;
        let (entity_id, collection_id, graph) = seed_single_neighbor(&h.store).await;
        seed_source_path(&h.store, entity_id, "NVD Feed").await;
        // cap=1：圖鄰居先寫，source 類 Candidate 必須被同一個 written 計數器截斷。
        // 若 source 有獨立配額，這裡會寫出 2 筆。
        h.store
            .put_collection_budget(&budget_with_cap(collection_id, 10, 3, 1))
            .await
            .unwrap();

        let summary = run_graph_expansion(
            &h.store,
            &graph,
            Some(&parameters(entity_id, collection_id, 0)),
        )
        .await
        .expect("run_graph_expansion 應該成功");
        assert!(summary.contains("寫入 1 筆"), "summary：{summary}");

        let candidates = h
            .store
            .list_candidates_by_collection(collection_id, None, None, 10)
            .await
            .unwrap();
        assert_eq!(candidates.len(), 1, "共用計數器應截成 1 筆：{candidates:?}");
        assert_eq!(
            candidates[0].candidate_type,
            CandidateType::Entity,
            "圖鄰居先寫，source 應被截斷"
        );
    }
}
