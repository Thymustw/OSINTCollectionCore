//! GraphStore／EmbeddingProvider／SearchStore／KeyValueStore 的確定性 mock。
//!
//! 給 Phase 1 的 semantic similarity、圖 API，以及 Phase 3 embedding-worker
//! 上層測試用，**不**打真實後端。Neo4j adapter 是 Phase 2 的 `storage-neo4j`，
//! OpenSearch adapter 是 `storage-opensearch`，不要把這裡當成它們的雛形。

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{EntityId, RelationshipId};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::error::StorageError;
use crate::health::{HealthProvider, StorageHealth};
use crate::traits::{
    BulkIndexResult, EmbeddingKind, EmbeddingModelRef, EmbeddingProvider, EmbeddingRequest,
    EmbeddingVector, GraphEdge, GraphNode, GraphPath, GraphPattern, GraphQuery, GraphStore,
    GraphTraversalOptions, KeyValueStore, ProjectionCheckpoint, ProjectionLag, ProjectionStore,
    QueryExpr, RebuildStatus, SearchDocument, SearchFilter, SearchHit, SearchHits, SearchQuery,
    SearchStore, StructuredSearch, VectorSearch, embedding_content_hash,
};

/// mock 拒絕超過這個跳數的遍歷。無界查詢會掃完整張圖。
pub const MOCK_GRAPH_MAX_HOPS: u32 = 32;

/// 英文 MiniLM。版本是上游的 `1.0.2`（ml-commons 文件裡的 `model_version`
/// 是 `"1"`，那是它自己的序號，見 `docs/developer/embedding.md` §2）。
pub const MOCK_MINILM_MODEL: &str = "huggingface/sentence-transformers/all-MiniLM-L6-v2";
pub const MOCK_MINILM_VERSION: &str = "1.0.2";

/// 多語 e5-small int8。上游沒有可讀版本號，用 zip 的
/// `model_content_hash_value` 頂替（`embedding.md` §2.1）。
pub const MOCK_E5_MODEL: &str = "intfloat/multilingual-e5-small-int8";
pub const MOCK_E5_VERSION: &str =
    "e8f6bd1be427a518c2160f1742fd3b70a1a1e0c01b4a93edf98671c8128c9ff6";

const MOCK_DEFAULT_DIM: usize = 384;

// ---------------------------------------------------------------------------
// MockGraphStore
// ---------------------------------------------------------------------------

#[derive(Default)]
struct GraphInner {
    nodes: HashMap<EntityId, GraphNode>,
    edges: HashMap<RelationshipId, GraphEdge>,
    /// 記憶體版 projection checkpoint。給 graph-worker 單元測試走 `rebuild()`。
    checkpoints: HashMap<String, ProjectionCheckpoint>,
    rebuilds: HashMap<String, RebuildStatus>,
}

/// 記憶體圖。CRUD 與有界遍歷足夠讓上層測試寫，不是 Neo4j 語意模擬器。
pub struct MockGraphStore {
    inner: Mutex<GraphInner>,
}

impl MockGraphStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(GraphInner::default()),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, GraphInner>, StorageError> {
        self.inner.lock().map_err(|_| StorageError::Unknown {
            backend: "mock-graph",
            message: "MockGraphStore mutex 已中毒（先前有 panic 持有鎖）。請重開測試行程".into(),
        })
    }
}

impl Default for MockGraphStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HealthProvider for MockGraphStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        Ok(StorageHealth::ok("mock-graph", "記憶體圖可用"))
    }
}

#[async_trait]
impl GraphStore for MockGraphStore {
    async fn upsert_node(&self, node: &GraphNode) -> Result<(), StorageError> {
        self.lock()?.nodes.insert(node.entity_id, node.clone());
        Ok(())
    }

    async fn upsert_edge(&self, edge: &GraphEdge) -> Result<(), StorageError> {
        if edge.source == edge.target {
            return Err(StorageError::ConstraintViolation {
                message:
                    "GraphEdge 的 source 與 target 不能是同一個節點。自環不是 Graph API 的語意，\
                          請檢查 relationship 寫入端"
                        .into(),
            });
        }
        self.lock()?
            .edges
            .insert(edge.relationship_id, edge.clone());
        Ok(())
    }

    async fn delete_node(&self, entity_id: &EntityId) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner.nodes.remove(entity_id);
        inner
            .edges
            .retain(|_, e| e.source != *entity_id && e.target != *entity_id);
        Ok(())
    }

    async fn delete_edge(&self, relationship_id: &RelationshipId) -> Result<(), StorageError> {
        self.lock()?.edges.remove(relationship_id);
        Ok(())
    }

    async fn neighbors(
        &self,
        entity_id: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphNode>, StorageError> {
        check_hops(options.max_hops)?;
        let inner = self.lock()?;
        let mut nodes = neighborhood(&inner, entity_id, options);
        nodes.retain(|n| n.entity_id != *entity_id);
        if let Some(types) = &options.entity_types {
            nodes.retain(|n| types.iter().any(|t| t == &n.entity_type));
        }
        nodes.sort_by_key(|n| n.entity_id);
        Ok(nodes)
    }

    async fn relationships(
        &self,
        entity_id: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphEdge>, StorageError> {
        check_hops(options.max_hops)?;
        let inner = self.lock()?;
        let dist = reachable_dist(&inner, entity_id, options);
        let mut out: Vec<GraphEdge> = inner
            .edges
            .values()
            .filter(|e| edge_matches(e, options))
            .filter(|e| dist.contains_key(&e.source) && dist.contains_key(&e.target))
            .filter(|e| closer_end_within_budget(e, entity_id, &dist, options.max_hops))
            .cloned()
            .collect();
        if let Some(types) = &options.entity_types {
            out.retain(|e| other_end_type_matches(&inner, e, entity_id, types));
        }
        out.sort_by_key(|e| e.relationship_id);
        Ok(out)
    }

    async fn shortest_path(
        &self,
        from: &EntityId,
        to: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Option<GraphPath>, StorageError> {
        check_hops(options.max_hops)?;
        let inner = self.lock()?;
        Ok(find_shortest_path(&inner, from, to, options))
    }

    async fn query(&self, query: &GraphQuery) -> Result<Vec<GraphPath>, StorageError> {
        if query.starts.is_empty() {
            return Err(StorageError::ConstraintViolation {
                message: "GraphQuery.starts 是空的。圖查詢必須指定起點，\
                          否則等於掃完整張圖——請至少給一個 entity id"
                    .into(),
            });
        }
        check_hops(query.options.max_hops)?;
        let inner = self.lock()?;
        let mut paths = Vec::new();
        match &query.pattern {
            GraphPattern::Neighbors => {
                for start in &query.starts {
                    for node in neighborhood(&inner, start, &query.options)
                        .into_iter()
                        .filter(|n| n.entity_id != *start)
                        .filter(|n| entity_type_ok(n, &query.options))
                    {
                        if let Some(path) =
                            find_shortest_path(&inner, start, &node.entity_id, &query.options)
                        {
                            paths.push(path);
                        }
                    }
                }
            }
            GraphPattern::Relationships => {
                for start in &query.starts {
                    let reachable = reachable_ids(&inner, start, &query.options);
                    let dist = reachable_dist(&inner, start, &query.options);
                    for edge in inner.edges.values() {
                        if !edge_matches(edge, &query.options) {
                            continue;
                        }
                        if !reachable.contains(&edge.source) || !reachable.contains(&edge.target) {
                            continue;
                        }
                        if !closer_end_within_budget(edge, start, &dist, query.options.max_hops) {
                            continue;
                        }
                        if let Some(types) = &query.options.entity_types {
                            if !other_end_type_matches(&inner, edge, start, types) {
                                continue;
                            }
                        }
                        if let Some(path) = path_for_edge(&inner, start, edge) {
                            paths.push(path);
                        }
                    }
                }
            }
            GraphPattern::ShortestPath { to } => {
                for start in &query.starts {
                    if let Some(path) = find_shortest_path(&inner, start, to, &query.options) {
                        paths.push(path);
                    }
                }
            }
            GraphPattern::BoundedWalk { end } => {
                for start in &query.starts {
                    collect_bounded_walks(
                        &inner,
                        start,
                        end.as_deref(),
                        &query.options,
                        &mut paths,
                    );
                }
            }
        }
        Ok(paths)
    }

    async fn wipe(&self) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner.nodes.clear();
        inner.edges.clear();
        Ok(())
    }
}

#[async_trait]
impl ProjectionStore for MockGraphStore {
    async fn checkpoint(
        &self,
        projection: &str,
    ) -> Result<Option<ProjectionCheckpoint>, StorageError> {
        Ok(self.lock()?.checkpoints.get(projection).cloned())
    }

    async fn save_checkpoint(&self, checkpoint: &ProjectionCheckpoint) -> Result<(), StorageError> {
        self.lock()?
            .checkpoints
            .insert(checkpoint.projection.clone(), checkpoint.clone());
        Ok(())
    }

    async fn projection_lag(
        &self,
        projection: &str,
        now: DateTime<Utc>,
    ) -> Result<ProjectionLag, StorageError> {
        let checkpoint = self.lock()?.checkpoints.get(projection).cloned();
        Ok(ProjectionLag::from_checkpoint(checkpoint, now))
    }

    async fn rebuild_status(&self, projection: &str) -> Result<RebuildStatus, StorageError> {
        Ok(self
            .lock()?
            .rebuilds
            .get(projection)
            .cloned()
            .unwrap_or_else(|| RebuildStatus::idle(projection)))
    }

    async fn set_rebuild_status(&self, status: &RebuildStatus) -> Result<(), StorageError> {
        self.lock()?
            .rebuilds
            .insert(status.projection.clone(), status.clone());
        Ok(())
    }

    async fn reset_projection(&self, projection: &str) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner.checkpoints.remove(projection);
        inner.rebuilds.remove(projection);
        Ok(())
    }
}

fn check_hops(max_hops: u32) -> Result<(), StorageError> {
    if max_hops > MOCK_GRAPH_MAX_HOPS {
        return Err(StorageError::ConstraintViolation {
            message: format!(
                "max_hops={max_hops} 超過 mock 上限 {MOCK_GRAPH_MAX_HOPS}。\
                 無界遍歷會掃完整張圖，請把跳數調小"
            ),
        });
    }
    Ok(())
}

fn edge_matches(edge: &GraphEdge, options: &GraphTraversalOptions) -> bool {
    if let Some(types) = &options.relationship_types {
        if !types.iter().any(|t| t == &edge.relationship_type) {
            return false;
        }
    }
    if let Some(min) = options.min_confidence {
        if edge.confidence < min {
            return false;
        }
    }
    if let Some((from, to)) = options.time_range {
        // 觀測區間與查詢區間重疊。用 last_seen 落在區間內會漏掉長壽命的邊。
        if !(edge.first_seen <= to && edge.last_seen >= from) {
            return false;
        }
    }
    true
}

fn entity_type_ok(node: &GraphNode, options: &GraphTraversalOptions) -> bool {
    match &options.entity_types {
        None => true,
        Some(types) => types.iter().any(|t| t == &node.entity_type),
    }
}

fn other_end(edge: &GraphEdge, start: &EntityId) -> EntityId {
    if edge.source == *start {
        edge.target
    } else {
        edge.source
    }
}

fn other_end_type_matches(
    inner: &GraphInner,
    edge: &GraphEdge,
    start: &EntityId,
    types: &[String],
) -> bool {
    let other = if edge.source == *start || edge.target == *start {
        other_end(edge, start)
    } else {
        // 多跳、邊不直接連 start：兩端任一符合即可。
        if inner
            .nodes
            .get(&edge.source)
            .is_some_and(|n| types.iter().any(|t| t == &n.entity_type))
        {
            return true;
        }
        edge.target
    };
    inner
        .nodes
        .get(&other)
        .is_some_and(|n| types.iter().any(|t| t == &n.entity_type))
}

fn incident<'a>(inner: &'a GraphInner, id: &EntityId) -> Vec<&'a GraphEdge> {
    inner
        .edges
        .values()
        .filter(|e| e.source == *id || e.target == *id)
        .collect()
}

/// BFS：節點 → 與 start 的跳數。只受邊過濾影響，不受 `entity_types` 影響
/// （那是結果過濾，見 `GraphStore` 文件）。
fn reachable_dist(
    inner: &GraphInner,
    start: &EntityId,
    options: &GraphTraversalOptions,
) -> HashMap<EntityId, u32> {
    let mut dist = HashMap::new();
    if !inner.nodes.contains_key(start) {
        return dist;
    }
    dist.insert(*start, 0);
    if options.max_hops == 0 {
        return dist;
    }
    let mut q = VecDeque::new();
    q.push_back(*start);
    while let Some(cur) = q.pop_front() {
        let d = dist[&cur];
        if d >= options.max_hops {
            continue;
        }
        for edge in incident(inner, &cur) {
            if !edge_matches(edge, options) {
                continue;
            }
            let nxt = other_end(edge, &cur);
            if !inner.nodes.contains_key(&nxt) {
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(slot) = dist.entry(nxt) {
                slot.insert(d + 1);
                q.push_back(nxt);
            }
        }
    }
    dist
}

fn reachable_ids(
    inner: &GraphInner,
    start: &EntityId,
    options: &GraphTraversalOptions,
) -> HashSet<EntityId> {
    reachable_dist(inner, start, options).into_keys().collect()
}

fn neighborhood(
    inner: &GraphInner,
    start: &EntityId,
    options: &GraphTraversalOptions,
) -> Vec<GraphNode> {
    reachable_dist(inner, start, options)
        .into_keys()
        .filter_map(|id| inner.nodes.get(&id).cloned())
        .collect()
}

fn closer_end_within_budget(
    edge: &GraphEdge,
    start: &EntityId,
    dist: &HashMap<EntityId, u32>,
    max_hops: u32,
) -> bool {
    if max_hops == 0 {
        return false;
    }
    let ds = dist.get(&edge.source).copied();
    let dt = dist.get(&edge.target).copied();
    match (ds, dt) {
        (Some(a), Some(b)) => a.min(b) < max_hops || edge.source == *start || edge.target == *start,
        _ => false,
    }
}

fn find_shortest_path(
    inner: &GraphInner,
    from: &EntityId,
    to: &EntityId,
    options: &GraphTraversalOptions,
) -> Option<GraphPath> {
    if !inner.nodes.contains_key(from) || !inner.nodes.contains_key(to) {
        return None;
    }
    if from == to {
        return Some(GraphPath {
            nodes: vec![inner.nodes[from].clone()],
            edges: vec![],
        });
    }
    if options.max_hops == 0 {
        return None;
    }
    let mut parent: HashMap<EntityId, (EntityId, GraphEdge)> = HashMap::new();
    let mut dist: HashMap<EntityId, u32> = HashMap::new();
    dist.insert(*from, 0);
    let mut q = VecDeque::new();
    q.push_back(*from);
    while let Some(cur) = q.pop_front() {
        if cur == *to {
            break;
        }
        let d = dist[&cur];
        if d >= options.max_hops {
            continue;
        }
        // 邊順序依 relationship_id，讓同長度的路徑是確定性的。
        let mut inc: Vec<&GraphEdge> = incident(inner, &cur);
        inc.sort_by_key(|e| e.relationship_id);
        for edge in inc {
            if !edge_matches(edge, options) {
                continue;
            }
            let nxt = other_end(edge, &cur);
            if !inner.nodes.contains_key(&nxt) {
                continue;
            }
            if let std::collections::hash_map::Entry::Vacant(slot) = dist.entry(nxt) {
                slot.insert(d + 1);
                parent.insert(nxt, (cur, edge.clone()));
                q.push_back(nxt);
            }
        }
    }
    if !parent.contains_key(to) {
        return None;
    }
    let mut nodes_rev = vec![inner.nodes[to].clone()];
    let mut edges_rev = Vec::new();
    let mut cur = *to;
    while cur != *from {
        let (prev, edge) = parent.get(&cur)?.clone();
        edges_rev.push(edge);
        nodes_rev.push(inner.nodes[&prev].clone());
        cur = prev;
    }
    nodes_rev.reverse();
    edges_rev.reverse();
    Some(GraphPath {
        nodes: nodes_rev,
        edges: edges_rev,
    })
}

fn path_for_edge(inner: &GraphInner, start: &EntityId, edge: &GraphEdge) -> Option<GraphPath> {
    let a = inner.nodes.get(&edge.source)?.clone();
    let b = inner.nodes.get(&edge.target)?.clone();
    // 面向 start：start 那一端放 nodes[0]。
    let (nodes, edges) = if edge.source == *start {
        (vec![a, b], vec![edge.clone()])
    } else if edge.target == *start {
        (vec![b, a], vec![edge.clone()])
    } else {
        (vec![a, b], vec![edge.clone()])
    };
    Some(GraphPath { nodes, edges })
}

fn collect_bounded_walks(
    inner: &GraphInner,
    start: &EntityId,
    end: Option<&[EntityId]>,
    options: &GraphTraversalOptions,
    out: &mut Vec<GraphPath>,
) {
    if !inner.nodes.contains_key(start) {
        return;
    }
    // DFS 有界簡單路徑。mock 圖很小；走到 256 條就停，避免測試寫成完全圖時炸記憶體。
    const PATH_CAP: usize = 256;
    struct WalkFrame {
        cur: EntityId,
        nodes: Vec<GraphNode>,
        edges: Vec<GraphEdge>,
        seen: HashSet<EntityId>,
    }
    let mut stack = vec![WalkFrame {
        cur: *start,
        nodes: vec![inner.nodes[start].clone()],
        edges: vec![],
        seen: HashSet::from([*start]),
    }];
    while let Some(frame) = stack.pop() {
        if out.len() >= PATH_CAP {
            return;
        }
        let hops = frame.edges.len() as u32;
        if hops > 0 {
            let last = frame.nodes.last().map(|n| n.entity_id);
            let end_ok = match end {
                None => true,
                Some(ids) => last.is_some_and(|id| ids.contains(&id)),
            };
            let type_ok = frame
                .nodes
                .last()
                .is_some_and(|n| entity_type_ok(n, options));
            if end_ok && type_ok {
                out.push(GraphPath {
                    nodes: frame.nodes.clone(),
                    edges: frame.edges.clone(),
                });
            }
        }
        if hops >= options.max_hops {
            continue;
        }
        let mut inc: Vec<&GraphEdge> = incident(inner, &frame.cur);
        inc.sort_by_key(|e| e.relationship_id);
        for edge in inc.into_iter().rev() {
            if !edge_matches(edge, options) {
                continue;
            }
            let nxt = other_end(edge, &frame.cur);
            if frame.seen.contains(&nxt) {
                continue;
            }
            let Some(node) = inner.nodes.get(&nxt) else {
                continue;
            };
            let mut seen = frame.seen.clone();
            seen.insert(nxt);
            let mut nodes = frame.nodes.clone();
            nodes.push(node.clone());
            let mut edges = frame.edges.clone();
            edges.push(edge.clone());
            stack.push(WalkFrame {
                cur: nxt,
                nodes,
                edges,
                seen,
            });
        }
    }
}

// ---------------------------------------------------------------------------
// MockEmbeddingProvider
// ---------------------------------------------------------------------------

/// 確定性假向量。語言路由與非對稱前綴的行為與生產預期一致，
/// 向量內容**不是**真的語意——只保證同一輸入穩定、不同 (model, kind, text) 不同。
///
/// [`MockEmbeddingProvider::unsupported`] 讓呼叫端測「還沒接語意相似度」的路徑。
#[derive(Clone)]
pub struct MockEmbeddingProvider {
    dimensions: usize,
    unsupported: bool,
    /// `embed`／`embed_batch` 實際打過幾次。給 embedding-worker Redis 快取
    /// 測試證明「命中時沒有再推論」。
    embed_calls: Arc<AtomicU64>,
}

impl MockEmbeddingProvider {
    /// 預設 384 維（與目前兩個生產模型相同），但這是**建構參數**不是編譯期常數。
    #[must_use]
    pub fn new() -> Self {
        Self {
            dimensions: MOCK_DEFAULT_DIM,
            unsupported: false,
            embed_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// 換維度。用來證明 [`EmbeddingProvider::dimensions`] 不是寫死 384。
    #[must_use]
    pub fn with_dimensions(dimensions: usize) -> Self {
        Self {
            dimensions,
            unsupported: false,
            embed_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// `embed`／`embed_batch` 一律回 [`StorageError::UnsupportedCapability`]。
    /// Phase 1 在還沒接 ml-commons 時注入這個，測「跳過語意相似度」的路徑。
    #[must_use]
    pub fn unsupported() -> Self {
        Self {
            dimensions: MOCK_DEFAULT_DIM,
            unsupported: true,
            embed_calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// `embed`／`embed_batch` 累計呼叫次數（含 `embed_batch` 裡每一筆 `embed`）。
    #[must_use]
    pub fn embed_calls(&self) -> u64 {
        self.embed_calls.load(Ordering::Relaxed)
    }

    fn route(language: Option<&str>) -> (bool /*english*/, &'static str, &'static str) {
        if is_english(language) {
            (true, MOCK_MINILM_MODEL, MOCK_MINILM_VERSION)
        } else {
            (false, MOCK_E5_MODEL, MOCK_E5_VERSION)
        }
    }

    fn prefixed_text(english: bool, kind: EmbeddingKind, text: &str) -> String {
        if english {
            // MiniLM 不加前綴。
            text.to_string()
        } else {
            match kind {
                EmbeddingKind::Query => format!("query: {text}"),
                EmbeddingKind::Passage => format!("passage: {text}"),
            }
        }
    }
}

impl Default for MockEmbeddingProvider {
    fn default() -> Self {
        Self::new()
    }
}

fn is_english(language: Option<&str>) -> bool {
    language.is_some_and(|lang| {
        lang.split(['-', '_'])
            .next()
            .is_some_and(|primary| primary.eq_ignore_ascii_case("en"))
    })
}

fn fake_vector(model: &str, prefixed: &str, dimensions: usize) -> Vec<f32> {
    let mut out = vec![0.0; dimensions];
    if dimensions == 0 {
        return out;
    }
    let mut block = Sha256::digest(format!("{model}\0{prefixed}").as_bytes()).to_vec();
    let mut offset = 0usize;
    let mut generated = 0u32;
    for slot in &mut out {
        if offset + 4 > block.len() {
            generated = generated.saturating_add(1);
            block = Sha256::digest({
                let mut d = Sha256::new();
                d.update(&block);
                d.update(generated.to_le_bytes());
                d.finalize()
            })
            .to_vec();
            offset = 0;
        }
        let mut bytes = [0u8; 4];
        bytes.copy_from_slice(&block[offset..offset + 4]);
        offset += 4;
        // 對應到 (-1, 1)。
        let u = u32::from_le_bytes(bytes);
        *slot = (u as f32 / u32::MAX as f32) * 2.0 - 1.0;
    }
    let norm = out.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut out {
            *x /= norm;
        }
    }
    out
}

#[async_trait]
impl HealthProvider for MockEmbeddingProvider {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        if self.unsupported {
            Ok(StorageHealth::ok(
                "mock-embedding",
                "mock 設成 UnsupportedCapability：embed() 會拒絕，用來測尚未接語意的路徑",
            ))
        } else {
            Ok(StorageHealth::ok("mock-embedding", "確定性假向量可用"))
        }
    }
}

#[async_trait]
impl EmbeddingProvider for MockEmbeddingProvider {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn dimensions_for(&self, _language: Option<&str>) -> usize {
        // 目前兩個模型維度相同；仍走方法而不是常數，換模型時 mock 可改這一行。
        self.dimensions
    }

    fn model_for(&self, language: Option<&str>) -> EmbeddingModelRef {
        let (_, model, version) = Self::route(language);
        EmbeddingModelRef {
            model: model.to_string(),
            model_version: version.to_string(),
            dimensions: self.dimensions,
        }
    }

    async fn embed(&self, request: &EmbeddingRequest) -> Result<EmbeddingVector, StorageError> {
        self.embed_calls.fetch_add(1, Ordering::Relaxed);
        if self.unsupported {
            return Err(StorageError::UnsupportedCapability {
                backend: "mock-embedding",
                capability: "embedding",
            });
        }
        let (english, model, version) = Self::route(request.language.as_deref());
        let prefixed = Self::prefixed_text(english, request.kind, &request.text);
        Ok(EmbeddingVector {
            model: model.to_string(),
            model_version: version.to_string(),
            dimensions: self.dimensions,
            content_hash: embedding_content_hash(&request.text),
            vector: fake_vector(model, &prefixed, self.dimensions),
        })
    }

    async fn embed_batch(
        &self,
        requests: &[EmbeddingRequest],
    ) -> Result<Vec<EmbeddingVector>, StorageError> {
        let mut out = Vec::with_capacity(requests.len());
        for request in requests {
            out.push(self.embed(request).await?);
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// MockSearchStore
// ---------------------------------------------------------------------------

#[derive(Default)]
struct SearchInner {
    docs: HashMap<(String, String), Value>,
    /// `(index, id)` → 還要再回幾次 [`StorageError::NotFound`]。
    ///
    /// 給 embedding-worker 測 indexer race：前 N 次 `update_fields` 假裝文件
    /// 還沒被 indexer 寫進 `osint-documents`。即使文件已經在 store 裡也照樣
    /// 回 NotFound，直到計數耗盡。
    not_found_remaining: HashMap<(String, String), u32>,
    /// 下一次 `vector_search` 回這個錯誤。測 Stage 5 基礎設施失敗必須回
    /// `Unsupported` 而不是 `Err`。
    vector_search_error: Option<String>,
}

/// 記憶體 SearchStore。CRUD 與 brute-force k-NN 足夠讓 embedding-worker
/// 單元測試寫，不是 OpenSearch 語意模擬器。
///
/// 對齊 [`SearchStore`] 契約的關鍵點：
///
/// * [`Self::index`] 整份覆寫 `_source`
/// * [`Self::update_fields`] 部分合併；id 不存在回 [`StorageError::NotFound`]，
///   **不** upsert（對齊 OpenSearch `_update` + `doc_as_upsert=false`）
/// * [`Self::bulk_upsert_fields`] 部分合併＋upsert：已存在就合併 `body` 的鍵，
///   不存在就整份寫入（對齊 OpenSearch `_update` + `doc_as_upsert=true`）
///
/// `Clone` 共用同一份記憶體（`Arc`），這樣測試才能從 `EmbeddingWorker`
/// 拿出 `search()` 再斷言寫入結果。
#[derive(Clone)]
pub struct MockSearchStore {
    inner: Arc<Mutex<SearchInner>>,
}

impl MockSearchStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SearchInner::default())),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, SearchInner>, StorageError> {
        self.inner.lock().map_err(|_| StorageError::Unknown {
            backend: "mock-search",
            message: "MockSearchStore mutex 已中毒（先前有 panic 持有鎖）。請重開測試行程".into(),
        })
    }

    /// 測試 helper：讀回目前的 `_source`。文件不存在回 `None`。
    pub fn get(&self, index: &str, id: &str) -> Result<Option<Value>, StorageError> {
        let inner = self.lock()?;
        Ok(inner
            .docs
            .get(&(index.to_string(), id.to_string()))
            .cloned())
    }

    /// 接下來 `n` 次 `update_fields(index, id)` 回 [`StorageError::NotFound`]，
    /// 即使文件已經在 store 裡。第 `n + 1` 次起才真正合併。
    ///
    /// 給 embedding-worker 測「indexer 還沒寫進 osint-documents」的 race，
    /// 不必真的連 OpenSearch。
    pub fn set_update_not_found_remaining(
        &self,
        index: &str,
        id: &str,
        n: u32,
    ) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner
            .not_found_remaining
            .insert((index.to_string(), id.to_string()), n);
        Ok(())
    }

    /// 下一次 `vector_search` 回 [`StorageError::Unavailable`]。測完自動清掉。
    pub fn fail_next_vector_search(&self, message: impl Into<String>) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner.vector_search_error = Some(message.into());
        Ok(())
    }
}

impl Default for MockSearchStore {
    fn default() -> Self {
        Self::new()
    }
}

fn match_query_string(source: &Value, query_string: &str) -> bool {
    let q = query_string.trim();
    if q.is_empty() || q == "*" {
        return true;
    }
    if let Some((field, value)) = q.split_once(':') {
        field_equals(source, field.trim(), value.trim())
    } else {
        source.to_string().contains(q)
    }
}

fn field_equals(source: &Value, field: &str, expected: &str) -> bool {
    source.get(field).is_some_and(|v| match v {
        Value::String(s) => s == expected,
        other => other.to_string().trim_matches('"') == expected,
    })
}

fn match_filters(source: &Value, filters: &[SearchFilter]) -> bool {
    filters.iter().all(|f| match f {
        SearchFilter::Term { field, value } => field_equals(source, field, value),
        SearchFilter::Missing { field } => source.get(field).is_none_or(Value::is_null),
        SearchFilter::DateRange { field, from, to } => {
            let Some(raw) = source.get(field).and_then(Value::as_str) else {
                return false;
            };
            let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(raw) else {
                return false;
            };
            let ts = parsed.with_timezone(&Utc);
            from.is_none_or(|start| ts >= start) && to.is_none_or(|end| ts <= end)
        }
        SearchFilter::Nested { path, terms } => source
            .get(path)
            .and_then(Value::as_array)
            .is_some_and(|arr| {
                arr.iter().any(|elem| {
                    terms.iter().all(|(k, v)| {
                        field_equals(elem, k, v) || field_equals(elem, &format!("{path}.{k}"), v)
                    })
                })
            }),
    })
}

fn match_expr(source: &Value, expr: &QueryExpr) -> bool {
    let haystack = source.to_string();
    match expr {
        QueryExpr::Term(t) | QueryExpr::Phrase(t) => haystack.contains(t),
        QueryExpr::And(xs) => xs.iter().all(|x| match_expr(source, x)),
        QueryExpr::Or(xs) => xs.iter().any(|x| match_expr(source, x)),
        QueryExpr::Not(inner) => !match_expr(source, inner),
    }
}

fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut na = 0.0f32;
    let mut nb = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

fn parse_vector(value: &Value) -> Option<Vec<f32>> {
    value.as_array().map(|arr| {
        arr.iter()
            .map(|n| n.as_f64().unwrap_or(0.0) as f32)
            .collect()
    })
}

#[async_trait]
impl HealthProvider for MockSearchStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        Ok(StorageHealth::ok("mock-search", "記憶體 SearchStore 可用"))
    }
}

#[async_trait]
impl SearchStore for MockSearchStore {
    async fn index(&self, document: SearchDocument) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner
            .docs
            .insert((document.index, document.id), document.body);
        Ok(())
    }

    async fn bulk_index(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError> {
        let n = documents.len() as u32;
        for document in documents {
            self.index(document).await?;
        }
        Ok(BulkIndexResult {
            indexed: n,
            errors: 0,
            failures: Vec::new(),
        })
    }

    async fn bulk_upsert_fields(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError> {
        // mock 是記憶體 map，沒有 OpenSearch 那種「只合併列出的欄位」的底層 API。
        // 行為對齊真實 adapter 的語意：已存在就合併 `body` 的鍵，不存在就整份寫入。
        let n = documents.len() as u32;
        let mut inner = self.lock()?;
        for document in documents {
            let key = (document.index, document.id);
            match inner.docs.get_mut(&key) {
                Some(existing) => {
                    if let (Some(obj), Some(patch)) =
                        (existing.as_object_mut(), document.body.as_object())
                    {
                        for (k, v) in patch {
                            obj.insert(k.clone(), v.clone());
                        }
                    } else {
                        inner.docs.insert(key, document.body);
                    }
                }
                None => {
                    inner.docs.insert(key, document.body);
                }
            }
        }
        Ok(BulkIndexResult {
            indexed: n,
            errors: 0,
            failures: Vec::new(),
        })
    }

    async fn query(&self, query: SearchQuery) -> Result<SearchHits, StorageError> {
        let inner = self.lock()?;
        let mut hits: Vec<SearchHit> = inner
            .docs
            .iter()
            .filter(|((index, _), source)| {
                index == &query.index && match_query_string(source, &query.query_string)
            })
            .map(|((_, id), source)| SearchHit {
                id: id.clone(),
                score: None,
                source: source.clone(),
                sort: Vec::new(),
                highlights: Default::default(),
            })
            .collect();
        let total = hits.len() as u64;
        let from = query.from as usize;
        if from >= hits.len() {
            hits.clear();
        } else {
            let end = (from + query.size as usize).min(hits.len());
            hits = hits[from..end].to_vec();
        }
        Ok(SearchHits { total, hits })
    }

    async fn search(&self, query: StructuredSearch) -> Result<SearchHits, StorageError> {
        let inner = self.lock()?;
        let mut hits: Vec<SearchHit> = inner
            .docs
            .iter()
            .filter(|((index, _), source)| {
                if index != &query.index {
                    return false;
                }
                if !match_filters(source, &query.filters) {
                    return false;
                }
                query
                    .expression
                    .as_ref()
                    .is_none_or(|expr| match_expr(source, expr))
            })
            .map(|((_, id), source)| SearchHit {
                id: id.clone(),
                score: None,
                source: source.clone(),
                sort: Vec::new(),
                highlights: Default::default(),
            })
            .collect();
        let total = hits.len() as u64;
        hits.truncate(query.size as usize);
        Ok(SearchHits { total, hits })
    }

    async fn delete(&self, index: &str, id: &str) -> Result<bool, StorageError> {
        let mut inner = self.lock()?;
        Ok(inner
            .docs
            .remove(&(index.to_string(), id.to_string()))
            .is_some())
    }

    async fn update_fields(
        &self,
        index: &str,
        id: &str,
        fields: Value,
    ) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        let key = (index.to_string(), id.to_string());
        if let Some(remaining) = inner.not_found_remaining.get_mut(&key) {
            if *remaining > 0 {
                *remaining -= 1;
                return Err(StorageError::NotFound {
                    message: format!(
                        "mock 延遲 NotFound：index `{index}` id `{id}` 還要再等 indexer 寫入"
                    ),
                });
            }
        }
        let Some(existing) = inner.docs.get_mut(&key) else {
            return Err(StorageError::NotFound {
                message: format!("index `{index}` 找不到 id `{id}`，無法部分更新"),
            });
        };
        let Some(obj) = existing.as_object_mut() else {
            return Err(StorageError::ConstraintViolation {
                message: format!("index `{index}` id `{id}` 的 _source 不是物件，無法合併欄位"),
            });
        };
        let Some(patch) = fields.as_object() else {
            return Err(StorageError::ConstraintViolation {
                message: "update_fields 的 fields 必須是 JSON 物件".into(),
            });
        };
        for (k, v) in patch {
            obj.insert(k.clone(), v.clone());
        }
        Ok(())
    }

    async fn vector_search(&self, query: VectorSearch) -> Result<SearchHits, StorageError> {
        let mut inner = self.lock()?;
        if let Some(message) = inner.vector_search_error.take() {
            return Err(StorageError::Unavailable {
                backend: "mock-search",
                message,
            });
        }
        let mut scored: Vec<(f32, SearchHit)> = inner
            .docs
            .iter()
            .filter(|((index, _), source)| {
                index == &query.index && match_filters(source, &query.filters)
            })
            .filter_map(|((_, id), source)| {
                let vec = source.get(&query.field).and_then(parse_vector)?;
                let score = cosine_sim(&query.vector, &vec);
                Some((
                    score,
                    SearchHit {
                        id: id.clone(),
                        score: Some(f64::from(score)),
                        source: source.clone(),
                        sort: Vec::new(),
                        highlights: Default::default(),
                    },
                ))
            })
            .collect();
        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.truncate(query.k as usize);
        let hits: Vec<SearchHit> = scored.into_iter().map(|(_, h)| h).collect();
        let total = hits.len() as u64;
        Ok(SearchHits { total, hits })
    }
}

// ---------------------------------------------------------------------------
// MockKeyValueStore
// ---------------------------------------------------------------------------

#[derive(Default)]
struct KvInner {
    entries: HashMap<String, (Vec<u8>, Option<Instant>)>,
}

/// 記憶體 [`KeyValueStore`]。TTL 用 [`Instant`]，過期後 `get` 當 miss。
///
/// 給 Stage 5／embedding-worker 的 Redis 快取路徑測，不打真實 Redis。
/// `Clone` 共用同一份記憶體。
#[derive(Clone)]
pub struct MockKeyValueStore {
    inner: Arc<Mutex<KvInner>>,
}

impl MockKeyValueStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(KvInner::default())),
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, KvInner>, StorageError> {
        self.inner.lock().map_err(|_| StorageError::Unknown {
            backend: "mock-kv",
            message: "MockKeyValueStore mutex 已中毒（先前有 panic 持有鎖）。請重開測試行程".into(),
        })
    }

    fn live_value(inner: &mut KvInner, key: &str) -> Option<Vec<u8>> {
        match inner.entries.get(key) {
            Some((_, Some(expires))) if *expires <= Instant::now() => {
                inner.entries.remove(key);
                None
            }
            Some((value, _)) => Some(value.clone()),
            None => None,
        }
    }
}

impl Default for MockKeyValueStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl HealthProvider for MockKeyValueStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        Ok(StorageHealth::ok("mock-kv", "記憶體 KeyValueStore 可用"))
    }
}

#[async_trait]
impl KeyValueStore for MockKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let mut inner = self.lock()?;
        Ok(Self::live_value(&mut inner, key))
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let mut inner = self.lock()?;
        inner
            .entries
            .insert(key.to_string(), (value.to_vec(), None));
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError> {
        if ttl.is_zero() {
            return Err(StorageError::Configuration {
                message: "KeyValueStore TTL 不可為 0。請傳正的 Duration".into(),
            });
        }
        let mut inner = self.lock()?;
        inner.entries.insert(
            key.to_string(),
            (value.to_vec(), Some(Instant::now() + ttl)),
        );
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<bool, StorageError> {
        let mut inner = self.lock()?;
        Ok(inner.entries.remove(key).is_some())
    }

    async fn expire(&self, key: &str, ttl: Duration) -> Result<bool, StorageError> {
        if ttl.is_zero() {
            return Err(StorageError::Configuration {
                message: "KeyValueStore TTL 不可為 0。請傳正的 Duration".into(),
            });
        }
        let mut inner = self.lock()?;
        match inner.entries.get_mut(key) {
            Some((_, expires)) => {
                *expires = Some(Instant::now() + ttl);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;
    use uuid::Uuid;

    fn ts(secs: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(1_757_000_000 + secs, 0).unwrap()
    }

    fn nid(n: u128) -> EntityId {
        Uuid::from_u128(n)
    }

    fn rid(n: u128) -> RelationshipId {
        Uuid::from_u128(n + 1000)
    }

    fn node(id: u128, ty: &str, name: &str) -> GraphNode {
        GraphNode {
            entity_id: nid(id),
            entity_type: ty.into(),
            display_name: name.into(),
            attributes: json!({}),
        }
    }

    fn edge(id: u128, src: u128, dst: u128, ty: &str, conf: f64) -> GraphEdge {
        GraphEdge {
            relationship_id: rid(id),
            source: nid(src),
            target: nid(dst),
            relationship_type: ty.into(),
            confidence: conf,
            first_seen: ts(0),
            last_seen: ts(100),
        }
    }

    async fn triangle() -> MockGraphStore {
        // 1 --mentions--> 2 --associated_with--> 3 --belongs_to--> 1
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "Alice")).await.unwrap();
        g.upsert_node(&node(2, "organization", "ExampleOrg"))
            .await
            .unwrap();
        g.upsert_node(&node(3, "domain", "example.com"))
            .await
            .unwrap();
        g.upsert_edge(&edge(1, 1, 2, "mentions", 0.9))
            .await
            .unwrap();
        g.upsert_edge(&edge(2, 2, 3, "associated_with", 0.4))
            .await
            .unwrap();
        g.upsert_edge(&edge(3, 3, 1, "belongs_to", 0.8))
            .await
            .unwrap();
        g
    }

    #[tokio::test]
    async fn upsert_is_idempotent_and_overwrites() {
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "Alice")).await.unwrap();
        g.upsert_node(&node(1, "person", "Alicia")).await.unwrap();
        let got = g
            .neighbors(&nid(1), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(got.is_empty());
        // 讀回 display_name：用 shortest_path 到自己。
        let me = g
            .shortest_path(&nid(1), &nid(1), &GraphTraversalOptions::one_hop())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(me.nodes[0].display_name, "Alicia");
    }

    #[tokio::test]
    async fn delete_node_cascades_edges() {
        let g = triangle().await;
        g.delete_node(&nid(2)).await.unwrap();
        // 1 與 3 之間原本靠 2 連，直接邊是 3→1。
        let rels = g
            .relationships(&nid(1), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].relationship_type, "belongs_to");
        // 指向 2 的 mentions 必須跟著消失，否則 shortest path 會走到幽靈節點。
        let to_org = g
            .shortest_path(&nid(1), &nid(2), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(to_org.is_none(), "節點已刪，不該還找得到路徑");
    }

    #[tokio::test]
    async fn delete_missing_is_ok() {
        let g = MockGraphStore::new();
        g.delete_node(&nid(99)).await.unwrap();
        g.delete_edge(&rid(99)).await.unwrap();
    }

    #[tokio::test]
    async fn wipe_clears_nodes_and_edges_but_health_stays_ok() {
        let g = triangle().await;
        g.wipe().await.unwrap();
        let n = g
            .neighbors(&nid(1), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(n.is_empty());
        let path = g
            .shortest_path(&nid(1), &nid(3), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(path.is_none());
        let health = g.health().await.unwrap();
        assert!(health.healthy, "{}", health.message);
        g.upsert_node(&node(1, "person", "AfterWipe"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn one_hop_neighbors_are_undirected() {
        let g = triangle().await;
        let mut n = g
            .neighbors(&nid(1), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        n.sort_by_key(|x| x.entity_id);
        let ids: Vec<_> = n.iter().map(|x| x.entity_id).collect();
        assert_eq!(
            ids,
            vec![nid(2), nid(3)],
            "mentions 出去、belongs_to 進來都算鄰居"
        );
    }

    #[tokio::test]
    async fn two_hop_reaches_the_far_node() {
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "A")).await.unwrap();
        g.upsert_node(&node(2, "person", "B")).await.unwrap();
        g.upsert_node(&node(3, "person", "C")).await.unwrap();
        g.upsert_edge(&edge(1, 1, 2, "associated_with", 1.0))
            .await
            .unwrap();
        g.upsert_edge(&edge(2, 2, 3, "associated_with", 1.0))
            .await
            .unwrap();
        let one = g
            .neighbors(&nid(1), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert_eq!(one.len(), 1);
        let mut two_opts = GraphTraversalOptions::one_hop();
        two_opts.max_hops = 2;
        let two = g.neighbors(&nid(1), &two_opts).await.unwrap();
        let ids: HashSet<_> = two.iter().map(|n| n.entity_id).collect();
        assert!(ids.contains(&nid(2)));
        assert!(ids.contains(&nid(3)), "兩跳必須走到 C");
    }

    #[tokio::test]
    async fn filters_relationship_type_confidence_and_time() {
        let g = triangle().await;
        let mut opts = GraphTraversalOptions::one_hop();
        opts.relationship_types = Some(vec!["mentions".into()]);
        let n = g.neighbors(&nid(1), &opts).await.unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].entity_id, nid(2));

        opts = GraphTraversalOptions::one_hop();
        opts.min_confidence = Some(0.85);
        let n = g.neighbors(&nid(1), &opts).await.unwrap();
        assert_eq!(n.len(), 1, "belongs_to 0.8 應被擋、mentions 0.9 留下");
        assert_eq!(n[0].entity_id, nid(2));

        opts = GraphTraversalOptions::one_hop();
        opts.time_range = Some((ts(200), ts(300))); // 與 [0,100] 不重疊
        let n = g.neighbors(&nid(1), &opts).await.unwrap();
        assert!(n.is_empty(), "時間區間不重疊的邊不該出現");
    }

    #[tokio::test]
    async fn entity_type_filters_results_not_the_walk() {
        // 1(person) - 2(org) - 3(domain)。兩跳、只要 domain：
        // 必須能經由 org 走到 domain。若 entity_types 被當成沿途過濾，
        // 這條路會斷，呼叫端會以為 1 跟 3 沒連上。
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "A")).await.unwrap();
        g.upsert_node(&node(2, "organization", "B")).await.unwrap();
        g.upsert_node(&node(3, "domain", "C")).await.unwrap();
        g.upsert_edge(&edge(1, 1, 2, "mentions", 1.0))
            .await
            .unwrap();
        g.upsert_edge(&edge(2, 2, 3, "associated_with", 1.0))
            .await
            .unwrap();
        let mut opts = GraphTraversalOptions::one_hop();
        opts.max_hops = 2;
        opts.entity_types = Some(vec!["domain".into()]);
        let n = g.neighbors(&nid(1), &opts).await.unwrap();
        assert_eq!(n.len(), 1);
        assert_eq!(n[0].entity_id, nid(3));
    }

    #[tokio::test]
    async fn shortest_path_none_when_disconnected() {
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "A")).await.unwrap();
        g.upsert_node(&node(2, "person", "B")).await.unwrap();
        let got = g
            .shortest_path(&nid(1), &nid(2), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(got.is_none());
        let missing = g
            .shortest_path(&nid(1), &nid(9), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(missing.is_none(), "節點不存在也是 None，不是 Err");
    }

    #[tokio::test]
    async fn shortest_path_respects_max_hops() {
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "A")).await.unwrap();
        g.upsert_node(&node(2, "person", "B")).await.unwrap();
        g.upsert_node(&node(3, "person", "C")).await.unwrap();
        g.upsert_edge(&edge(1, 1, 2, "associated_with", 1.0))
            .await
            .unwrap();
        g.upsert_edge(&edge(2, 2, 3, "associated_with", 1.0))
            .await
            .unwrap();
        let one = g
            .shortest_path(&nid(1), &nid(3), &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(one.is_none(), "兩跳的路不該被一跳找到");
        let mut two = GraphTraversalOptions::one_hop();
        two.max_hops = 2;
        let path = g
            .shortest_path(&nid(1), &nid(3), &two)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(path.nodes.len(), 3);
        assert_eq!(path.edges.len(), 2);
        assert_eq!(path.nodes[0].entity_id, nid(1));
        assert_eq!(path.nodes[2].entity_id, nid(3));
    }

    #[tokio::test]
    async fn query_rejects_empty_starts_and_unbounded_hops() {
        let g = MockGraphStore::new();
        let err = g
            .query(&GraphQuery {
                starts: vec![],
                pattern: GraphPattern::Neighbors,
                options: GraphTraversalOptions::one_hop(),
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ConstraintViolation { .. }),
            "{err}"
        );

        let err = g
            .query(&GraphQuery {
                starts: vec![nid(1)],
                pattern: GraphPattern::Neighbors,
                options: GraphTraversalOptions {
                    max_hops: MOCK_GRAPH_MAX_HOPS + 1,
                    ..GraphTraversalOptions::one_hop()
                },
            })
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::ConstraintViolation { .. }),
            "{err}"
        );
    }

    #[tokio::test]
    async fn query_shortest_path_and_neighbors() {
        let g = triangle().await;
        let paths = g
            .query(&GraphQuery {
                starts: vec![nid(1)],
                pattern: GraphPattern::ShortestPath { to: nid(2) },
                options: GraphTraversalOptions::one_hop(),
            })
            .await
            .unwrap();
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].edges.len(), 1);

        let npaths = g
            .query(&GraphQuery {
                starts: vec![nid(1)],
                pattern: GraphPattern::Neighbors,
                options: GraphTraversalOptions::one_hop(),
            })
            .await
            .unwrap();
        assert_eq!(npaths.len(), 2);
    }

    #[tokio::test]
    async fn graph_query_is_not_a_raw_string() {
        // 型別本身沒有 query 字串欄位。若有人加回去，這個序列化形狀會變。
        let q = GraphQuery {
            starts: vec![nid(1)],
            pattern: GraphPattern::Neighbors,
            options: GraphTraversalOptions::one_hop(),
        };
        let v = serde_json::to_value(&q).unwrap();
        assert!(v.get("query_string").is_none());
        assert!(v.get("cypher").is_none());
        assert_eq!(v["pattern"]["kind"], "neighbors");
    }

    #[tokio::test]
    async fn self_loop_is_rejected() {
        let g = MockGraphStore::new();
        g.upsert_node(&node(1, "person", "A")).await.unwrap();
        let err = g
            .upsert_edge(&GraphEdge {
                relationship_id: rid(1),
                source: nid(1),
                target: nid(1),
                relationship_type: "associated_with".into(),
                confidence: 1.0,
                first_seen: ts(0),
                last_seen: ts(1),
            })
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::ConstraintViolation { .. }));
    }

    // ----- EmbeddingProvider ------------------------------------------------

    fn req(text: &str, kind: EmbeddingKind, lang: Option<&str>) -> EmbeddingRequest {
        EmbeddingRequest {
            text: text.into(),
            kind,
            language: lang.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn language_routes_english_to_minilm_and_everything_else_to_e5() {
        let p = MockEmbeddingProvider::new();
        assert_eq!(p.model_for(Some("en")).model, MOCK_MINILM_MODEL);
        assert_eq!(p.model_for(Some("en-US")).model, MOCK_MINILM_MODEL);
        assert_eq!(p.model_for(Some("zh")).model, MOCK_E5_MODEL);
        assert_eq!(p.model_for(Some("zh-Hant")).model, MOCK_E5_MODEL);
        assert_eq!(
            p.model_for(None).model,
            MOCK_E5_MODEL,
            "語言未知必須走多語模型，不能默認英文 MiniLM"
        );

        let en = p
            .embed(&req("hello", EmbeddingKind::Query, Some("en")))
            .await
            .unwrap();
        let zh = p
            .embed(&req("hello", EmbeddingKind::Query, Some("zh")))
            .await
            .unwrap();
        assert_eq!(en.model, MOCK_MINILM_MODEL);
        assert_eq!(en.model_version, MOCK_MINILM_VERSION);
        assert_eq!(zh.model, MOCK_E5_MODEL);
        assert_eq!(zh.model_version, MOCK_E5_VERSION);
        assert_ne!(
            en.vector, zh.vector,
            "兩個模型的向量空間不相通——同一段文字也不能混著比"
        );
    }

    #[tokio::test]
    async fn e5_query_and_passage_produce_different_vectors_minilm_does_not() {
        let p = MockEmbeddingProvider::new();
        let text = "Microsoft 是一家科技公司";

        let q = p
            .embed(&req(text, EmbeddingKind::Query, Some("zh")))
            .await
            .unwrap();
        let d = p
            .embed(&req(text, EmbeddingKind::Passage, Some("zh")))
            .await
            .unwrap();
        assert_eq!(q.content_hash, d.content_hash, "hash 打在原文，不含前綴");
        assert_eq!(q.content_hash, embedding_content_hash(text));
        assert_ne!(
            q.vector, d.vector,
            "e5 的 query:/passage: 前綴必須讓兩端向量不同"
        );

        let eq = p
            .embed(&req(text, EmbeddingKind::Query, Some("en")))
            .await
            .unwrap();
        let ed = p
            .embed(&req(text, EmbeddingKind::Passage, Some("en")))
            .await
            .unwrap();
        assert_eq!(
            eq.vector, ed.vector,
            "MiniLM 不加前綴，Query 與 Passage 必須得到同一條向量"
        );
    }

    #[tokio::test]
    async fn dimensions_come_from_the_provider_not_a_constant() {
        let p = MockEmbeddingProvider::with_dimensions(768);
        assert_eq!(p.dimensions(), 768);
        assert_eq!(p.dimensions_for(Some("zh")), 768);
        let v = p
            .embed(&req("x", EmbeddingKind::Passage, Some("en")))
            .await
            .unwrap();
        assert_eq!(v.dimensions, 768);
        assert_eq!(v.vector.len(), 768);
        assert_eq!(p.model_for(Some("en")).dimensions, 768);
    }

    #[tokio::test]
    async fn unsupported_mock_fails_embed_with_capability_error() {
        let p = MockEmbeddingProvider::unsupported();
        let err = p
            .embed(&req("x", EmbeddingKind::Query, Some("en")))
            .await
            .unwrap_err();
        match err {
            StorageError::UnsupportedCapability {
                backend,
                capability,
            } => {
                assert_eq!(backend, "mock-embedding");
                assert_eq!(capability, "embedding");
            }
            other => panic!("預期 UnsupportedCapability，得到 {other}"),
        }
        // 維度與路由仍可問——呼叫端在決定要不要走語意路徑之前就需要這些。
        assert_eq!(p.dimensions(), MOCK_DEFAULT_DIM);
        assert_eq!(p.model_for(Some("zh")).model, MOCK_E5_MODEL);
    }

    #[tokio::test]
    async fn batch_preserves_order_and_per_item_routing() {
        let p = MockEmbeddingProvider::new();
        let out = p
            .embed_batch(&[
                req("a", EmbeddingKind::Query, Some("en")),
                req("a", EmbeddingKind::Query, Some("zh")),
            ])
            .await
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].model, MOCK_MINILM_MODEL);
        assert_eq!(out[1].model, MOCK_E5_MODEL);
    }

    #[tokio::test]
    async fn content_hash_is_stable_and_ignores_kind() {
        assert_eq!(
            embedding_content_hash("abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let p = MockEmbeddingProvider::new();
        let a = p
            .embed(&req("abc", EmbeddingKind::Query, Some("zh")))
            .await
            .unwrap();
        let b = p
            .embed(&req("abc", EmbeddingKind::Passage, Some("en")))
            .await
            .unwrap();
        assert_eq!(a.content_hash, b.content_hash);
        assert_ne!(a.vector, b.vector);
    }

    #[tokio::test]
    async fn search_index_overwrites_whole_source() {
        let s = MockSearchStore::new();
        s.index(SearchDocument {
            index: "osint-documents".into(),
            id: "d1".into(),
            body: json!({"title": "old", "keep": 1}),
        })
        .await
        .unwrap();
        s.index(SearchDocument {
            index: "osint-documents".into(),
            id: "d1".into(),
            body: json!({"title": "new"}),
        })
        .await
        .unwrap();
        let got = s.get("osint-documents", "d1").unwrap().unwrap();
        assert_eq!(got["title"], "new");
        assert!(got.get("keep").is_none(), "index() 必須整份覆寫，不能合併");
    }

    #[tokio::test]
    async fn update_fields_merges_and_missing_is_not_found() {
        let s = MockSearchStore::new();
        s.index(SearchDocument {
            index: "osint-documents".into(),
            id: "d1".into(),
            body: json!({"title": "keep", "body": "x"}),
        })
        .await
        .unwrap();
        s.update_fields(
            "osint-documents",
            "d1",
            json!({"embedding_en": [0.1, 0.2], "embedding_en_model_version": "v"}),
        )
        .await
        .unwrap();
        let got = s.get("osint-documents", "d1").unwrap().unwrap();
        assert_eq!(got["title"], "keep");
        assert_eq!(got["embedding_en_model_version"], "v");

        let err = s
            .update_fields("osint-documents", "missing", json!({"a": 1}))
            .await
            .unwrap_err();
        assert!(
            matches!(err, StorageError::NotFound { .. }),
            "缺文件必須 NotFound，不可 upsert：{err}"
        );
    }

    #[tokio::test]
    async fn bulk_upsert_fields_merges_and_creates() {
        let s = MockSearchStore::new();
        s.index(SearchDocument {
            index: "osint-documents".into(),
            id: "d1".into(),
            body: json!({"title": "keep", "overlay": "vector"}),
        })
        .await
        .unwrap();
        let result = s
            .bulk_upsert_fields(vec![
                SearchDocument {
                    index: "osint-documents".into(),
                    id: "d1".into(),
                    body: json!({"title": "new", "body": "x"}),
                },
                SearchDocument {
                    index: "osint-documents".into(),
                    id: "d2".into(),
                    body: json!({"title": "created"}),
                },
            ])
            .await
            .unwrap();
        assert_eq!(result.indexed, 2);
        let d1 = s.get("osint-documents", "d1").unwrap().unwrap();
        assert_eq!(d1["title"], "new");
        assert_eq!(d1["body"], "x");
        assert_eq!(d1["overlay"], "vector", "呼叫端沒寫進 body 的欄位必須留下");
        let d2 = s.get("osint-documents", "d2").unwrap().unwrap();
        assert_eq!(d2["title"], "created");
    }

    #[tokio::test]
    async fn delayed_not_found_then_succeeds() {
        let s = MockSearchStore::new();
        s.index(SearchDocument {
            index: "osint-documents".into(),
            id: "d1".into(),
            body: json!({"title": "t"}),
        })
        .await
        .unwrap();
        s.set_update_not_found_remaining("osint-documents", "d1", 2)
            .unwrap();
        for i in 0..2 {
            let err = s
                .update_fields("osint-documents", "d1", json!({"embedding_en": [1.0]}))
                .await
                .unwrap_err();
            assert!(
                matches!(err, StorageError::NotFound { .. }),
                "第 {i} 次應為延遲 NotFound：{err}"
            );
        }
        s.update_fields("osint-documents", "d1", json!({"embedding_en": [1.0]}))
            .await
            .unwrap();
        let got = s.get("osint-documents", "d1").unwrap().unwrap();
        assert_eq!(got["title"], "t");
        assert_eq!(got["embedding_en"][0], 1.0);
    }

    #[tokio::test]
    async fn vector_search_ranks_by_cosine() {
        let s = MockSearchStore::new();
        s.index(SearchDocument {
            index: "osint-entities".into(),
            id: "near".into(),
            body: json!({"name": "near", "vec": [1.0, 0.0, 0.0]}),
        })
        .await
        .unwrap();
        s.index(SearchDocument {
            index: "osint-entities".into(),
            id: "far".into(),
            body: json!({"name": "far", "vec": [0.0, 1.0, 0.0]}),
        })
        .await
        .unwrap();
        let hits = s
            .vector_search(VectorSearch {
                index: "osint-entities".into(),
                field: "vec".into(),
                vector: vec![1.0, 0.0, 0.0],
                k: 1,
                filters: vec![],
            })
            .await
            .unwrap();
        assert_eq!(hits.hits.len(), 1);
        assert_eq!(hits.hits[0].id, "near");
    }
}
