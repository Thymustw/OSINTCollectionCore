//! Neo4j GraphStore + ProjectionStore adapter。
//!
//! 連線字串（`bolt_uri`／帳密）由呼叫端注入，**不**在這個 crate 讀 env，
//! 也不寫死本機 `bolt://127.0.0.1:7687`。
//!
//! # Cypher schema 決策
//!
//! - 節點：共用 `:Entity` + 具體型別 label（`person` → `:Person`，見 [`snake_to_pascal`]）。
//!   `entity_id` 存 hyphenated UUID 字串，unique constraint 掛在 `:Entity`。
//! - 邊：寫入永遠用 `GraphEdge.source → GraphEdge.target` 的方向；
//!   查詢用無方向的 `-[r]-`，語意對齊 [`storage_core::mock::MockGraphStore`]。
//!   Neo4j 底層關聯有方向，這是「寫入確定、讀取當無向」的取捨。
//! - `attributes`：Neo4j 沒有原生 JSON 型別，整包 `serde_json::to_string` 存成
//!   `attributes_json` 字串。目前多數呼叫端是 `{}`，不拆成原生 property。
//! - 時間：`first_seen`／`last_seen`／投影狀態時間戳一律 RFC 3339 `Z` 字串。
//!   字串比較在固定 `Z` 下等同時間序；避開 neo4rs 0.8 對 `DateTime<Utc>` 沒有
//!   `From`（只接 `DateTime<FixedOffset>`）的轉換坑。
//! - 動態 label：Neo4j 5.26 支援 `n:$($label)`／`SET n:$($label)`／`REMOVE n:$($old)`
//!   （與關聯型別 `$($relType)` 同一組擴充）。本機 Community 無 APOC，不走
//!   `apoc.create.addLabels`。label 字串經過 [`sanitize_label`] 才進 query。
//! - relationship unique constraint 只對 [`KNOWN_RELATIONSHIP_TYPES`] 17 種建；
//!   執行期未知型別靠 `MERGE (s)-[r:$($relType)]->(t)` 保證同一對同一型別只有一條。
//!
//! # ProjectionStore
//!
//! 狀態存在獨立的 `:ProjectionState` 節點，**不**掛在 `:Entity` 上。
//! graph-worker 的 `--rebuild --drop` 清空圖時必須排除這個 label
//! （那是 worker 的責任）；這個 adapter 的 ProjectionStore 方法只操作
//! `:ProjectionState`，不會碰 `:Entity`。

mod error;
mod labels;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use core_model::{EntityId, RelationshipId};
use neo4rs::{Graph, Node, Path, Relation, Row, query};
use serde_json::Value;
use storage_core::{
    CapabilityDescriptor, GraphEdge, GraphNode, GraphPath, GraphPattern, GraphQuery, GraphStore,
    GraphTraversalOptions, HealthProvider, ProjectionCheckpoint, ProjectionLag, ProjectionStore,
    RebuildState, RebuildStatus, StorageAdapter, StorageError, StorageHealth,
};
use uuid::Uuid;

pub use labels::{KNOWN_RELATIONSHIP_TYPES, snake_to_pascal, to_rel_type};

use error::map_neo;
use labels::{relationship_type_cypher_name, sanitize_label, sanitize_rel_type};

/// 遍歷硬上限。trait 規定 adapter 必須自己夾；無界 `[*]` 會掃完整張圖。
pub const GRAPH_MAX_HOPS: u32 = 10;

const BACKEND: &str = "neo4j";

/// Neo4j 圖投影。
#[derive(Clone)]
pub struct Neo4jStore {
    graph: Graph,
}

impl Neo4jStore {
    /// 建立連線池並確保 schema constraint。
    ///
    /// `pool_max` 不可為 0。不會讀 env——URI／帳密由呼叫端從設定注入。
    pub async fn connect(
        bolt_uri: &str,
        username: &str,
        password: &str,
        pool_max: usize,
    ) -> Result<Self, StorageError> {
        if pool_max == 0 {
            return Err(StorageError::Configuration {
                message: "storage.graph.pool_max 不可為 0".into(),
            });
        }
        if bolt_uri.is_empty() {
            return Err(StorageError::Configuration {
                message: "Neo4j bolt URI 是空的。請由呼叫端傳入，例如 bolt://127.0.0.1:7687".into(),
            });
        }
        let config = neo4rs::ConfigBuilder::default()
            .uri(bolt_uri)
            .user(username)
            .password(password)
            .max_connections(pool_max)
            .build()
            .map_err(map_neo)?;
        let graph = Graph::connect(config).await.map_err(map_neo)?;
        let store = Self { graph };
        store.ensure_constraints().await?;
        tracing::debug!(
            uri = %StorageError::sanitize(bolt_uri),
            pool_max,
            "Neo4j adapter 已連線並確認 constraint"
        );
        Ok(store)
    }

    /// `CREATE CONSTRAINT IF NOT EXISTS`：`:Entity.entity_id` 與 17 種已知
    /// 關聯型別的 `relationship_id` uniqueness。重複呼叫安全。
    pub async fn ensure_constraints(&self) -> Result<(), StorageError> {
        self.run(
            "CREATE CONSTRAINT entity_id_unique IF NOT EXISTS \
             FOR (n:Entity) REQUIRE n.entity_id IS UNIQUE",
        )
        .await?;
        // 走 exhaustive match，RelationshipType 新增變體時這裡編譯失敗。
        use core_model::RelationshipType::*;
        let known = [
            Mentions,
            References,
            PublishedBy,
            AuthoredBy,
            LinksTo,
            Affects,
            BelongsTo,
            MemberOf,
            Owns,
            Uses,
            LocatedAt,
            AssociatedWith,
            DerivedFrom,
            Indicates,
            AttributedTo,
            Targets,
            Mitigates,
        ]
        .map(relationship_type_cypher_name);
        for rel in known {
            // constraint 名稱含型別，避免 17 條撞名。
            let cypher = format!(
                "CREATE CONSTRAINT rel_id_unique_{rel} IF NOT EXISTS \
                 FOR ()-[r:{rel}]-() REQUIRE r.relationship_id IS UNIQUE"
            );
            self.run(&cypher).await?;
        }
        Ok(())
    }

    async fn run(&self, cypher: &str) -> Result<(), StorageError> {
        self.graph.run(query(cypher)).await.map_err(map_neo)
    }

    async fn execute_one(&self, q: neo4rs::Query) -> Result<Option<Row>, StorageError> {
        let mut stream = self.graph.execute(q).await.map_err(map_neo)?;
        stream.next().await.map_err(map_neo)
    }

    async fn execute_all(&self, q: neo4rs::Query) -> Result<Vec<Row>, StorageError> {
        let mut stream = self.graph.execute(q).await.map_err(map_neo)?;
        let mut rows = Vec::new();
        while let Some(row) = stream.next().await.map_err(map_neo)? {
            rows.push(row);
        }
        Ok(rows)
    }

    /// 測試結尾清掉這次建立的節點與邊。不刪 constraint、不刪 `:ProjectionState`。
    pub async fn delete_entities(&self, ids: &[Uuid]) -> Result<(), StorageError> {
        if ids.is_empty() {
            return Ok(());
        }
        let raw: Vec<String> = ids.iter().map(Uuid::to_string).collect();
        self.graph
            .run(
                query("MATCH (n:Entity) WHERE n.entity_id IN $ids DETACH DELETE n")
                    .param("ids", raw),
            )
            .await
            .map_err(map_neo)
    }
}

fn rfc3339(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn parse_rfc3339(raw: &str, field: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| StorageError::CorruptionSuspected {
            message: format!(
                "Neo4j 欄位 `{field}` 的值 `{raw}` 不是 RFC 3339：{err}。請檢查寫入端是否用同一套編碼"
            ),
        })
}

fn node_from_bolt(node: &Node) -> Result<GraphNode, StorageError> {
    let entity_id_raw: String =
        node.get("entity_id")
            .map_err(|err| StorageError::CorruptionSuspected {
                message: format!(":Entity 缺少 entity_id：{err}"),
            })?;
    let entity_id =
        Uuid::parse_str(&entity_id_raw).map_err(|err| StorageError::CorruptionSuspected {
            message: format!("entity_id `{entity_id_raw}` 不是 UUID：{err}"),
        })?;
    let entity_type: String = node.get("entity_type").unwrap_or_default();
    let display_name: String = node.get("display_name").unwrap_or_default();
    let attributes = match node.get::<String>("attributes_json") {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or(Value::Object(Default::default())),
        Err(_) => Value::Object(Default::default()),
    };
    Ok(GraphNode {
        entity_id,
        entity_type,
        display_name,
        attributes,
    })
}

fn edge_from_relation(
    rel: &Relation,
    source: Uuid,
    target: Uuid,
) -> Result<GraphEdge, StorageError> {
    edge_from_props(
        rel.get::<String>("relationship_id").ok(),
        rel.get::<String>("relationship_type").ok(),
        Some(rel.typ().to_string()),
        rel.get::<f64>("confidence").ok(),
        rel.get::<String>("first_seen").ok(),
        rel.get::<String>("last_seen").ok(),
        source,
        target,
    )
}

fn edge_from_unbounded(
    rel: &neo4rs::UnboundedRelation,
    source: Uuid,
    target: Uuid,
) -> Result<GraphEdge, StorageError> {
    edge_from_props(
        rel.get::<String>("relationship_id").ok(),
        rel.get::<String>("relationship_type").ok(),
        Some(rel.typ().to_string()),
        rel.get::<f64>("confidence").ok(),
        rel.get::<String>("first_seen").ok(),
        rel.get::<String>("last_seen").ok(),
        source,
        target,
    )
}

#[allow(clippy::too_many_arguments)]
fn edge_from_props(
    relationship_id: Option<String>,
    relationship_type_prop: Option<String>,
    typ_fallback: Option<String>,
    confidence: Option<f64>,
    first_seen: Option<String>,
    last_seen: Option<String>,
    source: Uuid,
    target: Uuid,
) -> Result<GraphEdge, StorageError> {
    let rid_raw = relationship_id.ok_or_else(|| StorageError::CorruptionSuspected {
        message: "邊缺少 relationship_id".into(),
    })?;
    let relationship_id =
        Uuid::parse_str(&rid_raw).map_err(|err| StorageError::CorruptionSuspected {
            message: format!("relationship_id `{rid_raw}` 不是 UUID：{err}"),
        })?;
    let relationship_type = relationship_type_prop
        .or(typ_fallback)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let first_seen = parse_rfc3339(&first_seen.unwrap_or_default(), "first_seen")?;
    let last_seen = parse_rfc3339(&last_seen.unwrap_or_default(), "last_seen")?;
    Ok(GraphEdge {
        relationship_id,
        source,
        target,
        relationship_type,
        confidence: confidence.unwrap_or(0.0),
        first_seen,
        last_seen,
    })
}

fn path_from_bolt(path: &Path) -> Result<GraphPath, StorageError> {
    let nodes: Result<Vec<GraphNode>, _> = path.nodes().iter().map(node_from_bolt).collect();
    let nodes = nodes?;
    let rels = path.rels();
    let mut edges = Vec::with_capacity(rels.len());
    for (i, rel) in rels.iter().enumerate() {
        let source =
            nodes
                .get(i)
                .map(|n| n.entity_id)
                .ok_or_else(|| StorageError::CorruptionSuspected {
                    message: "Path 的邊比節點多，對不上 nodes[i] --edges[i]--> nodes[i+1]".into(),
                })?;
        let target = nodes.get(i + 1).map(|n| n.entity_id).ok_or_else(|| {
            StorageError::CorruptionSuspected {
                message: "Path 的邊比節點多，對不上 nodes[i] --edges[i]--> nodes[i+1]".into(),
            }
        })?;
        edges.push(edge_from_unbounded(rel, source, target)?);
    }
    Ok(GraphPath { nodes, edges })
}

fn clamp_hops(max_hops: u32) -> Result<u32, StorageError> {
    if max_hops > GRAPH_MAX_HOPS {
        return Err(StorageError::ConstraintViolation {
            message: format!(
                "max_hops={max_hops} 超過 Neo4j adapter 上限 {GRAPH_MAX_HOPS}。\
                 無界遍歷會掃完整張圖，請把跳數調小"
            ),
        });
    }
    Ok(max_hops)
}

/// 動態組遍歷的邊過濾（關係型別、信心、時間重疊）。
struct EdgeFilters {
    rel_types: Option<Vec<String>>,
    min_confidence: Option<f64>,
    time_from: Option<String>,
    time_to: Option<String>,
}

impl EdgeFilters {
    fn from_options(options: &GraphTraversalOptions) -> Result<Self, StorageError> {
        let rel_types = match &options.relationship_types {
            None => None,
            Some(types) => {
                let mut out = Vec::with_capacity(types.len());
                for raw in types {
                    let upper = to_rel_type(raw);
                    sanitize_rel_type(&upper)?;
                    out.push(upper);
                }
                Some(out)
            }
        };
        let (time_from, time_to) = match options.time_range {
            Some((from, to)) => (Some(rfc3339(from)), Some(rfc3339(to))),
            None => (None, None),
        };
        Ok(Self {
            rel_types,
            min_confidence: options.min_confidence,
            time_from,
            time_to,
        })
    }

    /// 變長路徑永遠用 `[*1..N]`。
    ///
    /// Neo4j 5.26 的 `[:$any($relTypes)*1..N]` 在**變長**時不過濾型別
    /// （單跳 `[:$any($relTypes)]` 正常；`*1..1` 卻把其他型別也掃進來）。
    /// 型別改放進 [`Self::where_all_rels`]。
    fn hop_pattern(hops: u32) -> String {
        format!("[*1..{hops}]")
    }

    fn apply(&self, mut q: neo4rs::Query) -> neo4rs::Query {
        if let Some(rel) = &self.rel_types {
            q = q.param("relTypes", rel.clone());
        }
        q
    }

    /// `WHERE ALL(r IN relationships(path) WHERE ...)` 片段。
    fn where_all_rels(&self) -> String {
        let mut parts: Vec<String> = Vec::new();
        if self.rel_types.is_some() {
            parts.push("type(r) IN $relTypes".into());
        }
        if self.min_confidence.is_some() {
            parts.push("r.confidence >= $minC".into());
        }
        if self.time_from.is_some() {
            parts.push("r.first_seen <= $timeTo AND r.last_seen >= $timeFrom".into());
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!(
                " WHERE ALL(r IN relationships(path) WHERE {})",
                parts.join(" AND ")
            )
        }
    }

    fn bind_filters(&self, mut q: neo4rs::Query) -> neo4rs::Query {
        q = self.apply(q);
        if let Some(min) = self.min_confidence {
            q = q.param("minC", min);
        }
        if let (Some(from), Some(to)) = (&self.time_from, &self.time_to) {
            q = q
                .param("timeFrom", from.clone())
                .param("timeTo", to.clone());
        }
        q
    }
}

fn entity_type_where(options: &GraphTraversalOptions, alias: &str) -> String {
    if options.entity_types.is_some() {
        format!(" AND {alias}.entity_type IN $entityTypes")
    } else {
        String::new()
    }
}

fn bind_entity_types(options: &GraphTraversalOptions, mut q: neo4rs::Query) -> neo4rs::Query {
    if let Some(types) = &options.entity_types {
        q = q.param("entityTypes", types.clone());
    }
    q
}

#[async_trait]
impl HealthProvider for Neo4jStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let row = self
            .execute_one(query("RETURN 1 AS n"))
            .await?
            .ok_or_else(|| StorageError::Unavailable {
                backend: BACKEND,
                message: "RETURN 1 沒有回列".into(),
            })?;
        let n: i64 = row.get("n").unwrap_or(0);
        if n != 1 {
            return Ok(StorageHealth::down(BACKEND, "RETURN 1 沒有回 1"));
        }
        Ok(StorageHealth::ok(BACKEND, "RETURN 1 成功"))
    }
}

impl StorageAdapter for Neo4jStore {
    fn descriptor(&self) -> CapabilityDescriptor {
        CapabilityDescriptor::new(BACKEND, env!("CARGO_PKG_VERSION"), &["graph", "projection"])
            .with_feature("max_hops", serde_json::json!(GRAPH_MAX_HOPS))
    }
}

#[async_trait]
impl GraphStore for Neo4jStore {
    async fn upsert_node(&self, node: &GraphNode) -> Result<(), StorageError> {
        let label = snake_to_pascal(&node.entity_type);
        sanitize_label(&label)?;
        let attributes_json =
            serde_json::to_string(&node.attributes).unwrap_or_else(|_| "{}".into());
        let q = query(
            "MERGE (n:Entity {entity_id: $id}) \
             SET n.entity_type = $etype, n.display_name = $name, n.attributes_json = $attrs \
             SET n:$($label) \
             RETURN labels(n) AS labels",
        )
        .param("id", node.entity_id.to_string())
        .param("etype", node.entity_type.clone())
        .param("name", node.display_name.clone())
        .param("attrs", attributes_json)
        .param("label", label.clone());
        let row = self
            .execute_one(q)
            .await?
            .ok_or_else(|| StorageError::Unknown {
                backend: BACKEND,
                message: "upsert_node 沒有回 labels".into(),
            })?;
        let labels: Vec<String> = row.get("labels").unwrap_or_default();
        for extra in labels {
            if extra != "Entity" && extra != label {
                // extra 來自 Neo4j 自己存的既有 label，不是使用者輸入；
                // 仍走 sanitize，擋掉理論上不該出現的值。
                sanitize_label(&extra)?;
                self.graph
                    .run(
                        query("MATCH (n:Entity {entity_id: $id}) REMOVE n:$($old)")
                            .param("id", node.entity_id.to_string())
                            .param("old", extra),
                    )
                    .await
                    .map_err(map_neo)?;
            }
        }
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
        let rel_type = to_rel_type(&edge.relationship_type);
        sanitize_rel_type(&rel_type)?;
        let q = query(
            "MATCH (s:Entity {entity_id: $src}), (t:Entity {entity_id: $tgt}) \
             MERGE (s)-[r:$($relType)]->(t) \
             ON CREATE SET r.relationship_id = $rid, r.relationship_type = $rtype, \
                           r.confidence = $c, r.first_seen = $fs, r.last_seen = $ls \
             ON MATCH SET r.confidence = $c, r.last_seen = $ls, r.relationship_type = $rtype \
             RETURN r",
        )
        .param("src", edge.source.to_string())
        .param("tgt", edge.target.to_string())
        .param("relType", rel_type)
        .param("rid", edge.relationship_id.to_string())
        .param("rtype", edge.relationship_type.clone())
        .param("c", edge.confidence)
        .param("fs", rfc3339(edge.first_seen))
        .param("ls", rfc3339(edge.last_seen));
        let row = self.execute_one(q).await?;
        if row.is_none() {
            return Err(StorageError::NotFound {
                message: format!(
                    "upsert_edge 找不到端點節點（source={} target={}）。請先 upsert_node",
                    edge.source, edge.target
                ),
            });
        }
        Ok(())
    }

    async fn delete_node(&self, entity_id: &EntityId) -> Result<(), StorageError> {
        self.graph
            .run(
                query("MATCH (n:Entity {entity_id: $id}) DETACH DELETE n")
                    .param("id", entity_id.to_string()),
            )
            .await
            .map_err(map_neo)
    }

    async fn delete_edge(&self, relationship_id: &RelationshipId) -> Result<(), StorageError> {
        self.graph
            .run(
                query("MATCH ()-[r {relationship_id: $rid}]-() DELETE r")
                    .param("rid", relationship_id.to_string()),
            )
            .await
            .map_err(map_neo)
    }

    async fn neighbors(
        &self,
        entity_id: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphNode>, StorageError> {
        let hops = clamp_hops(options.max_hops)?;
        if hops == 0 {
            return Ok(Vec::new());
        }
        let filters = EdgeFilters::from_options(options)?;
        let pattern = EdgeFilters::hop_pattern(hops);
        let extra = filters.where_all_rels();
        let type_filter = entity_type_where(options, "n");
        let cypher = format!(
            "MATCH (s:Entity {{entity_id: $id}}) \
             MATCH path = (s)-{pattern}-(n:Entity) \
             {extra} \
             WITH DISTINCT n \
             WHERE n.entity_id <> $id {type_filter} \
             RETURN n \
             ORDER BY n.entity_id"
        );
        // WHERE ALL 前面可能是空字串，Cypher 需要 WHERE 或什麼都沒有。
        // extra 已含前導 WHERE ALL...；若空則 MATCH 後直接 WITH。
        let cypher = if extra.is_empty() {
            format!(
                "MATCH (s:Entity {{entity_id: $id}}) \
                 MATCH path = (s)-{pattern}-(n:Entity) \
                 WITH DISTINCT n \
                 WHERE n.entity_id <> $id {type_filter} \
                 RETURN n \
                 ORDER BY n.entity_id"
            )
        } else {
            cypher
        };
        let mut q = query(&cypher).param("id", entity_id.to_string());
        q = filters.bind_filters(q);
        q = bind_entity_types(options, q);
        let rows = self.execute_all(q).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let node: Node = row
                .get("n")
                .map_err(|err| StorageError::CorruptionSuspected {
                    message: format!("neighbors 解 Node 失敗：{err}"),
                })?;
            out.push(node_from_bolt(&node)?);
        }
        Ok(out)
    }

    async fn relationships(
        &self,
        entity_id: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphEdge>, StorageError> {
        let hops = clamp_hops(options.max_hops)?;
        if hops == 0 {
            return Ok(Vec::new());
        }
        let filters = EdgeFilters::from_options(options)?;
        let pattern = EdgeFilters::hop_pattern(hops);
        let extra = filters.where_all_rels();
        let type_filter = entity_type_where(options, "other");
        // 取路徑上每一條邊；端點 entity_type 過濾套在「離 start 較遠」那一端
        // 對一跳就是鄰居，對多跳則是路徑上非 start 的任一端（與 mock 的
        // other_end_type_matches 對一跳一致；多跳時 mock 是「兩端任一符合」，
        // 這裡用「路徑上出現過符合的節點」近似，conformance 只驗一跳過濾）。
        let cypher = if extra.is_empty() {
            format!(
                "MATCH (s:Entity {{entity_id: $id}}) \
                 MATCH path = (s)-{pattern}-(other:Entity) \
                 WITH s, relationships(path) AS rels, nodes(path) AS ns \
                 UNWIND rels AS r \
                 WITH DISTINCT r, startNode(r) AS a, endNode(r) AS b, s \
                 WITH r, a, b, CASE WHEN a.entity_id = s.entity_id THEN b ELSE a END AS other \
                 WHERE true {type_filter} \
                 RETURN r, a, b \
                 ORDER BY r.relationship_id"
            )
        } else {
            format!(
                "MATCH (s:Entity {{entity_id: $id}}) \
                 MATCH path = (s)-{pattern}-(other:Entity) \
                 {extra} \
                 WITH s, relationships(path) AS rels \
                 UNWIND rels AS r \
                 WITH DISTINCT r, startNode(r) AS a, endNode(r) AS b, s \
                 WITH r, a, b, CASE WHEN a.entity_id = s.entity_id THEN b ELSE a END AS other \
                 WHERE true {type_filter} \
                 RETURN r, a, b \
                 ORDER BY r.relationship_id"
            )
        };
        let mut q = query(&cypher).param("id", entity_id.to_string());
        q = filters.bind_filters(q);
        q = bind_entity_types(options, q);
        let rows = self.execute_all(q).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let rel: Relation = row
                .get("r")
                .map_err(|err| StorageError::CorruptionSuspected {
                    message: format!("relationships 解 Relation 失敗：{err}"),
                })?;
            let a: Node = row
                .get("a")
                .map_err(|err| StorageError::CorruptionSuspected {
                    message: format!("relationships 解 startNode 失敗：{err}"),
                })?;
            let b: Node = row
                .get("b")
                .map_err(|err| StorageError::CorruptionSuspected {
                    message: format!("relationships 解 endNode 失敗：{err}"),
                })?;
            let source = node_from_bolt(&a)?.entity_id;
            let target = node_from_bolt(&b)?.entity_id;
            out.push(edge_from_relation(&rel, source, target)?);
        }
        Ok(out)
    }

    async fn shortest_path(
        &self,
        from: &EntityId,
        to: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Option<GraphPath>, StorageError> {
        let hops = clamp_hops(options.max_hops)?;
        if from == to {
            // 自己到自己：不走邊。節點不存在則 None。
            let row = self
                .execute_one(
                    query("MATCH (n:Entity {entity_id: $id}) RETURN n")
                        .param("id", from.to_string()),
                )
                .await?;
            return match row {
                None => Ok(None),
                Some(row) => {
                    let node: Node =
                        row.get("n")
                            .map_err(|err| StorageError::CorruptionSuspected {
                                message: format!("shortest_path 解 Node 失敗：{err}"),
                            })?;
                    Ok(Some(GraphPath {
                        nodes: vec![node_from_bolt(&node)?],
                        edges: vec![],
                    }))
                }
            };
        }
        if hops == 0 {
            return Ok(None);
        }
        let filters = EdgeFilters::from_options(options)?;
        let extra = filters.where_all_rels();
        let type_filter = match &options.entity_types {
            None => String::new(),
            Some(_) => {
                // 終點必須符合 entity_types；沿途不過濾（與 neighbors 同一語意）。
                " WHERE endNode.entity_type IN $entityTypes".into()
            }
        };
        // shortestPath 的 pattern 用 `[*1..N]` + WHERE ALL（含關係型別）。
        // 變長 `$any()` 在 Neo4j 5.26 不過濾型別，見 EdgeFilters::hop_pattern。
        let cypher = format!(
            "MATCH (a:Entity {{entity_id: $from}}), (b:Entity {{entity_id: $to}}) \
             MATCH path = shortestPath((a)-[*1..{hops}]-(b)) \
             {extra} \
             WITH path, nodes(path)[-1] AS endNode \
             {type_filter} \
             RETURN path"
        );
        // 上面 type_filter 在 extra 為空且 entity_types 為 None 時是空的 WITH...RETURN，
        // `WITH path, nodes(path)[-1] AS endNode RETURN path` 仍合法。
        let mut q = query(&cypher)
            .param("from", from.to_string())
            .param("to", to.to_string());
        q = filters.bind_filters(q);
        q = bind_entity_types(options, q);
        let Some(row) = self.execute_one(q).await? else {
            return Ok(None);
        };
        // 找不到路徑時有的 Cypher 形狀會回一列 null，不是零列。
        let Ok(path) = row.get::<Path>("path") else {
            return Ok(None);
        };
        Ok(Some(path_from_bolt(&path)?))
    }

    async fn wipe(&self) -> Result<(), StorageError> {
        // 只刪 `:Entity`。`:ProjectionState` 是 ProjectionStore 的事，
        // `--rebuild --drop` 會另外呼叫 `reset_projection`。
        self.run("MATCH (n:Entity) DETACH DELETE n").await
    }

    async fn query(&self, query_req: &GraphQuery) -> Result<Vec<GraphPath>, StorageError> {
        if query_req.starts.is_empty() {
            return Err(StorageError::ConstraintViolation {
                message: "GraphQuery.starts 是空的。圖查詢必須指定起點，\
                          否則等於掃完整張圖——請至少給一個 entity id"
                    .into(),
            });
        }
        clamp_hops(query_req.options.max_hops)?;
        let mut paths = Vec::new();
        match &query_req.pattern {
            GraphPattern::Neighbors => {
                for start in &query_req.starts {
                    for node in self.neighbors(start, &query_req.options).await? {
                        if let Some(path) = self
                            .shortest_path(start, &node.entity_id, &query_req.options)
                            .await?
                        {
                            paths.push(path);
                        }
                    }
                }
            }
            GraphPattern::Relationships => {
                for start in &query_req.starts {
                    for edge in self.relationships(start, &query_req.options).await? {
                        let a = self.get_node(edge.source).await?;
                        let b = self.get_node(edge.target).await?;
                        let (Some(a), Some(b)) = (a, b) else {
                            continue;
                        };
                        let (nodes, edges) = if edge.source == *start {
                            (vec![a, b], vec![edge])
                        } else if edge.target == *start {
                            (vec![b, a], vec![edge])
                        } else {
                            (vec![a, b], vec![edge])
                        };
                        paths.push(GraphPath { nodes, edges });
                    }
                }
            }
            GraphPattern::ShortestPath { to } => {
                for start in &query_req.starts {
                    if let Some(path) = self.shortest_path(start, to, &query_req.options).await? {
                        paths.push(path);
                    }
                }
            }
            GraphPattern::BoundedWalk { end } => {
                for start in &query_req.starts {
                    paths.extend(
                        self.bounded_walk(start, end.as_deref(), &query_req.options)
                            .await?,
                    );
                }
            }
        }
        Ok(paths)
    }
}

impl Neo4jStore {
    async fn get_node(&self, id: Uuid) -> Result<Option<GraphNode>, StorageError> {
        let row = self
            .execute_one(
                query("MATCH (n:Entity {entity_id: $id}) RETURN n").param("id", id.to_string()),
            )
            .await?;
        match row {
            None => Ok(None),
            Some(row) => {
                let node: Node = row
                    .get("n")
                    .map_err(|err| StorageError::CorruptionSuspected {
                        message: format!("get_node 解 Node 失敗：{err}"),
                    })?;
                Ok(Some(node_from_bolt(&node)?))
            }
        }
    }

    async fn bounded_walk(
        &self,
        start: &Uuid,
        end: Option<&[Uuid]>,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphPath>, StorageError> {
        let hops = clamp_hops(options.max_hops)?;
        if hops == 0 {
            return Ok(Vec::new());
        }
        let filters = EdgeFilters::from_options(options)?;
        let extra = filters.where_all_rels();
        let type_filter = entity_type_where(options, "endNode");
        let end_filter = if end.is_some() {
            " AND endNode.entity_id IN $endIds"
        } else {
            ""
        };
        let cypher = if extra.is_empty() {
            format!(
                "MATCH (s:Entity {{entity_id: $id}}) \
                 MATCH path = (s)-[*1..{hops}]-(endNode:Entity) \
                 WHERE length(path) >= 1 {type_filter}{end_filter} \
                 RETURN path \
                 LIMIT 256"
            )
        } else {
            format!(
                "MATCH (s:Entity {{entity_id: $id}}) \
                 MATCH path = (s)-[*1..{hops}]-(endNode:Entity) \
                 {extra} \
                 WITH path, nodes(path)[-1] AS endNode \
                 WHERE length(path) >= 1 {type_filter}{end_filter} \
                 RETURN path \
                 LIMIT 256"
            )
        };
        let mut q = query(&cypher).param("id", start.to_string());
        q = filters.bind_filters(q);
        q = bind_entity_types(options, q);
        if let Some(ids) = end {
            let raw: Vec<String> = ids.iter().map(Uuid::to_string).collect();
            q = q.param("endIds", raw);
        }
        let rows = self.execute_all(q).await?;
        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let path: Path = row
                .get("path")
                .map_err(|err| StorageError::CorruptionSuspected {
                    message: format!("bounded_walk 解 Path 失敗：{err}"),
                })?;
            out.push(path_from_bolt(&path)?);
        }
        Ok(out)
    }
}

#[async_trait]
impl ProjectionStore for Neo4jStore {
    async fn checkpoint(
        &self,
        projection: &str,
    ) -> Result<Option<ProjectionCheckpoint>, StorageError> {
        let row = self
            .execute_one(
                query("MATCH (s:ProjectionState {projection: $name}) RETURN s")
                    .param("name", projection),
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let node: Node = row
            .get("s")
            .map_err(|err| StorageError::CorruptionSuspected {
                message: format!("checkpoint 解 ProjectionState 失敗：{err}"),
            })?;
        let Some(updated_at_raw) = node.get::<String>("checkpoint_updated_at").ok() else {
            return Ok(None);
        };
        let updated_at = parse_rfc3339(&updated_at_raw, "checkpoint_updated_at")?;
        let last_source_at = node
            .get::<String>("last_source_at")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| parse_rfc3339(&s, "last_source_at"))
            .transpose()?;
        let last_object_id = node
            .get::<String>("last_object_id")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|raw| Uuid::parse_str(&raw).ok());
        let objects_written: i64 = node.get("objects_written").unwrap_or(0);
        Ok(Some(ProjectionCheckpoint {
            projection: projection.to_string(),
            last_source_at,
            last_object_id,
            objects_written: objects_written.max(0) as u64,
            updated_at,
        }))
    }

    async fn save_checkpoint(&self, checkpoint: &ProjectionCheckpoint) -> Result<(), StorageError> {
        self.graph
            .run(
                query(
                    "MERGE (s:ProjectionState {projection: $name}) \
                     SET s.checkpoint_updated_at = $ts, \
                         s.last_source_at = $src_at, \
                         s.last_object_id = $obj_id, \
                         s.objects_written = $n",
                )
                .param("name", checkpoint.projection.clone())
                .param("ts", rfc3339(checkpoint.updated_at))
                .param(
                    "src_at",
                    checkpoint.last_source_at.map(rfc3339).unwrap_or_default(),
                )
                .param(
                    "obj_id",
                    checkpoint
                        .last_object_id
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                )
                .param("n", checkpoint.objects_written as i64),
            )
            .await
            .map_err(map_neo)
    }

    async fn projection_lag(
        &self,
        projection: &str,
        now: DateTime<Utc>,
    ) -> Result<ProjectionLag, StorageError> {
        Ok(ProjectionLag::from_checkpoint(
            self.checkpoint(projection).await?,
            now,
        ))
    }

    async fn rebuild_status(&self, projection: &str) -> Result<RebuildStatus, StorageError> {
        let row = self
            .execute_one(
                query("MATCH (s:ProjectionState {projection: $name}) RETURN s")
                    .param("name", projection),
            )
            .await?;
        let Some(row) = row else {
            return Ok(RebuildStatus::idle(projection));
        };
        let node: Node = row
            .get("s")
            .map_err(|err| StorageError::CorruptionSuspected {
                message: format!("rebuild_status 解 ProjectionState 失敗：{err}"),
            })?;
        let Some(state_raw) = node.get::<String>("rebuild_state").ok() else {
            return Ok(RebuildStatus::idle(projection));
        };
        let state: RebuildState = serde_json::from_value(Value::String(state_raw.clone()))
            .map_err(|err| StorageError::Unknown {
                backend: BACKEND,
                message: format!(
                    "投影 `{projection}` 的 rebuild_state 是無法識別的值 `{state_raw}`：{err}。\
                     請檢查 :ProjectionState 那一列"
                ),
            })?;
        Ok(RebuildStatus {
            projection: projection.to_string(),
            state,
            started_at: node
                .get::<String>("rebuild_started_at")
                .ok()
                .filter(|s| !s.is_empty())
                .map(|s| parse_rfc3339(&s, "rebuild_started_at"))
                .transpose()?,
            finished_at: node
                .get::<String>("rebuild_finished_at")
                .ok()
                .filter(|s| !s.is_empty())
                .map(|s| parse_rfc3339(&s, "rebuild_finished_at"))
                .transpose()?,
            scanned: node.get::<i64>("rebuild_scanned").unwrap_or(0).max(0) as u64,
            written: node.get::<i64>("rebuild_written").unwrap_or(0).max(0) as u64,
            failed: node.get::<i64>("rebuild_failed").unwrap_or(0).max(0) as u64,
            last_error: node
                .get::<String>("rebuild_last_error")
                .ok()
                .filter(|s| !s.is_empty()),
        })
    }

    async fn set_rebuild_status(&self, status: &RebuildStatus) -> Result<(), StorageError> {
        let state = serde_json::to_value(status.state)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_else(|| "idle".into());
        self.graph
            .run(
                query(
                    "MERGE (s:ProjectionState {projection: $name}) \
                     SET s.rebuild_state = $state, \
                         s.rebuild_started_at = $started, \
                         s.rebuild_finished_at = $finished, \
                         s.rebuild_scanned = $scanned, \
                         s.rebuild_written = $written, \
                         s.rebuild_failed = $failed, \
                         s.rebuild_last_error = $err",
                )
                .param("name", status.projection.clone())
                .param("state", state)
                .param(
                    "started",
                    status.started_at.map(rfc3339).unwrap_or_default(),
                )
                .param(
                    "finished",
                    status.finished_at.map(rfc3339).unwrap_or_default(),
                )
                .param("scanned", status.scanned as i64)
                .param("written", status.written as i64)
                .param("failed", status.failed as i64)
                .param(
                    "err",
                    status
                        .last_error
                        .as_deref()
                        .map(StorageError::sanitize)
                        .unwrap_or_default(),
                ),
            )
            .await
            .map_err(map_neo)
    }

    async fn reset_projection(&self, projection: &str) -> Result<(), StorageError> {
        self.graph
            .run(
                query("MATCH (s:ProjectionState {projection: $name}) DELETE s")
                    .param("name", projection),
            )
            .await
            .map_err(map_neo)
    }
}
