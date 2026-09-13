//! `relationship.changed` → Neo4j 圖投影。
//!
//! # 為什麼事件內容不可信
//!
//! payload 只告訴我們「哪條邊變了」。`confidence`／時間戳／型別一律重讀
//! PostgreSQL——那才是 canonical。直接拿 payload 當內容會讓亂序或重放的
//! 事件覆蓋掉較新的狀態。payload 裡的 `relationship_type` **刻意不用**。
//!
//! # 冪等
//!
//! `upsert_node`／`upsert_edge`／`delete_edge` 都是冪等的。同一則事件重送
//! 一萬次還是同一條邊。刻意沒有 provenance claim：投影是可重建的衍生資料。

use chrono::{DateTime, Utc};
use core_model::{Entity, Relationship, RelationshipId};
use core_observability::MetricsRegistry;
use serde_json::Value;
use storage_core::codec::encode_enum;
use storage_core::{
    GraphEdge, GraphNode, GraphStore, ProjectionCheckpoint, ProjectionStore, RebuildState,
    RebuildStatus, RelationalStore, StorageError,
};
use uuid::Uuid;

use crate::error::GraphWorkerError;

pub const PROCESSOR: &str = "graph-worker";

/// 一則 `relationship.changed` 處理完的結果。給呼叫端記 log 與 metrics。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessOutcome {
    /// 兩端都是 Entity，已 upsert 節點與邊。
    Applied {
        relationship_id: RelationshipId,
        /// 這條邊在 canonical 上的 `last_seen`，給 checkpoint 當來源時間戳。
        last_seen: DateTime<Utc>,
    },
    /// 任一端不是 Entity（例如 Document → Entity 的 `mentions`）。
    /// 這是**預期行為**，不是錯誤——那種邊不該投影進圖。
    SkippedNonEntity { relationship_id: RelationshipId },
    /// `change_kind: upserted` 但 PostgreSQL 已查不到這條邊（發出後又被刪，race）。
    /// 已當刪除處理。
    SkippedRace { relationship_id: RelationshipId },
    /// `change_kind: deleted`，已呼叫 `delete_edge`（冪等）。
    Deleted { relationship_id: RelationshipId },
}

impl ProcessOutcome {
    #[must_use]
    pub fn relationship_id(&self) -> RelationshipId {
        match *self {
            Self::Applied {
                relationship_id, ..
            }
            | Self::SkippedNonEntity { relationship_id }
            | Self::SkippedRace { relationship_id }
            | Self::Deleted { relationship_id } => relationship_id,
        }
    }

    fn metric_name(&self) -> &'static str {
        match self {
            Self::Applied { .. } => "osint_graph_worker_applied_total",
            Self::SkippedNonEntity { .. } => "osint_graph_worker_skipped_non_entity_total",
            Self::SkippedRace { .. } => "osint_graph_worker_race_total",
            Self::Deleted { .. } => "osint_graph_worker_deleted_total",
        }
    }
}

/// 從 payload 抽出的變更。`relationship_type` 欄位即使存在也不收——
/// 一律以 PostgreSQL 查到的為準。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParsedChange {
    pub relationship_id: RelationshipId,
    pub source_object_id: Uuid,
    pub target_object_id: Uuid,
    pub kind: ChangeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeKind {
    Upserted,
    Deleted,
}

/// 生產用 graph-worker。`R`／`G` 泛型是為了單元測試能注入
/// `SqliteEmbeddedStore` + `MockGraphStore`；生產路徑是
/// `PostgresCanonicalStore` + `Neo4jStore`。
#[derive(Clone)]
pub struct GraphWorker<R, G> {
    store: R,
    graph: G,
    metrics: MetricsRegistry,
    projection: String,
}

impl<R, G> GraphWorker<R, G> {
    #[must_use]
    pub fn new(
        store: R,
        graph: G,
        metrics: MetricsRegistry,
        projection: impl Into<String>,
    ) -> Self {
        Self {
            store,
            graph,
            metrics,
            projection: projection.into(),
        }
    }

    #[must_use]
    pub fn projection(&self) -> &str {
        &self.projection
    }

    #[must_use]
    pub fn store(&self) -> &R {
        &self.store
    }

    #[must_use]
    pub fn graph(&self) -> &G {
        &self.graph
    }
}

impl<R: RelationalStore, G: GraphStore> GraphWorker<R, G> {
    /// 從 `relationship.changed` payload 處理一則變更。
    ///
    /// 事件只用來知道「哪條邊變了」；內容一律重讀 PostgreSQL。
    pub async fn process_change(
        &self,
        payload: &Value,
    ) -> Result<ProcessOutcome, GraphWorkerError> {
        let parsed = parse_change(payload)?;
        let outcome = apply_change(&self.store, &self.graph, parsed).await?;
        self.metrics.inc(outcome.metric_name(), 1);
        Ok(outcome)
    }
}

impl<R: RelationalStore, G: GraphStore + ProjectionStore> GraphWorker<R, G> {
    /// 把一批的進度寫進 projection checkpoint。
    ///
    /// # 失敗只 warn，不回錯
    ///
    /// checkpoint 是**可觀測性**，不是資料路徑：邊已經進 Neo4j 了，
    /// 為了一筆進度寫不進去而讓事件重送只會把問題放大。
    /// 但 warn 必須講清楚後果——**lag 會停在舊值**，儀表板上看起來像投影卡住，
    /// 而實際卡住的只有這個計數器。
    pub async fn record_checkpoint(
        &self,
        source_at: Option<DateTime<Utc>>,
        object_id: Option<Uuid>,
    ) {
        let now = Utc::now();
        let existing = match self.graph.checkpoint(&self.projection).await {
            Ok(existing) => existing,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    projection = %self.projection,
                    "讀不到 projection checkpoint，本則的進度不會被累加。\
                     圖投影本身不受影響；後果是 projection lag 會停在舊值（看起來像投影落後）"
                );
                return;
            }
        };
        let mut checkpoint =
            existing.unwrap_or_else(|| ProjectionCheckpoint::empty(&self.projection, now));
        checkpoint.advance(source_at, object_id, 1, now);
        if let Err(err) = self.graph.save_checkpoint(&checkpoint).await {
            tracing::warn!(
                error = %err,
                projection = %self.projection,
                objects_written = checkpoint.objects_written,
                "寫不進 projection checkpoint。圖投影本身不受影響（邊已經在 Neo4j 裡），\
                 後果是 projection lag 會停在舊值、累積計數少算這一則——\
                 看起來像投影落後，實際落後的只有這個計數器"
            );
        }
    }

    /// 寫重建狀態；失敗只 warn。理由同 [`GraphWorker::record_checkpoint`]。
    async fn record_rebuild_status(&self, status: &RebuildStatus) {
        if let Err(err) = self.graph.set_rebuild_status(status).await {
            tracing::warn!(
                error = %err,
                projection = %self.projection,
                state = ?status.state,
                "寫不進 rebuild 狀態。重建本身照常進行；後果是 Operations Center 會顯示\
                 過時的（或完全沒有）重建進度，`--rebuild` 的最終結果請看本行之後的 log 與 exit code"
            );
        }
    }

    /// 從 PostgreSQL 全量重建 Neo4j 投影（CLAUDE.md §5：Neo4j 是可重建的 projection）。
    ///
    /// # 這是「補上缺的」還是「完整重建」
    ///
    /// `drop_graph = false` 時是**補上缺的**：既有邊會被覆寫成最新內容，
    /// 但**已經不該存在的節點／邊不會被刪掉**（例如 Entity 在 PostgreSQL 被刪除之後）。
    /// 要真正的完整重建請用 `drop_graph = true`（呼叫 [`GraphStore::wipe`]）。
    ///
    /// Neo4j 沒有 OpenSearch 那種 `refresh`；重建結束不需要額外一步。
    pub async fn rebuild(
        &self,
        options: RebuildOptions,
    ) -> Result<RebuildReport, GraphWorkerError> {
        let started_at = Utc::now();
        if options.drop_graph {
            // 狀態要在 wipe **之前**清掉，順序不能反：先寫 Running 再 reset，
            // 那個 Running 會被 reset 一起清掉，於是重建過程中 rebuild_status 是 Idle——
            // 看起來像沒有人在重建，於是有人再開一個。
            if let Err(err) = self.graph.reset_projection(&self.projection).await {
                tracing::warn!(
                    error = %err,
                    projection = %self.projection,
                    "清不掉舊的 projection 狀態。重建照常進行，但 objects_written 會從舊值\
                     繼續累加（--drop 之後應該歸零），那個計數之後會偏高"
                );
            }
            self.graph.wipe().await?;
            tracing::warn!(
                projection = %self.projection,
                "已清空 :Entity 節點與邊（:ProjectionState 未動），將從零重建。\
                 重建完成前圖查詢會回較少的結果（或空結果）"
            );
        }

        let mut status = RebuildStatus {
            projection: self.projection.clone(),
            state: RebuildState::Running,
            started_at: Some(started_at),
            finished_at: None,
            scanned: 0,
            written: 0,
            failed: 0,
            last_error: None,
        };
        self.record_rebuild_status(&status).await;

        let result = self.rebuild_pages(options, &mut status).await;

        status.finished_at = Some(Utc::now());
        match &result {
            Ok(report) => {
                status.state = RebuildState::Completed;
                status.scanned = report.scanned;
                status.written = report.applied;
                status.failed = report.failed;
            }
            Err(err) => {
                status.state = RebuildState::Failed;
                status.last_error = Some(StorageError::sanitize(&err.to_string()));
            }
        }
        self.record_rebuild_status(&status).await;
        result
    }

    /// `rebuild` 的主迴圈。拆出來是為了讓外層無論成功或失敗都能寫一次最終狀態——
    /// 內層到處是 `?`，混在一起寫的話早退路徑會讓狀態永遠停在 `Running`。
    async fn rebuild_pages(
        &self,
        options: RebuildOptions,
        status: &mut RebuildStatus,
    ) -> Result<RebuildReport, GraphWorkerError> {
        let page_size = options.page_size.clamp(1, 100);
        let mut report = RebuildReport::default();
        let mut cursor: Option<RelationshipId> = None;
        let started = std::time::Instant::now();

        loop {
            let page = self.store.list_relationships(cursor, page_size).await?;
            let got = page.len() as u32;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|rel| rel.id);

            for rel in page {
                report.scanned += 1;
                match apply_relationship(&self.store, &self.graph, &rel).await {
                    Ok(ProcessOutcome::Applied { last_seen, .. }) => {
                        report.applied += 1;
                        self.metrics.inc("osint_graph_worker_applied_total", 1);
                        self.record_checkpoint(Some(last_seen), Some(rel.id)).await;
                    }
                    Ok(ProcessOutcome::SkippedNonEntity { .. }) => {
                        report.skipped_non_entity += 1;
                        self.metrics
                            .inc("osint_graph_worker_skipped_non_entity_total", 1);
                    }
                    Ok(ProcessOutcome::Deleted { .. } | ProcessOutcome::SkippedRace { .. }) => {
                        // rebuild 掃的是現存的 Relationship 列，不會走到刪除／race。
                    }
                    Err(err) => {
                        report.failed += 1;
                        self.metrics.inc("osint_graph_worker_errors_total", 1);
                        tracing::error!(
                            error = %err,
                            relationship_id = %rel.id,
                            "rebuild 寫入一條邊失敗。這條邊不會出現在圖上；\
                             其餘繼續。要補回請修好後再跑 --rebuild"
                        );
                    }
                }
            }

            status.scanned = report.scanned;
            status.written = report.applied;
            status.failed = report.failed;
            self.record_rebuild_status(status).await;

            tracing::info!(
                scanned = report.scanned,
                applied = report.applied,
                skipped_non_entity = report.skipped_non_entity,
                failed = report.failed,
                elapsed_secs = started.elapsed().as_secs(),
                "rebuild 進度"
            );

            if got < page_size {
                break;
            }
        }

        tracing::info!(
            projection = %self.projection,
            scanned = report.scanned,
            applied = report.applied,
            skipped_non_entity = report.skipped_non_entity,
            failed = report.failed,
            elapsed_secs = started.elapsed().as_secs(),
            "rebuild 完成"
        );
        Ok(report)
    }
}

/// `--rebuild` 的選項。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildOptions {
    /// 重建前先清空 `:Entity` 節點與邊（不碰 `:ProjectionState`）。
    ///
    /// Entity 在 Postgres 被刪之後，Neo4j 裡對應的節點不會自動消失；
    /// 只有 upsert 沒有 diff-delete。要真正從零開始必須開這個。
    pub drop_graph: bool,
    /// 一次從 PostgreSQL 取幾筆 Relationship（`RelationalStore` 夾在 1..=100）。
    pub page_size: u32,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            drop_graph: false,
            page_size: 100,
        }
    }
}

/// 重建結果。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RebuildReport {
    /// 掃過的 Relationship 列數（含 Document→Entity）。
    pub scanned: u64,
    /// 成功寫進圖的 Entity→Entity 邊數。
    pub applied: u64,
    /// 因為有一端不是 Entity 而跳過的數量。
    pub skipped_non_entity: u64,
    /// 寫入失敗的筆數。
    pub failed: u64,
}

/// 解析 `relationship.changed` payload。
///
/// `relationship_type` 欄位即使存在也**不讀**——一律以 PostgreSQL 查到的為準。
/// 漏用這個欄位不是 bug。
pub fn parse_change(payload: &Value) -> Result<ParsedChange, GraphWorkerError> {
    let relationship_id = required_uuid(payload, "relationship_id")?;
    let source_object_id = required_uuid(payload, "source_object_id")?;
    let target_object_id = required_uuid(payload, "target_object_id")?;
    let kind = match payload.get("change_kind").and_then(Value::as_str) {
        Some("upserted") => ChangeKind::Upserted,
        Some("deleted") => ChangeKind::Deleted,
        Some(other) => {
            return Err(GraphWorkerError::InvalidChangeKind {
                got: other.to_string(),
            });
        }
        None => {
            return Err(GraphWorkerError::MissingField {
                field: "change_kind".into(),
            });
        }
    };
    Ok(ParsedChange {
        relationship_id,
        source_object_id,
        target_object_id,
        kind,
    })
}

fn required_uuid(payload: &Value, field: &str) -> Result<Uuid, GraphWorkerError> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| GraphWorkerError::MissingField {
            field: field.into(),
        })
}

async fn apply_change<R: RelationalStore, G: GraphStore>(
    store: &R,
    graph: &G,
    parsed: ParsedChange,
) -> Result<ProcessOutcome, GraphWorkerError> {
    match parsed.kind {
        ChangeKind::Deleted => {
            graph.delete_edge(&parsed.relationship_id).await?;
            tracing::info!(
                relationship_id = %parsed.relationship_id,
                source_object_id = %parsed.source_object_id,
                target_object_id = %parsed.target_object_id,
                "圖邊已刪（change_kind=deleted；delete_edge 冪等，邊不存在也 Ok）"
            );
            Ok(ProcessOutcome::Deleted {
                relationship_id: parsed.relationship_id,
            })
        }
        ChangeKind::Upserted => {
            let Some(rel) = store.get_relationship(parsed.relationship_id).await? else {
                // 邊在事件發出後又被刪了。視同 deleted，不是錯誤。
                graph.delete_edge(&parsed.relationship_id).await?;
                tracing::info!(
                    relationship_id = %parsed.relationship_id,
                    source_object_id = %parsed.source_object_id,
                    target_object_id = %parsed.target_object_id,
                    "upserted 事件對應的 Relationship 已不在 Postgres（race：發出後被刪），\
                     已當刪除處理。這不是錯誤"
                );
                return Ok(ProcessOutcome::SkippedRace {
                    relationship_id: parsed.relationship_id,
                });
            };
            apply_relationship(store, graph, &rel).await
        }
    }
}

/// 把一條 canonical Relationship 投影進圖。事件路徑與 rebuild 共用，不要複製一份。
///
/// 兩端都必須是 Entity；任一端 `get_entity` 回 `None` 就跳過。
pub async fn apply_relationship<R: RelationalStore, G: GraphStore>(
    store: &R,
    graph: &G,
    rel: &Relationship,
) -> Result<ProcessOutcome, GraphWorkerError> {
    let source = store.get_entity(rel.source_object_id).await?;
    let target = store.get_entity(rel.target_object_id).await?;
    let (Some(source), Some(target)) = (source, target) else {
        tracing::debug!(
            relationship_id = %rel.id,
            source_object_id = %rel.source_object_id,
            target_object_id = %rel.target_object_id,
            "這條邊有一端不是 Entity，不投影進圖。Document→Entity（例如 mentions）是預期行為，不是錯誤"
        );
        return Ok(ProcessOutcome::SkippedNonEntity {
            relationship_id: rel.id,
        });
    };

    graph.upsert_node(&graph_node(&source)?).await?;
    graph.upsert_node(&graph_node(&target)?).await?;
    graph.upsert_edge(&graph_edge(rel)?).await?;
    tracing::debug!(
        relationship_id = %rel.id,
        source = %source.id,
        target = %target.id,
        "圖邊已 upsert"
    );
    Ok(ProcessOutcome::Applied {
        relationship_id: rel.id,
        last_seen: rel.last_seen,
    })
}

fn graph_node(entity: &Entity) -> Result<GraphNode, GraphWorkerError> {
    Ok(GraphNode {
        entity_id: entity.id,
        // 用 serde 的 snake_case，與寫進資料庫、API 請求是同一套。
        // 用 Debug 會變成 `Vulnerability`，過濾永遠比不中而且不會報錯。
        entity_type: encode_enum(&entity.entity_type)?,
        display_name: entity.name.clone(),
        attributes: entity.attributes.clone(),
    })
}

fn graph_edge(rel: &Relationship) -> Result<GraphEdge, GraphWorkerError> {
    Ok(GraphEdge {
        relationship_id: rel.id,
        source: rel.source_object_id,
        target: rel.target_object_id,
        relationship_type: encode_enum(&rel.relationship_type)?,
        confidence: rel.confidence,
        first_seen: rel.first_seen,
        last_seen: rel.last_seen,
    })
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use chrono::{TimeZone, Utc};
    use core_model::{Entity, EntityType, Relationship, RelationshipType};
    use serde_json::json;
    use storage_core::conformance::find_workspace_root;
    use storage_core::mock::MockGraphStore;
    use storage_core::{GraphStore, GraphTraversalOptions, RelationalStore};
    use storage_sqlite::SqliteEmbeddedStore;

    use super::*;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, 10, 0, 0).unwrap()
    }

    fn entity(name: &str, entity_type: EntityType) -> Entity {
        Entity {
            id: Uuid::now_v7(),
            entity_type,
            name: name.into(),
            normalized_name: name.to_ascii_lowercase(),
            description: None,
            confidence: 0.9,
            first_seen: ts(),
            last_seen: ts(),
            merged_into: None,
            attributes: json!({}),
        }
    }

    fn relationship(source: Uuid, ty: RelationshipType, target: Uuid) -> Relationship {
        Relationship {
            id: Uuid::now_v7(),
            source_object_id: source,
            relationship_type: ty,
            target_object_id: target,
            confidence: 0.8,
            first_seen: ts(),
            last_seen: ts(),
            evidence_count: 1,
            created_at: ts(),
            updated_at: ts(),
        }
    }

    fn payload(rel: &Relationship, kind: &str) -> Value {
        json!({
            "relationship_id": rel.id,
            "source_object_id": rel.source_object_id,
            "target_object_id": rel.target_object_id,
            "relationship_type": "mentions",
            "change_kind": kind,
        })
    }

    struct Harness {
        worker: GraphWorker<SqliteEmbeddedStore, MockGraphStore>,
        db: SqliteEmbeddedStore,
        path: PathBuf,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
            let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
        }
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    async fn open_harness() -> Harness {
        let root = find_workspace_root().expect("workspace root");
        let path: PathBuf = root.join(format!("var/osint-graph-worker-{}.sqlite", Uuid::now_v7()));
        cleanup(&path);
        let writer = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite writer");
        writer.migrate().await.expect("migrate");
        let db = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite reader");
        let worker = GraphWorker::new(
            writer,
            MockGraphStore::new(),
            MetricsRegistry::new(),
            "osint-graph-test",
        );
        Harness { worker, db, path }
    }

    #[test]
    fn payload_without_relationship_id_is_an_error() {
        let err = parse_change(&json!({
            "source_object_id": Uuid::now_v7(),
            "target_object_id": Uuid::now_v7(),
            "change_kind": "upserted",
        }))
        .unwrap_err();
        assert!(matches!(err, GraphWorkerError::MissingField { .. }));
        assert!(err.to_string().contains("relationship_id"), "{err}");
    }

    #[test]
    fn payload_without_change_kind_is_an_error() {
        let err = parse_change(&json!({
            "relationship_id": Uuid::now_v7(),
            "source_object_id": Uuid::now_v7(),
            "target_object_id": Uuid::now_v7(),
        }))
        .unwrap_err();
        assert!(matches!(err, GraphWorkerError::MissingField { .. }));
        assert!(err.to_string().contains("change_kind"), "{err}");
    }

    #[test]
    fn unknown_change_kind_is_an_error() {
        let err = parse_change(&json!({
            "relationship_id": Uuid::now_v7(),
            "source_object_id": Uuid::now_v7(),
            "target_object_id": Uuid::now_v7(),
            "change_kind": "updated",
        }))
        .unwrap_err();
        assert!(matches!(err, GraphWorkerError::InvalidChangeKind { .. }));
        assert!(err.to_string().contains("updated"), "{err}");
    }

    #[test]
    fn relationship_type_in_payload_is_ignored() {
        // 這個欄位存在但 parse_change 不讀它。漏用不是 bug。
        let id = Uuid::now_v7();
        let parsed = parse_change(&json!({
            "relationship_id": id,
            "source_object_id": Uuid::now_v7(),
            "target_object_id": Uuid::now_v7(),
            "relationship_type": "this-is-not-used",
            "change_kind": "upserted",
        }))
        .unwrap();
        assert_eq!(parsed.relationship_id, id);
        assert_eq!(parsed.kind, ChangeKind::Upserted);
    }

    #[tokio::test]
    async fn entity_to_entity_is_applied() {
        let h = open_harness().await;
        let a = entity("alice", EntityType::Person);
        let b = entity("acme", EntityType::Organization);
        h.db.put_entity(&a).await.unwrap();
        h.db.put_entity(&b).await.unwrap();
        let rel = relationship(a.id, RelationshipType::AssociatedWith, b.id);
        h.db.put_relationship(&rel).await.unwrap();

        let outcome = h
            .worker
            .process_change(&payload(&rel, "upserted"))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ProcessOutcome::Applied {
                relationship_id: rel.id,
                last_seen: rel.last_seen,
            }
        );

        let neighbors = h
            .worker
            .graph()
            .neighbors(&a.id, &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].entity_id, b.id);
        assert_eq!(neighbors[0].entity_type, "organization");
        assert_eq!(neighbors[0].display_name, "acme");
    }

    #[tokio::test]
    async fn document_to_entity_is_skipped() {
        let h = open_harness().await;
        let entity = entity("cve-2026-0001", EntityType::Vulnerability);
        h.db.put_entity(&entity).await.unwrap();
        // source 是一個不存在於 entities 的 UUID，模擬 Document id。
        let document_id = Uuid::now_v7();
        let rel = relationship(document_id, RelationshipType::Mentions, entity.id);
        h.db.put_relationship(&rel).await.unwrap();

        let outcome = h
            .worker
            .process_change(&payload(&rel, "upserted"))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ProcessOutcome::SkippedNonEntity {
                relationship_id: rel.id
            }
        );
        let neighbors = h
            .worker
            .graph()
            .neighbors(&entity.id, &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(neighbors.is_empty(), "Document→Entity 不該進圖");
    }

    #[tokio::test]
    async fn missing_relationship_is_treated_as_race_delete() {
        let h = open_harness().await;
        let a = entity("alice", EntityType::Person);
        let b = entity("acme", EntityType::Organization);
        h.db.put_entity(&a).await.unwrap();
        h.db.put_entity(&b).await.unwrap();
        let rel = relationship(a.id, RelationshipType::AssociatedWith, b.id);
        h.db.put_relationship(&rel).await.unwrap();
        h.worker
            .process_change(&payload(&rel, "upserted"))
            .await
            .unwrap();
        h.db.delete_relationship(rel.id).await.unwrap();

        let outcome = h
            .worker
            .process_change(&payload(&rel, "upserted"))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ProcessOutcome::SkippedRace {
                relationship_id: rel.id
            }
        );
        let neighbors = h
            .worker
            .graph()
            .neighbors(&a.id, &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(neighbors.is_empty(), "race 刪除後圖上不該還有這條邊");
    }

    #[tokio::test]
    async fn deleted_change_kind_removes_edge() {
        let h = open_harness().await;
        let a = entity("alice", EntityType::Person);
        let b = entity("acme", EntityType::Organization);
        h.db.put_entity(&a).await.unwrap();
        h.db.put_entity(&b).await.unwrap();
        let rel = relationship(a.id, RelationshipType::AssociatedWith, b.id);
        h.db.put_relationship(&rel).await.unwrap();
        h.worker
            .process_change(&payload(&rel, "upserted"))
            .await
            .unwrap();

        let outcome = h
            .worker
            .process_change(&payload(&rel, "deleted"))
            .await
            .unwrap();
        assert_eq!(
            outcome,
            ProcessOutcome::Deleted {
                relationship_id: rel.id
            }
        );
        let neighbors = h
            .worker
            .graph()
            .neighbors(&a.id, &GraphTraversalOptions::one_hop())
            .await
            .unwrap();
        assert!(neighbors.is_empty());
    }
}
