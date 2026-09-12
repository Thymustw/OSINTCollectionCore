//! `entity.extracted` → OpenSearch `osint-documents` 投影（SPEC §18）。
//!
//! # 為什麼訂閱 `entity.extracted` 而不是 `dedup.completed`
//!
//! index 裡要有 entity 才能支援 SPEC §18 的 entity 過濾（「找提到 CVE-2026-0001 的文章」）。
//! 訂 `dedup.completed` 的話會在 entity 抽出來**之前**就把文件寫進去，
//! 於是那份文件永遠帶著空的 entities——除非之後有東西再來更新它。沒有那個東西。
//! 這不會報錯，只會讓 entity 過濾漏掉最近的文件。
//!
//! 副作用是「duplicate 不會被索引」變成結構性保證：entity-worker 對 `is_duplicate=true`
//! 根本不發 `entity.extracted`。
//!
//! # 冪等
//!
//! OpenSearch 的 `_id` 就是 `Document.id`（見 [`crate::projection`]）。
//! index 動作對既有 `_id` 是覆寫，同一則事件重送一萬次還是一筆 hit。
//! 這裡**刻意沒有** provenance claim：投影是可重建的衍生資料，
//! 為它寫一列 canonical 的 claim 會讓「重建」變成需要先刪 claim 才能跑。
//!
//! # 已知限制
//!
//! * 一份 Document 最多收錄前 100 筆 entity extraction（`RelationalStore` 的
//!   `list_entity_extractions_by_object` 把 limit 夾在 1..=100，沒有 cursor 版本）。
//!   超過的部分不會進 index，那份文件的 entity 過濾會不完整。
//! * V0.1 沒有獨立的 DLQ topic。bulk 的永久性失敗記在 log（error）與
//!   `osint_indexer_bulk_permanent_total`，不會靜默丟掉，但也不會自動重放。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use core_events::{EventProducer, EventTopic};
use core_model::Document;
use core_observability::MetricsRegistry;
use serde_json::{Value, json};
use storage_core::{
    BulkFailure, ProjectionCheckpoint, ProjectionStore, RebuildState, RebuildStatus,
    RelationalStore, SearchDocument, SearchStore, StorageError,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

use crate::error::IndexerError;
use crate::projection::{self, IndexEntity, Provenance};
use crate::schema;

pub const PROCESSOR: &str = "indexer";

/// 一份 Document 最多取回幾筆 extraction。`RelationalStore` 的硬上限就是 100。
const EXTRACTION_LIMIT: u32 = 100;

/// indexer 的各項上限。每一項都必須有值，沒有「不限」這個選項。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexBounds {
    /// 一次 bulk 最多送幾筆。
    pub batch_size: u32,
    /// 累積不到 `batch_size` 時，最久等多久就強制送出。
    ///
    /// 沒有這個逾時的話，流量低的時候最後幾筆會**永遠卡在記憶體裡**不進 index，
    /// 而且 offset 也不會提交——看起來像「搜尋少了最新的文件」。
    pub batch_timeout: Duration,
    /// 單一文字欄位寫進 index 的 byte 上限。
    pub max_field_bytes: usize,
    /// 暫時性 bulk 失敗最多重試幾次（指數退避）。
    pub bulk_max_retries: u32,
    /// consumer lag 超過這個值就啟動降速。
    pub lag_threshold: u64,
    /// 降速時每批之間睡多久。
    pub backpressure_sleep: Duration,
}

impl Default for IndexBounds {
    fn default() -> Self {
        Self {
            // 200：OpenSearch 官方建議 bulk 大小抓 5–15 MB。本專案的文件
            // 平均 `_source` 約 10–40 KB（body 上限 256 KiB），200 筆落在 2–8 MB，
            // 最壞情況（全部都是 256 KiB 上限）也還在 50 MB 以下的單一請求可接受範圍。
            batch_size: 200,
            batch_timeout: Duration::from_millis(1000),
            max_field_bytes: projection::DEFAULT_MAX_FIELD_BYTES,
            bulk_max_retries: 3,
            lag_threshold: 5_000,
            backpressure_sleep: Duration::from_millis(200),
        }
    }
}

/// 準備一份 Document 的結果。
#[derive(Debug, Clone, PartialEq)]
pub enum PrepareOutcome {
    /// 可以送進 bulk。
    Ready(Box<SearchDocument>),
    /// 這份在 DB 上是 duplicate。已經從 index 移除（若原本在裡面）。
    RemovedDuplicate {
        document_id: Uuid,
        was_indexed: bool,
    },
    /// 事件提到的 Document 不在 DB。
    Missing { document_id: Uuid },
}

/// 批次裡一筆文件的 (id, 來源時間戳)。
///
/// 存在的理由是 [`Indexer::flush`] **會吃掉整個批次**（`Vec<SearchDocument>` by value），
/// 而 checkpoint 要在 flush **之後**才能知道哪幾筆成功。所以在 flush 之前先留下這份
/// 精簡副本；整批 clone 一次只為了寫 checkpoint 是白花的記憶體（`_source` 可含 256 KiB 正文）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceMark {
    /// OpenSearch 的 `_id`，也就是 `Document.id` 的字串形式。
    pub id: String,
    pub source_at: Option<DateTime<Utc>>,
}

/// 一批寫入對 projection checkpoint 的貢獻。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BatchProgress {
    /// 本批**成功寫入者**之中最大的來源時間戳。
    pub last_source_at: Option<DateTime<Utc>>,
    /// 與 `last_source_at` 成對的那一筆 document id。
    pub last_object_id: Option<Uuid>,
    /// 成功寫入的筆數。
    pub written: u64,
}

impl BatchProgress {
    /// 從本批的 [`SourceMark`] 與 flush 結果算出進度。
    ///
    /// **永久性失敗的那幾筆會被排除。** checkpoint 的語意是「最後一筆**成功寫入**的
    /// 來源物件」；把失敗的那筆算進去，checkpoint 就會宣稱進度已經過了它，
    /// 而它其實不在 index 裡——lag 看起來正常，資料卻少一筆。
    #[must_use]
    pub fn from_marks(marks: &[SourceMark], report: &FlushReport) -> Self {
        let failed: Vec<&str> = report
            .permanent_failures
            .iter()
            .map(|failure| failure.id.as_str())
            .collect();
        let mut progress = Self {
            written: u64::from(report.indexed),
            ..Self::default()
        };
        for mark in marks {
            if failed.contains(&mark.id.as_str()) {
                continue;
            }
            let Some(source_at) = mark.source_at else {
                continue;
            };
            if progress
                .last_source_at
                .is_none_or(|current| source_at > current)
            {
                progress.last_source_at = Some(source_at);
                progress.last_object_id = Uuid::parse_str(&mark.id).ok();
            }
        }
        progress
    }
}

/// 一次 flush 的結果。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FlushReport {
    pub submitted: usize,
    pub indexed: u32,
    /// 重試後仍失敗、且不值得再重試的筆數。
    pub permanent_failures: Vec<BulkFailure>,
    /// 實際重試了幾次。
    pub retries: u32,
}

/// 生產用 indexer。
#[derive(Clone)]
pub struct Indexer {
    store: PostgresCanonicalStore,
    search: OpenSearchStore,
    producer: Option<Arc<EventProducer>>,
    metrics: MetricsRegistry,
    index: String,
    bounds: IndexBounds,
}

impl Indexer {
    #[must_use]
    pub fn new(
        store: PostgresCanonicalStore,
        search: OpenSearchStore,
        producer: Option<Arc<EventProducer>>,
        metrics: MetricsRegistry,
        index: impl Into<String>,
        bounds: IndexBounds,
    ) -> Self {
        Self {
            store,
            search,
            producer,
            metrics,
            index: index.into(),
            bounds,
        }
    }

    #[must_use]
    pub fn index_name(&self) -> &str {
        &self.index
    }

    #[must_use]
    pub fn bounds(&self) -> IndexBounds {
        self.bounds
    }

    #[must_use]
    pub fn search_store(&self) -> &OpenSearchStore {
        &self.search
    }

    #[must_use]
    pub fn store(&self) -> &PostgresCanonicalStore {
        &self.store
    }

    /// 建立（或補齊）index 的 settings 與 mapping。啟動時與 rebuild 前都會呼叫。
    ///
    /// 同時建立投影狀態 index（`osint-projection-state`）。在這裡一起做是為了
    /// **啟動時就發現 mapping 寫錯**——留到第一次 flush 才建的話，那個 400 會出現在
    /// 一個只 warn 的路徑上（見 [`Indexer::record_checkpoint`]），等於永遠沒有 checkpoint
    /// 而服務看起來完全正常。
    pub async fn ensure_index(&self) -> Result<bool, IndexerError> {
        self.search.ensure_projection_state_index().await?;
        let created = self
            .search
            .ensure_index_with(
                &self.index,
                &schema::index_settings(),
                &schema::index_mappings(),
            )
            .await?;
        if created {
            tracing::info!(index = %self.index, "建立 OpenSearch index 與 mapping");
        } else {
            tracing::debug!(index = %self.index, "index 已存在，已套用 mapping（只會新增欄位）");
        }
        Ok(created)
    }

    /// 從 `entity.extracted` payload 取出 `document_id`。
    pub fn document_id_from_payload(payload: &Value) -> Result<Uuid, IndexerError> {
        payload
            .get("document_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| IndexerError::MissingField {
                field: "document_id".into(),
            })
    }

    /// 讀 PostgreSQL、組出要寫進 index 的文件。
    ///
    /// PostgreSQL 是 truth：事件只用來知道「哪一份要重算」，內容一律重新讀。
    /// 直接拿事件 payload 當內容會讓亂序或重放的事件覆蓋掉較新的狀態。
    pub async fn prepare(&self, document_id: Uuid) -> Result<PrepareOutcome, IndexerError> {
        let Some(document) = self.store.get_document(document_id).await? else {
            tracing::warn!(
                %document_id,
                "entity.extracted 提到的 Document 不在 Postgres。\
                 事件可能早於資料被清掉，或 entity-worker 與本服務看的不是同一個資料庫；\
                 跳過不中斷消費"
            );
            return Ok(PrepareOutcome::Missing { document_id });
        };

        if document.duplicate_of.is_some() {
            // entity-worker 不該對 duplicate 發事件，但 DB 上的 duplicate_of 才是事實：
            // 一份文件可能在被索引之後才被判成重複（事件亂序、或 dedup 重跑）。
            // 那時它已經在 index 裡了，只是不再送新版本並不會讓它消失。
            let was_indexed = self
                .search
                .delete(&self.index, &document_id.to_string())
                .await?;
            tracing::info!(
                %document_id,
                canonical_object_id = ?document.duplicate_of,
                was_indexed,
                "Document 在 DB 上已標記為重複，不索引（原本在 index 裡的話已移除）"
            );
            self.metrics.inc("osint_indexer_skipped_duplicate_total", 1);
            return Ok(PrepareOutcome::RemovedDuplicate {
                document_id,
                was_indexed,
            });
        }

        let entities = self.load_entities(document_id).await?;
        let provenance = self.load_provenance(&document).await?;
        Ok(PrepareOutcome::Ready(Box::new(
            projection::build_search_document(
                &self.index,
                &document,
                provenance,
                &entities,
                Utc::now(),
                self.bounds.max_field_bytes,
            ),
        )))
    }

    /// 取這份 Document 抽出來的 Entity（依 extraction 去重，保持穩定順序）。
    async fn load_entities(&self, document_id: Uuid) -> Result<Vec<IndexEntity>, IndexerError> {
        let extractions = self
            .store
            .list_entity_extractions_by_object(document_id, EXTRACTION_LIMIT)
            .await?;
        if extractions.len() as u32 == EXTRACTION_LIMIT {
            tracing::warn!(
                %document_id,
                limit = EXTRACTION_LIMIT,
                "extraction 數達到查詢上限，index 裡的 entity 可能不完整。\
                 這份文件的 entity 過濾會漏掉超出上限的部分"
            );
            self.metrics
                .inc("osint_indexer_entities_truncated_total", 1);
        }
        // BTreeMap 而不是 HashMap：`_source` 的 entities 陣列順序若每次不同，
        // 同一份文件重新索引會產生內容相同但位元組不同的文件，
        // 讓「有沒有變」這種比對永遠說有變。
        let mut by_id: BTreeMap<Uuid, IndexEntity> = BTreeMap::new();
        for extraction in extractions {
            if by_id.contains_key(&extraction.entity_id) {
                continue;
            }
            if let Some(entity) = self.store.get_entity(extraction.entity_id).await? {
                by_id.insert(entity.id, IndexEntity::from_entity(&entity));
            }
        }
        Ok(by_id.into_values().collect())
    }

    /// 取 `raw_evidence_id`（normalizer 寫在 attributes）與它對應的 source／connector。
    ///
    /// **Acceptance E 的鏈就是靠這三欄從 search result 開始的。**
    async fn load_provenance(&self, document: &Document) -> Result<Provenance, IndexerError> {
        let raw_evidence_id = document
            .attributes
            .get("raw_evidence_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok());
        let Some(raw_evidence_id) = raw_evidence_id else {
            tracing::warn!(
                document_id = %document.id,
                "Document.attributes 沒有 raw_evidence_id，search hit 將無法回查原始證據。\
                 這份文件多半是舊版 normalizer 產生的"
            );
            return Ok(Provenance::default());
        };
        let evidence = self.store.get_raw_evidence(raw_evidence_id).await?;
        Ok(Provenance {
            raw_evidence_id: Some(raw_evidence_id),
            source_id: evidence.as_ref().map(|e| e.source_id),
            connector_id: evidence.as_ref().map(|e| e.connector_id),
        })
    }

    /// 送出一批。暫時性失敗會退避重試；永久性失敗回報在 [`FlushReport`] 裡。
    pub async fn flush(&self, batch: Vec<SearchDocument>) -> Result<FlushReport, IndexerError> {
        let submitted = batch.len();
        if submitted == 0 {
            return Ok(FlushReport::default());
        }
        let mut pending = batch;
        let mut report = FlushReport {
            submitted,
            ..FlushReport::default()
        };

        for attempt in 0..=self.bounds.bulk_max_retries {
            let result = self.search.bulk_index(pending.clone()).await?;
            report.indexed += result.indexed;

            if result.failures.is_empty() {
                self.metrics
                    .inc("osint_indexer_indexed_total", u64::from(result.indexed));
                return Ok(report);
            }

            // 逐筆分類。永久性的不再重送——重送一百次結果一樣，只是把 partition 卡住。
            let (retryable, permanent): (Vec<_>, Vec<_>) = result
                .failures
                .into_iter()
                .partition(BulkFailure::is_retryable);
            for failure in &permanent {
                tracing::error!(
                    document_id = %failure.id,
                    status = failure.status,
                    reason = %failure.reason,
                    "bulk 永久性失敗，這份文件不會出現在搜尋結果裡。\
                     status 400 通常是 mapping 不符（mapping 是 dynamic:strict）"
                );
            }
            report.permanent_failures.extend(permanent);

            if retryable.is_empty() {
                self.metrics
                    .inc("osint_indexer_indexed_total", u64::from(result.indexed));
                self.metrics.inc(
                    "osint_indexer_bulk_permanent_total",
                    report.permanent_failures.len() as u64,
                );
                return Ok(report);
            }
            if attempt == self.bounds.bulk_max_retries {
                self.metrics
                    .inc("osint_indexer_bulk_retry_exhausted_total", 1);
                return Err(IndexerError::BulkRetriesExhausted {
                    attempts: attempt + 1,
                    status: retryable[0].status,
                });
            }

            // 只留要重試的那幾筆。整批重送會把已經成功的再寫一次——
            // 因為是 upsert 所以不會產生重複，但那是白花的頻寬與 OpenSearch 負載，
            // 而且會在已經過載的時候讓情況更糟。
            let retry_ids: Vec<&str> = retryable.iter().map(|f| f.id.as_str()).collect();
            pending.retain(|doc| retry_ids.contains(&doc.id.as_str()));
            report.retries += 1;
            self.metrics.inc("osint_indexer_bulk_retry_total", 1);
            let backoff = Duration::from_millis(200 * (1 << attempt.min(5)));
            tracing::warn!(
                retry_count = pending.len(),
                attempt = attempt + 1,
                backoff_ms = backoff.as_millis() as u64,
                "bulk 有暫時性失敗（OpenSearch 佇列滿或暫時不可用），退避後重送這幾筆"
            );
            tokio::time::sleep(backoff).await;
        }
        Ok(report)
    }

    /// 本批每一筆的 (id, 來源時間戳)。**在 [`Indexer::flush`] 之前呼叫**——
    /// flush 會吃掉批次。
    #[must_use]
    pub fn source_marks(batch: &[SearchDocument]) -> Vec<SourceMark> {
        batch
            .iter()
            .map(|document| SourceMark {
                id: document.id.clone(),
                source_at: projection::source_timestamp(&document.body),
            })
            .collect()
    }

    /// 把一批的進度寫進 projection checkpoint。
    ///
    /// # 失敗只 warn，不回錯
    ///
    /// checkpoint 是**可觀測性**，不是資料路徑：文件已經進 OpenSearch 了，
    /// 為了一筆進度寫不進去而讓整批重送（或讓 offset 不提交）只會把問題放大。
    /// 但 warn 必須講清楚後果——**lag 會停在舊值**，於是儀表板上看起來像投影卡住，
    /// 而實際卡住的只有這個計數器。沒寫清楚的話下一個人會去查一個不存在的 backlog。
    ///
    /// # 併發限制（已知）
    ///
    /// 累加是「讀 → 加 → 寫」，沒有 `if_seq_no` 樂觀鎖。同一個投影跑**兩個以上**
    /// indexer 實例時 `objects_written` 會少算（後寫的覆蓋前寫的）。
    /// `last_source_at` 因為只前進，最壞情況是慢一批才更新。
    /// V0.2 的部署假設是一個投影一個 writer；要多 writer 得加上 seq_no 條件更新。
    pub async fn record_checkpoint(&self, progress: BatchProgress) {
        if progress.written == 0 && progress.last_source_at.is_none() {
            return;
        }
        let now = Utc::now();
        let existing = match self.search.checkpoint(&self.index).await {
            Ok(existing) => existing,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    projection = %self.index,
                    "讀不到 projection checkpoint，本批的進度不會被累加。\
                     索引本身不受影響；後果是 projection lag 會停在舊值（看起來像投影落後）"
                );
                return;
            }
        };
        let mut checkpoint =
            existing.unwrap_or_else(|| ProjectionCheckpoint::empty(&self.index, now));
        checkpoint.advance(
            progress.last_source_at,
            progress.last_object_id,
            progress.written,
            now,
        );
        if let Err(err) = self.search.save_checkpoint(&checkpoint).await {
            tracing::warn!(
                error = %err,
                projection = %self.index,
                objects_written = checkpoint.objects_written,
                "寫不進 projection checkpoint。索引本身不受影響（文件已經在 index 裡），\
                 後果是 projection lag 會停在舊值、累積計數少算這一批——\
                 看起來像投影落後，實際落後的只有這個計數器"
            );
        }
    }

    /// 寫重建狀態；失敗只 warn。理由同 [`Indexer::record_checkpoint`]。
    async fn record_rebuild_status(&self, status: &RebuildStatus) {
        if let Err(err) = self.search.set_rebuild_status(status).await {
            tracing::warn!(
                error = %err,
                projection = %self.index,
                state = ?status.state,
                "寫不進 rebuild 狀態。重建本身照常進行；後果是 Operations Center 會顯示\
                 過時的（或完全沒有）重建進度，`--rebuild` 的最終結果請看本行之後的 log 與 exit code"
            );
        }
    }

    /// 發 `search.index.completed`。
    pub async fn publish_completed(
        &self,
        document_ids: &[Uuid],
        report: &FlushReport,
    ) -> Result<(), IndexerError> {
        let Some(producer) = &self.producer else {
            return Ok(());
        };
        if document_ids.is_empty() {
            return Ok(());
        }
        let payload = json!({
            "index": self.index,
            "document_ids": document_ids,
            "indexed": report.indexed,
            "failed": report.permanent_failures.len(),
            "failed_document_ids": report
                .permanent_failures
                .iter()
                .map(|f| f.id.clone())
                .collect::<Vec<_>>(),
            "retries": report.retries,
        });
        // partition key 用第一筆 document id：同一份文件的索引事件會落在同一個
        // partition，下游要按文件排序時才有意義。
        producer
            .publish(
                EventTopic::SearchIndexCompleted,
                document_ids.first().map(ToString::to_string).as_deref(),
                document_ids.first().copied(),
                payload,
            )
            .await?;
        Ok(())
    }

    /// 目前 index 裡的文件數。
    pub async fn indexed_count(&self) -> Result<u64, IndexerError> {
        self.search.refresh(&self.index).await?;
        Ok(self.search.count(&self.index).await?)
    }
}

/// `--rebuild` 的選項。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildOptions {
    /// 重建前先刪掉整個 index。
    ///
    /// mapping 有**破壞性**變更（欄位改型別、analyzer 換掉）時必須開——
    /// OpenSearch 的 `_mapping` 只能新增欄位。不開的話重建會成功，
    /// 但舊欄位仍用舊型別，查詢行為與新叢集不同且不會報錯。
    pub drop_index: bool,
    /// 一次從 PostgreSQL 取幾筆 Document（`RelationalStore` 夾在 1..=100）。
    pub page_size: u32,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            drop_index: false,
            page_size: 100,
        }
    }
}

/// 重建結果。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RebuildReport {
    /// 掃過的 canonical Document 數（不含 duplicate）。
    pub scanned: u64,
    /// 因為是 duplicate 而跳過的數量。
    pub skipped_duplicates: u64,
    /// 成功寫進 index 的數量。
    pub indexed: u64,
    /// 永久性失敗的 document id。
    pub failed: Vec<String>,
}

impl Indexer {
    /// 從 PostgreSQL 全量重建 OpenSearch 投影（CLAUDE.md §5：OpenSearch 是可重建的 projection）。
    ///
    /// # 這是「補上缺的」還是「完整重建」
    ///
    /// `drop_index = false` 時是**補上缺的**：既有文件會被覆寫成最新內容，
    /// 但**已經不該存在的文件不會被刪掉**（例如 Document 在 PostgreSQL 被刪除之後）。
    /// 要真正的完整重建請用 `drop_index = true`。
    /// 這個差別很容易被當成「重建過了就一定一致」，所以在這裡寫清楚。
    ///
    /// # 進度
    ///
    /// 每處理完一頁就記一次 info log（掃過幾筆、寫了幾筆）。V0.1 沒有可查詢的
    /// 進度 endpoint；長時間重建請看 log 或 `/metrics`。
    pub async fn rebuild(&self, options: RebuildOptions) -> Result<RebuildReport, IndexerError> {
        let started_at = Utc::now();
        if options.drop_index {
            // 狀態要在刪 index **之前**清掉，順序不能反：先寫 Running 再 reset，
            // 那個 Running 會被 reset 一起清掉，於是重建過程中 rebuild_status 是 Idle——
            // 看起來像沒有人在重建，於是有人再開一個。
            if let Err(err) = self.search.reset_projection(&self.index).await {
                tracing::warn!(
                    error = %err,
                    projection = %self.index,
                    "清不掉舊的 projection 狀態。重建照常進行，但 objects_written 會從舊值\
                     繼續累加（--drop 之後應該歸零），那個計數之後會偏高"
                );
            }
            let existed = self.search.delete_index(&self.index).await?;
            tracing::warn!(
                index = %self.index,
                existed,
                "已刪除 index，將從零重建。重建完成前搜尋會回較少的結果（或空結果）"
            );
        }
        self.ensure_index().await?;

        let mut status = RebuildStatus {
            projection: self.index.clone(),
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
                status.written = report.indexed;
                status.failed = report.failed.len() as u64;
                // 部分文件永久性失敗**仍然是 Completed**：重建確實跑完了，
                // 失敗筆數在 `failed` 欄位。把它當 Failed 會讓「跑不完」與
                // 「跑完了但有 3 筆 mapping 不符」變成同一個狀態，處理方式完全不同。
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
    ) -> Result<RebuildReport, IndexerError> {
        let page_size = options.page_size.clamp(1, 100);
        let mut report = RebuildReport::default();
        let mut cursor: Option<Uuid> = None;
        let started = std::time::Instant::now();

        loop {
            let page = self.store.list_documents(cursor, page_size).await?;
            let got = page.len() as u32;
            if page.is_empty() {
                break;
            }
            cursor = page.last().map(|doc| doc.id);

            let mut batch = Vec::with_capacity(page.len());
            let mut ids = Vec::with_capacity(page.len());
            for document in page {
                if document.duplicate_of.is_some() {
                    report.skipped_duplicates += 1;
                    continue;
                }
                report.scanned += 1;
                let entities = self.load_entities(document.id).await?;
                let provenance = self.load_provenance(&document).await?;
                ids.push(document.id);
                batch.push(projection::build_search_document(
                    &self.index,
                    &document,
                    provenance,
                    &entities,
                    Utc::now(),
                    self.bounds.max_field_bytes,
                ));
            }

            if !batch.is_empty() {
                let marks = Self::source_marks(&batch);
                let flushed = self.flush(batch).await?;
                report.indexed += u64::from(flushed.indexed);
                report
                    .failed
                    .extend(flushed.permanent_failures.iter().map(|f| f.id.clone()));
                // checkpoint 在重建時照樣寫。掃描是 id DESC（最新在前），所以第二頁
                // 之後的來源時間戳比第一頁舊——靠 `ProjectionCheckpoint::advance`
                // 的「只前進」保證 checkpoint 不會被倒退回最舊那一頁。
                self.record_checkpoint(BatchProgress::from_marks(&marks, &flushed))
                    .await;
                self.publish_completed(&ids, &flushed).await?;
            }

            // 每頁更新一次進度，長時間重建才看得到它在動（SPEC_V0.2 §27 的 reindex jobs）。
            status.scanned = report.scanned;
            status.written = report.indexed;
            status.failed = report.failed.len() as u64;
            self.record_rebuild_status(status).await;

            tracing::info!(
                scanned = report.scanned,
                indexed = report.indexed,
                skipped_duplicates = report.skipped_duplicates,
                failed = report.failed.len(),
                elapsed_secs = started.elapsed().as_secs(),
                "rebuild 進度"
            );

            if got < page_size {
                break;
            }
        }

        self.search.refresh(&self.index).await?;
        tracing::info!(
            index = %self.index,
            scanned = report.scanned,
            indexed = report.indexed,
            skipped_duplicates = report.skipped_duplicates,
            failed = report.failed.len(),
            elapsed_secs = started.elapsed().as_secs(),
            "rebuild 完成"
        );
        if !report.failed.is_empty() {
            tracing::error!(
                failed_ids = ?report.failed,
                "rebuild 有文件寫入失敗，這些文件不會出現在搜尋結果裡"
            );
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_without_document_id_is_an_error_not_a_default() {
        // 預設一個 id 會讓錯的文件被重新索引，而且不會有任何跡象。
        let err = Indexer::document_id_from_payload(&json!({"entity_count": 3})).unwrap_err();
        assert!(matches!(err, IndexerError::MissingField { .. }));
        assert!(err.to_string().contains("document_id"), "{err}");
    }

    #[test]
    fn payload_with_document_id_parses() {
        let id = Uuid::now_v7();
        assert_eq!(
            Indexer::document_id_from_payload(&json!({"document_id": id})).unwrap(),
            id
        );
    }

    #[test]
    fn default_bounds_are_all_finite() {
        let bounds = IndexBounds::default();
        assert!(bounds.batch_size > 0);
        assert!(
            !bounds.batch_timeout.is_zero(),
            "沒有逾時的話低流量時最後幾筆永遠不進 index"
        );
        assert!(bounds.max_field_bytes > 0);
        assert!(bounds.lag_threshold > 0);
    }

    fn mark(id: Uuid, source_at: DateTime<Utc>) -> SourceMark {
        SourceMark {
            id: id.to_string(),
            source_at: Some(source_at),
        }
    }

    #[test]
    fn batch_progress_takes_the_newest_source_timestamp() {
        let older = Uuid::now_v7();
        let newer = Uuid::now_v7();
        let now = Utc::now();
        let marks = vec![
            mark(older, now - chrono::Duration::seconds(600)),
            mark(newer, now),
        ];
        let report = FlushReport {
            submitted: 2,
            indexed: 2,
            ..FlushReport::default()
        };
        let progress = BatchProgress::from_marks(&marks, &report);
        assert_eq!(progress.last_source_at, Some(now));
        assert_eq!(progress.last_object_id, Some(newer));
        assert_eq!(progress.written, 2);
    }

    #[test]
    fn batch_progress_skips_permanently_failed_documents() {
        // 失敗那筆若被算進 checkpoint，進度就宣稱已經過了它，而它不在 index 裡——
        // lag 看起來正常，資料卻少一筆。
        let ok = Uuid::now_v7();
        let bad = Uuid::now_v7();
        let now = Utc::now();
        let marks = vec![
            mark(ok, now - chrono::Duration::seconds(60)),
            mark(bad, now),
        ];
        let report = FlushReport {
            submitted: 2,
            indexed: 1,
            permanent_failures: vec![BulkFailure {
                id: bad.to_string(),
                status: 400,
                reason: "mapping 不符".into(),
            }],
            retries: 0,
        };
        let progress = BatchProgress::from_marks(&marks, &report);
        assert_eq!(progress.last_object_id, Some(ok));
        assert_eq!(
            progress.last_source_at,
            Some(now - chrono::Duration::seconds(60))
        );
        assert_eq!(progress.written, 1);
    }

    #[test]
    fn batch_progress_without_timestamps_still_counts() {
        // 舊版投影寫的文件沒有可解析的 collected_at。計數要照算，
        // 但 last_source_at 維持 None——不要編一個時間戳進去假裝沒落後。
        let marks = vec![SourceMark {
            id: Uuid::now_v7().to_string(),
            source_at: None,
        }];
        let report = FlushReport {
            submitted: 1,
            indexed: 1,
            ..FlushReport::default()
        };
        let progress = BatchProgress::from_marks(&marks, &report);
        assert_eq!(progress.last_source_at, None);
        assert_eq!(progress.last_object_id, None);
        assert_eq!(progress.written, 1);
    }

    #[test]
    fn source_marks_are_read_from_the_projection_body() {
        let document = Document {
            id: Uuid::now_v7(),
            object_type: core_model::DocumentType::Article,
            schema_version: "1".into(),
            title: None,
            body: None,
            summary: None,
            language: None,
            author: None,
            published_at: None,
            modified_at: None,
            observed_at: Utc::now(),
            collected_at: Utc::now(),
            source_url: None,
            canonical_url: None,
            normalized_content_hash: None,
            confidence: 1.0,
            labels: Vec::new(),
            attributes: json!({}),
            external_key: None,
            simhash: None,
            duplicate_of: None,
        };
        let batch = vec![projection::build_search_document(
            "osint-documents-test",
            &document,
            Provenance::default(),
            &[],
            Utc::now(),
            4096,
        )];
        let marks = Indexer::source_marks(&batch);
        assert_eq!(marks.len(), 1);
        assert_eq!(marks[0].id, document.id.to_string());
        assert_eq!(
            marks[0].source_at,
            Some(document.collected_at),
            "checkpoint 的來源時間戳必須從投影裡讀得回來"
        );
    }

    #[test]
    fn rebuild_page_size_default_matches_storage_limit() {
        // RelationalStore 把 limit 夾在 1..=100。傳更大的值不會報錯，只會靜默回 100。
        assert_eq!(RebuildOptions::default().page_size, 100);
        assert!(!RebuildOptions::default().drop_index);
    }
}
