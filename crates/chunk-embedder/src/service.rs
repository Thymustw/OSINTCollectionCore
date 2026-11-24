//! `document.chunked` → 從 PostgreSQL 重讀段落 → osint-inference → `osint-chunks`。
//!
//! # 事件內容不可信
//!
//! payload 只取 `document_id`。段落文字、id、數量一律從 PostgreSQL 重讀：事件可能是舊的
//! （之後又重切過），也可能被竄改。`chunk_ids` 等欄位只給人看，不拿來決定寫什麼。
//!
//! # 一份文件的處理
//!
//! 1. 讀文件與段落。文件不在或已標重複 → 刪掉該文件在 index 裡的所有向量。
//! 2. 段落分批（`batch_size`）送推論，`passage: ` + `chunker::embedding_text(header, text)`。
//! 3. `bulk_index`（`_id` ＝ 段落 id，整份覆寫）。
//! 4. 刪掉 index 裡這份文件「現在已經不存在的段落 id」（重切後段數變少）。
//!
//! 推論暫時性失敗（503／連線）退避後重試同一批、不放棄；永久性失敗（400）回錯誤。

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use chunker::{PASSAGE_PREFIX, embedding_text};
use core_model::Document;
use core_observability::MetricsRegistry;
use serde_json::{Value, json};
use storage_core::{
    ChunkStore, DocumentChunk, RelationalStore, SearchDocument, SearchFilter, SearchStore,
    SortField, StructuredSearch,
};
use tokio::sync::watch;
use uuid::Uuid;

use crate::error::ChunkEmbedderError;
use crate::inference::{InferenceClient, InferenceError};
use crate::schema;

/// 一份文件最多查回幾個既有向量 id 來比對刪除。M1 實測單份最多 91 段。
const MAX_EXISTING_PER_DOCUMENT: u32 = 1000;

#[derive(Debug, Clone)]
pub struct EmbedderConfig {
    pub index: String,
    /// 每次送推論的段落數。推論服務內部逐筆跑，這只省 HTTP 來回（見 services/inference/README.md）。
    pub batch_size: usize,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    /// 單批耗時超過「移動基準 × 這個倍數」就暫停 `slow_pause`（owner 裁決 2026-11-05：
    /// 同批工作每批延遲明顯變慢就暫停背景工作）。倍數待 M2-4 實測後定。
    pub slow_factor: f64,
    pub slow_pause: Duration,
}

impl Default for EmbedderConfig {
    fn default() -> Self {
        Self {
            index: schema::DEFAULT_INDEX.into(),
            batch_size: 8,
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(30),
            slow_factor: 3.0,
            slow_pause: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedOutcome {
    Embedded {
        document_id: Uuid,
        chunks: usize,
        removed: usize,
    },
    /// 文件不在、已標重複、或沒有段落 → 刪掉 index 裡的舊向量（`removed` 筆）。
    Cleared { document_id: Uuid, removed: usize },
}

impl EmbedOutcome {
    #[must_use]
    pub fn document_id(&self) -> Uuid {
        match self {
            Self::Embedded { document_id, .. } | Self::Cleared { document_id, .. } => *document_id,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebuildReport {
    pub documents: u64,
    pub chunks: u64,
    pub failed: u64,
    pub interrupted: bool,
}

pub struct ChunkEmbedder<S, O> {
    store: S,
    search: O,
    inference: InferenceClient,
    metrics: MetricsRegistry,
    config: EmbedderConfig,
    /// 每批耗時的指數移動平均（毫秒）。0 代表還沒有樣本。
    baseline_ms: AtomicU64,
}

impl<S, O> ChunkEmbedder<S, O>
where
    S: RelationalStore + ChunkStore,
    O: SearchStore,
{
    pub fn new(
        store: S,
        search: O,
        inference: InferenceClient,
        metrics: MetricsRegistry,
        config: EmbedderConfig,
    ) -> Self {
        Self {
            store,
            search,
            inference,
            metrics,
            config,
            baseline_ms: AtomicU64::new(0),
        }
    }

    pub async fn handle_payload(
        &self,
        payload: &Value,
        stop: &watch::Receiver<bool>,
    ) -> Result<EmbedOutcome, ChunkEmbedderError> {
        let started = Instant::now();
        let document_id = payload
            .get("document_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| ChunkEmbedderError::MissingField {
                field: "document_id".into(),
            })?;
        let result = self.embed_document(document_id, stop).await;
        self.metrics.observe_processing_latency_ms(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        if result.is_err() {
            self.metrics.inc_failed_jobs(1);
            self.metrics.inc("osint_chunk_embed_failed_total", 1);
        }
        result
    }

    pub async fn embed_document(
        &self,
        document_id: Uuid,
        stop: &watch::Receiver<bool>,
    ) -> Result<EmbedOutcome, ChunkEmbedderError> {
        let document = self.store.get_document(document_id).await?;
        let chunks = match &document {
            Some(d) if d.duplicate_of.is_none() => {
                self.store.list_document_chunks(document_id).await?
            }
            _ => Vec::new(),
        };
        let Some(document) = document.filter(|d| d.duplicate_of.is_none() && !chunks.is_empty())
        else {
            let removed = self.remove_stale(document_id, &BTreeSet::new()).await?;
            self.metrics.inc("osint_chunk_embed_cleared_total", 1);
            return Ok(EmbedOutcome::Cleared {
                document_id,
                removed,
            });
        };
        let collection_ids = self.store.document_collection_ids(document_id).await?;

        let mut written = 0usize;
        for batch in chunks.chunks(self.config.batch_size.max(1)) {
            let inputs: Vec<String> = batch
                .iter()
                .map(|c| {
                    format!(
                        "{PASSAGE_PREFIX}{}",
                        embedding_text(c.header.as_deref(), &c.text)
                    )
                })
                .collect();
            let (model, vectors) = self.infer_with_backoff(&inputs, stop).await?;
            let docs: Vec<SearchDocument> = batch
                .iter()
                .zip(vectors)
                .map(|(c, v)| SearchDocument {
                    index: self.config.index.clone(),
                    id: c.id.to_string(),
                    body: chunk_body(c, &document, &collection_ids, &model, v),
                })
                .collect();
            let result = self.search.bulk_index(docs).await?;
            if result.errors > 0 {
                let first = result
                    .failures
                    .first()
                    .map(|f| format!("{f:?}"))
                    .unwrap_or_default();
                return Err(ChunkEmbedderError::Bulk {
                    document_id,
                    errors: result.errors,
                    first,
                });
            }
            written += batch.len();
        }
        let keep: BTreeSet<String> = chunks.iter().map(|c| c.id.to_string()).collect();
        let removed = self.remove_stale(document_id, &keep).await?;
        self.metrics.inc("osint_chunk_embed_documents_total", 1);
        self.metrics
            .inc("osint_chunk_embed_vectors_total", written as u64);
        if removed > 0 {
            self.metrics
                .inc("osint_chunk_embed_removed_total", removed as u64);
        }
        Ok(EmbedOutcome::Embedded {
            document_id,
            chunks: written,
            removed,
        })
    }

    /// 暫時性失敗無限退避重試（收到停止訊號就回錯誤，事件不提交、下次重送）；永久性失敗直接回。
    async fn infer_with_backoff(
        &self,
        inputs: &[String],
        stop: &watch::Receiver<bool>,
    ) -> Result<(String, Vec<Vec<f32>>), ChunkEmbedderError> {
        let mut delay = self.config.backoff_initial;
        loop {
            if *stop.borrow() {
                return Err(ChunkEmbedderError::Stopped);
            }
            let started = Instant::now();
            match self
                .inference
                .embed(inputs, schema::DIMENSIONS as usize)
                .await
            {
                Ok(out) => {
                    self.observe_batch(started.elapsed()).await;
                    return Ok(out);
                }
                Err(InferenceError::Retryable {
                    reason,
                    retry_after,
                }) => {
                    self.metrics.inc("osint_chunk_embed_retries_total", 1);
                    let wait = retry_after.unwrap_or(delay).max(delay);
                    tracing::warn!(%reason, wait_ms = wait.as_millis() as u64, "推論暫時不可用，退避後重試同一批");
                    let mut stop = stop.clone();
                    tokio::select! {
                        _ = tokio::time::sleep(wait) => {}
                        _ = stop.changed() => return Err(ChunkEmbedderError::Stopped),
                    }
                    delay = (delay * 2).min(self.config.backoff_max);
                }
                Err(err @ InferenceError::Permanent { .. }) => return Err(err.into()),
            }
        }
    }

    /// 單批明顯變慢（> 基準 × slow_factor）就暫停一下，讓出 CPU 給前景工作。
    /// 基準是指數移動平均（α＝0.1），慢批不併入基準——否則持續變慢時基準跟著升、永遠不觸發。
    async fn observe_batch(&self, elapsed: Duration) {
        let ms = u64::try_from(elapsed.as_millis())
            .unwrap_or(u64::MAX)
            .max(1);
        let base = self.baseline_ms.load(Ordering::Relaxed);
        if base == 0 {
            self.baseline_ms.store(ms, Ordering::Relaxed);
            return;
        }
        if (ms as f64) > (base as f64) * self.config.slow_factor {
            self.metrics.inc("osint_chunk_embed_slow_pauses_total", 1);
            tracing::info!(
                batch_ms = ms,
                baseline_ms = base,
                "推論批次明顯變慢，暫停讓出 CPU"
            );
            tokio::time::sleep(self.config.slow_pause).await;
            return;
        }
        let next = ((base as f64) * 0.9 + (ms as f64) * 0.1).round() as u64;
        self.baseline_ms.store(next.max(1), Ordering::Relaxed);
    }

    /// 刪掉 index 裡屬於 `document_id`、但不在 `keep` 裡的向量。回刪掉幾筆。
    async fn remove_stale(
        &self,
        document_id: Uuid,
        keep: &BTreeSet<String>,
    ) -> Result<usize, ChunkEmbedderError> {
        let hits = self
            .search
            .search(StructuredSearch {
                index: self.config.index.clone(),
                expression: None,
                fields: Vec::new(),
                filters: vec![SearchFilter::Term {
                    field: schema::F_DOCUMENT_ID.into(),
                    value: document_id.to_string(),
                }],
                size: MAX_EXISTING_PER_DOCUMENT,
                search_after: None,
                sort: vec![SortField {
                    field: schema::F_CHUNK_ID.into(),
                    ascending: true,
                }],
                highlight_fields: Vec::new(),
                highlight_expression: None,
                normalized: None,
            })
            .await;
        let hits = match hits {
            Ok(h) => h,
            // index 還不存在（第一次跑、或剛 --drop）：沒有舊向量可刪。
            Err(storage_core::StorageError::NotFound { .. }) => return Ok(0),
            Err(err) => return Err(err.into()),
        };
        let mut removed = 0;
        for hit in hits.hits {
            if !keep.contains(&hit.id) && self.search.delete(&self.config.index, &hit.id).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// 依段落表分頁重建全部向量。不發事件、不消費事件；頁與頁之間檢查停止訊號。
    pub async fn rebuild(
        &self,
        page_size: u32,
        stop: &watch::Receiver<bool>,
    ) -> Result<RebuildReport, ChunkEmbedderError> {
        let mut report = RebuildReport::default();
        let mut after = None;
        loop {
            if *stop.borrow() {
                report.interrupted = true;
                break;
            }
            let ids = self
                .store
                .list_chunked_document_ids(after, page_size)
                .await?;
            let Some(last) = ids.last() else { break };
            after = Some(*last);
            for id in ids {
                match self.embed_document(id, stop).await {
                    Ok(EmbedOutcome::Embedded { chunks, .. }) => {
                        report.documents += 1;
                        report.chunks += chunks as u64;
                    }
                    Ok(EmbedOutcome::Cleared { .. }) => {}
                    Err(ChunkEmbedderError::Stopped) => {
                        report.interrupted = true;
                        return Ok(report);
                    }
                    Err(err) => {
                        report.failed += 1;
                        tracing::error!(document_id = %id, error = %err, "rebuild：這份文件向量化失敗，繼續下一份");
                    }
                }
            }
            tracing::info!(
                documents = report.documents,
                chunks = report.chunks,
                "rebuild 進度"
            );
        }
        Ok(report)
    }
}

fn chunk_body(
    chunk: &DocumentChunk,
    document: &Document,
    collection_ids: &[Uuid],
    model: &str,
    vector: Vec<f32>,
) -> Value {
    json!({
        schema::F_CHUNK_ID: chunk.id,
        schema::F_DOCUMENT_ID: chunk.document_id,
        schema::F_SEQ: chunk.seq,
        schema::F_TEXT: chunk.text,
        schema::F_TEXT_S: indexer::script::to_simplified(&chunk.text),
        schema::F_HEADER: chunk.header,
        schema::F_EMBEDDING: vector,
        schema::F_MODEL: model,
        schema::F_STRATEGY: chunk.strategy,
        schema::F_STRATEGY_VERSION: chunk.strategy_version,
        schema::F_TOKEN_COUNT: chunk.token_count,
        schema::F_COLLECTION_IDS: collection_ids,
        schema::F_OBJECT_TYPE: document.object_type,
        schema::F_LANGUAGE: document.language,
        schema::F_PUBLISHED_AT: document.published_at,
        schema::F_COLLECTED_AT: document.collected_at,
        schema::F_TEXT_NORM_VERSION: indexer::script::TEXT_NORM_VERSION,
        schema::F_INDEXED_AT: Utc::now(),
    })
}
