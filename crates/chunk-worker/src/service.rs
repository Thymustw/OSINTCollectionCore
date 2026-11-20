//! `dedup.completed` → 切段 → `document_chunks` → `document.chunked`。
//!
//! # 冪等
//!
//! 每次都重切，再和庫裡現有段落比（id／偏移／文字／header／token 數）。完全相同就不寫、
//! 不發事件——重送的 `dedup.completed` 不會讓下游（M2 向量化）重做一次。
//! 不同（第一次切、或策略版本升了）才 `replace_document_chunks`（同一交易刪舊寫新）再發事件。
//!
//! # 為什麼不像 entity-worker 用 provenance claim
//!
//! 段落是可整批重切的衍生資料（ADR-016 D5），「這份文件有沒有段落、是不是最新策略」
//! 本身就寫在 `document_chunks` 上；再寫一列 provenance 只是多一個會和它不一致的地方。

use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use chunker::{ChunkOutput, Chunker};
use core_events::{EventProducer, EventTopic};
use core_model::Document;
use core_observability::MetricsRegistry;
use serde_json::{Value, json};
use storage_core::{ChunkStore, DocumentChunk, RelationalStore};
use tokio::sync::watch;
use uuid::Uuid;

use crate::error::ChunkWorkerError;

/// 單份文件的處理結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkOutcome {
    /// 寫入了新段落並發了 `document.chunked`。
    Chunked {
        document_id: Uuid,
        chunk_count: usize,
        strategy: &'static str,
    },
    /// 重切結果與庫裡相同，沒寫也沒發事件。
    Unchanged { document_id: Uuid },
    /// 事件說是重複，或 DB 上 `duplicate_of` 已有值。
    SkippedDuplicate { document_id: Uuid },
    /// 事件提到的文件不在 Postgres。
    DocumentMissing { document_id: Uuid },
}

impl ChunkOutcome {
    #[must_use]
    pub fn document_id(&self) -> Uuid {
        match self {
            Self::Chunked { document_id, .. }
            | Self::Unchanged { document_id }
            | Self::SkippedDuplicate { document_id }
            | Self::DocumentMissing { document_id } => *document_id,
        }
    }
}

/// `--rebuild` 的統計。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RebuildReport {
    pub scanned: u64,
    pub chunked: u64,
    pub unchanged: u64,
    pub skipped_duplicate: u64,
    pub failed: u64,
    pub chunks_written: u64,
    /// 收到停止訊號而提前結束。
    pub interrupted: bool,
}

pub struct ChunkWorker<S> {
    store: S,
    chunker: Arc<Chunker>,
    producer: Option<Arc<EventProducer>>,
    metrics: MetricsRegistry,
}

impl<S> ChunkWorker<S>
where
    S: RelationalStore + ChunkStore + Clone + 'static,
{
    pub fn new(
        store: S,
        chunker: Arc<Chunker>,
        producer: Option<Arc<EventProducer>>,
        metrics: MetricsRegistry,
    ) -> Self {
        Self {
            store,
            chunker,
            producer,
            metrics,
        }
    }

    /// 處理一則 `dedup.completed` payload：`{ document_id, is_duplicate, ... }`。
    pub async fn handle_payload(&self, payload: &Value) -> Result<ChunkOutcome, ChunkWorkerError> {
        let started = Instant::now();
        let result = self.handle_payload_inner(payload).await;
        self.metrics.observe_processing_latency_ms(
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        );
        if result.is_err() {
            self.metrics.inc_failed_jobs(1);
            self.metrics.inc("osint_chunk_failed_total", 1);
        }
        result
    }

    async fn handle_payload_inner(
        &self,
        payload: &Value,
    ) -> Result<ChunkOutcome, ChunkWorkerError> {
        let document_id = payload
            .get("document_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| ChunkWorkerError::MissingField {
                field: "document_id".into(),
            })?;
        // 缺 is_duplicate 要報錯，不預設 false：預設 false 會讓所有重複文件都被切段，而且沒有任何跡象。
        let is_duplicate = payload
            .get("is_duplicate")
            .and_then(Value::as_bool)
            .ok_or_else(|| ChunkWorkerError::MissingField {
                field: "is_duplicate".into(),
            })?;
        if is_duplicate {
            self.metrics.inc("osint_chunk_skipped_duplicate_total", 1);
            return Ok(ChunkOutcome::SkippedDuplicate { document_id });
        }
        self.chunk_document_id(document_id, true).await
    }

    /// 讀文件、切段、與現有段落比對、必要時寫入（`publish` 為真才發事件）。
    pub async fn chunk_document_id(
        &self,
        document_id: Uuid,
        publish: bool,
    ) -> Result<ChunkOutcome, ChunkWorkerError> {
        let Some(document) = self.store.get_document(document_id).await? else {
            tracing::warn!(
                %document_id,
                "dedup.completed 提到的 Document 不在 Postgres，跳過不中斷消費"
            );
            self.metrics.inc("osint_chunk_document_missing_total", 1);
            return Ok(ChunkOutcome::DocumentMissing { document_id });
        };
        // 第二道防線：事件可能早於「被判成重複」，DB 上的 duplicate_of 才是事實。
        if document.duplicate_of.is_some() {
            self.metrics.inc("osint_chunk_skipped_duplicate_total", 1);
            return Ok(ChunkOutcome::SkippedDuplicate { document_id });
        }
        self.chunk_document(document, publish).await
    }

    async fn chunk_document(
        &self,
        document: Document,
        publish: bool,
    ) -> Result<ChunkOutcome, ChunkWorkerError> {
        let document_id = document.id;
        // tokenizer 是 CPU-bound（長文數十毫秒），不能卡住 Tokio executor（CLAUDE.md §5）。
        let chunker = Arc::clone(&self.chunker);
        let output: ChunkOutput =
            tokio::task::spawn_blocking(move || chunker.chunk_document(&document, Utc::now()))
                .await
                .map_err(|err| ChunkWorkerError::TaskFailed {
                    message: err.to_string(),
                })??;

        let existing = self.store.list_document_chunks(document_id).await?;
        if same_chunks(&existing, &output.chunks) {
            self.metrics.inc("osint_chunk_unchanged_total", 1);
            return Ok(ChunkOutcome::Unchanged { document_id });
        }

        self.store
            .replace_document_chunks(document_id, &output.chunks)
            .await?;
        self.record_metrics(&output);
        tracing::info!(
            %document_id,
            strategy = output.strategy.name(),
            chunks = output.chunks.len(),
            fell_back_to_fixed = output.fell_back_to_fixed,
            hard_cuts = output.hard_cuts,
            truncated = output.truncated,
            "段落已寫入"
        );
        if publish {
            self.publish(document_id, &output).await?;
        }
        Ok(ChunkOutcome::Chunked {
            document_id,
            chunk_count: output.chunks.len(),
            strategy: output.strategy.name(),
        })
    }

    fn record_metrics(&self, output: &ChunkOutput) {
        self.metrics.inc("osint_chunk_documents_total", 1);
        self.metrics.inc(
            "osint_chunk_chunks_written_total",
            output.chunks.len() as u64,
        );
        if output.fell_back_to_fixed {
            self.metrics.inc("osint_chunk_fallback_fixed_total", 1);
        }
        if output.hard_cuts > 0 {
            self.metrics
                .inc("osint_chunk_hard_cuts_total", u64::from(output.hard_cuts));
        }
        if output.truncated {
            self.metrics.inc("osint_chunk_truncated_total", 1);
        }
    }

    async fn publish(
        &self,
        document_id: Uuid,
        output: &ChunkOutput,
    ) -> Result<(), ChunkWorkerError> {
        let Some(producer) = &self.producer else {
            return Ok(());
        };
        let payload = json!({
            "document_id": document_id,
            "chunk_count": output.chunks.len(),
            "chunk_ids": output.chunks.iter().map(|c| c.id).collect::<Vec<_>>(),
            "strategy": output.strategy.name(),
            "strategy_version": output.strategy.version(),
            "max_token_count": output.chunks.iter().map(|c| c.token_count).max().unwrap_or(0),
            "truncated": output.truncated,
        });
        producer
            .publish(
                EventTopic::DocumentChunked,
                Some(&document_id.to_string()),
                None,
                payload,
            )
            .await?;
        Ok(())
    }

    /// 從 Postgres 掃全部非重複文件重切。**不發事件**：M2 的向量化自己從 PG 重建，
    /// 一次 rebuild 發上萬則事件只會把下游淹掉。
    ///
    /// 每頁之間檢查 `stop`；處理中的那一頁跑完才停（最多 `page_size` 份）。
    /// 單份失敗只計數、記 error，不中斷整個 rebuild。
    pub async fn rebuild(
        &self,
        page_size: u32,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<RebuildReport, ChunkWorkerError> {
        let mut report = RebuildReport::default();
        let mut after = None;
        loop {
            if *stop.borrow() {
                report.interrupted = true;
                break;
            }
            let page = self
                .store
                .list_documents_filtered(None, false, after, page_size)
                .await?;
            let Some(last) = page.last() else { break };
            after = Some(last.id);
            for document in page {
                report.scanned += 1;
                let document_id = document.id;
                if document.duplicate_of.is_some() {
                    report.skipped_duplicate += 1;
                    continue;
                }
                match self.chunk_document(document, false).await {
                    Ok(ChunkOutcome::Chunked { chunk_count, .. }) => {
                        report.chunked += 1;
                        report.chunks_written += chunk_count as u64;
                    }
                    Ok(ChunkOutcome::Unchanged { .. }) => report.unchanged += 1,
                    Ok(_) => report.skipped_duplicate += 1,
                    Err(err) => {
                        report.failed += 1;
                        tracing::error!(%document_id, error = %err, "rebuild：這份文件切段失敗，繼續下一份");
                    }
                }
            }
            tracing::info!(
                scanned = report.scanned,
                chunked = report.chunked,
                unchanged = report.unchanged,
                "rebuild 進度"
            );
        }
        Ok(report)
    }
}

/// 比對寫入會改變的欄位。`created_at` 不比（每次重切都不同）。
fn same_chunks(existing: &[DocumentChunk], fresh: &[DocumentChunk]) -> bool {
    existing.len() == fresh.len()
        && existing.iter().zip(fresh).all(|(a, b)| {
            a.id == b.id
                && a.start_char == b.start_char
                && a.end_char == b.end_char
                && a.text == b.text
                && a.header == b.header
                && a.token_count == b.token_count
                && a.strategy == b.strategy
                && a.strategy_version == b.strategy_version
        })
}
