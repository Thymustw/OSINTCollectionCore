//! `entity.extracted` → Document／Entity 向量投影。
//!
//! # 為什麼事件內容不可信
//!
//! payload 只告訴我們「哪一份 Document、哪些 Entity」。文字、語言、
//! `merged_into`／`duplicate_of` 一律重讀 PostgreSQL——那才是 canonical。
//!
//! # 冪等
//!
//! re-generate 的唯一 gate 是 [`RelationalStore::find_embedding`]
//! （同一目標、同一模型、同一內容雜湊）。`put_embedding` 撞 UNIQUE 回
//! Conflict，當成功吞掉——兩個 consumer 同時算到同一段文字時會發生，
//! 不是錯誤。OpenSearch 寫入：Document 走 `update_fields`（疊加）、
//! Entity 走 `index`（本 worker 擁有 `osint-entities`，覆寫沒問題）。
//!
//! # indexer race
//!
//! indexer 與本服務是同一個 `entity.extracted` topic 上的獨立 consumer
//! group，沒有順序保證。`update_fields` 回 NotFound 代表 indexer 還沒
//! 把那份文件寫進 `osint-documents`。重試後仍失敗就記 log／metrics，
//! **仍提交 offset**（與 indexer 永久性 bulk 失敗同一慣例；漏寫靠
//! `--rebuild` 回填）。卡住 partition 等 indexer 沒有意義——indexer
//! 可能永遠寫不進去（mapping 不符），本服務不該跟著卡死。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use core_model::{Document, Embedding, EmbeddingTarget, Entity, EntityId};
use core_observability::MetricsRegistry;
use indexer::schema as doc_schema;
use serde_json::{Value, json};
use storage_core::codec::encode_enum;
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, EmbeddingVector, RelationalStore,
    SearchDocument, SearchStore, StorageError, embedding_content_hash,
};
use tokio::sync::Semaphore;
use uuid::Uuid;

use crate::error::EmbeddingWorkerError;
use crate::schema as entity_schema;

pub const PROCESSOR: &str = "embedding-worker";

/// `update_fields` 遇到 NotFound 時，在初始嘗試之後再睡這三段再試。
/// 合計 4 次嘗試（1 次立即 + 3 次退避），給 indexer 時間把文件寫進去。
const DEFAULT_UPDATE_FIELDS_RETRY_BACKOFFS: [Duration; 3] = [
    Duration::from_millis(200),
    Duration::from_millis(500),
    Duration::from_millis(1000),
];

/// 一則 `entity.extracted` 處理完的結果。給呼叫端記 log 與 metrics。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ProcessReport {
    pub document_id: Option<Uuid>,
    pub title_applied: bool,
    pub body_applied: bool,
    pub title_skipped_cached: bool,
    pub body_skipped_cached: bool,
    pub entities_applied: u64,
    pub entities_skipped_merged: u64,
    pub entities_skipped_no_description: u64,
    pub entities_skipped_cached: u64,
    pub entities_missing: u64,
    pub document_missing: bool,
    pub document_duplicate: bool,
    pub update_fields_retries: u64,
    pub update_fields_exhausted: u64,
    pub conflicts: u64,
}

/// `--rebuild` 的選項。
///
/// 刪 index 的動作**不**在這裡——`SearchStore` 沒有 `delete_index`。
/// `main.rs` 在呼叫 [`EmbeddingWorker::rebuild`] 之前，若 `--drop` 就對
/// OpenSearch 刪 `osint-entities`。單元測試的 Mock 沒有真 index 可刪。
#[derive(Debug, Clone)]
pub struct RebuildOptions {
    /// 一次從 PostgreSQL 取幾筆（`RelationalStore` 夾在 1..=100）。
    pub page_size: u32,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self { page_size: 100 }
    }
}

/// 重建結果。
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RebuildReport {
    pub documents_scanned: u64,
    pub documents_skipped_duplicate: u64,
    pub entities_scanned: u64,
    pub entities_skipped_merged: u64,
    pub title_applied: u64,
    pub body_applied: u64,
    pub entities_applied: u64,
    pub failed: u64,
}

/// 推論批次與兩個 index 名稱。從 `[embedding]`／`[embedding_worker]`／`[indexer]` 組出來。
///
/// 獨立 struct 是為了讓 [`EmbeddingWorker::new`] 不超過 clippy 的參數上限；
/// 語意上這組本來就是「投影目的地 + 推論上限」，不該拆成八個位置參數。
#[derive(Debug, Clone)]
pub struct EmbeddingBounds {
    pub documents_index: String,
    pub entities_index: String,
    pub batch_size: usize,
    pub concurrent_inferences: usize,
}

impl EmbeddingBounds {
    #[must_use]
    pub fn new(
        documents_index: impl Into<String>,
        entities_index: impl Into<String>,
        batch_size: usize,
        concurrent_inferences: usize,
    ) -> Self {
        Self {
            documents_index: documents_index.into(),
            entities_index: entities_index.into(),
            batch_size: batch_size.max(1),
            concurrent_inferences: concurrent_inferences.max(1),
        }
    }
}

/// 生產用 embedding-worker。`R`／`E`／`S` 泛型是為了單元測試能注入
/// `SqliteEmbeddedStore` + `MockEmbeddingProvider` + `MockSearchStore`；
/// 生產路徑是 `PostgresCanonicalStore` + `MlCommonsEmbeddingProvider` +
/// `OpenSearchStore`。
#[derive(Clone)]
pub struct EmbeddingWorker<R, E, S> {
    store: R,
    embeddings: E,
    search: S,
    metrics: MetricsRegistry,
    documents_index: String,
    entities_index: String,
    batch_size: usize,
    concurrent_inferences: usize,
    /// `update_fields` NotFound 的退避。測試可設成空（只試一次）或全 0ms。
    update_fields_backoffs: Vec<Duration>,
}

impl<R, E, S> EmbeddingWorker<R, E, S> {
    #[must_use]
    pub fn new(
        store: R,
        embeddings: E,
        search: S,
        metrics: MetricsRegistry,
        bounds: EmbeddingBounds,
    ) -> Self {
        Self {
            store,
            embeddings,
            search,
            metrics,
            documents_index: bounds.documents_index,
            entities_index: bounds.entities_index,
            batch_size: bounds.batch_size,
            concurrent_inferences: bounds.concurrent_inferences,
            update_fields_backoffs: DEFAULT_UPDATE_FIELDS_RETRY_BACKOFFS.to_vec(),
        }
    }

    /// 測試用：把 NotFound 退避改成指定值（可為空＝只試一次、可全 0ms）。
    #[must_use]
    pub fn with_update_fields_backoffs(mut self, backoffs: Vec<Duration>) -> Self {
        self.update_fields_backoffs = backoffs;
        self
    }

    #[must_use]
    pub fn documents_index(&self) -> &str {
        &self.documents_index
    }

    #[must_use]
    pub fn entities_index(&self) -> &str {
        &self.entities_index
    }

    #[must_use]
    pub fn search(&self) -> &S {
        &self.search
    }

    #[must_use]
    pub fn store(&self) -> &R {
        &self.store
    }
}

/// 從 payload 抽出的目標。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedExtracted {
    pub document_id: Uuid,
    pub entity_ids: Vec<EntityId>,
}

pub fn parse_extracted(payload: &Value) -> Result<ParsedExtracted, EmbeddingWorkerError> {
    let document_id = payload
        .get("document_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .ok_or_else(|| EmbeddingWorkerError::MissingField {
            field: "document_id".into(),
        })?;
    let entity_ids = match payload.get("entity_ids") {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .filter_map(|s| Uuid::parse_str(s).ok())
            .collect(),
        None => Vec::new(),
        Some(_) => {
            return Err(EmbeddingWorkerError::MissingField {
                field: "entity_ids".into(),
            });
        }
    };
    Ok(ParsedExtracted {
        document_id,
        entity_ids,
    })
}

impl<R, E, S> EmbeddingWorker<R, E, S>
where
    R: RelationalStore,
    E: EmbeddingProvider,
    S: SearchStore,
{
    /// 從 `entity.extracted` payload 處理一則事件。
    pub async fn process_extracted(
        &self,
        payload: &Value,
    ) -> Result<ProcessReport, EmbeddingWorkerError> {
        let parsed = parse_extracted(payload)?;
        self.process_document_and_entities(parsed.document_id, &parsed.entity_ids)
            .await
    }

    async fn process_document_and_entities(
        &self,
        document_id: Uuid,
        entity_ids: &[EntityId],
    ) -> Result<ProcessReport, EmbeddingWorkerError> {
        let mut report = ProcessReport {
            document_id: Some(document_id),
            ..ProcessReport::default()
        };

        match self.store.get_document(document_id).await? {
            None => {
                tracing::error!(
                    %document_id,
                    "entity.extracted 對應的 Document 已不在 Postgres，跳過文件向量。\
                     請確認 upstream 沒有在發事件後刪文件；這則事件仍會提交 offset"
                );
                report.document_missing = true;
                self.metrics
                    .inc("osint_embedding_worker_document_missing_total", 1);
            }
            Some(doc) if doc.duplicate_of.is_some() => {
                tracing::debug!(
                    %document_id,
                    duplicate_of = ?doc.duplicate_of,
                    "Document 已被標成 duplicate，不寫向量。indexer 也不索引它"
                );
                report.document_duplicate = true;
                self.metrics
                    .inc("osint_embedding_worker_skipped_duplicate_total", 1);
            }
            Some(doc) => {
                self.embed_document_fields(&doc, &mut report, false).await?;
            }
        }

        for entity_id in entity_ids {
            self.embed_one_entity(*entity_id, &mut report, false)
                .await?;
        }

        self.record_process_metrics(&report);
        Ok(report)
    }

    async fn embed_document_fields(
        &self,
        doc: &Document,
        report: &mut ProcessReport,
        force: bool,
    ) -> Result<(), EmbeddingWorkerError> {
        let language = doc.language.as_deref();
        let model = self.embeddings.model_for(language);
        let (field, version_field) =
            if model.model.contains("MiniLM") || model.model.contains("all-MiniLM") {
                (
                    doc_schema::F_EMBEDDING_EN,
                    doc_schema::F_EMBEDDING_EN_MODEL_VERSION,
                )
            } else {
                (
                    doc_schema::F_EMBEDDING_MULTI,
                    doc_schema::F_EMBEDDING_MULTI_MODEL_VERSION,
                )
            };

        // title 先、body 後：`osint-documents` 只有一個向量欄位，後寫蓋前寫。
        // body 比較能代表整份文件；沒有 body 才留下 title 的向量。
        let mut pending: Vec<(EmbeddingTarget, String)> = Vec::new();
        if let Some(title) = doc.title.as_deref().filter(|s| !s.is_empty()) {
            if force {
                pending.push((EmbeddingTarget::DocumentTitle, title.to_string()));
            } else {
                match self
                    .should_embed(doc.id, EmbeddingTarget::DocumentTitle, &model.model, title)
                    .await?
                {
                    EmbedDecision::SkipCached => {
                        report.title_skipped_cached = true;
                        self.metrics
                            .inc("osint_embedding_worker_skipped_cached_total", 1);
                    }
                    EmbedDecision::Run => {
                        pending.push((EmbeddingTarget::DocumentTitle, title.to_string()))
                    }
                }
            }
        }
        if let Some(body) = doc.body.as_deref().filter(|s| !s.is_empty()) {
            if force {
                pending.push((EmbeddingTarget::DocumentBody, body.to_string()));
            } else {
                match self
                    .should_embed(doc.id, EmbeddingTarget::DocumentBody, &model.model, body)
                    .await?
                {
                    EmbedDecision::SkipCached => {
                        report.body_skipped_cached = true;
                        self.metrics
                            .inc("osint_embedding_worker_skipped_cached_total", 1);
                    }
                    EmbedDecision::Run => {
                        pending.push((EmbeddingTarget::DocumentBody, body.to_string()))
                    }
                }
            }
        }
        if pending.is_empty() {
            return Ok(());
        }

        let requests: Vec<EmbeddingRequest> = pending
            .iter()
            .map(|(_, text)| EmbeddingRequest {
                text: text.clone(),
                kind: EmbeddingKind::Passage,
                language: doc.language.clone(),
            })
            .collect();
        let vectors = self.embed_bounded(&requests).await?;
        if vectors.len() != pending.len() {
            return Err(EmbeddingWorkerError::Configuration {
                message: format!(
                    "embed_batch 回了 {} 筆、送出 {} 筆。請檢查 EmbeddingProvider 實作有沒有丟項目",
                    vectors.len(),
                    pending.len()
                ),
            });
        }

        let mut overlay = serde_json::Map::new();
        for ((target, _), vector) in pending.iter().zip(vectors.iter()) {
            self.persist_embedding_record(doc.id, *target, vector, report)
                .await?;
            overlay.insert(field.to_string(), json!(vector.vector));
            overlay.insert(version_field.to_string(), json!(vector.model_version));
            match target {
                EmbeddingTarget::DocumentTitle => report.title_applied = true,
                EmbeddingTarget::DocumentBody => report.body_applied = true,
                _ => {}
            }
        }
        if !overlay.is_empty() {
            self.update_document_fields(doc.id, Value::Object(overlay), report)
                .await?;
        }
        Ok(())
    }

    async fn embed_one_entity(
        &self,
        entity_id: EntityId,
        report: &mut ProcessReport,
        force: bool,
    ) -> Result<(), EmbeddingWorkerError> {
        let Some(entity) = self.store.get_entity(entity_id).await? else {
            tracing::debug!(
                %entity_id,
                "entity.extracted 列出的 entity_id 已不在 Postgres，跳過"
            );
            report.entities_missing += 1;
            self.metrics
                .inc("osint_embedding_worker_entity_missing_total", 1);
            return Ok(());
        };
        if let Some(merged_into) = entity.merged_into {
            tracing::debug!(
                %entity_id,
                %merged_into,
                "Entity 已被 merge，跳過向量。payload 可能含之後被併掉的 id"
            );
            report.entities_skipped_merged += 1;
            self.metrics
                .inc("osint_embedding_worker_skipped_merged_total", 1);
            return Ok(());
        }
        let Some(description) = entity
            .description
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(str::to_string)
        else {
            tracing::debug!(%entity_id, "Entity 沒有 description，跳過向量");
            report.entities_skipped_no_description += 1;
            self.metrics
                .inc("osint_embedding_worker_skipped_no_description_total", 1);
            return Ok(());
        };

        // Entity 沒有 language。None = 未知 → 多語 e5，不是 MiniLM。
        let model = self.embeddings.model_for(None);
        if !force {
            match self
                .should_embed(
                    entity.id,
                    EmbeddingTarget::EntityDescription,
                    &model.model,
                    &description,
                )
                .await?
            {
                EmbedDecision::SkipCached => {
                    report.entities_skipped_cached += 1;
                    self.metrics
                        .inc("osint_embedding_worker_skipped_cached_total", 1);
                    return Ok(());
                }
                EmbedDecision::Run => {}
            }
        }

        let request = EmbeddingRequest {
            text: description,
            kind: EmbeddingKind::Passage,
            language: None,
        };
        let vectors = self.embed_bounded(&[request]).await?;
        let Some(vector) = vectors.into_iter().next() else {
            return Err(EmbeddingWorkerError::Configuration {
                message: "embed_batch 對單筆 Entity description 回了空 vec".into(),
            });
        };
        self.persist_embedding_record(
            entity.id,
            EmbeddingTarget::EntityDescription,
            &vector,
            report,
        )
        .await?;
        self.index_entity(&entity, &vector).await?;
        report.entities_applied += 1;
        self.metrics
            .inc("osint_embedding_worker_entity_applied_total", 1);
        Ok(())
    }

    async fn should_embed(
        &self,
        target_id: Uuid,
        target_type: EmbeddingTarget,
        model: &str,
        text: &str,
    ) -> Result<EmbedDecision, EmbeddingWorkerError> {
        let hash = embedding_content_hash(text);
        match self
            .store
            .find_embedding(target_id, target_type, model, &hash)
            .await?
        {
            Some(_) => Ok(EmbedDecision::SkipCached),
            None => Ok(EmbedDecision::Run),
        }
    }

    async fn persist_embedding_record(
        &self,
        target_id: Uuid,
        target_type: EmbeddingTarget,
        vector: &EmbeddingVector,
        report: &mut ProcessReport,
    ) -> Result<(), EmbeddingWorkerError> {
        let record = Embedding {
            id: Uuid::now_v7(),
            target_id,
            target_type,
            model: vector.model.clone(),
            model_version: vector.model_version.clone(),
            dimensions: i32::try_from(vector.dimensions).unwrap_or(i32::MAX),
            content_hash: vector.content_hash.clone(),
            created_at: Utc::now(),
        };
        match self.store.put_embedding(&record).await {
            Ok(()) => Ok(()),
            Err(StorageError::Conflict { .. }) => {
                report.conflicts += 1;
                self.metrics.inc("osint_embedding_worker_conflict_total", 1);
                tracing::debug!(
                    %target_id,
                    ?target_type,
                    "put_embedding 撞 UNIQUE（另一個 worker 剛寫完同一段文字），當成功"
                );
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn update_document_fields(
        &self,
        document_id: Uuid,
        fields: Value,
        report: &mut ProcessReport,
    ) -> Result<(), EmbeddingWorkerError> {
        let id = document_id.to_string();
        let mut attempt = 0u32;
        loop {
            match self
                .search
                .update_fields(&self.documents_index, &id, fields.clone())
                .await
            {
                Ok(()) => {
                    self.metrics
                        .inc("osint_embedding_worker_document_applied_total", 1);
                    return Ok(());
                }
                Err(StorageError::NotFound { message }) => {
                    if attempt < self.update_fields_backoffs.len() as u32 {
                        let sleep_for = self.update_fields_backoffs[attempt as usize];
                        tracing::debug!(
                            %document_id,
                            attempt = attempt + 1,
                            sleep_ms = sleep_for.as_millis() as u64,
                            %message,
                            "osint-documents 還沒有這份文件（等 indexer），稍後重試"
                        );
                        if !sleep_for.is_zero() {
                            tokio::time::sleep(sleep_for).await;
                        }
                        attempt += 1;
                        report.update_fields_retries += 1;
                        self.metrics
                            .inc("osint_embedding_worker_update_fields_retry_total", 1);
                        continue;
                    }
                    report.update_fields_exhausted += 1;
                    self.metrics
                        .inc("osint_embedding_worker_update_fields_exhausted_total", 1);
                    tracing::error!(
                        %document_id,
                        index = %self.documents_index,
                        %message,
                        "update_fields 重試耗盡仍 NotFound。向量 metadata 已寫進 Postgres，\
                         但 osint-documents 沒疊上向量欄位。請確認 indexer 有沒有寫這份文件；\
                         修好後跑 osint-embedding-worker --rebuild 回填。本則仍提交 offset"
                    );
                    return Ok(());
                }
                Err(err) => return Err(err.into()),
            }
        }
    }

    async fn index_entity(
        &self,
        entity: &Entity,
        vector: &EmbeddingVector,
    ) -> Result<(), EmbeddingWorkerError> {
        let entity_type = encode_enum(&entity.entity_type)?;
        let body = json!({
            entity_schema::F_ENTITY_ID: entity.id,
            entity_schema::F_ENTITY_TYPE: entity_type,
            entity_schema::F_NAME: entity.name,
            entity_schema::F_NORMALIZED_NAME: entity.normalized_name,
            entity_schema::F_DESCRIPTION_VECTOR_MULTI: vector.vector,
            entity_schema::F_DESCRIPTION_VECTOR_MULTI_MODEL_VERSION: vector.model_version,
        });
        self.search
            .index(SearchDocument {
                index: self.entities_index.clone(),
                id: entity.id.to_string(),
                body,
            })
            .await?;
        Ok(())
    }

    async fn embed_bounded(
        &self,
        requests: &[EmbeddingRequest],
    ) -> Result<Vec<EmbeddingVector>, EmbeddingWorkerError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let semaphore = Arc::new(Semaphore::new(self.concurrent_inferences));
        let mut out = Vec::with_capacity(requests.len());
        for chunk in requests.chunks(self.batch_size) {
            let _permit =
                semaphore
                    .acquire()
                    .await
                    .map_err(|err| EmbeddingWorkerError::Configuration {
                        message: format!("推論 semaphore 關閉：{err}"),
                    })?;
            let batch = self.embeddings.embed_batch(chunk).await?;
            out.extend(batch);
        }
        Ok(out)
    }

    fn record_process_metrics(&self, report: &ProcessReport) {
        if report.title_applied {
            self.metrics
                .inc("osint_embedding_worker_title_applied_total", 1);
        }
        if report.body_applied {
            self.metrics
                .inc("osint_embedding_worker_body_applied_total", 1);
        }
    }

    /// 從 PostgreSQL 掃非 duplicate Documents 與非 merged Entities，補齊向量。
    ///
    /// `--drop` 只刪 `osint-entities`，而且是 `main.rs` 在呼叫本方法**之前**做。
    /// Document 向量靠 `update_fields` 疊加，不需要、也**不可以**刪
    /// `osint-documents`。
    ///
    /// rebuild **略過** `find_embedding` cache：Postgres `embeddings` 表只存
    /// metadata，沒有向量本體。indexer `--rebuild --drop` 之後 `osint-documents`
    /// 是空的，cache hit 會讓向量永遠回不去 OpenSearch。
    pub async fn rebuild(
        &self,
        options: RebuildOptions,
    ) -> Result<RebuildReport, EmbeddingWorkerError> {
        let page = options.page_size.clamp(1, 100);
        let mut report = RebuildReport::default();

        let mut after_doc = None;
        loop {
            let page_docs = self.store.list_documents(after_doc, page).await?;
            if page_docs.is_empty() {
                break;
            }
            after_doc = page_docs.last().map(|d| d.id);
            for doc in page_docs {
                report.documents_scanned += 1;
                if doc.duplicate_of.is_some() {
                    report.documents_skipped_duplicate += 1;
                    continue;
                }
                let mut one = ProcessReport::default();
                match self.embed_document_fields(&doc, &mut one, true).await {
                    Ok(()) => {
                        if one.title_applied {
                            report.title_applied += 1;
                        }
                        if one.body_applied {
                            report.body_applied += 1;
                        }
                    }
                    Err(err) => {
                        report.failed += 1;
                        tracing::error!(
                            error = %err,
                            document_id = %doc.id,
                            "rebuild 寫 Document 向量失敗"
                        );
                    }
                }
            }
        }

        let mut after_ent = None;
        loop {
            let page_ents = self.store.list_entities(after_ent, page).await?;
            if page_ents.is_empty() {
                break;
            }
            after_ent = page_ents.last().map(|e| e.id);
            for entity in page_ents {
                report.entities_scanned += 1;
                if entity.merged_into.is_some() {
                    report.entities_skipped_merged += 1;
                    continue;
                }
                let mut one = ProcessReport::default();
                match self.embed_one_entity(entity.id, &mut one, true).await {
                    Ok(()) => {
                        report.entities_applied += one.entities_applied;
                    }
                    Err(err) => {
                        report.failed += 1;
                        tracing::error!(
                            error = %err,
                            entity_id = %entity.id,
                            "rebuild 寫 Entity 向量失敗"
                        );
                    }
                }
            }
        }

        Ok(report)
    }
}

enum EmbedDecision {
    Run,
    SkipCached,
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use chrono::{TimeZone, Utc};
    use core_model::{Document, DocumentType, Entity, EntityType};
    use storage_core::conformance::find_workspace_root;
    use storage_core::mock::{
        MOCK_E5_MODEL, MOCK_MINILM_MODEL, MockEmbeddingProvider, MockSearchStore,
    };
    use storage_core::{RelationalStore, SearchStore};
    use storage_sqlite::SqliteEmbeddedStore;

    use super::*;

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 15, 10, 0, 0).unwrap()
    }

    fn document(title: Option<&str>, body: Option<&str>, language: Option<&str>) -> Document {
        Document {
            id: Uuid::now_v7(),
            object_type: DocumentType::Article,
            schema_version: "1".into(),
            title: title.map(str::to_string),
            body: body.map(str::to_string),
            summary: None,
            language: language.map(str::to_string),
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
        }
    }

    fn entity(name: &str, description: Option<&str>) -> Entity {
        Entity {
            id: Uuid::now_v7(),
            entity_type: EntityType::Organization,
            name: name.into(),
            normalized_name: name.to_ascii_lowercase(),
            description: description.map(str::to_string),
            confidence: 0.9,
            first_seen: ts(),
            last_seen: ts(),
            merged_into: None,
            attributes: json!({}),
        }
    }

    struct Harness {
        worker: EmbeddingWorker<SqliteEmbeddedStore, MockEmbeddingProvider, MockSearchStore>,
        db: SqliteEmbeddedStore,
        search: MockSearchStore,
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
        let path: PathBuf = root.join(format!(
            "var/osint-embedding-worker-{}.sqlite",
            Uuid::now_v7()
        ));
        cleanup(&path);
        let writer = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite writer");
        writer.migrate().await.expect("migrate");
        let db = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite reader");
        let search = MockSearchStore::new();
        let worker = EmbeddingWorker::new(
            writer,
            MockEmbeddingProvider::new(),
            search,
            MetricsRegistry::new(),
            EmbeddingBounds::new("osint-documents", "osint-entities", 32, 4),
        )
        .with_update_fields_backoffs(vec![Duration::from_millis(0); 3]);
        let search = worker.search().clone();
        Harness {
            worker,
            db,
            search,
            path,
        }
    }

    fn payload(doc_id: Uuid, entity_ids: &[Uuid]) -> Value {
        json!({
            "document_id": doc_id,
            "entity_ids": entity_ids,
        })
    }

    #[test]
    fn payload_without_document_id_is_an_error() {
        let err = parse_extracted(&json!({"entity_ids": []})).unwrap_err();
        assert!(matches!(err, EmbeddingWorkerError::MissingField { .. }));
        assert!(err.to_string().contains("document_id"), "{err}");
    }

    #[tokio::test]
    async fn english_document_writes_embedding_en() {
        let h = open_harness().await;
        let doc = document(Some("Hello title"), Some("Hello body"), Some("en"));
        h.db.put_document(&doc).await.unwrap();
        h.search
            .index(SearchDocument {
                index: "osint-documents".into(),
                id: doc.id.to_string(),
                body: json!({"title": "Hello title", "body": "Hello body"}),
            })
            .await
            .unwrap();

        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[]))
            .await
            .unwrap();
        assert!(report.title_applied);
        assert!(report.body_applied);

        let got = h
            .search
            .get("osint-documents", &doc.id.to_string())
            .unwrap()
            .unwrap();
        assert!(got.get(doc_schema::F_EMBEDDING_EN).is_some());
        assert!(got.get(doc_schema::F_EMBEDDING_MULTI).is_none());
        assert_eq!(got["title"], "Hello title", "update_fields 不可覆寫本體");

        let found =
            h.db.find_embedding(
                doc.id,
                EmbeddingTarget::DocumentTitle,
                MOCK_MINILM_MODEL,
                &embedding_content_hash("Hello title"),
            )
            .await
            .unwrap();
        assert!(found.is_some());
    }

    #[tokio::test]
    async fn unknown_language_writes_embedding_multi() {
        let h = open_harness().await;
        let doc = document(Some("標題"), Some("內文"), Some("zh"));
        h.db.put_document(&doc).await.unwrap();
        h.search
            .index(SearchDocument {
                index: "osint-documents".into(),
                id: doc.id.to_string(),
                body: json!({"title": "標題"}),
            })
            .await
            .unwrap();

        h.worker
            .process_extracted(&payload(doc.id, &[]))
            .await
            .unwrap();
        let got = h
            .search
            .get("osint-documents", &doc.id.to_string())
            .unwrap()
            .unwrap();
        assert!(got.get(doc_schema::F_EMBEDDING_MULTI).is_some());
        assert!(got.get(doc_schema::F_EMBEDDING_EN).is_none());
    }

    #[tokio::test]
    async fn find_embedding_skips_regenerate() {
        let h = open_harness().await;
        let doc = document(Some("cached"), Some("cached-body"), Some("en"));
        h.db.put_document(&doc).await.unwrap();
        h.search
            .index(SearchDocument {
                index: "osint-documents".into(),
                id: doc.id.to_string(),
                body: json!({"title": "cached"}),
            })
            .await
            .unwrap();
        h.worker
            .process_extracted(&payload(doc.id, &[]))
            .await
            .unwrap();
        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[]))
            .await
            .unwrap();
        assert!(report.title_skipped_cached);
        assert!(report.body_skipped_cached);
        assert!(!report.title_applied);
        assert!(!report.body_applied);
    }

    #[tokio::test]
    async fn merged_entity_is_skipped() {
        let h = open_harness().await;
        let doc = document(None, None, Some("en"));
        h.db.put_document(&doc).await.unwrap();
        // `merged_into` 是 FK，必須先有 survivor，否則 SQLite 回 787。
        let survivor = entity("survivor", Some("kept"));
        h.db.put_entity(&survivor).await.unwrap();
        let mut ent = entity("acme", Some("a company"));
        ent.merged_into = Some(survivor.id);
        h.db.put_entity(&ent).await.unwrap();

        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[ent.id]))
            .await
            .unwrap();
        assert_eq!(report.entities_skipped_merged, 1);
        assert_eq!(report.entities_applied, 0);
        assert!(
            h.search
                .get("osint-entities", &ent.id.to_string())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn entity_without_description_is_skipped() {
        let h = open_harness().await;
        let doc = document(None, None, None);
        h.db.put_document(&doc).await.unwrap();
        let ent = entity("bare", None);
        h.db.put_entity(&ent).await.unwrap();

        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[ent.id]))
            .await
            .unwrap();
        assert_eq!(report.entities_skipped_no_description, 1);
        assert!(
            h.search
                .get("osint-entities", &ent.id.to_string())
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn entity_description_writes_multi_never_en() {
        let h = open_harness().await;
        let doc = document(None, None, None);
        h.db.put_document(&doc).await.unwrap();
        let ent = entity("acme", Some("A software company in Taipei"));
        h.db.put_entity(&ent).await.unwrap();

        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[ent.id]))
            .await
            .unwrap();
        assert_eq!(report.entities_applied, 1);
        let got = h
            .search
            .get("osint-entities", &ent.id.to_string())
            .unwrap()
            .unwrap();
        assert!(got.get(entity_schema::F_DESCRIPTION_VECTOR_MULTI).is_some());
        assert!(
            got.get(entity_schema::F_DESCRIPTION_VECTOR_EN).is_none(),
            "V0.2 Entity 語言未知，不可寫 en 欄"
        );
        let found =
            h.db.find_embedding(
                ent.id,
                EmbeddingTarget::EntityDescription,
                MOCK_E5_MODEL,
                &embedding_content_hash("A software company in Taipei"),
            )
            .await
            .unwrap();
        assert!(found.is_some());
    }

    #[tokio::test]
    async fn update_fields_retries_then_succeeds() {
        let h = open_harness().await;
        let doc = document(Some("race"), None, Some("en"));
        h.db.put_document(&doc).await.unwrap();
        h.search
            .index(SearchDocument {
                index: "osint-documents".into(),
                id: doc.id.to_string(),
                body: json!({"title": "race"}),
            })
            .await
            .unwrap();
        h.search
            .set_update_not_found_remaining("osint-documents", &doc.id.to_string(), 2)
            .unwrap();

        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[]))
            .await
            .unwrap();
        assert_eq!(report.update_fields_retries, 2);
        assert_eq!(report.update_fields_exhausted, 0);
        assert!(report.title_applied);
        let got = h
            .search
            .get("osint-documents", &doc.id.to_string())
            .unwrap()
            .unwrap();
        assert!(got.get(doc_schema::F_EMBEDDING_EN).is_some());
    }

    #[tokio::test]
    async fn update_fields_exhausted_still_ok() {
        let h = open_harness().await;
        let doc = document(Some("gone"), None, Some("en"));
        h.db.put_document(&doc).await.unwrap();
        // 故意不 index 進 MockSearchStore，且 backoffs 長度 3 → 4 次 NotFound。
        let report = h
            .worker
            .process_extracted(&payload(doc.id, &[]))
            .await
            .unwrap();
        assert_eq!(report.update_fields_exhausted, 1);
        assert!(report.title_applied, "metadata 仍應寫進 Postgres");
        let found =
            h.db.find_embedding(
                doc.id,
                EmbeddingTarget::DocumentTitle,
                MOCK_MINILM_MODEL,
                &embedding_content_hash("gone"),
            )
            .await
            .unwrap();
        assert!(found.is_some(), "耗盡後 metadata 必須還在");
    }

    #[tokio::test]
    async fn missing_document_is_skipped_not_error() {
        let h = open_harness().await;
        let missing = Uuid::now_v7();
        let report = h
            .worker
            .process_extracted(&payload(missing, &[]))
            .await
            .unwrap();
        assert!(report.document_missing);
    }

    #[tokio::test]
    async fn rebuild_skips_duplicate_and_merged() {
        let h = open_harness().await;
        let canonical = document(Some("canon"), None, Some("en"));
        let mut dup = document(Some("dup"), None, Some("en"));
        dup.duplicate_of = Some(canonical.id);
        h.db.put_document(&canonical).await.unwrap();
        h.db.put_document(&dup).await.unwrap();
        h.search
            .index(SearchDocument {
                index: "osint-documents".into(),
                id: canonical.id.to_string(),
                body: json!({"title": "canon"}),
            })
            .await
            .unwrap();

        let live = entity("live", Some("desc"));
        let mut merged = entity("merged", Some("desc"));
        merged.merged_into = Some(live.id);
        h.db.put_entity(&live).await.unwrap();
        h.db.put_entity(&merged).await.unwrap();

        let report = h.worker.rebuild(RebuildOptions::default()).await.unwrap();
        assert_eq!(report.documents_skipped_duplicate, 1);
        assert_eq!(report.entities_skipped_merged, 1);
        assert_eq!(report.title_applied, 1);
        assert_eq!(report.entities_applied, 1);
        assert!(
            h.search
                .get("osint-entities", &merged.id.to_string())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn event_description_is_not_a_processed_target() {
        // 編譯期契約：V0.2 沒有 Event 抽取管線。這個測試存在是為了讓
        // 之後有人在 process 路徑加上 EventDescription 時，會先看到這段說明。
        let _ = EmbeddingTarget::EventDescription;
    }
}
