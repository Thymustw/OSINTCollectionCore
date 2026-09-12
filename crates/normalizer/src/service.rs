//! 把一筆 RawEvidence 正規化成 Document。冪等：同一 raw_evidence_id 不重複寫。

use chrono::{DateTime, Utc};
use connector_rss::parse_feed;
use connector_static_web::parse_html;
use core_events::{EventProducer, EventTopic};
use core_model::{Document, DocumentType, Provenance, RawEvidence};
use core_observability::MetricsRegistry;
use import_format::{ImportKind, ImportSpec};
use serde_json::{Value, json};
use storage_core::{ObjectStore, RelationalStore, TransactionalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use uuid::Uuid;

use crate::content::{ContentClass, body_looks_like_feed, body_looks_like_html, classify_content};
use crate::error::NormalizerError;

pub const PROCESSOR: &str = "normalizer";
pub const ACTION_NORMALIZED: &str = "normalized";
pub const ACTION_DERIVED: &str = "derived_from";
pub const SCHEMA_VERSION: &str = "1";

/// 一次處理結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizeOutcome {
    Created { document_ids: Vec<Uuid> },
    AlreadyDone { document_ids: Vec<Uuid> },
    SkippedUnsupported { content_type: Option<String> },
    SkippedUnparseable { message: String },
}

/// 生產用 normalizer。
#[derive(Clone)]
pub struct Normalizer {
    store: PostgresCanonicalStore,
    objects: S3ObjectStore,
    producer: Option<std::sync::Arc<EventProducer>>,
    metrics: MetricsRegistry,
}

impl Normalizer {
    #[must_use]
    pub fn new(
        store: PostgresCanonicalStore,
        objects: S3ObjectStore,
        producer: Option<std::sync::Arc<EventProducer>>,
        metrics: MetricsRegistry,
    ) -> Self {
        Self {
            store,
            objects,
            producer,
            metrics,
        }
    }

    #[must_use]
    pub fn store(&self) -> &PostgresCanonicalStore {
        &self.store
    }

    /// 從 envelope payload 取出 raw_evidence_id。
    ///
    /// 這一層負責 SPEC §24 的兩個 metric：`processing latency`（處理一則事件的耗時）
    /// 與 `failed jobs`（處理失敗數）。**量在這裡而不是 `normalize_raw`**，
    /// 因為 §24 要的是「一則事件從拿到到處理完多久」，而 payload 解析失敗
    /// 同樣是一次失敗的處理，必須被算進去。
    pub async fn handle_payload(
        &self,
        payload: &Value,
    ) -> Result<NormalizeOutcome, NormalizerError> {
        let started = std::time::Instant::now();
        let result = self.handle_payload_inner(payload).await;
        self.metrics
            .observe_processing_latency_ms(started.elapsed().as_millis() as u64);
        if result.is_err() {
            self.metrics.inc_failed_jobs(1);
        }
        result
    }

    async fn handle_payload_inner(
        &self,
        payload: &Value,
    ) -> Result<NormalizeOutcome, NormalizerError> {
        let id = payload
            .get("raw_evidence_id")
            .and_then(Value::as_str)
            .ok_or_else(|| NormalizerError::MissingField {
                field: "raw_evidence_id".into(),
            })?;
        let id = Uuid::parse_str(id).map_err(|_| NormalizerError::MissingField {
            field: "raw_evidence_id".into(),
        })?;
        self.normalize_raw(id).await
    }

    /// 對一筆 RawEvidence 做正規化。重複呼叫只會留下同一組 Document。
    pub async fn normalize_raw(
        &self,
        raw_evidence_id: Uuid,
    ) -> Result<NormalizeOutcome, NormalizerError> {
        let existing = self
            .store
            .list_provenance_by_raw_evidence(raw_evidence_id)
            .await?;
        if let Some(done) = existing.iter().find(|p| p.action == ACTION_NORMALIZED) {
            let ids = document_ids_from(done);
            return Ok(NormalizeOutcome::AlreadyDone { document_ids: ids });
        }

        let evidence = self
            .store
            .get_raw_evidence(raw_evidence_id)
            .await?
            .ok_or_else(|| NormalizerError::EvidenceMissing {
                id: raw_evidence_id.to_string(),
            })?;

        let class = classify_content(
            evidence.content_type.as_deref(),
            evidence.mime_type.as_deref(),
            &evidence.storage_path,
        );
        let body = self
            .objects
            .get(&evidence.storage_path)
            .await?
            .ok_or_else(|| NormalizerError::BodyMissing {
                path: evidence.storage_path.clone(),
            })?;

        let now = Utc::now();

        // push 路徑（POST /api/v1/import）會把 ImportSpec 寫進 metadata。有 spec 才知道
        // 哪個鍵是 title、哪個是 body——同樣是 JSON，REST API 抓回來的那份沒有這個資訊，
        // 所以沒有 spec 的 JSON 仍然是 SkippedUnsupported，不是這裡少做了什麼。
        if let Some(spec) = import_spec(&evidence) {
            if spec.kind == ImportKind::Manual {
                tracing::info!(
                    raw_evidence_id = %evidence.id,
                    content_type = ?evidence.content_type,
                    "manual 上傳不拆 Document，跳過正規化"
                );
                return Ok(NormalizeOutcome::SkippedUnsupported {
                    content_type: evidence.content_type.clone(),
                });
            }
            let documents = match build_import_documents(&evidence, &body, &spec, now) {
                Ok(documents) => documents,
                Err(outcome) => return Ok(outcome),
            };
            return self.persist_documents(&evidence, documents, now).await;
        }

        let class = match class {
            ContentClass::Unsupported if body_looks_like_feed(&body) => ContentClass::RssOrAtom,
            ContentClass::Unsupported if body_looks_like_html(&body) => ContentClass::Html,
            other => other,
        };

        match class {
            ContentClass::Json | ContentClass::Csv | ContentClass::Unsupported => {
                tracing::info!(
                    raw_evidence_id = %evidence.id,
                    content_type = ?evidence.content_type,
                    "不支援的 content type，跳過正規化"
                );
                return Ok(NormalizeOutcome::SkippedUnsupported {
                    content_type: evidence.content_type.clone(),
                });
            }
            ContentClass::RssOrAtom | ContentClass::Html => {}
        }

        let items = match class {
            ContentClass::RssOrAtom => match parse_feed(&body) {
                Ok(items) => items,
                Err(err) => {
                    tracing::warn!(
                        raw_evidence_id = %evidence.id,
                        error = %err,
                        "feed-rs 無法解析，記錄後繼續消費下一則"
                    );
                    return Ok(NormalizeOutcome::SkippedUnparseable {
                        message: err.to_string(),
                    });
                }
            },
            ContentClass::Html => match parse_html(&body) {
                Ok(items) => items,
                Err(err) => {
                    tracing::warn!(
                        raw_evidence_id = %evidence.id,
                        error = %err,
                        "HTML 無法抽取，記錄後繼續消費下一則"
                    );
                    return Ok(NormalizeOutcome::SkippedUnparseable {
                        message: err.to_string(),
                    });
                }
            },
            // 上面那個 match 已經把這三種擋掉了，所以理論上到不了這裡。
            //
            // **但這裡刻意不寫 `unreachable!()`。** 這個函式處理的是外部內容：
            // 只要之後有人動了上面的分類邏輯（多一個 ContentClass、或把某個
            // early return 拿掉），panic 會沿著 consumer 迴圈炸掉整個服務——
            // 一則畸形的來源內容就能讓正規化停擺。回 SkippedUnsupported 的話
            // 最壞情況只是「這一則沒被處理」，而且 log 裡看得到。
            ContentClass::Json | ContentClass::Csv | ContentClass::Unsupported => {
                tracing::error!(
                    raw_evidence_id = %evidence.id,
                    ?class,
                    "分類邏輯不一致：這個 content class 不該走到解析階段。跳過這一則，\
                     請檢查 normalizer::service 的 ContentClass 判斷"
                );
                return Ok(NormalizeOutcome::SkippedUnsupported {
                    content_type: evidence.content_type.clone(),
                });
            }
        };

        let mut documents = Vec::new();
        for item in items {
            let title = item.title.clone();
            let summary = item.summary.clone();
            let body_text = item
                .attributes
                .get("body_text")
                .and_then(Value::as_str)
                .map(str::to_string);
            let source_url = item
                .url
                .clone()
                .or_else(|| Some(evidence.source_url.clone()));
            let content_hash = core_model::content_hash(
                title.as_deref(),
                summary.as_deref(),
                body_text.as_deref(),
            );
            let object_type = if class == ContentClass::Html {
                DocumentType::WebPage
            } else {
                DocumentType::Article
            };
            let doc = Document {
                id: Uuid::now_v7(),
                object_type,
                schema_version: SCHEMA_VERSION.into(),
                title,
                body: body_text,
                summary,
                language: None,
                author: None,
                published_at: item.published_at,
                modified_at: None,
                observed_at: now,
                collected_at: evidence.retrieved_at,
                source_url: source_url.clone(),
                canonical_url: source_url,
                normalized_content_hash: Some(content_hash),
                confidence: 0.8,
                labels: Vec::new(),
                attributes: json!({
                    "raw_evidence_id": evidence.id,
                    "external_id": item.external_id,
                    "feed_type": item.attributes.get("feed_type"),
                }),
                // dedup 欄位由 deduplicator 填。normalizer 不做去重判斷，
                // 也不要在這裡猜一個值——空值就是「還沒判斷過」的唯一表示法。
                external_key: None,
                simhash: None,
                duplicate_of: None,
            };
            documents.push(doc);
        }

        self.persist_documents(&evidence, documents, now).await
    }

    /// 寫 Document／provenance、佔 normalized claim、發 `object.normalized`。
    ///
    /// feed／HTML 與 JSON／CSV 匯入共用同一段：冪等保證只能有一份實作，
    /// 兩份遲早會分岔。
    async fn persist_documents(
        &self,
        evidence: &RawEvidence,
        documents: Vec<Document>,
        now: DateTime<Utc>,
    ) -> Result<NormalizeOutcome, NormalizerError> {
        let raw_evidence_id = evidence.id;
        // Document + derived_from + normalized claim 在**同一個交易**裡。
        //
        // V0.2 Phase 0e 之前這裡是 write-then-claim：先寫 Document，最後才佔 unique
        // index（action=normalized）。那個順序刻意接受一個 crash window——Document 寫完、
        // claim 前 crash，重跑會再產生一組重複 Document，留給 dedup pipeline 收。
        // `storage-core` 補上 `TransactionalStore` 之後那個取捨不再需要：
        // 中途死掉就整批回滾（連沒 commit 就 drop 都會回滾），
        // 狀態只剩「全在」或「全不在」，不會有重複也不會有靜默遺失。
        //
        // 併發雙寫（兩個 consumer 同時通過上面的「尚未正規化」檢查）由 claim 的 unique
        // index 決定勝負：輸的那一邊整個交易回滾，它寫的 Document 不會留下來。
        let tx = self.store.begin().await?;
        let db = tx.store();
        for doc in &documents {
            db.put_document(doc).await?;
            let prov = Provenance {
                id: Uuid::now_v7(),
                subject_id: doc.id,
                action: ACTION_DERIVED.into(),
                parent_id: Some(evidence.id),
                raw_evidence_id: Some(evidence.id),
                processor: PROCESSOR.into(),
                processor_version: env!("CARGO_PKG_VERSION").into(),
                timestamp: now,
                metadata: json!({ "document_id": doc.id }),
            };
            db.put_provenance(&prov).await?;
        }

        match claim_normalized(db, evidence, &documents, now).await {
            Ok(()) => tx.commit().await?,
            Err(NormalizerError::Storage(storage_core::StorageError::Conflict { .. })) => {
                // 別人先佔到 claim。整批回滾——這一輪寫的 Document 不留下來，
                // 否則就變回「重複 Document 要靠 dedup 收」的舊代價。
                // PostgreSQL 在錯誤之後交易已經 aborted，本來也只能 rollback。
                tx.rollback().await?;
                let again = self
                    .store
                    .list_provenance_by_raw_evidence(raw_evidence_id)
                    .await?;
                if let Some(done) = again.iter().find(|p| p.action == ACTION_NORMALIZED) {
                    return Ok(NormalizeOutcome::AlreadyDone {
                        document_ids: document_ids_from(done),
                    });
                }
                return Err(NormalizerError::Storage(
                    storage_core::StorageError::Conflict {
                        message: "normalized provenance 衝突，但讀不到既有列。請查 provenance 表"
                            .into(),
                    },
                ));
            }
            Err(err) => {
                // 明確回滾而不是靠 drop：錯誤路徑要看得出意圖。
                tx.rollback().await?;
                return Err(err);
            }
        }

        let ids: Vec<Uuid> = documents.iter().map(|d| d.id).collect();
        self.metrics.inc_collected(ids.len() as u64);
        if let Some(producer) = &self.producer {
            let key = ids.first().map(ToString::to_string);
            producer
                .publish(
                    EventTopic::ObjectNormalized,
                    key.as_deref(),
                    Some(evidence.id),
                    json!({
                        "raw_evidence_id": evidence.id,
                        "document_ids": ids,
                        "count": ids.len(),
                    }),
                )
                .await?;
        }
        Ok(NormalizeOutcome::Created { document_ids: ids })
    }
}

/// 佔下 `action='normalized'` 的 unique claim。
///
/// 寫入對象是呼叫端給的 `db`——正常路徑上那是**交易 handle**，claim 與 Document
/// 必須在同一個交易裡，否則交易就白開了。
async fn claim_normalized(
    db: &dyn RelationalStore,
    evidence: &RawEvidence,
    documents: &[Document],
    now: chrono::DateTime<Utc>,
) -> Result<(), NormalizerError> {
    let ids: Vec<Uuid> = documents.iter().map(|d| d.id).collect();
    let subject = ids.first().copied().unwrap_or(evidence.id);
    let claim = Provenance {
        id: Uuid::now_v7(),
        subject_id: subject,
        action: ACTION_NORMALIZED.into(),
        parent_id: Some(evidence.id),
        raw_evidence_id: Some(evidence.id),
        processor: PROCESSOR.into(),
        processor_version: env!("CARGO_PKG_VERSION").into(),
        timestamp: now,
        metadata: json!({
            "document_ids": ids,
            "item_count": ids.len(),
        }),
    };
    db.put_provenance(&claim).await?;
    Ok(())
}

/// 從 `RawEvidence.metadata["import"]` 取出上傳當下寫下的 `ImportSpec`。
///
/// 解不出來就當作沒有——寧可回報「不支援」也不要自己猜一組對映，
/// 猜錯會產生看起來正常、內容其實錯位的 Document。
fn import_spec(evidence: &RawEvidence) -> Option<ImportSpec> {
    let raw = evidence.metadata.get("import")?;
    match serde_json::from_value::<ImportSpec>(raw.clone()) {
        Ok(spec) => Some(spec),
        Err(err) => {
            tracing::warn!(
                raw_evidence_id = %evidence.id,
                error = %err,
                "metadata.import 不是合法的 ImportSpec，當作非匯入證據處理"
            );
            None
        }
    }
}

/// JSON／CSV 匯入 → Document。解析失敗回 `SkippedUnparseable`，不讓 consumer 掛掉。
///
/// 這裡的上限用的是 spec 裡存的那份（上傳當下的設定），不是目前的 config：
/// 已經被接受的證據不該因為之後有人調小上限就永遠正規化不了。
fn build_import_documents(
    evidence: &RawEvidence,
    body: &[u8],
    spec: &ImportSpec,
    now: DateTime<Utc>,
) -> Result<Vec<Document>, NormalizeOutcome> {
    let outcome = match import_format::parse(body, spec) {
        Some(Ok(outcome)) => outcome,
        Some(Err(err)) => {
            tracing::warn!(
                raw_evidence_id = %evidence.id,
                kind = spec.kind.as_str(),
                error = %err,
                "匯入內容無法解析，記錄後繼續消費下一則"
            );
            return Err(NormalizeOutcome::SkippedUnparseable {
                message: err.to_string(),
            });
        }
        None => {
            return Err(NormalizeOutcome::SkippedUnsupported {
                content_type: evidence.content_type.clone(),
            });
        }
    };

    let documents = outcome
        .records
        .into_iter()
        .map(|record| {
            let source_url = record
                .url
                .clone()
                .or_else(|| Some(evidence.source_url.clone()));
            let content_hash = core_model::content_hash(
                record.title.as_deref(),
                record.summary.as_deref(),
                record.body.as_deref(),
            );
            Document {
                id: Uuid::now_v7(),
                object_type: spec.object_type,
                schema_version: SCHEMA_VERSION.into(),
                title: record.title,
                body: record.body,
                summary: record.summary,
                language: record.language,
                author: record.author,
                published_at: record.published_at,
                modified_at: None,
                observed_at: now,
                collected_at: evidence.retrieved_at,
                source_url: source_url.clone(),
                canonical_url: source_url,
                normalized_content_hash: Some(content_hash),
                confidence: 0.8,
                labels: Vec::new(),
                attributes: json!({
                    "raw_evidence_id": evidence.id,
                    "external_id": record.external_id,
                    "import_kind": spec.kind.as_str(),
                    "record_index": record.index,
                    // 解析不出時間時保留原字串，之後要補格式才有依據。
                    "published_at_raw": record
                        .published_at
                        .is_none()
                        .then_some(record.published_at_raw)
                        .flatten(),
                }),
                // 同上：dedup 欄位由 deduplicator 填。
                external_key: None,
                simhash: None,
                duplicate_of: None,
            }
        })
        .collect();
    Ok(documents)
}

fn document_ids_from(prov: &Provenance) -> Vec<Uuid> {
    prov.metadata
        .get("document_ids")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .filter_map(|s| Uuid::parse_str(s).ok())
                .collect()
        })
        .unwrap_or_default()
}
