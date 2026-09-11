//! 把一筆 RawEvidence 正規化成 Document。冪等：同一 raw_evidence_id 不重複寫。

use chrono::Utc;
use connector_rss::parse_feed;
use core_events::{EventProducer, EventTopic};
use core_model::{Document, DocumentType, Provenance, RawEvidence};
use core_observability::MetricsRegistry;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use uuid::Uuid;

use crate::content::{ContentClass, body_looks_like_feed, classify_content};
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
    pub async fn handle_payload(
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

        let treat_as_feed = class == ContentClass::RssOrAtom || body_looks_like_feed(&body);
        if !treat_as_feed {
            tracing::info!(
                raw_evidence_id = %evidence.id,
                content_type = ?evidence.content_type,
                "不支援的 content type，跳過正規化"
            );
            return Ok(NormalizeOutcome::SkippedUnsupported {
                content_type: evidence.content_type.clone(),
            });
        }

        let items = match parse_feed(&body) {
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
        };

        let now = Utc::now();
        let mut documents = Vec::new();
        for item in items {
            let title = item.title.clone();
            let summary = item.summary.clone();
            let source_url = item.url.clone();
            let hash_src = format!(
                "{}|{}",
                title.as_deref().unwrap_or(""),
                summary.as_deref().unwrap_or("")
            );
            let doc = Document {
                id: Uuid::now_v7(),
                object_type: DocumentType::Article,
                schema_version: SCHEMA_VERSION.into(),
                title,
                body: None,
                summary,
                language: None,
                author: None,
                published_at: item.published_at,
                modified_at: None,
                observed_at: now,
                collected_at: evidence.retrieved_at,
                source_url: source_url.clone(),
                canonical_url: source_url,
                normalized_content_hash: Some(sha256_hex(hash_src.as_bytes())),
                confidence: 0.8,
                labels: Vec::new(),
                attributes: json!({
                    "raw_evidence_id": evidence.id,
                    "external_id": item.external_id,
                    "feed_type": item.attributes.get("feed_type"),
                }),
            };
            documents.push(doc);
        }

        // write-then-claim（刻意的選擇，不是 claim-first）：先寫 Document，最後才佔
        // unique index（action=normalized）。storage-core 目前沒有跨表交易能力，兩個寫入
        // 順序都無法完全原子化，兩種失效模式代價不對等，故意選代價較小的一邊：
        //   - write-then-claim：Document 寫完、claim 前 crash → 重跑會重新產生一組
        //     「重複」Document。資料還在，且 SPEC Phase 4 的 dedup pipeline本來就會處理
        //     這種重複，是可回收的代價。
        //   - claim-first（曾經改過去，已改回）：claim 成功、Document 還沒寫就 crash →
        //     之後永遠回報 AlreadyDone，但 Document 根本不存在——靜默資料遺失，無法偵測
        //     也無法回收，違反「不遺失證據」的核心目的，代價遠高於前者。
        // 併發雙寫的邊界案例（兩個 consumer 同時通過上面的「尚未正規化」檢查）目前仍可能
        // 各自寫出一組 Document，只有其中一個 claim 會成功——這跟 crash-window 重複是
        // 同一類可回收風險，不是新增的問題。
        for doc in &documents {
            self.store.put_document(doc).await?;
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
            self.store.put_provenance(&prov).await?;
        }

        match self.claim_normalized(&evidence, &documents, now).await {
            Ok(()) => {}
            Err(NormalizerError::Storage(storage_core::StorageError::Conflict { .. })) => {
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
            Err(err) => return Err(err),
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

    async fn claim_normalized(
        &self,
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
        self.store.put_provenance(&claim).await?;
        Ok(())
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
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
