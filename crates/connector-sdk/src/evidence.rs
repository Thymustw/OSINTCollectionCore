//! RawEvidence 落地：body → ObjectStore，metadata → RelationalStore。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{CollectionId, ConnectorId, RawEvidence, RawEvidenceId, SourceId};
use serde_json::Value;
use sha2::{Digest, Sha256};
use storage_core::{ObjectStore, RelationalStore, StorageError};
use uuid::Uuid;

use crate::ConnectorError;

/// 把一筆 RawEvidence 寫進物件儲存 + canonical store。
#[async_trait]
pub trait EvidenceSink: Send + Sync {
    async fn persist(&self, evidence: NewRawEvidence) -> Result<RawEvidence, ConnectorError>;
}

/// 尚未給 id／sha256／path 的抓取結果。
#[derive(Debug, Clone)]
pub struct NewRawEvidence {
    pub source_id: SourceId,
    pub connector_id: ConnectorId,
    pub collection_id: Option<CollectionId>,
    pub external_id: Option<String>,
    pub source_url: String,
    pub retrieved_at: DateTime<Utc>,
    pub content_type: Option<String>,
    pub mime_type: Option<String>,
    pub http_status: Option<i32>,
    pub http_headers: Value,
    pub metadata: Value,
    pub collector_version: String,
    pub body: Vec<u8>,
}

/// SHA256 hex（小寫）。
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    hex::encode(digest)
}

/// 物件儲存 key：`raw/{source_id}/{yyyy}/{mm}/{dd}/{id}`。
#[must_use]
pub fn storage_path(source_id: SourceId, retrieved_at: DateTime<Utc>, id: RawEvidenceId) -> String {
    format!(
        "raw/{}/{}/{}",
        source_id,
        retrieved_at.format("%Y/%m/%d"),
        id
    )
}

/// `RelationalStore` + `ObjectStore` 的落地實作。
pub struct StoreEvidenceSink<R, O> {
    relational: R,
    objects: O,
}

impl<R, O> StoreEvidenceSink<R, O> {
    pub fn new(relational: R, objects: O) -> Self {
        Self {
            relational,
            objects,
        }
    }
}

#[async_trait]
impl<R, O> EvidenceSink for StoreEvidenceSink<R, O>
where
    R: RelationalStore + Send + Sync,
    O: ObjectStore + Send + Sync,
{
    async fn persist(&self, evidence: NewRawEvidence) -> Result<RawEvidence, ConnectorError> {
        let id = Uuid::now_v7();
        let sha256 = sha256_hex(&evidence.body);
        let path = storage_path(evidence.source_id, evidence.retrieved_at, id);
        let content_length = i64::try_from(evidence.body.len()).unwrap_or(i64::MAX);
        self.objects
            .put(&path, &evidence.body, evidence.content_type.as_deref())
            .await
            .map_err(map_storage)?;
        let record = RawEvidence {
            id,
            source_id: evidence.source_id,
            connector_id: evidence.connector_id,
            collection_id: evidence.collection_id,
            external_id: evidence.external_id,
            source_url: evidence.source_url,
            retrieved_at: evidence.retrieved_at,
            content_type: evidence.content_type,
            mime_type: evidence.mime_type,
            content_length: Some(content_length),
            sha256,
            storage_path: path,
            http_status: evidence.http_status,
            http_headers: evidence.http_headers,
            metadata: evidence.metadata,
            collector_version: evidence.collector_version,
        };
        match self.relational.insert_raw_evidence(&record).await {
            Ok(()) => Ok(record),
            Err(err) => {
                let _ = self.objects.delete(&record.storage_path).await;
                Err(map_storage(err))
            }
        }
    }
}

fn map_storage(err: StorageError) -> ConnectorError {
    ConnectorError::Storage {
        message: err.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_known() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
