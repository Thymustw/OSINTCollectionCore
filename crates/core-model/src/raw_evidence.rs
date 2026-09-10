use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{CollectionId, ConnectorId, RawEvidenceId, SourceId};

/// 原始證據（SPEC §8）。寫入後不可變；同 URL 新版本必須是新的一筆。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawEvidence {
    pub id: RawEvidenceId,
    pub source_id: SourceId,
    pub connector_id: ConnectorId,
    pub collection_id: Option<CollectionId>,
    pub external_id: Option<String>,
    pub source_url: String,
    pub retrieved_at: DateTime<Utc>,
    pub content_type: Option<String>,
    pub mime_type: Option<String>,
    pub content_length: Option<i64>,
    pub sha256: String,
    pub storage_path: String,
    pub http_status: Option<i32>,
    pub http_headers: Value,
    pub metadata: Value,
    pub collector_version: String,
}
