use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{ConnectorId, RawEvidenceId, SourceId};

/// 原始證據（SPEC §8）。寫入後不可變；同 URL 新版本必須是新的一筆。
///
/// 調查集合歸屬不在這份 struct 上：一筆證據可以同時屬於多個集合，
/// 關聯寫在 `raw_evidence_collections`，由 `insert_raw_evidence` 在同一個交易裡寫入。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawEvidence {
    pub id: RawEvidenceId,
    pub source_id: SourceId,
    pub connector_id: ConnectorId,
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
