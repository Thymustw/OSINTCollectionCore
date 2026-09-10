use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::DocumentType;
use crate::ids::DocumentId;

/// Canonical document（SPEC §9）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub object_type: DocumentType,
    pub schema_version: String,
    pub title: Option<String>,
    pub body: Option<String>,
    pub summary: Option<String>,
    pub language: Option<String>,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub modified_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub collected_at: DateTime<Utc>,
    pub source_url: Option<String>,
    pub canonical_url: Option<String>,
    pub normalized_content_hash: Option<String>,
    pub confidence: f64,
    pub labels: Vec<String>,
    pub attributes: Value,
}
