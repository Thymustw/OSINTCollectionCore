use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::SourceType;
use crate::ids::SourceId;

/// 採集來源（SPEC §5）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Source {
    pub id: SourceId,
    pub name: String,
    pub source_type: SourceType,
    pub platform: Option<String>,
    pub base_url: Option<String>,
    pub description: Option<String>,
    pub language: Option<String>,
    pub country: Option<String>,
    pub enabled: bool,
    pub collection_policy: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_seen: Option<DateTime<Utc>>,
}
