use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{ConnectorId, SourceId};

/// Connector（SPEC §6）。
///
/// `credential_reference` / `proxy_reference` 只存 SecretRef 字串，禁止明文 password/token。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Connector {
    pub id: ConnectorId,
    pub source_id: SourceId,
    pub name: String,
    /// Connector 實作種類（rss / atom / static_web / rest_api / …）。規格未列舉，維持字串。
    #[serde(rename = "type")]
    pub connector_type: String,
    pub version: String,
    pub enabled: bool,
    pub configuration: Value,
    pub credential_reference: Option<String>,
    pub schedule: Option<String>,
    pub rate_limit: Value,
    pub timeout: Value,
    pub proxy_reference: Option<String>,
    pub checkpoint: Value,
    pub last_run: Option<DateTime<Utc>>,
    pub last_success: Option<DateTime<Utc>>,
    pub status: String,
    pub error_count: i32,
}
