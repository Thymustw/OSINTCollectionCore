use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::{ObjectId, ProvenanceId, RawEvidenceId};

/// 溯源紀錄（SPEC §14）。最低可追溯：Search → Canonical Object → Raw Evidence → Source → Connector。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Provenance {
    pub id: ProvenanceId,
    pub subject_id: ObjectId,
    pub action: String,
    pub parent_id: Option<ObjectId>,
    pub raw_evidence_id: Option<RawEvidenceId>,
    pub processor: String,
    pub processor_version: String,
    pub timestamp: DateTime<Utc>,
    pub metadata: Value,
}
