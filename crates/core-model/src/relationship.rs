use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::RelationshipType;
use crate::ids::{ObjectId, RawEvidenceId, RelationshipEvidenceId, RelationshipId};

/// Relationship 是一級物件（SPEC §11）。
///
/// `source_object_id` / `target_object_id` 可指向 document 或 entity，因此不綁單一 FK。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relationship {
    pub id: RelationshipId,
    pub source_object_id: ObjectId,
    pub relationship_type: RelationshipType,
    pub target_object_id: ObjectId,
    pub confidence: f64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    pub evidence_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Relationship 的單筆證據（SPEC §12）。
///
/// 規格未列 `id`；資料表需要主鍵，因此補 UUID v7。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RelationshipEvidence {
    pub id: RelationshipEvidenceId,
    pub relationship_id: RelationshipId,
    pub object_id: ObjectId,
    pub raw_evidence_id: Option<RawEvidenceId>,
    pub excerpt: Option<String>,
    pub confidence: f64,
    pub created_at: DateTime<Utc>,
}
