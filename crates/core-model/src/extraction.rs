use serde::{Deserialize, Serialize};

use crate::ids::{EntityExtractionId, EntityId, ObjectId};

/// Entity extraction 紀錄（SPEC §17）。
///
/// 規格未列 `id`；資料表需要主鍵，因此補 UUID v7。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityExtraction {
    pub id: EntityExtractionId,
    pub object_id: ObjectId,
    pub entity_id: EntityId,
    pub extractor: String,
    pub extractor_version: String,
    pub confidence: f64,
    pub text_offset: Option<i32>,
    pub excerpt: Option<String>,
}
