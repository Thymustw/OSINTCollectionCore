use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::EmbeddingTarget;
use crate::ids::{EmbeddingId, ObjectId};

/// Embedding canonical record（SPEC §11）。**不含向量本體**——向量只投影進
/// OpenSearch k-NN index（Step 2），PostgreSQL 只存「這個目標、這個模型、
/// 這個內容雜湊算過了」的事實，用來判斷要不要重算（re-generate）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Embedding {
    pub id: EmbeddingId,
    pub target_id: ObjectId,
    pub target_type: EmbeddingTarget,
    /// 例如 `"huggingface/sentence-transformers/all-MiniLM-L6-v2"`。
    pub model: String,
    /// **不是** ml-commons 內部的遞增序號。是模型內容的 SHA-256
    /// （`storage_opensearch::MINILM_CONTENT_HASH`／`E5_CONTENT_HASH`，
    /// 見 `crates/storage-opensearch/src/embedding.rs` 的說明）。
    pub model_version: String,
    pub dimensions: i32,
    /// 原始文字（不含 query:/passage: 前綴）的 SHA-256。
    /// `storage_core::embedding_content_hash` 算的就是這個。
    pub content_hash: String,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 14, 12, 0, 0).unwrap()
    }

    #[test]
    fn embedding_round_trip() {
        let original = Embedding {
            id: Uuid::parse_str("01993c6a-7c3e-7a11-8000-7c3e7a110001").unwrap(),
            target_id: Uuid::parse_str("01993c6a-7c3e-7a11-8000-7c3e7a110002").unwrap(),
            target_type: EmbeddingTarget::DocumentBody,
            model: "huggingface/sentence-transformers/all-MiniLM-L6-v2".into(),
            model_version: "a".repeat(64),
            dimensions: 384,
            content_hash: "b".repeat(64),
            created_at: ts(),
        };
        let json = serde_json::to_string(&original).expect("serialize");
        let back: Embedding = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(original, back);
        let v = serde_json::to_value(&original).unwrap();
        assert_eq!(v["target_type"], "document_body");
    }

    #[test]
    fn embedding_target_serialises_as_snake_case() {
        assert_eq!(
            serde_json::to_value(EmbeddingTarget::DocumentTitle).unwrap(),
            "document_title"
        );
        assert_eq!(
            serde_json::to_value(EmbeddingTarget::EntityDescription).unwrap(),
            "entity_description"
        );
        assert_eq!(
            serde_json::to_value(EmbeddingTarget::EventDescription).unwrap(),
            "event_description"
        );
        assert_eq!(
            serde_json::from_value::<EmbeddingTarget>(serde_json::Value::String(
                "document_body".into()
            ))
            .unwrap(),
            EmbeddingTarget::DocumentBody
        );
    }
}
