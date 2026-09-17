use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{
    CandidateEvidenceId, CandidateId, EntityId, ObjectId, RawEvidenceId, RelationshipId,
};

/// 「這個 Candidate 為什麼被發現」的一筆證據（SPEC_V0.3 §7）。
///
/// # Explainability（Acceptance C）
///
/// 任何 Candidate 必須能回答「Why was this discovered?」——這張表就是那個
/// 回答的落地方式。四個參照欄位（`object_id`／`entity_id`／`relationship_id`／
/// `raw_evidence_id`）**全部是 `Option`，且不要求恰好一個有值**：同一筆證據
/// 可能同時指向一個 Document 與它裡面抽出的一個 Entity（例如「這個帳號在
/// 這篇文章裡被這個 Entity 提到」），也可能只有其中一個成立。呼叫端寫入時
/// 應該至少填一個，但這個 struct 本身不強制檢查——驗證留給寫入端的 domain
/// service（比照這個 repo `RelationalStore` trait 大多把跨欄位驗證留給呼叫端
/// 的既有慣例，不在型別層加沒有共識的約束）。
///
/// `id`／`created_at` 是 SPEC 沒列但比照 `RelationshipEvidence` 既有慣例補上
/// 的欄位——沒有主鍵無法被 `RelationalStore::get_*` 定址。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateEvidence {
    pub id: CandidateEvidenceId,
    pub candidate_id: CandidateId,
    pub object_id: Option<ObjectId>,
    pub entity_id: Option<EntityId>,
    pub relationship_id: Option<RelationshipId>,
    pub raw_evidence_id: Option<RawEvidenceId>,
    pub reason: String,
    pub weight: f64,
    pub created_at: DateTime<Utc>,
}
