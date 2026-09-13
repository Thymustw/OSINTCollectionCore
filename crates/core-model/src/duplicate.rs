use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{DuplicateGroupId, ObjectId, RawEvidenceId};

/// Duplicate group（SPEC §16）。
///
/// 不得刪 duplicate evidence。規格列出 canonical_object_id、member、method、similarity、first_seen；
/// 資料表需要主鍵，因此補 `id`。member 拆成 object 與 raw evidence 兩個可空欄位。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DuplicateGroup {
    pub id: DuplicateGroupId,
    pub canonical_object_id: ObjectId,
    pub member_object_id: Option<ObjectId>,
    pub member_raw_evidence_id: Option<RawEvidenceId>,
    pub method: String,
    pub similarity: f64,
    pub first_seen: DateTime<Utc>,
    /// 語意判定用的模型名稱（`Some` 才是 Stage 5 真的判的；Stage 1-4
    /// 的方法不靠模型，是 `None`）。SPEC §17「method/model」。
    pub model: Option<String>,
}
