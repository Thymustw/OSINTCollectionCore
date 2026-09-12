use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::ResolutionStatus;
use crate::ids::{EntityId, ResolutionCandidateId};

/// SPEC_V0.2 §6 列出的 resolution method 名稱。
///
/// 這是**參考清單，不是白名單**：§6 的原文是「至少」，之後新增方法不需要改這裡，
/// 資料庫欄位也仍然是自由字串。放在這裡只是讓十個名字有一個統一拼法的來源，
/// 避免 `account_handle` 與 `account-handle` 在不同 crate 裡各寫一種。
pub const RESOLUTION_METHODS: [&str; 10] = [
    "exact_identifier",
    "normalized_name",
    "alias",
    "domain",
    "url",
    "account_handle",
    "email",
    "external_id",
    "semantic_similarity",
    "graph_context",
];

/// Entity 合併候選（SPEC_V0.2 §5）。
///
/// # 一筆 = 一個 (pair, method)，不是一個 pair
///
/// SPEC §5 的欄位寫的是 `methods`（複數），這裡落地成**單數** `method`，
/// 一對 Entity 被三種方法命中就是三列。理由是 `score` 與 `status` 都只有在
/// 「針對某一種方法」時才有明確意義：把三種方法塞進同一列，`score` 是誰的分數、
/// 審核者 reject 的是哪一條證據，都會變成講不清楚的事。
/// SPEC 講的「這一對有哪些 methods」等於這張表上同一對的列集合。
///
/// ⚠️ 這是**與 SPEC §5 字面不同**的一處，實作 resolver／Console 時要知道。
///
/// # `entity_a_id < entity_b_id` 是寫入端的責任
///
/// 「同一對用同一方法只有一筆」靠 `(entity_a_id, entity_b_id, method)` 的
/// unique index 保證，而 unique index 分不出 `(A,B)` 與 `(B,A)`——不規範順序的話
/// 同一對會存成兩列，而且**不會有任何錯誤**，只會在 Resolution Review 畫面上
/// 看到重複項目。所以 migration 0007 另外加了 CHECK constraint 強制
/// `entity_a_id < entity_b_id`（UUID 的位元組序，`Uuid: Ord` 與 PG／SQLite 的
/// 比較結果一致）。呼叫端請用 [`ResolutionCandidate::ordered_pair`] 排好再寫。
///
/// SPEC §6 另有一條規則要記得：**禁止只因同 username 就判定同一真實人物**——
/// 這張表存的是「候選」，`auto_confirmed` 以外的狀態都還沒有結論。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionCandidate {
    pub id: ResolutionCandidateId,
    pub entity_a_id: EntityId,
    pub entity_b_id: EntityId,
    pub score: f64,
    /// SPEC §6 的方法名。自由字串，建議取自 [`RESOLUTION_METHODS`]。
    pub method: String,
    /// 支持這個候選的證據（命中的識別碼、片段、相似度細節……）。
    pub evidence: Value,
    pub status: ResolutionStatus,
    pub created_at: DateTime<Utc>,
    /// 人工審核（或自動確認）發生的時間。`None` = 還沒被審過。
    pub reviewed_at: Option<DateTime<Utc>>,
}

impl ResolutionCandidate {
    /// 把兩個 Entity id 排成 `(小, 大)`，符合 migration 0007 的 CHECK constraint。
    #[must_use]
    pub fn ordered_pair(a: EntityId, b: EntityId) -> (EntityId, EntityId) {
        if a <= b { (a, b) } else { (b, a) }
    }
}
