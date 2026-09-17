use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::{CandidateStatus, CandidateType};
use crate::ids::{CandidateId, CollectionId};

/// SPEC_V0.3 §8 列出的 Discovery method 名稱。
///
/// 跟 `resolution::RESOLUTION_METHODS` 同一種設計：參考清單，不是白名單。
/// `Candidate::discovery_method` 欄位仍是自由字串，之後新增 discovery method
/// 不需要改這裡或動 migration。
pub const DISCOVERY_METHODS: [&str; 11] = [
    "entity_expansion",
    "link_expansion",
    "account_expansion",
    "channel_expansion",
    "mention_expansion",
    "hashtag_expansion",
    "semantic_expansion",
    "graph_expansion",
    "source_expansion",
    "repository_expansion",
    "query_expansion",
];

/// Discovery Engine 找到的候選（SPEC_V0.3 §6）。
///
/// # Candidate first（Acceptance B）
///
/// 任何未經確認的發現（例如一個帳號）一律先落地成 Candidate，不能直接寫成
/// confirmed 的 Entity/Relationship。`status` 從 `pending` 走到
/// `approved`／`auto_approved` 才代表被接受；`auto_approved` 與 `approved`
/// 分開的理由跟 `ResolutionStatus::AutoConfirmed` 一樣——保留「這筆有沒有
/// 人看過」的區別。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Candidate {
    pub id: CandidateId,
    pub candidate_type: CandidateType,
    pub value: String,
    pub normalized_value: String,
    pub collection_id: Option<CollectionId>,
    /// 是誰／哪個機制發現的（例如 seed id、entity id、或一個 worker 名稱）。
    /// SPEC 沒有限定格式，自由字串。
    pub discovered_by: String,
    /// 見 [`DISCOVERY_METHODS`]。
    pub discovery_method: String,
    pub confidence: f64,
    pub score: f64,
    pub status: CandidateStatus,
    pub depth: i32,
    pub created_at: DateTime<Utc>,
    /// 人工審核（或自動核准）發生的時間。`None` = 還沒被審過。
    pub reviewed_at: Option<DateTime<Utc>>,
}
