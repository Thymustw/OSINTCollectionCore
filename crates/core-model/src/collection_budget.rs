use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::CollectionId;

/// 一個 collection 的 Discovery 配額設定（SPEC_V0.3 §10／§11）。
///
/// 沒有這筆設定時（`RelationalStore::get_collection_budget` 回 `None`），
/// 呼叫端應該用 [`CollectionBudget::conservative_default`] 當退回值——
/// SPEC §11 的用意是「防止 graph explosion / runaway crawling /
/// runaway AI cost」，沒有設定不代表沒有限制，退回一組保守但非零的
/// 預設值，不是無界。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CollectionBudget {
    pub collection_id: CollectionId,
    /// 單次 Discovery run 最多產生幾筆 Candidate。
    pub max_candidates_per_run: i32,
    /// 單次 run 最多發出幾次外部請求（HTTP／API 呼叫，不含 AI）。
    pub max_requests_per_run: i32,
    /// 單次 run 最多呼叫幾次 AI Gateway。
    pub max_ai_calls_per_run: i32,
    /// Discovery 展開的最大深度（跟 `Seed.depth`／`Candidate.depth` 對齊）。
    pub max_depth: i32,
    /// 這個 collection 每日最多幾次外部請求，跨多次 run 累計。
    pub daily_request_budget: i64,
    /// 這個 collection 每日最多幾次 AI 呼叫，跨多次 run 累計。
    pub daily_ai_budget: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CollectionBudget {
    /// 沒有明確設定時的保守預設值。這些數字沒有經過真實 Discovery 流量驗證
    /// （Discovery Engine 本身要到 Phase 3 才存在），選擇原則是「讓一次正常
    /// 規模的 run 跑得完，但不放任無限展開」——之後有真實使用數據再調整，
    /// 不是隨手編的魔法數字。
    #[must_use]
    pub fn conservative_default(collection_id: CollectionId, now: DateTime<Utc>) -> Self {
        Self {
            collection_id,
            max_candidates_per_run: 200,
            max_requests_per_run: 500,
            max_ai_calls_per_run: 50,
            max_depth: 3,
            daily_request_budget: 5_000,
            daily_ai_budget: 500,
            created_at: now,
            updated_at: now,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn conservative_default_is_bounded_not_zero() {
        let now = Utc::now();
        let budget = CollectionBudget::conservative_default(Uuid::nil(), now);
        // 「保守」不等於「零」——零會讓 Discovery 完全跑不動，不是防護，是關閉。
        assert!(budget.max_candidates_per_run > 0);
        assert!(budget.max_requests_per_run > 0);
        assert!(budget.max_ai_calls_per_run > 0);
        assert!(budget.max_depth > 0);
        assert!(budget.daily_request_budget > 0);
        assert!(budget.daily_ai_budget > 0);
    }
}
