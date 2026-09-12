//! `exact_identifier` 衝突 → [`ResolutionCandidate`] 的純函式 helper。
//!
//! entity-worker（或未來任何 identifier 寫入者）在 `put_entity_identifier`
//! 收到 [`storage_core::StorageError::Conflict`] 時呼叫：既有 owner 是誰、
//! 衝突的新 entity 是誰，組出一筆 SPEC §6「exact identifier」的候選。
//!
//! **純函式，不寫入 store**——呼叫端自己決定要不要 `put_resolution_candidate`。
//!
//! # 目前沒有任何呼叫端
//!
//! entity-worker 還沒寫 `entity_identifiers`（V0.2 Phase 0 只把欄位留著，
//! 見 `core_model::EntityIdentifier` 的模組說明）。這個 helper 是刻意留給
//! 未來的接線點，不要假裝已經接到抽取管線上。

use chrono::Utc;
use core_model::{
    EntityId, EntityIdentifier, RESOLUTION_METHODS, ResolutionCandidate, ResolutionStatus,
};
use serde_json::json;
use uuid::Uuid;

/// SPEC §6「exact identifier」在寫入衝突時的分數。
///
/// 同一個 `(namespace, normalized_value)` 被兩個 Entity 宣稱，是最硬的
/// 識別碼碰撞；0.95 高到值得進 Review，但不到 `auto_confirmed`——
/// SPEC 禁止只因同 username 就判定同一真實人物，namespace 衝突同樣
/// 可能是帳號共用或資料髒了，還是要人看。
pub const EXACT_IDENTIFIER_CONFLICT_SCORE: f64 = 0.95;

/// 把 identifier 寫入衝突編成一筆 pending 的 [`ResolutionCandidate`]。
///
/// `existing_owner` 是目前佔著 `(namespace, normalized_value)` 的那一列，
/// `conflicting_entity_id` 是這次想寫進去、被擋下的 Entity。
///
/// 用 [`ResolutionCandidate::ordered_pair`] 排序，符合 migration 0007 的
/// `entity_a_id < entity_b_id` CHECK。不寫入 store。
#[must_use]
pub fn resolution_candidate_from_identifier_conflict(
    existing_owner: &EntityIdentifier,
    conflicting_entity_id: EntityId,
) -> ResolutionCandidate {
    let (entity_a_id, entity_b_id) =
        ResolutionCandidate::ordered_pair(existing_owner.entity_id, conflicting_entity_id);
    ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id,
        entity_b_id,
        score: EXACT_IDENTIFIER_CONFLICT_SCORE,
        method: RESOLUTION_METHODS[0].to_string(),
        evidence: json!({
            "method": RESOLUTION_METHODS[0],
            "namespace": existing_owner.namespace,
            "value": existing_owner.value,
            "normalized_value": existing_owner.normalized_value,
            "existing_owner_entity_id": existing_owner.entity_id,
            "conflicting_entity_id": conflicting_entity_id,
            "trigger": "write_conflict",
        }),
        status: ResolutionStatus::Pending,
        created_at: Utc::now(),
        reviewed_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use core_model::EntityIdentifier;
    use uuid::Uuid;

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 12, 8, 0, 0).unwrap()
    }

    fn identifier(owner: EntityId, namespace: &str, value: &str) -> EntityIdentifier {
        EntityIdentifier {
            id: Uuid::now_v7(),
            entity_id: owner,
            namespace: namespace.into(),
            value: value.into(),
            normalized_value: value.to_ascii_lowercase(),
            confidence: 1.0,
            source_id: None,
            first_seen: ts(),
            last_seen: ts(),
        }
    }

    #[test]
    fn conflict_helper_sets_score_method_evidence_and_ordered_pair() {
        let larger = Uuid::from_u128(0xbbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb_bbbb);
        let smaller = Uuid::from_u128(0xaaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa_aaaa);
        let owner = identifier(larger, "github_username", "AcmeBot");
        let candidate = resolution_candidate_from_identifier_conflict(&owner, smaller);

        assert_eq!(candidate.score, EXACT_IDENTIFIER_CONFLICT_SCORE);
        assert_eq!(candidate.method, "exact_identifier");
        assert_eq!(candidate.status, ResolutionStatus::Pending);
        assert!(candidate.reviewed_at.is_none());
        assert_eq!(candidate.entity_a_id, smaller);
        assert_eq!(candidate.entity_b_id, larger);
        assert!(candidate.entity_a_id < candidate.entity_b_id);

        assert_eq!(candidate.evidence["method"], "exact_identifier");
        assert_eq!(candidate.evidence["namespace"], "github_username");
        assert_eq!(candidate.evidence["value"], "AcmeBot");
        assert_eq!(candidate.evidence["normalized_value"], "acmebot");
        assert_eq!(
            candidate.evidence["existing_owner_entity_id"],
            serde_json::Value::String(larger.to_string())
        );
        assert_eq!(
            candidate.evidence["conflicting_entity_id"],
            serde_json::Value::String(smaller.to_string())
        );
        assert_eq!(candidate.evidence["trigger"], "write_conflict");
    }

    #[test]
    fn conflict_helper_orders_regardless_of_argument_order() {
        let a = Uuid::from_u128(1);
        let b = Uuid::from_u128(2);
        let owner_is_smaller = identifier(a, "email", "soc@example.com");
        let c1 = resolution_candidate_from_identifier_conflict(&owner_is_smaller, b);
        let owner_is_larger = identifier(b, "email", "soc@example.com");
        let c2 = resolution_candidate_from_identifier_conflict(&owner_is_larger, a);
        assert_eq!(c1.entity_a_id, a);
        assert_eq!(c1.entity_b_id, b);
        assert_eq!(c2.entity_a_id, a);
        assert_eq!(c2.entity_b_id, b);
    }
}
