//! Relationship 碰撞分類。純函式，不碰 store。
//!
//! merge 把指向 merged Entity 的端點改成 survivor 時，`(source, type, target)`
//! UNIQUE 可能撞號。三種互斥結果：
//!
//! * **self_loop**：repoint 後兩端變成同一個 Entity，語意無效，要刪。
//! * **collision**：survivor 已經有同一條邊，merged 那條要被吸收。
//! * **safe_repoint**：改一端即可，不會撞 UNIQUE。
//!
//! 三者合起來就是 `merged_rels` 的全部，沒有一條會被靜默丟掉。

use core_model::{EntityId, Relationship, RelationshipType};

use crate::repoint::repointed_ends;

/// 一對因 UNIQUE 撞號而要吸收合併的 relationship。
#[derive(Debug, Clone, PartialEq)]
pub struct Collision {
    /// 屬於 merged Entity、會被刪掉的那條。
    pub absorbed: Relationship,
    /// 屬於 survivor、會留下並吃掉 evidence 的那條。
    pub absorber: Relationship,
}

/// 把 `merged_rels` 分成 (self_loops, collisions, safe_repoints)。
///
/// 對每一條 merged relationship：
/// 1. 把等於 `merged_id` 的端點換成 `survivor_id`（兩端都可能是）。
/// 2. 若新的兩端相等 → self_loop。
/// 3. 否則在 `survivor_rels` 找 `(relationship_type, new_source, new_target)`
///    完全相同的一條 → collision。
/// 4. 否則 → safe_repoint。
///
/// 方向算在 UNIQUE 裡：`A --Owns--> B` 與 `B --Owns--> A` 是兩條不同的邊，
/// 不會互相吸收。同一 triple 在 `survivor_rels` 出現多次時取第一筆
/// （schema UNIQUE 保證不該發生）。
#[must_use]
pub fn classify_relationships(
    merged_rels: &[Relationship],
    survivor_rels: &[Relationship],
    merged_id: EntityId,
    survivor_id: EntityId,
) -> (Vec<Relationship>, Vec<Collision>, Vec<Relationship>) {
    let mut self_loops = Vec::new();
    let mut collisions = Vec::new();
    let mut safe_repoints = Vec::new();

    for rel in merged_rels {
        let (new_source, new_target) = repointed_ends(rel, merged_id, survivor_id);
        if new_source == new_target {
            self_loops.push(rel.clone());
            continue;
        }
        if let Some(absorber) =
            find_absorber(survivor_rels, rel.relationship_type, new_source, new_target)
        {
            collisions.push(Collision {
                absorbed: rel.clone(),
                absorber: absorber.clone(),
            });
            continue;
        }
        safe_repoints.push(rel.clone());
    }

    (self_loops, collisions, safe_repoints)
}

fn find_absorber(
    survivor_rels: &[Relationship],
    relationship_type: RelationshipType,
    source: EntityId,
    target: EntityId,
) -> Option<&Relationship> {
    survivor_rels.iter().find(|r| {
        r.relationship_type == relationship_type
            && r.source_object_id == source
            && r.target_object_id == target
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use core_model::RelationshipType;
    use uuid::Uuid;

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, 8, 0, 0).unwrap()
    }

    fn rel(id: u128, source: EntityId, ty: RelationshipType, target: EntityId) -> Relationship {
        Relationship {
            id: Uuid::from_u128(id),
            source_object_id: source,
            relationship_type: ty,
            target_object_id: target,
            confidence: 0.5,
            first_seen: ts(),
            last_seen: ts(),
            evidence_count: 1,
            created_at: ts(),
            updated_at: ts(),
        }
    }

    fn e(n: u128) -> EntityId {
        Uuid::from_u128(n)
    }

    #[test]
    fn empty_inputs_yield_empty_buckets() {
        let (loops, collisions, safe) = classify_relationships(&[], &[], e(1), e(2));
        assert!(loops.is_empty());
        assert!(collisions.is_empty());
        assert!(safe.is_empty());
    }

    #[test]
    fn merged_to_third_is_safe_repoint() {
        let merged = e(1);
        let survivor = e(2);
        let third = e(3);
        let merged_rels = [rel(10, merged, RelationshipType::Owns, third)];
        let (loops, collisions, safe) = classify_relationships(&merged_rels, &[], merged, survivor);
        assert!(loops.is_empty());
        assert!(collisions.is_empty());
        assert_eq!(safe.len(), 1);
        assert_eq!(safe[0].id, Uuid::from_u128(10));
    }

    #[test]
    fn third_to_merged_is_safe_repoint() {
        let merged = e(1);
        let survivor = e(2);
        let third = e(3);
        let merged_rels = [rel(10, third, RelationshipType::Owns, merged)];
        let (loops, collisions, safe) = classify_relationships(&merged_rels, &[], merged, survivor);
        assert!(loops.is_empty());
        assert!(collisions.is_empty());
        assert_eq!(safe.len(), 1);
    }

    #[test]
    fn same_triple_on_survivor_is_collision() {
        let merged = e(1);
        let survivor = e(2);
        let third = e(3);
        let absorbed = rel(10, merged, RelationshipType::Owns, third);
        let absorber = rel(20, survivor, RelationshipType::Owns, third);
        let (loops, collisions, safe) = classify_relationships(
            std::slice::from_ref(&absorbed),
            std::slice::from_ref(&absorber),
            merged,
            survivor,
        );
        assert!(loops.is_empty());
        assert!(safe.is_empty());
        assert_eq!(collisions.len(), 1);
        assert_eq!(collisions[0].absorbed.id, absorbed.id);
        assert_eq!(collisions[0].absorber.id, absorber.id);
    }

    #[test]
    fn opposite_direction_is_not_collision() {
        let merged = e(1);
        let survivor = e(2);
        let third = e(3);
        // merged --Owns--> third  vs  third --Owns--> survivor
        let merged_rels = [rel(10, merged, RelationshipType::Owns, third)];
        let survivor_rels = [rel(20, third, RelationshipType::Owns, survivor)];
        let (loops, collisions, safe) =
            classify_relationships(&merged_rels, &survivor_rels, merged, survivor);
        assert!(loops.is_empty());
        assert!(collisions.is_empty());
        assert_eq!(safe.len(), 1);
    }

    #[test]
    fn different_type_is_not_collision() {
        let merged = e(1);
        let survivor = e(2);
        let third = e(3);
        let merged_rels = [rel(10, merged, RelationshipType::Owns, third)];
        let survivor_rels = [rel(20, survivor, RelationshipType::Uses, third)];
        let (loops, collisions, safe) =
            classify_relationships(&merged_rels, &survivor_rels, merged, survivor);
        assert!(loops.is_empty());
        assert!(collisions.is_empty());
        assert_eq!(safe.len(), 1);
    }

    #[test]
    fn merged_to_survivor_is_self_loop() {
        let merged = e(1);
        let survivor = e(2);
        let merged_rels = [rel(10, merged, RelationshipType::AssociatedWith, survivor)];
        let (loops, collisions, safe) = classify_relationships(&merged_rels, &[], merged, survivor);
        assert_eq!(loops.len(), 1);
        assert!(collisions.is_empty());
        assert!(safe.is_empty());
    }

    #[test]
    fn survivor_to_merged_is_self_loop() {
        let merged = e(1);
        let survivor = e(2);
        let merged_rels = [rel(10, survivor, RelationshipType::AssociatedWith, merged)];
        let (loops, collisions, safe) = classify_relationships(&merged_rels, &[], merged, survivor);
        assert_eq!(loops.len(), 1);
        assert!(collisions.is_empty());
        assert!(safe.is_empty());
    }

    #[test]
    fn both_ends_merged_is_self_loop() {
        let merged = e(1);
        let survivor = e(2);
        let merged_rels = [rel(10, merged, RelationshipType::AssociatedWith, merged)];
        let (loops, collisions, safe) = classify_relationships(&merged_rels, &[], merged, survivor);
        assert_eq!(loops.len(), 1);
        assert!(collisions.is_empty());
        assert!(safe.is_empty());
    }

    #[test]
    fn three_buckets_are_mutually_exclusive_and_complete() {
        let merged = e(1);
        let survivor = e(2);
        let third = e(3);
        let fourth = e(4);
        let loop_rel = rel(10, merged, RelationshipType::AssociatedWith, survivor);
        let collide_rel = rel(11, merged, RelationshipType::Owns, third);
        let safe_rel = rel(12, merged, RelationshipType::Uses, fourth);
        let absorber = rel(20, survivor, RelationshipType::Owns, third);
        let (loops, collisions, safe) = classify_relationships(
            &[loop_rel.clone(), collide_rel.clone(), safe_rel.clone()],
            &[absorber],
            merged,
            survivor,
        );
        assert_eq!(loops.len() + collisions.len() + safe.len(), 3);
        assert_eq!(loops[0].id, loop_rel.id);
        assert_eq!(collisions[0].absorbed.id, collide_rel.id);
        assert_eq!(safe[0].id, safe_rel.id);
    }
}
