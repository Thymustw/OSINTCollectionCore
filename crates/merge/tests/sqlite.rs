//! `MergeService` 對 SQLite `TransactionalStore` 的整合測試。
//!
//! 每個測試開自己的檔，跑完刪掉（含 WAL／SHM），不留磁碟。

use std::path::{Path, PathBuf};

use chrono::{TimeZone, Utc};
use core_model::{
    Entity, EntityAlias, EntityExtraction, EntityId, EntityIdentifier, EntityType, Relationship,
    RelationshipEvidence, RelationshipType,
};
use merge::{MERGE_REF_CAP, MergeError, MergeService};
use serde_json::json;
use storage_core::RelationalStore;
use storage_core::conformance::find_workspace_root;
use storage_sqlite::SqliteEmbeddedStore;
use uuid::Uuid;

fn ts() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 13, 9, 0, 0).unwrap()
}

fn entity(name: &str, entity_type: EntityType) -> Entity {
    Entity {
        id: Uuid::now_v7(),
        entity_type,
        name: name.into(),
        normalized_name: name.to_ascii_lowercase(),
        description: None,
        confidence: 0.9,
        first_seen: ts(),
        last_seen: ts(),
        merged_into: None,
        attributes: json!({}),
    }
}

fn alias(entity_id: EntityId, text: &str) -> EntityAlias {
    EntityAlias {
        id: Uuid::now_v7(),
        entity_id,
        alias: text.into(),
        alias_type: "name".into(),
        source_id: None,
        confidence: 0.8,
        first_seen: ts(),
        last_seen: ts(),
    }
}

fn identifier(entity_id: EntityId, namespace: &str, value: &str) -> EntityIdentifier {
    EntityIdentifier {
        id: Uuid::now_v7(),
        entity_id,
        namespace: namespace.into(),
        value: value.into(),
        normalized_value: value.to_ascii_lowercase(),
        confidence: 0.95,
        source_id: None,
        first_seen: ts(),
        last_seen: ts(),
    }
}

fn extraction(entity_id: EntityId) -> EntityExtraction {
    EntityExtraction {
        id: Uuid::now_v7(),
        object_id: Uuid::now_v7(),
        entity_id,
        extractor: "test".into(),
        extractor_version: "0.1.0".into(),
        confidence: 0.7,
        text_offset: Some(0),
        excerpt: Some("excerpt".into()),
    }
}

fn relationship(
    source: EntityId,
    ty: RelationshipType,
    target: EntityId,
    evidence_count: i32,
    confidence: f64,
) -> Relationship {
    Relationship {
        id: Uuid::now_v7(),
        source_object_id: source,
        relationship_type: ty,
        target_object_id: target,
        confidence,
        first_seen: ts(),
        last_seen: ts(),
        evidence_count,
        created_at: ts(),
        updated_at: ts(),
    }
}

fn evidence(relationship_id: Uuid) -> RelationshipEvidence {
    RelationshipEvidence {
        id: Uuid::now_v7(),
        relationship_id,
        object_id: Uuid::now_v7(),
        raw_evidence_id: None,
        excerpt: Some("ev".into()),
        confidence: 0.6,
        created_at: ts(),
    }
}

struct Harness {
    service: MergeService<SqliteEmbeddedStore>,
    db: SqliteEmbeddedStore,
    path: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        cleanup(&self.path);
    }
}

fn cleanup(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

async fn open_harness() -> Harness {
    let root = find_workspace_root().expect("workspace root");
    let path: PathBuf = root.join(format!("var/osint-merge-{}.sqlite", Uuid::now_v7()));
    let writer = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite writer");
    writer.migrate().await.expect("migrate");
    let db = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite reader");
    Harness {
        service: MergeService::new(writer),
        db,
        path,
    }
}

#[tokio::test]
async fn basic_merge_repoints_aliases_identifiers_extractions_and_relationships() {
    let h = open_harness().await;
    let survivor = entity("acme-surv", EntityType::Organization);
    let merged = entity("acme-merged", EntityType::Organization);
    let third = entity("partner", EntityType::Organization);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();
    h.db.put_entity(&third).await.unwrap();

    let a = alias(merged.id, "ACME Inc");
    let ident = identifier(merged.id, "domain", "acme.example");
    let ex = extraction(merged.id);
    h.db.put_entity_alias(&a).await.unwrap();
    h.db.put_entity_identifier(&ident).await.unwrap();
    h.db.put_entity_extraction(&ex).await.unwrap();

    let from_merged = relationship(merged.id, RelationshipType::Owns, third.id, 1, 0.4);
    let from_survivor = relationship(survivor.id, RelationshipType::Uses, third.id, 1, 0.3);
    h.db.put_relationship(&from_merged).await.unwrap();
    h.db.put_relationship(&from_survivor).await.unwrap();

    let history = h
        .service
        .execute_merge(
            survivor.id,
            merged.id,
            "測試基本 merge".into(),
            "tester".into(),
        )
        .await
        .expect("merge");

    assert_eq!(history.survivor_id, survivor.id);
    assert_eq!(history.merged_id, merged.id);
    assert!(history.undone_at.is_none());
    assert!(history.merged_relationships.is_empty());

    let got_merged = h.db.get_entity(merged.id).await.unwrap().unwrap();
    assert_eq!(got_merged.merged_into, Some(survivor.id));

    let got_alias = h.db.get_entity_alias(a.id).await.unwrap().unwrap();
    assert_eq!(got_alias.entity_id, survivor.id);
    let got_ident = h.db.get_entity_identifier(ident.id).await.unwrap().unwrap();
    assert_eq!(got_ident.entity_id, survivor.id);
    let got_ex = h.db.get_entity_extraction(ex.id).await.unwrap().unwrap();
    assert_eq!(got_ex.entity_id, survivor.id);

    let got_rel =
        h.db.get_relationship(from_merged.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(got_rel.source_object_id, survivor.id);
    assert_eq!(got_rel.target_object_id, third.id);

    let tables: Vec<_> = history
        .repointed_references
        .iter()
        .map(|r| r.table.as_str())
        .collect();
    assert!(tables.contains(&"relationships"));
    assert!(tables.contains(&"entity_aliases"));
    assert!(tables.contains(&"entity_identifiers"));
    assert!(tables.contains(&"entity_extractions"));
    for r in &history.repointed_references {
        assert_eq!(r.previous_value, merged.id);
    }
}

#[tokio::test]
async fn relationship_collision_absorbs_and_moves_evidence() {
    let h = open_harness().await;
    let survivor = entity("s-col", EntityType::Person);
    let merged = entity("m-col", EntityType::Person);
    let third = entity("t-col", EntityType::Organization);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();
    h.db.put_entity(&third).await.unwrap();

    let absorbed = relationship(merged.id, RelationshipType::MemberOf, third.id, 2, 0.8);
    let absorber = relationship(survivor.id, RelationshipType::MemberOf, third.id, 1, 0.5);
    h.db.put_relationship(&absorbed).await.unwrap();
    h.db.put_relationship(&absorber).await.unwrap();
    let ev1 = evidence(absorbed.id);
    let ev2 = evidence(absorbed.id);
    let ev_abs = evidence(absorber.id);
    h.db.put_relationship_evidence(&ev1).await.unwrap();
    h.db.put_relationship_evidence(&ev2).await.unwrap();
    h.db.put_relationship_evidence(&ev_abs).await.unwrap();

    let history = h
        .service
        .execute_merge(survivor.id, merged.id, "碰撞".into(), "tester".into())
        .await
        .expect("merge");

    assert_eq!(history.merged_relationships.len(), 1);
    let mr = &history.merged_relationships[0];
    assert_eq!(mr.absorbed_relationship_id, absorbed.id);
    assert_eq!(mr.absorber_relationship_id, Some(absorber.id));
    assert_eq!(mr.moved_evidence_ids.len(), 2);
    assert!(mr.absorber_pre_merge.is_some());
    let pre = mr.absorber_pre_merge.as_ref().unwrap();
    assert_eq!(pre.evidence_count, 1);
    assert_eq!(pre.confidence, 0.5);

    assert!(h.db.get_relationship(absorbed.id).await.unwrap().is_none());
    let got_abs = h.db.get_relationship(absorber.id).await.unwrap().unwrap();
    assert_eq!(got_abs.evidence_count, 3);
    assert_eq!(got_abs.confidence, 0.8);

    let moved =
        h.db.list_relationship_evidence(absorber.id, MERGE_REF_CAP)
            .await
            .unwrap();
    assert_eq!(moved.len(), 3);
    assert!(moved.iter().any(|e| e.id == ev1.id));
    assert!(moved.iter().any(|e| e.id == ev2.id));
    assert!(moved.iter().any(|e| e.id == ev_abs.id));
}

#[tokio::test]
async fn self_loop_relationship_is_deleted() {
    let h = open_harness().await;
    let survivor = entity("s-loop", EntityType::Organization);
    let merged = entity("m-loop", EntityType::Organization);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();

    let loop_rel = relationship(
        merged.id,
        RelationshipType::AssociatedWith,
        survivor.id,
        1,
        0.4,
    );
    h.db.put_relationship(&loop_rel).await.unwrap();
    let ev = evidence(loop_rel.id);
    h.db.put_relationship_evidence(&ev).await.unwrap();

    let history = h
        .service
        .execute_merge(survivor.id, merged.id, "自迴圈".into(), "tester".into())
        .await
        .expect("merge");

    assert_eq!(history.merged_relationships.len(), 1);
    let mr = &history.merged_relationships[0];
    assert_eq!(mr.absorbed_relationship_id, loop_rel.id);
    assert!(mr.absorber_relationship_id.is_none());
    assert!(mr.moved_evidence_ids.is_empty());
    assert!(h.db.get_relationship(loop_rel.id).await.unwrap().is_none());
    assert!(
        h.db.get_relationship_evidence(ev.id)
            .await
            .unwrap()
            .is_none(),
        "自迴圈 evidence 應被 CASCADE 刪掉"
    );
}

#[tokio::test]
async fn prechecks_fail_without_touching_store() {
    let h = open_harness().await;
    let person = entity("alice", EntityType::Person);
    let org = entity("acme", EntityType::Organization);
    h.db.put_entity(&person).await.unwrap();
    h.db.put_entity(&org).await.unwrap();

    let err = h
        .service
        .execute_merge(person.id, person.id, "自己".into(), "tester".into())
        .await
        .unwrap_err();
    assert!(
        matches!(err, MergeError::SelfMerge { entity_id } if entity_id == person.id),
        "實際：{err:?}"
    );

    let err = h
        .service
        .execute_merge(person.id, org.id, "跨型".into(), "tester".into())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            MergeError::TypeMismatch {
                survivor_type: EntityType::Person,
                merged_type: EntityType::Organization
            }
        ),
        "實際：{err:?}"
    );
    let still = h.db.get_entity(org.id).await.unwrap().unwrap();
    assert!(still.merged_into.is_none());

    let a = entity("a-already", EntityType::Person);
    let b = entity("b-already", EntityType::Person);
    let c = entity("c-already", EntityType::Person);
    h.db.put_entity(&a).await.unwrap();
    h.db.put_entity(&b).await.unwrap();
    h.db.put_entity(&c).await.unwrap();
    h.service
        .execute_merge(b.id, a.id, "先併".into(), "tester".into())
        .await
        .unwrap();
    let err = h
        .service
        .execute_merge(c.id, a.id, "再併".into(), "tester".into())
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            MergeError::AlreadyMerged { entity_id, merged_into }
                if entity_id == a.id && merged_into == b.id
        ),
        "實際：{err:?}"
    );
    let still_a = h.db.get_entity(a.id).await.unwrap().unwrap();
    assert_eq!(still_a.merged_into, Some(b.id));
}

#[tokio::test]
async fn undo_restores_basic_merge() {
    let h = open_harness().await;
    let survivor = entity("s-undo", EntityType::Domain);
    let merged = entity("m-undo", EntityType::Domain);
    let third = entity("t-undo", EntityType::Domain);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();
    h.db.put_entity(&third).await.unwrap();

    let a = alias(merged.id, "old-name");
    let ident = identifier(merged.id, "domain", "old.example");
    let ex = extraction(merged.id);
    let rel = relationship(third.id, RelationshipType::BelongsTo, merged.id, 1, 0.5);
    h.db.put_entity_alias(&a).await.unwrap();
    h.db.put_entity_identifier(&ident).await.unwrap();
    h.db.put_entity_extraction(&ex).await.unwrap();
    h.db.put_relationship(&rel).await.unwrap();

    let history = h
        .service
        .execute_merge(survivor.id, merged.id, "undo 基本".into(), "tester".into())
        .await
        .unwrap();
    h.service.undo_merge(history.id).await.expect("undo");

    let got_merged = h.db.get_entity(merged.id).await.unwrap().unwrap();
    assert!(got_merged.merged_into.is_none());
    assert_eq!(
        h.db.get_entity_alias(a.id)
            .await
            .unwrap()
            .unwrap()
            .entity_id,
        merged.id
    );
    assert_eq!(
        h.db.get_entity_identifier(ident.id)
            .await
            .unwrap()
            .unwrap()
            .entity_id,
        merged.id
    );
    assert_eq!(
        h.db.get_entity_extraction(ex.id)
            .await
            .unwrap()
            .unwrap()
            .entity_id,
        merged.id
    );
    let got_rel = h.db.get_relationship(rel.id).await.unwrap().unwrap();
    assert_eq!(got_rel.target_object_id, merged.id);
    let hist = h.db.get_merge_history(history.id).await.unwrap().unwrap();
    assert!(hist.undone_at.is_some());
}

#[tokio::test]
async fn undo_restores_collision() {
    let h = open_harness().await;
    let survivor = entity("s-ucol", EntityType::Person);
    let merged = entity("m-ucol", EntityType::Person);
    let third = entity("t-ucol", EntityType::Organization);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();
    h.db.put_entity(&third).await.unwrap();

    let absorbed = relationship(merged.id, RelationshipType::MemberOf, third.id, 2, 0.9);
    let absorber = relationship(survivor.id, RelationshipType::MemberOf, third.id, 1, 0.4);
    h.db.put_relationship(&absorbed).await.unwrap();
    h.db.put_relationship(&absorber).await.unwrap();
    let ev = evidence(absorbed.id);
    h.db.put_relationship_evidence(&ev).await.unwrap();

    let history = h
        .service
        .execute_merge(survivor.id, merged.id, "undo 碰撞".into(), "tester".into())
        .await
        .unwrap();
    h.service.undo_merge(history.id).await.expect("undo");

    let restored = h.db.get_relationship(absorbed.id).await.unwrap().unwrap();
    assert_eq!(restored.source_object_id, merged.id);
    assert_eq!(restored.evidence_count, 2);
    assert_eq!(restored.confidence, 0.9);

    let got_abs = h.db.get_relationship(absorber.id).await.unwrap().unwrap();
    assert_eq!(got_abs.evidence_count, 1);
    assert_eq!(got_abs.confidence, 0.4);

    let got_ev =
        h.db.get_relationship_evidence(ev.id)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(got_ev.relationship_id, absorbed.id);
}

#[tokio::test]
async fn undo_twice_is_rejected() {
    let h = open_harness().await;
    let survivor = entity("s-twice", EntityType::Hash);
    let merged = entity("m-twice", EntityType::Hash);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();

    let history = h
        .service
        .execute_merge(survivor.id, merged.id, "兩次 undo".into(), "tester".into())
        .await
        .unwrap();
    h.service.undo_merge(history.id).await.unwrap();
    let err = h.service.undo_merge(history.id).await.unwrap_err();
    assert!(
        matches!(err, MergeError::AlreadyUndone { id, .. } if id == history.id),
        "實際：{err:?}"
    );
}
