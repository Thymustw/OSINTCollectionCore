//! [`MergeService`]：在一筆交易裡執行 merge／undo。

use chrono::Utc;
use core_model::{
    AbsorberSnapshot, Entity, EntityId, MergeHistory, MergeHistoryId, MergedRelationship,
    Relationship, RelationshipEvidence, RepointedReference,
};
use storage_core::{RelationalStore, TransactionalStore};
use tracing::warn;
use uuid::Uuid;

use crate::collision::classify_relationships;
use crate::error::MergeError;
use crate::repoint::{relationship_repoint_column, repointed, repointed_ends};

/// 收集 merge 要改寫的參照時，每個 `list_*` 的上限。
///
/// 任務原文寫 1000，但 [`storage_core::RelationalStore`] 的 list 方法契約與
/// adapter 實作都把 `limit` 夾在 1..=100（見 `storage-sqlite`／`storage-postgres`
/// 的 `clamp_limit`）。傳 1000 會被靜默截成 100，呼叫端還以為收齊了。
/// 這裡對齊實際能拿到的上限；達到就中止，不允許半改。
pub const MERGE_REF_CAP: u32 = 100;

/// 執行 Entity merge／undo。
///
/// 用具體型別參數 `S: TransactionalStore`，不包 `Arc<dyn TransactionalStore>`：
/// `begin()` 已經回 `Box<dyn Transaction>`，再包一層 dyn 沒有額外的 object-safety
/// 收益，測試與呼叫端直接持有 adapter 即可。
pub struct MergeService<S: TransactionalStore> {
    store: S,
}

impl<S: TransactionalStore> MergeService<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// 把 `merged_id` 併進 `survivor_id`。成功回這次的 [`MergeHistory`]。
    ///
    /// 前置檢查在交易外；通過後開交易寫入。任何一步 `Err` 都讓交易 drop 回滾。
    pub async fn execute_merge(
        &self,
        survivor_id: EntityId,
        merged_id: EntityId,
        reason: String,
        operator: String,
    ) -> Result<MergeHistory, MergeError> {
        if survivor_id == merged_id {
            return Err(MergeError::SelfMerge {
                entity_id: survivor_id,
            });
        }

        let survivor = self.require_entity(survivor_id).await?;
        let merged = self.require_entity(merged_id).await?;

        if survivor.entity_type != merged.entity_type {
            return Err(MergeError::TypeMismatch {
                survivor_type: survivor.entity_type,
                merged_type: merged.entity_type,
            });
        }
        if let Some(into) = merged.merged_into {
            return Err(MergeError::AlreadyMerged {
                entity_id: merged_id,
                merged_into: into,
            });
        }
        if let Some(into) = survivor.merged_into {
            return Err(MergeError::AlreadyMerged {
                entity_id: survivor_id,
                merged_into: into,
            });
        }

        let tx = self.store.begin().await?;
        let history = {
            let db = tx.store();
            execute_in_tx(db, survivor, merged, reason, operator).await?
        };
        tx.commit().await?;
        Ok(history)
    }

    /// 依 [`MergeHistory`] 還原一次 merge。
    ///
    /// `repointed_references` 與 `merged_relationships` 都逆序處理：連續 merge
    /// 可能改寫同一列兩次，後面的先還原才對（見 [`RepointedReference`] 的說明）。
    pub async fn undo_merge(&self, merge_history_id: MergeHistoryId) -> Result<(), MergeError> {
        let history = self
            .store
            .get_merge_history(merge_history_id)
            .await?
            .ok_or(MergeError::HistoryNotFound {
                id: merge_history_id,
            })?;
        if let Some(undone_at) = history.undone_at {
            return Err(MergeError::AlreadyUndone {
                id: merge_history_id,
                undone_at,
            });
        }
        let survivor = self.require_entity(history.survivor_id).await?;
        if let Some(into) = survivor.merged_into {
            return Err(MergeError::SurvivorLaterMerged {
                entity_id: history.survivor_id,
                merged_into: into,
            });
        }
        // merged Entity 必須還在：merge 刻意不刪列。
        self.require_entity(history.merged_id).await?;

        let tx = self.store.begin().await?;
        {
            let db = tx.store();
            undo_in_tx(db, history).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn require_entity(&self, entity_id: EntityId) -> Result<Entity, MergeError> {
        self.store
            .get_entity(entity_id)
            .await?
            .ok_or(MergeError::EntityNotFound { entity_id })
    }
}

async fn execute_in_tx(
    db: &dyn RelationalStore,
    survivor: Entity,
    mut merged: Entity,
    reason: String,
    operator: String,
) -> Result<MergeHistory, MergeError> {
    let survivor_id = survivor.id;
    let merged_id = merged.id;

    let merged_rels = reject_truncated(
        "relationships(merged)",
        merged_id,
        db.list_relationships_by_object(merged_id, MERGE_REF_CAP)
            .await?,
    )?;
    let survivor_rels = reject_truncated(
        "relationships(survivor)",
        survivor_id,
        db.list_relationships_by_object(survivor_id, MERGE_REF_CAP)
            .await?,
    )?;
    let merged_aliases = reject_truncated(
        "entity_aliases",
        merged_id,
        db.list_entity_aliases_by_entity(merged_id, MERGE_REF_CAP)
            .await?,
    )?;
    let merged_identifiers = reject_truncated(
        "entity_identifiers",
        merged_id,
        db.list_entity_identifiers_by_entity(merged_id, MERGE_REF_CAP)
            .await?,
    )?;
    let merged_extractions = reject_truncated(
        "entity_extractions",
        merged_id,
        db.list_entity_extractions_by_entity(merged_id, MERGE_REF_CAP)
            .await?,
    )?;

    let (self_loops, collisions, safe_repoints) =
        classify_relationships(&merged_rels, &survivor_rels, merged_id, survivor_id);

    let mut repointed_references = Vec::new();
    let mut merged_relationships = Vec::new();

    for rel in self_loops {
        let snapshot = rel.clone();
        db.delete_relationship(rel.id).await?;
        merged_relationships.push(MergedRelationship {
            absorbed_relationship_id: rel.id,
            absorber_relationship_id: None,
            absorbed_snapshot: snapshot,
            absorber_pre_merge: None,
            moved_evidence_ids: Vec::new(),
        });
    }

    for collision in collisions {
        let absorbed = collision.absorbed;
        let absorber = collision.absorber;
        let evidence = reject_truncated(
            "relationship_evidence",
            absorbed.id,
            db.list_relationship_evidence(absorbed.id, MERGE_REF_CAP)
                .await?,
        )?;
        let mut moved_ids = Vec::with_capacity(evidence.len());
        for ev in evidence {
            let updated = RelationshipEvidence {
                relationship_id: absorber.id,
                ..ev
            };
            db.put_relationship_evidence(&updated).await?;
            moved_ids.push(ev.id);
        }
        let pre_merge = AbsorberSnapshot {
            evidence_count: absorber.evidence_count,
            confidence: absorber.confidence,
            first_seen: absorber.first_seen,
            last_seen: absorber.last_seen,
        };
        let now = Utc::now();
        let updated_absorber = Relationship {
            evidence_count: absorber.evidence_count + absorbed.evidence_count,
            confidence: absorber.confidence.max(absorbed.confidence),
            first_seen: absorber.first_seen.min(absorbed.first_seen),
            last_seen: absorber.last_seen.max(absorbed.last_seen),
            updated_at: now,
            ..absorber.clone()
        };
        db.put_relationship(&updated_absorber).await?;
        db.delete_relationship(absorbed.id).await?;
        merged_relationships.push(MergedRelationship {
            absorbed_relationship_id: absorbed.id,
            absorber_relationship_id: Some(absorber.id),
            absorbed_snapshot: absorbed,
            absorber_pre_merge: Some(pre_merge),
            moved_evidence_ids: moved_ids,
        });
    }

    for rel in safe_repoints {
        let column = relationship_repoint_column(&rel, merged_id).ok_or_else(|| {
            MergeError::UnknownReferenceColumn {
                table: "relationships".into(),
                row_id: rel.id,
                column: "source_object_id/target_object_id".into(),
            }
        })?;
        let (new_source, new_target) = repointed_ends(&rel, merged_id, survivor_id);
        let now = Utc::now();
        let updated = Relationship {
            source_object_id: new_source,
            target_object_id: new_target,
            updated_at: now,
            ..rel.clone()
        };
        db.put_relationship(&updated).await?;
        repointed_references.push(repointed("relationships", rel.id, column, merged_id));
    }

    for alias in merged_aliases {
        let updated = core_model::EntityAlias {
            entity_id: survivor_id,
            ..alias.clone()
        };
        db.put_entity_alias(&updated).await?;
        repointed_references.push(repointed(
            "entity_aliases",
            alias.id,
            "entity_id",
            merged_id,
        ));
    }

    for identifier in merged_identifiers {
        let updated = core_model::EntityIdentifier {
            entity_id: survivor_id,
            ..identifier.clone()
        };
        db.put_entity_identifier(&updated).await?;
        repointed_references.push(repointed(
            "entity_identifiers",
            identifier.id,
            "entity_id",
            merged_id,
        ));
    }

    for extraction in merged_extractions {
        let updated = core_model::EntityExtraction {
            entity_id: survivor_id,
            ..extraction.clone()
        };
        db.put_entity_extraction(&updated).await?;
        repointed_references.push(repointed(
            "entity_extractions",
            extraction.id,
            "entity_id",
            merged_id,
        ));
    }

    merged.merged_into = Some(survivor_id);
    db.put_entity(&merged).await?;

    let history = MergeHistory {
        id: Uuid::now_v7(),
        survivor_id,
        merged_id,
        reason,
        operator,
        timestamp: Utc::now(),
        repointed_references,
        merged_relationships,
        undone_at: None,
    };
    db.put_merge_history(&history).await?;
    Ok(history)
}

async fn undo_in_tx(db: &dyn RelationalStore, mut history: MergeHistory) -> Result<(), MergeError> {
    for r in history.repointed_references.iter().rev() {
        restore_reference(db, r).await?;
    }

    for mr in history.merged_relationships.iter().rev() {
        db.put_relationship(&mr.absorbed_snapshot).await?;
        if let Some(absorber_id) = mr.absorber_relationship_id {
            for ev_id in &mr.moved_evidence_ids {
                let ev = db.get_relationship_evidence(*ev_id).await?.ok_or(
                    MergeError::ReferenceMissing {
                        table: "relationship_evidence".into(),
                        row_id: *ev_id,
                        column: "relationship_id".into(),
                    },
                )?;
                let updated = RelationshipEvidence {
                    relationship_id: mr.absorbed_relationship_id,
                    ..ev
                };
                db.put_relationship_evidence(&updated).await?;
            }
            let mut absorber =
                db.get_relationship(absorber_id)
                    .await?
                    .ok_or(MergeError::ReferenceMissing {
                        table: "relationships".into(),
                        row_id: absorber_id,
                        column: "evidence_count".into(),
                    })?;
            let pre =
                mr.absorber_pre_merge
                    .as_ref()
                    .ok_or(MergeError::MissingAbsorberSnapshot {
                        id: history.id,
                        absorber_id,
                    })?;
            absorber.evidence_count = pre.evidence_count;
            absorber.confidence = pre.confidence;
            absorber.first_seen = pre.first_seen;
            absorber.last_seen = pre.last_seen;
            absorber.updated_at = Utc::now();
            db.put_relationship(&absorber).await?;
        } else {
            warn!(
                absorbed_relationship_id = %mr.absorbed_relationship_id,
                merge_history_id = %history.id,
                "自迴圈 relationship 已重建，但 relationship_evidence 在 merge 時被 CASCADE 刪掉，無法復原"
            );
        }
    }

    let mut merged_entity =
        db.get_entity(history.merged_id)
            .await?
            .ok_or(MergeError::EntityNotFound {
                entity_id: history.merged_id,
            })?;
    merged_entity.merged_into = None;
    db.put_entity(&merged_entity).await?;

    history.undone_at = Some(Utc::now());
    db.put_merge_history(&history).await?;
    Ok(())
}

async fn restore_reference(
    db: &dyn RelationalStore,
    r: &RepointedReference,
) -> Result<(), MergeError> {
    match r.table.as_str() {
        "relationships" => {
            let mut row =
                db.get_relationship(r.row_id)
                    .await?
                    .ok_or(MergeError::ReferenceMissing {
                        table: r.table.clone(),
                        row_id: r.row_id,
                        column: r.column.clone(),
                    })?;
            match r.column.as_str() {
                "source_object_id" => row.source_object_id = r.previous_value,
                "target_object_id" => row.target_object_id = r.previous_value,
                other => {
                    return Err(MergeError::UnknownReferenceColumn {
                        table: r.table.clone(),
                        row_id: r.row_id,
                        column: other.to_string(),
                    });
                }
            }
            row.updated_at = Utc::now();
            db.put_relationship(&row).await?;
        }
        "entity_aliases" => {
            let mut row =
                db.get_entity_alias(r.row_id)
                    .await?
                    .ok_or(MergeError::ReferenceMissing {
                        table: r.table.clone(),
                        row_id: r.row_id,
                        column: r.column.clone(),
                    })?;
            if r.column != "entity_id" {
                return Err(MergeError::UnknownReferenceColumn {
                    table: r.table.clone(),
                    row_id: r.row_id,
                    column: r.column.clone(),
                });
            }
            row.entity_id = r.previous_value;
            db.put_entity_alias(&row).await?;
        }
        "entity_identifiers" => {
            let mut row =
                db.get_entity_identifier(r.row_id)
                    .await?
                    .ok_or(MergeError::ReferenceMissing {
                        table: r.table.clone(),
                        row_id: r.row_id,
                        column: r.column.clone(),
                    })?;
            if r.column != "entity_id" {
                return Err(MergeError::UnknownReferenceColumn {
                    table: r.table.clone(),
                    row_id: r.row_id,
                    column: r.column.clone(),
                });
            }
            row.entity_id = r.previous_value;
            db.put_entity_identifier(&row).await?;
        }
        "entity_extractions" => {
            let mut row =
                db.get_entity_extraction(r.row_id)
                    .await?
                    .ok_or(MergeError::ReferenceMissing {
                        table: r.table.clone(),
                        row_id: r.row_id,
                        column: r.column.clone(),
                    })?;
            if r.column != "entity_id" {
                return Err(MergeError::UnknownReferenceColumn {
                    table: r.table.clone(),
                    row_id: r.row_id,
                    column: r.column.clone(),
                });
            }
            row.entity_id = r.previous_value;
            db.put_entity_extraction(&row).await?;
        }
        other => {
            return Err(MergeError::UnknownReferenceTable {
                table: other.to_string(),
                row_id: r.row_id,
            });
        }
    }
    Ok(())
}

/// 回傳筆數等於 [`MERGE_REF_CAP`] 就當成截斷並失敗。
///
/// 剛好等於上限時無法分辨「正好 N 筆」與「還有更多」——對 merge 來說漏收
/// 會讓圖接錯且不報錯，所以寧可誤殺（剛好 100 筆也中止），不要漏收。
fn reject_truncated<T>(
    collection: &'static str,
    id: Uuid,
    rows: Vec<T>,
) -> Result<Vec<T>, MergeError> {
    if rows.len() >= MERGE_REF_CAP as usize {
        warn!(
            collection,
            %id,
            cap = MERGE_REF_CAP,
            "merge 收集參照達到上限，中止以免漏改"
        );
        return Err(MergeError::CollectionTruncated {
            collection,
            id,
            cap: MERGE_REF_CAP,
        });
    }
    Ok(rows)
}
