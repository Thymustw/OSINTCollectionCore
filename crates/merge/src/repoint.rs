//! 組 [`RepointedReference`] 與 relationship 兩端改寫的小工具。

use core_model::{EntityId, Relationship, RepointedReference};
use uuid::Uuid;

/// 把 relationship 裡等於 `from` 的端點換成 `to`，回傳新的 (source, target)。
///
/// 兩端都可能是 `from`（自迴圈的前身）。都不命中時原樣回傳——呼叫端不該拿
/// 一條與 `from` 無關的邊進來，但函式本身不 panic。
#[must_use]
pub fn repointed_ends(rel: &Relationship, from: EntityId, to: EntityId) -> (EntityId, EntityId) {
    let source = if rel.source_object_id == from {
        to
    } else {
        rel.source_object_id
    };
    let target = if rel.target_object_id == from {
        to
    } else {
        rel.target_object_id
    };
    (source, target)
}

/// 這條 relationship 要改哪一欄才能把 `from` 換成 survivor。
///
/// 兩端都是 `from` 時回 `None`：那種邊 repoint 後是自迴圈，不該走
/// [`RepointedReference`]，該走 [`core_model::MergedRelationship`]。
#[must_use]
pub fn relationship_repoint_column(rel: &Relationship, from: EntityId) -> Option<&'static str> {
    match (rel.source_object_id == from, rel.target_object_id == from) {
        (true, false) => Some("source_object_id"),
        (false, true) => Some("target_object_id"),
        _ => None,
    }
}

/// 組一筆「某表某列某欄從 `previous` 改走」的紀錄。
#[must_use]
pub fn repointed(
    table: impl Into<String>,
    row_id: Uuid,
    column: impl Into<String>,
    previous_value: Uuid,
) -> RepointedReference {
    RepointedReference {
        table: table.into(),
        row_id,
        column: column.into(),
        previous_value,
    }
}
