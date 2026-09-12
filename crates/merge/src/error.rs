//! merge 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use chrono::{DateTime, Utc};
use core_model::{EntityId, EntityType, MergeHistoryId, RelationshipId};
use storage_core::StorageError;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum MergeError {
    #[error(
        "找不到 Entity `{entity_id}`，無法執行 merge／undo。\
         請確認兩端都已寫入 canonical store"
    )]
    EntityNotFound { entity_id: EntityId },

    #[error("不能把 Entity 併入自己：{entity_id}")]
    SelfMerge { entity_id: EntityId },

    #[error(
        "entity_type 不同不能合併：survivor={survivor_type:?}，merged={merged_type:?}。\
         請確認 Resolution Review 送進來的是同一種類的兩個 Entity"
    )]
    TypeMismatch {
        survivor_type: EntityType,
        merged_type: EntityType,
    },

    #[error(
        "Entity `{entity_id}` 已經被併掉（merged_into={merged_into}），不能再當 merge 的一端。\
         若要重做，請先 undo 那次 merge"
    )]
    AlreadyMerged {
        entity_id: EntityId,
        merged_into: EntityId,
    },

    #[error("MergeHistory `{id}` 不存在。請確認傳入的是 put_merge_history 寫入的 id")]
    HistoryNotFound { id: MergeHistoryId },

    #[error(
        "MergeHistory `{id}` 已經被 undo 過（{undone_at}）。\
         重複 undo 會把後來無關的參照一併改回去"
    )]
    AlreadyUndone {
        id: MergeHistoryId,
        undone_at: DateTime<Utc>,
    },

    #[error(
        "Survivor `{entity_id}` 之後又被併進了 `{merged_into}`，請先 undo 那次 merge，\
         再 undo 這一筆。順序顛倒會把參照寫到已不獨立的 Entity 上"
    )]
    SurvivorLaterMerged {
        entity_id: EntityId,
        merged_into: EntityId,
    },

    #[error(
        "收集 merge 參照時 `{collection}` 達到上限 {cap}（id `{id}`）。\
         RelationalStore 的 list 方法實際只會回最多 100 筆，多出來的列不會被處理。\
         這次操作已中止，以免留下半改的圖。請先減少該 Entity 的關聯密度再重試"
    )]
    CollectionTruncated {
        collection: &'static str,
        id: Uuid,
        cap: u32,
    },

    #[error(
        "undo merge 時找不到 `{table}` 的列 `{row_id}`（欄位 `{column}`）。\
         MergeHistory 記錄的參照與資料庫不一致，已中止以免寫錯列。\
         請查 merge_history 與該表"
    )]
    ReferenceMissing {
        table: String,
        row_id: Uuid,
        column: String,
    },

    #[error(
        "MergeHistory 裡有無法處理的 table `{table}`（列 `{row_id}`）。\
         這次 undo 已中止。請確認 merge 與 undo 支援同一組表"
    )]
    UnknownReferenceTable { table: String, row_id: Uuid },

    #[error(
        "MergeHistory 裡 `{table}` 列 `{row_id}` 的欄位 `{column}` 無法還原。\
         這次 undo 已中止"
    )]
    UnknownReferenceColumn {
        table: String,
        row_id: Uuid,
        column: String,
    },

    #[error(
        "MergeHistory `{id}` 的碰撞紀錄缺少 absorber_pre_merge，\
         無法還原 absorber `{absorber_id}` 的聚合欄位。已中止 undo"
    )]
    MissingAbsorberSnapshot {
        id: MergeHistoryId,
        absorber_id: RelationshipId,
    },

    #[error(transparent)]
    Storage(#[from] StorageError),
}
