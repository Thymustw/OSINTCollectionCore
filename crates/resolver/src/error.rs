//! resolver 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_model::EntityId;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(
        "找不到 Entity `{entity_id}`，無法跑 resolution。\
         請確認呼叫端傳入的是已寫入 canonical store 的 id；\
         可先用 `get_entity` 查，或核對上游事件的 payload 是否過期"
    )]
    EntityNotFound { entity_id: EntityId },
}
