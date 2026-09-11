//! deduplicator 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum DeduplicatorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(
        "object.normalized payload 缺少 `{field}`。請確認 normalizer 送出的 envelope.payload 含 document_ids 陣列"
    )]
    MissingField { field: String },
    #[error(
        "Document `{id}` 的 duplicate 鏈超過 {max} 層仍找不到 canonical。\
         這代表 documents.duplicate_of 形成了環，請查 `SELECT id, duplicate_of FROM documents WHERE id = '{id}'` 往上追"
    )]
    DuplicateChainTooDeep { id: String, max: u32 },
    #[error("SimHash 計算 task 中止：{message}。這通常代表行程正在關閉，重新消費該事件即可")]
    HashTaskFailed { message: String },
    #[error("{message}")]
    Configuration { message: String },
}
