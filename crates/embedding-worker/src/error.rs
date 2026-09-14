//! embedding-worker 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingWorkerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(
        "entity.extracted payload 缺少 `{field}`。\
         請確認 entity-worker 送出的 envelope.payload 含 document_id（UUID）\
         與 entity_ids（UUID 陣列）"
    )]
    MissingField { field: String },
    #[error("{message}")]
    Configuration { message: String },
}
