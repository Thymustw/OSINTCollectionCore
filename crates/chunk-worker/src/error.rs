//! chunk-worker 錯誤。訊息要指出下一步做什麼。

use chunker::ChunkerError;
use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum ChunkWorkerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Chunker(#[from] ChunkerError),
    #[error(
        "dedup.completed payload 缺少 `{field}`。\
         請確認 deduplicator 送出的 envelope.payload 含 document_id 與 is_duplicate"
    )]
    MissingField { field: String },
    #[error("切段 task 中止：{message}。通常代表行程正在關閉，重新消費該事件即可（段落尚未寫入）")]
    TaskFailed { message: String },
    #[error("{message}")]
    Configuration { message: String },
}
