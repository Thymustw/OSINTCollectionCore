//! chunk-embedder 錯誤。

use core_events::EventError;
use storage_core::StorageError;
use uuid::Uuid;

use crate::inference::InferenceError;

#[derive(Debug, thiserror::Error)]
pub enum ChunkEmbedderError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Inference(#[from] InferenceError),
    #[error("document.chunked payload 缺少 `{field}`")]
    MissingField { field: String },
    #[error(
        "文件 {document_id} 的段落寫入 OpenSearch 有 {errors} 筆失敗（第一筆：{first}）。\
         mapping 不符要 --rebuild --drop；暫時性錯誤重送事件即可"
    )]
    Bulk {
        document_id: Uuid,
        errors: u32,
        first: String,
    },
    /// 收到停止訊號時正在退避或推論。事件**不提交**，下次啟動重送。
    #[error("收到停止訊號，這份文件未完成，下次啟動會重送")]
    Stopped,
    #[error("{message}")]
    Configuration { message: String },
}
