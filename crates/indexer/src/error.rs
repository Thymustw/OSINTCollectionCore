//! indexer 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum IndexerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(
        "entity.extracted payload 缺少 `{field}`。\
         請確認 entity-worker 送出的 envelope.payload 含 document_id"
    )]
    MissingField { field: String },
    #[error(
        "OpenSearch bulk 有 {count} 筆永久性失敗（重試無效），最早一筆：id={id} status={status} {reason}。\
         status 400 多半是 mapping 不符——mapping 是 dynamic:strict，\
         新增欄位必須同時改 crates/indexer/src/schema.rs 並跑 `osint-indexer --rebuild`"
    )]
    BulkPermanentFailure {
        count: usize,
        id: String,
        status: u16,
        reason: String,
    },
    #[error(
        "OpenSearch bulk 連續重試 {attempts} 次仍有暫時性失敗（最後一筆 status={status}）。\
         OpenSearch 可能負載過高：請看 `_cluster/health`、把 [indexer].batch_size 調小，\
         或降低上游採集速率"
    )]
    BulkRetriesExhausted { attempts: u32, status: u16 },
    #[error("{message}")]
    Configuration { message: String },
}
