//! collector 錯誤。訊息會指出下一步。

use connector_sdk::ConnectorError;
use core_events::EventError;
use core_jobs::JobError;
use storage_core::StorageError;

/// 排程／一次收集失敗。
#[derive(Debug, thiserror::Error)]
pub enum CollectorError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Connector(#[from] ConnectorError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Job(#[from] JobError),
    #[error("找不到 source `{id}`。請確認 connectors.source_id 對應的 sources 列還在")]
    SourceMissing { id: String },
    #[error("cron `{schedule}` 無效：{message}。請用 5 欄（分 時 日 月 週），例如 `*/15 * * * *`")]
    InvalidSchedule { schedule: String, message: String },
    #[error("{message}")]
    Configuration { message: String },
}
