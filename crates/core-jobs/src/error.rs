//! Job 系統錯誤。

use core_events::EventError;
use core_model::JobStatus;
use storage_core::StorageError;

/// Job CRUD／狀態轉換／派工失敗。
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("找不到 job `{id}`。請確認 id，或用 GET /api/v1/jobs 列出")]
    NotFound { id: String },
    #[error(
        "job 不可從 `{from:?}` 轉成 `{to:?}`。合法轉換：queued→running|cancelled、running→completed|failed|retrying|cancelled、retrying→running|failed|cancelled、failed→retrying。終態 completed/cancelled 不可再轉"
    )]
    InvalidTransition { from: JobStatus, to: JobStatus },
    #[error(
        "job `{id}` 目前是 `{status:?}`，不能重試。只有 failed 的 job 可以 retry；\
             running 的請等它結束，completed／cancelled 是終態（要重跑請建一個新的 job）"
    )]
    NotRetryable { id: String, status: JobStatus },
    #[error("job `{id}` 目前是 `{status:?}`，不能派工。只有 queued 或 retrying 可以 dispatch")]
    NotDispatchable { id: String, status: JobStatus },
}

impl JobError {
    #[must_use]
    pub fn from_status(from: JobStatus, to: JobStatus) -> Self {
        Self::InvalidTransition { from, to }
    }
}
