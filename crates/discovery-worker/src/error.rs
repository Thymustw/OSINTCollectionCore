//! discovery-worker 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryWorkerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("{message}")]
    Configuration { message: String },
}
