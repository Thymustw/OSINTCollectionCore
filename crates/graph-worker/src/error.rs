//! graph-worker 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum GraphWorkerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(
        "relationship.changed payload 缺少 `{field}`。\
         請確認 entity-worker／merge 送出的 envelope.payload 含 relationship_id、\
         source_object_id、target_object_id、change_kind"
    )]
    MissingField { field: String },
    #[error(
        "relationship.changed 的 change_kind `{got}` 不是 upserted 或 deleted。\
         請確認生產者沒有自己發明第三種值；這則事件不會寫進圖，也不該被當成成功"
    )]
    InvalidChangeKind { got: String },
    #[error("{message}")]
    Configuration { message: String },
}
