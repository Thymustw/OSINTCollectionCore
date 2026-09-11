//! normalizer 錯誤。訊息會指出下一步。

use core_events::EventError;
use storage_core::StorageError;

/// 正規化失敗。
#[derive(Debug, thiserror::Error)]
pub enum NormalizerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(
        "raw.collected payload 缺少 `{field}`。請確認 collector 送出的 envelope.payload 含 raw_evidence_id"
    )]
    MissingField { field: String },
    #[error("找不到 RawEvidence `{id}`。請確認 collector 已寫入 Postgres，或這則事件已過期")]
    EvidenceMissing { id: String },
    #[error(
        "MinIO 讀不到 `{path}`。請確認 collector 有把 body 寫進物件儲存，且 bucket 是 raw-evidence"
    )]
    BodyMissing { path: String },
    #[error("{message}")]
    Configuration { message: String },
}
