//! stix-worker 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
pub enum StixWorkerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error("{message}")]
    Configuration { message: String },
}

/// 單次 `stix_import` 在轉成 Job Failed 之前的內部錯誤。
///
/// 與 [`StixWorkerError`] 分開：這裡的失敗代表「這份 Job 本身跑失敗了，事件已經
/// 消費完」，不是 health／broker 設定問題。
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error(
        "stix_import Job 缺少參數 `{field}`。\
         請確認 API 建立 Job 時 parameters 含 source_id 與 raw_evidence_id（UUID）；\
         這個 Job 不會重試"
    )]
    MissingParameter { field: String },
    #[error(
        "找不到 raw_evidence `{id}`。\
         請確認 Job 參數 raw_evidence_id 是否指到已落地的證據"
    )]
    EvidenceNotFound { id: Uuid },
    #[error(
        "物件儲存沒有 key `{path}`（raw_evidence `{raw_evidence_id}`）。\
         請確認 [storage.object] 的 bucket／endpoint 與 RawEvidence.storage_path 一致，\
         並確認匯入當下有把 bundle 寫進物件儲存"
    )]
    BlobMissing { raw_evidence_id: Uuid, path: String },
    #[error(
        "STIX bundle JSON 無法解析：{message}。\
         請確認物件儲存裡的檔案是 STIX 2.1 bundle（type=bundle、id、objects）"
    )]
    BundleParse { message: String },
    #[error(
        "這個 STIX bundle 對應出 {mapped} 個 Entity，超過 [stix_worker].max_objects_per_tx={max}。\
         請拆成較小的 bundle 分次匯入，或請管理者調高該上限。\
         本次不會拆交易、整批已標記 Failed"
    )]
    TooManyObjects { mapped: usize, max: usize },
    #[error(
        "stix_export Job 的 filter 參數解析失敗：{message}。\
         請確認 entity_types 是合法的 EntityType（snake_case，例如 person／threat_actor）、\
         entity_ids 是 UUID 陣列"
    )]
    InvalidFilter { message: String },
    #[error(
        "這次匯出符合 {matched} 個 Entity，超過 [stix].max_objects={max}。\
         請縮小 filter（entity_ids／entity_types／depth）或請管理者調高該上限。\
         這個 Job 已標記 Failed，不會自動重試"
    )]
    TooManyExportObjects { matched: usize, max: usize },
    #[error(transparent)]
    Storage(#[from] StorageError),
}
