//! entity-worker 錯誤。訊息要指出下一步做什麼，不要只把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum EntityWorkerError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(
        "dedup.completed payload 缺少 `{field}`。\
         請確認 deduplicator 送出的 envelope.payload 含 document_id 與 is_duplicate 兩個欄位"
    )]
    MissingField { field: String },
    #[error(
        "抽取 task 中止：{message}。這通常代表行程正在關閉，重新消費該事件即可\
         （claim 尚未寫入，不會被當成已處理）"
    )]
    ExtractionTaskFailed { message: String },
    #[error(
        "Entity `{entity_type}` / `{normalized_name}` 的自然鍵發生衝突，但重查不到既有列。\
         請查 `SELECT * FROM entities WHERE entity_type = '{entity_type}' \
         AND normalized_name = '{normalized_name}'`——\
         查得到代表 idx_entities_natural_key 與 Entity id 的 UUID v5 推導不一致，\
         查不到代表 migration 0005 沒有套用"
    )]
    EntityConflict {
        entity_type: String,
        normalized_name: String,
    },
    #[error("{message}")]
    Configuration { message: String },
}
