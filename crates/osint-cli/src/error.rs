//! CLI 錯誤。每一則訊息都要讓使用者知道「下一步做什麼」，不是把底層例外原文丟出來。

use core_events::EventError;
use storage_core::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error(
        "讀取設定失敗：{message}\n下一步：請在 repo 根目錄執行 osint-cli（它會往上找 config/default.toml），\n或用 OSINT_CONFIG_FILE 指定設定檔路徑。"
    )]
    Config { message: String },

    #[error(
        "連不上 PostgreSQL：{message}\n下一步：\n  1. 確認 repo 根目錄有 `.env`（可從 `.env.example` 複製）且 DATABASE_URL 正確\n  2. 執行 `make compose-up` 啟動本機基礎建設\n  3. 用 `make compose-ps` 確認 osint-core-postgres-1 是 healthy"
    )]
    PostgresUnavailable { message: String },

    #[error(
        "連不上物件儲存（MinIO/S3）：{message}\n下一步：確認 `.env` 的 S3_ENDPOINT／MINIO_ROOT_USER／MINIO_ROOT_PASSWORD，\n並用 `make compose-ps` 確認 osint-core-minio-1 是 healthy。"
    )]
    ObjectStoreUnavailable { message: String },

    #[error(
        "連不上搜尋投影（OpenSearch）：{message}\n下一步：\n  1. 確認 `.env` 的 OPENSEARCH_URL（本機 dev 是 http://127.0.0.1:19200，不是 9200——那是別的堆疊）\n  2. 用 `make compose-ps` 確認 osint-core-opensearch-1 是 healthy\n  3. 若從沒跑過索引，先 `make run-indexer` 或 `make rebuild-index`"
    )]
    SearchUnavailable { message: String },

    #[error("查詢失敗：{message}")]
    Storage { message: String },

    #[error(
        "找不到 {kind} `{id}`。\n下一步：用 `osint-cli {list_hint}` 看目前有哪些 id；id 是 UUID，請整串貼上不要截斷。"
    )]
    NotFound {
        kind: &'static str,
        id: String,
        list_hint: &'static str,
    },

    /// 參數值不合法。**要在查詢之前就擋下來**——打錯字的篩選值若直接送進查詢，
    /// 使用者看到的會是「沒有資料」，於是以為資料庫是空的，而不是自己少打一個字母。
    #[error("參數不合法：{message}")]
    InvalidArgument { message: String },

    #[error("Redpanda 查詢失敗：{message}")]
    Broker { message: String },

    #[error("輸出序列化失敗：{message}。這是 osint-cli 的 bug，請回報。")]
    Output { message: String },
}

impl From<StorageError> for CliError {
    fn from(err: StorageError) -> Self {
        Self::Storage {
            message: with_schema_hint(err.to_string()),
        }
    }
}

/// 資料表不存在時，底層訊息只會說 `relation "documents" does not exist`——
/// 那完全不指向真因（migration 沒跑），使用者會以為是 CLI 壞了。在這裡補上動作指引。
fn with_schema_hint(message: String) -> String {
    let lowered = message.to_lowercase();
    if lowered.contains("does not exist") || lowered.contains("no such table") {
        format!(
            "{message}\n下一步：資料表還不存在，請先跑 `make migrate-postgres`（SQLite 則是 `make migrate-sqlite`）。"
        )
    } else {
        message
    }
}

impl From<EventError> for CliError {
    fn from(err: EventError) -> Self {
        Self::Broker {
            message: err.to_string(),
        }
    }
}

impl From<serde_json::Error> for CliError {
    fn from(err: serde_json::Error) -> Self {
        Self::Output {
            message: err.to_string(),
        }
    }
}
