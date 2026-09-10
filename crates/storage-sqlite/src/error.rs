use sqlx::Error as SqlxError;
use storage_core::StorageError;

pub fn map_sqlx(err: SqlxError) -> StorageError {
    match &err {
        SqlxError::PoolTimedOut => StorageError::Timeout {
            backend: "sqlite",
            message: "連線池等待逾時。SQLite 寫入應維持低併發，請檢查是否有長交易卡住".into(),
        },
        SqlxError::PoolClosed => StorageError::Unavailable {
            backend: "sqlite",
            message: "連線池已關閉".into(),
        },
        SqlxError::Database(db) => map_db(db.as_ref()),
        SqlxError::Io(_) => StorageError::Unavailable {
            backend: "sqlite",
            message: StorageError::sanitize(&err.to_string()),
        },
        SqlxError::RowNotFound => StorageError::NotFound {
            message: "列不存在".into(),
        },
        SqlxError::ColumnDecode { .. } | SqlxError::Decode(_) | SqlxError::TypeNotFound { .. } => {
            StorageError::CorruptionSuspected {
                message: format!(
                    "解碼 SQLite 列失敗：{err}。請確認欄位是 TEXT UUID / RFC3339 / JSON 字串"
                ),
            }
        }
        SqlxError::Migrate(migrate) => StorageError::MigrationRequired {
            message: format!(
                "SQLite migration 失敗：{migrate}。請跑 `make migrate-sqlite` 或確認檔案可寫"
            ),
        },
        _ => StorageError::Unknown {
            backend: "sqlite",
            message: StorageError::sanitize(&err.to_string()),
        },
    }
}

fn map_db(db: &dyn sqlx::error::DatabaseError) -> StorageError {
    let message = StorageError::sanitize(&db.to_string());
    if db.is_unique_violation() || message.contains("UNIQUE constraint") {
        return StorageError::Conflict { message };
    }
    let code = db.code();
    let code = code.as_deref().unwrap_or("");
    if code == "787"
        || message.contains("FOREIGN KEY constraint")
        || message.contains("CHECK constraint")
        || message.contains("NOT NULL constraint")
    {
        return StorageError::ConstraintViolation { message };
    }
    if code == "5" || message.contains("database is locked") {
        return StorageError::Timeout {
            backend: "sqlite",
            message,
        };
    }
    if code == "8" || message.contains("attempt to write a readonly") {
        return StorageError::PermissionDenied { message };
    }
    if code == "13" || message.contains("database or disk is full") {
        return StorageError::CapacityExceeded { message };
    }
    if code == "11" || code == "26" || message.contains("malformed") {
        return StorageError::CorruptionSuspected { message };
    }
    StorageError::Unknown {
        backend: "sqlite",
        message,
    }
}
