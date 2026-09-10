use sqlx::Error as SqlxError;
use storage_core::StorageError;

pub fn map_sqlx(err: SqlxError) -> StorageError {
    match &err {
        SqlxError::PoolTimedOut => StorageError::Timeout {
            backend: "postgres",
            message: "連線池等待逾時。請把 pool_max 調大，或檢查 Postgres 是否過載".into(),
        },
        SqlxError::PoolClosed => StorageError::Unavailable {
            backend: "postgres",
            message: "連線池已關閉".into(),
        },
        SqlxError::Database(db) => map_db(db.as_ref()),
        SqlxError::Io(_) | SqlxError::Tls(_) | SqlxError::Protocol(_) => {
            StorageError::Unavailable {
                backend: "postgres",
                message: StorageError::sanitize(&err.to_string()),
            }
        }
        SqlxError::RowNotFound => StorageError::NotFound {
            message: "列不存在".into(),
        },
        SqlxError::ColumnDecode { .. } | SqlxError::Decode(_) | SqlxError::TypeNotFound { .. } => {
            StorageError::CorruptionSuspected {
                message: format!(
                    "解碼 Postgres 列失敗：{err}。請檢查 schema 是否與 core-model 對齊"
                ),
            }
        }
        SqlxError::Migrate(migrate) => StorageError::MigrationRequired {
            message: format!("Postgres migration 失敗：{migrate}。請跑 `make migrate-postgres`"),
        },
        _ => StorageError::Unknown {
            backend: "postgres",
            message: StorageError::sanitize(&err.to_string()),
        },
    }
}

fn map_db(db: &dyn sqlx::error::DatabaseError) -> StorageError {
    let message = StorageError::sanitize(&db.to_string());
    if db.is_unique_violation() {
        return StorageError::Conflict { message };
    }
    match db.code().as_deref() {
        Some("23505") => StorageError::Conflict { message },
        Some("23503" | "23502" | "23514" | "22P02") => {
            StorageError::ConstraintViolation { message }
        }
        Some("42501") => StorageError::PermissionDenied { message },
        Some("57014") => StorageError::Timeout {
            backend: "postgres",
            message,
        },
        Some("53300" | "53400") => StorageError::CapacityExceeded { message },
        Some("57P01" | "57P02" | "57P03") => StorageError::Unavailable {
            backend: "postgres",
            message,
        },
        _ => StorageError::Unknown {
            backend: "postgres",
            message,
        },
    }
}
