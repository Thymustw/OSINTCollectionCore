//! neo4rs 錯誤 → [`storage_core::StorageError`]。

use neo4rs::Error;
use storage_core::StorageError;

const BACKEND: &str = "neo4j";

pub(crate) fn map_neo(err: Error) -> StorageError {
    let message = StorageError::sanitize(&err.to_string());
    match err {
        Error::IOError { .. } | Error::ConnectionError => StorageError::Unavailable {
            backend: BACKEND,
            message,
        },
        Error::AuthenticationError(_) => StorageError::PermissionDenied { message },
        Error::UrlParseError(_) | Error::UnsupportedScheme(_) | Error::InvalidConfig => {
            StorageError::Configuration { message }
        }
        Error::Neo4j(neo) => map_neo4j_code(neo.code(), message),
        Error::DeserializationError(_) | Error::ConversionError | Error::UnknownType(_) => {
            StorageError::CorruptionSuspected {
                message: format!(
                    "解碼 Neo4j 列失敗：{message}。請檢查節點／邊的 property 是否與 GraphNode／GraphEdge 對齊"
                ),
            }
        }
        _ => StorageError::Unknown {
            backend: BACKEND,
            message,
        },
    }
}

fn map_neo4j_code(code: &str, message: String) -> StorageError {
    // Neo4j 5 的分類碼形如 Neo.ClientError.Schema.ConstraintValidationFailed。
    // `neo4rs::Neo4jErrorKind::new` 是 `pub(crate)`，不能從這個 crate 呼叫，
    // 所以用碼字串分類。
    if code.contains("Security")
        || code.contains("Authentication")
        || code.contains("Authorization")
        || code.contains("Forbidden")
    {
        return StorageError::PermissionDenied { message };
    }
    if code.contains("ConstraintValidationFailed") || code.contains("ConstraintViolation") {
        return StorageError::ConstraintViolation { message };
    }
    if code.contains("ConstraintAlreadyExists") {
        // `CREATE CONSTRAINT IF NOT EXISTS` 不該走到這裡；若走到，當 Conflict。
        return StorageError::Conflict { message };
    }
    if code.contains("Syntax") || code.contains("TypeError") || code.contains("ParameterMissing") {
        return StorageError::ConstraintViolation { message };
    }
    if code.contains("Transient") || code.contains("Deadlock") || code.contains("OutOfMemory") {
        return StorageError::Unavailable {
            backend: BACKEND,
            message: format!("{message}。這是暫時性錯誤，可稍後再試"),
        };
    }
    if code.contains("DatabaseUnavailable") || code.contains("DatabaseNotFound") {
        return StorageError::Unavailable {
            backend: BACKEND,
            message,
        };
    }
    StorageError::Unknown {
        backend: BACKEND,
        message,
    }
}
