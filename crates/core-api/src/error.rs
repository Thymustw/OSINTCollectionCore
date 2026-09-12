//! 統一錯誤 JSON。

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use core_jobs::JobError;
use core_security::SecurityError;
use storage_core::StorageError;

/// API 錯誤本體。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ErrorBody {
    pub error: String,
    pub message: String,
}

/// 可轉 HTTP 的 API 錯誤。
#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub error: &'static str,
    pub message: String,
}

impl ApiError {
    #[must_use]
    pub fn new(status: StatusCode, error: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            error,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request", message)
    }

    #[must_use]
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    #[must_use]
    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden", message)
    }

    #[must_use]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }

    #[must_use]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    #[must_use]
    pub fn unprocessable(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, "unprocessable", message)
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = ErrorBody {
            error: self.error.to_string(),
            message: self.message,
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<SecurityError> for ApiError {
    fn from(err: SecurityError) -> Self {
        match err {
            SecurityError::Unauthenticated
            | SecurityError::JwtInvalid { .. }
            | SecurityError::MalformedApiToken
            | SecurityError::TokenRevoked
            | SecurityError::TokenExpired => Self::unauthorized(err.to_string()),
            SecurityError::Forbidden { .. } => Self::forbidden(err.to_string()),
            SecurityError::TokenNotFound { .. } => Self::not_found(err.to_string()),
            SecurityError::JwtSecretTooShort { .. } => Self::internal(err.to_string()),
            other => Self::unprocessable(other.to_string()),
        }
    }
}

impl From<JobError> for ApiError {
    fn from(err: JobError) -> Self {
        match err {
            JobError::NotFound { .. } => Self::not_found(err.to_string()),
            JobError::InvalidTransition { .. } | JobError::NotDispatchable { .. } => {
                Self::conflict(err.to_string())
            }
            JobError::Storage(storage) => storage.into(),
            JobError::Event(event) => Self::internal(event.to_string()),
        }
    }
}

impl From<StorageError> for ApiError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::NotFound { .. } => Self::not_found(err.to_string()),
            StorageError::Conflict { .. } => Self::conflict(err.to_string()),
            StorageError::ConstraintViolation { .. } | StorageError::Configuration { .. } => {
                Self::bad_request(err.to_string())
            }
            StorageError::Timeout { .. } => {
                Self::new(StatusCode::GATEWAY_TIMEOUT, "timeout", err.to_string())
            }
            StorageError::Unavailable { .. } => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                err.to_string(),
            ),
            other => Self::internal(other.to_string()),
        }
    }
}
