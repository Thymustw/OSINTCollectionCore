//! 各 adapter 共用的 health 模型。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::StorageError;

/// 單次 health check 結果。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageHealth {
    pub backend: String,
    pub healthy: bool,
    pub message: String,
    #[serde(default)]
    pub details: Value,
}

impl StorageHealth {
    #[must_use]
    pub fn ok(backend: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            healthy: true,
            message: message.into(),
            details: Value::Null,
        }
    }

    #[must_use]
    pub fn down(backend: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            healthy: false,
            message: message.into(),
            details: Value::Null,
        }
    }

    #[must_use]
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
}

/// 每個 adapter 都必須能做 health check。
#[async_trait]
pub trait HealthProvider: Send + Sync {
    async fn health(&self) -> Result<StorageHealth, StorageError>;
}
