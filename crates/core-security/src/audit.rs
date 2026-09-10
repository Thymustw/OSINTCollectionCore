//! AuditLog trait。V0.1 skeleton 不接到真的儲存。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::SecurityError;

/// 一筆稽核紀錄。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    pub resource: String,
    pub outcome: String,
    pub metadata: Value,
}

impl AuditEntry {
    #[must_use]
    pub fn new(
        actor: impl Into<String>,
        action: impl Into<String>,
        resource: impl Into<String>,
        outcome: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            timestamp: Utc::now(),
            actor: actor.into(),
            action: action.into(),
            resource: resource.into(),
            outcome: outcome.into(),
            metadata: Value::Null,
        }
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }
}

/// 稽核寫入。實作可接到 Postgres／檔案；這裡只定義介面。
#[async_trait]
pub trait AuditLog: Send + Sync {
    async fn append(&self, entry: AuditEntry) -> Result<(), SecurityError>;
}

/// 記憶體稽核，給測試與尚未接 DB 的 skeleton。
#[derive(Debug, Default, Clone)]
pub struct MemoryAuditLog {
    inner: Arc<Mutex<Vec<AuditEntry>>>,
}

impl MemoryAuditLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn entries(&self) -> Vec<AuditEntry> {
        self.inner.lock().expect("audit lock").clone()
    }
}

#[async_trait]
impl AuditLog for MemoryAuditLog {
    async fn append(&self, entry: AuditEntry) -> Result<(), SecurityError> {
        self.inner.lock().expect("audit lock").push(entry);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_append() {
        let log = MemoryAuditLog::new();
        log.append(AuditEntry::new("alice", "job.retry", "job/1", "ok"))
            .await
            .unwrap();
        assert_eq!(log.entries().len(), 1);
        assert_eq!(log.entries()[0].action, "job.retry");
    }
}
