//! AuditLog trait 與記憶體實作。
//!
//! # 為什麼 resource 拆成 `resource_type` + `resource_id`
//!
//! Phase 6a 之前這裡只有一個 `resource: String`，內容是 `"source/<uuid>"` 這種
//! 人看得懂但機器要 parse 的字串。稽核一旦落地成資料表，最常見的查詢就是
//! 「這個 job 身上發生過什麼」——用單一字串欄位只能 `LIKE 'job/%'`，走不到索引，
//! 而且 `resource_id` 是否為 UUID 完全靠慣例維持。
//!
//! 所以改成兩欄，並在 `0006_audit_log_api_tokens.sql` 建
//! `(resource_type, resource_id)` 複合索引。要顯示成原本那種字串時用
//! [`AuditEntry::resource`]。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::SecurityError;

/// list 查詢的每頁上限。與 storage adapter 的 `clamp_limit` 同一套語意：
/// 呼叫端傳 0 或超大值都夾回 1..=100，稽核表不接受無界查詢。
fn clamp_limit(limit: u32) -> usize {
    limit.clamp(1, 100) as usize
}

/// 一筆稽核紀錄。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    /// 資源種類，例如 `job`／`source`／`api_token`／`auth`。
    pub resource_type: String,
    /// 資源識別碼。動作不對應單一資源時（例如認證失敗）為 `None`。
    pub resource_id: Option<String>,
    pub outcome: String,
    /// 呼叫端 IP。取不到（非 HTTP 路徑、或前面沒有可信任的 proxy）時為 `None`。
    pub ip: Option<String>,
    pub metadata: Value,
}

impl AuditEntry {
    /// `id` 用 UUID v7：稽核的 cursor 分頁靠它排序，必須有時間序。
    #[must_use]
    pub fn new(
        actor: impl Into<String>,
        action: impl Into<String>,
        resource_type: impl Into<String>,
        resource_id: Option<String>,
        outcome: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            timestamp: Utc::now(),
            actor: actor.into(),
            action: action.into(),
            resource_type: resource_type.into(),
            resource_id,
            outcome: outcome.into(),
            ip: None,
            metadata: Value::Null,
        }
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: Value) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub fn with_ip(mut self, ip: Option<String>) -> Self {
        self.ip = ip;
        self
    }

    /// 顯示用的 `type/id`（沒有 id 時就只有 type）。
    #[must_use]
    pub fn resource(&self) -> String {
        match &self.resource_id {
            Some(id) => format!("{}/{id}", self.resource_type),
            None => self.resource_type.clone(),
        }
    }
}

/// 稽核寫入與查詢。
///
/// **查詢方法不是裝飾。** 只能寫不能讀的稽核等於沒有稽核——沒有人能在事後回答
/// 「誰撤銷了那把 token」。`list` 與 `list_by_resource` 是 Operations Center
/// 與事件調查的唯一入口。
#[async_trait]
pub trait AuditLog: Send + Sync {
    async fn append(&self, entry: AuditEntry) -> Result<(), SecurityError>;

    /// 依 `id`（UUID v7）由新到舊列出。`after` 是上一頁最後一筆 id（**嚴格小於**），
    /// `limit` 由實作夾在 1..=100。cursor 語意與 `RelationalStore::list_jobs` 一致。
    async fn list(&self, after: Option<Uuid>, limit: u32)
    -> Result<Vec<AuditEntry>, SecurityError>;

    /// 某一個資源身上的全部稽核，依 `id` 由新到舊，最多 100 筆。
    async fn list_by_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Vec<AuditEntry>, SecurityError>;
}

/// 記憶體稽核，給測試與尚未接 DB 的本機執行用。
///
/// ⚠️ 行程結束就消失。生產一律用 `storage_postgres::PostgresAuditLog`。
#[derive(Debug, Default, Clone)]
pub struct MemoryAuditLog {
    inner: Arc<Mutex<Vec<AuditEntry>>>,
}

impl MemoryAuditLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// 依寫入順序回傳全部紀錄（測試用；不做 cursor 分頁）。
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

    async fn list(
        &self,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<AuditEntry>, SecurityError> {
        let guard = self.inner.lock().expect("audit lock");
        let mut rows: Vec<AuditEntry> = guard
            .iter()
            .filter(|e| after.is_none_or(|cursor| e.id < cursor))
            .cloned()
            .collect();
        drop(guard);
        rows.sort_by_key(|row| std::cmp::Reverse(row.id));
        rows.truncate(clamp_limit(limit));
        Ok(rows)
    }

    async fn list_by_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Vec<AuditEntry>, SecurityError> {
        let guard = self.inner.lock().expect("audit lock");
        let mut rows: Vec<AuditEntry> = guard
            .iter()
            .filter(|e| {
                e.resource_type == resource_type && e.resource_id.as_deref() == Some(resource_id)
            })
            .cloned()
            .collect();
        drop(guard);
        rows.sort_by_key(|row| std::cmp::Reverse(row.id));
        rows.truncate(clamp_limit(100));
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_append() {
        let log = MemoryAuditLog::new();
        log.append(AuditEntry::new(
            "alice",
            "job.retry",
            "job",
            Some("1".into()),
            "ok",
        ))
        .await
        .unwrap();
        assert_eq!(log.entries().len(), 1);
        assert_eq!(log.entries()[0].action, "job.retry");
        assert_eq!(log.entries()[0].resource(), "job/1");
    }

    #[tokio::test]
    async fn memory_list_is_newest_first_and_clamped() {
        let log = MemoryAuditLog::new();
        for i in 0..5 {
            log.append(AuditEntry::new(
                "alice",
                "job.retry",
                "job",
                Some(i.to_string()),
                "ok",
            ))
            .await
            .unwrap();
        }
        let rows = log.list(None, 3).await.unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.windows(2).all(|w| w[0].id > w[1].id), "必須由新到舊");

        // limit=0 要被夾成 1，不可變成無界查詢。
        assert_eq!(log.list(None, 0).await.unwrap().len(), 1);

        // cursor 是嚴格小於：拿最新那筆的 id 當 cursor 就不該再看到它自己。
        let newest = rows[0].id;
        assert!(
            log.list(Some(newest), 10)
                .await
                .unwrap()
                .iter()
                .all(|e| e.id < newest)
        );
    }

    #[tokio::test]
    async fn memory_list_by_resource_filters_both_columns() {
        let log = MemoryAuditLog::new();
        log.append(AuditEntry::new(
            "alice",
            "job.create",
            "job",
            Some("a".into()),
            "ok",
        ))
        .await
        .unwrap();
        log.append(AuditEntry::new(
            "alice",
            "source.create",
            "source",
            Some("a".into()),
            "ok",
        ))
        .await
        .unwrap();
        let rows = log.list_by_resource("job", "a").await.unwrap();
        assert_eq!(rows.len(), 1, "resource_type 不同不該混進來");
        assert_eq!(rows[0].action, "job.create");
    }
}
