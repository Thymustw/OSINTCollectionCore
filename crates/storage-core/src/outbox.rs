//! Transactional outbox（V0.3 階段 4-2）。
//!
//! 與業務資料同一個交易寫入；郵差另外 claim／標記。這是獨立 capability，
//! 不要把方法塞進 [`crate::traits::RelationalStore`]。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::error::StorageError;

/// `event_outbox` 的一列。
///
/// `published_at IS NULL` 代表待送（含退避中）。`id` 是 UUID v7，郵差發送時
/// 用作 envelope id——重試同一列必須送出同一個 id，下游才能辨識重複。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxEvent {
    pub id: Uuid,
    pub topic: String,
    pub partition_key: String,
    pub correlation_id: Option<Uuid>,
    pub payload: Value,
    pub source_service: String,
    pub created_at: DateTime<Utc>,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub next_attempt_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
}

impl OutboxEvent {
    /// 一筆立刻可送的新列（`attempts = 0`、`published_at = None`、
    /// `next_attempt_at = now`）。
    #[must_use]
    pub fn pending(
        topic: impl Into<String>,
        partition_key: impl Into<String>,
        correlation_id: Option<Uuid>,
        payload: Value,
        source_service: impl Into<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::now_v7(),
            topic: topic.into(),
            partition_key: partition_key.into(),
            correlation_id,
            payload,
            source_service: source_service.into(),
            created_at: now,
            attempts: 0,
            last_error: None,
            next_attempt_at: now,
            published_at: None,
        }
    }
}

/// Transactional outbox 的讀寫口。
///
/// # 交易語意
///
/// [`Self::enqueue_outbox`] **必須能在既有交易內寫**：呼叫端先 `begin()`，
/// 再 `tx.store().put_*` + `tx.outbox().enqueue_outbox`，最後 `commit`。
/// 業務寫入失敗時 outbox 列也不該看得到。
///
/// # 多郵差
///
/// PostgreSQL 用 `FOR UPDATE SKIP LOCKED`，兩個郵差不會拿到同一列。
/// SQLite **沒有 SKIP LOCKED**，前提是單一寫入者（SQLite 寫鎖本身會序列化）。
#[async_trait]
pub trait EventOutboxStore: Send + Sync {
    /// 插入一筆待送事件。同一 `id` 再寫回衝突。
    async fn enqueue_outbox(&self, event: &OutboxEvent) -> Result<(), StorageError>;

    /// 取出最多 `limit` 筆已到期、尚未送出的列，並租約到 `lease_until`。
    ///
    /// 實作必須：
    /// - 只取 `published_at IS NULL AND next_attempt_at <= now`
    /// - 依 `created_at` 升序（先寫先送）
    /// - 把 `attempts` + 1、`next_attempt_at` 設成 `lease_until`（避免兩個郵差
    ///   在租約結束前再拿到同一列）
    /// - `limit` 夾在 1..=500
    ///
    /// PostgreSQL：`FOR UPDATE SKIP LOCKED`。SQLite：單一寫入者前提，見模組說明。
    async fn claim_pending_outbox(
        &self,
        limit: u32,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<Vec<OutboxEvent>, StorageError>;

    /// 標記已送。只動 `published_at IS NULL` 的列。
    async fn mark_outbox_published(
        &self,
        id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    /// 標記這次發送失敗並排下次。`attempts` 在 claim 時已經加過，這裡不動。
    async fn mark_outbox_failed(
        &self,
        id: Uuid,
        error: &str,
        next_attempt_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    /// 目前待送列數（`published_at IS NULL`），含尚未到期的退避列。
    async fn count_pending_outbox(&self) -> Result<u64, StorageError>;

    /// 最舊一筆待送列的年齡（秒）。沒有待送列時回 `None`。
    async fn oldest_pending_outbox_age_secs(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<i64>, StorageError>;

    /// 刪除 `published_at < older_than` 的已送列，最多 `limit` 筆。
    /// `limit` 夾在 1..=500。回傳實際刪除列數。
    async fn purge_published_outbox(
        &self,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError>;
}

/// claim／purge 的硬上限。郵差 batch 預設 50，500 夠用且避免一次鎖太多列。
pub const OUTBOX_CLAIM_LIMIT_MAX: u32 = 500;

/// 把呼叫端給的 limit 夾到 1..=[`OUTBOX_CLAIM_LIMIT_MAX`]。
#[must_use]
pub fn clamp_outbox_limit(limit: u32) -> u32 {
    limit.clamp(1, OUTBOX_CLAIM_LIMIT_MAX)
}

/// 測試專用：依 id／correlation_id 讀、claim、刪 outbox 列。
///
/// 正式 [`EventOutboxStore`] **沒有**這些方法——conformance／e2e 若用
/// `claim_pending_outbox(500)` 掃共用開發表，會把別人的待送列 `attempts+1`
/// 並租約 30 秒。只在 `test-support` feature 下編譯。
#[cfg(feature = "test-support")]
#[async_trait]
pub trait EventOutboxTestSupport: EventOutboxStore {
    /// 依主鍵讀一列（含已送）。不存在回 `None`。不改 attempts／租約。
    async fn get_outbox(&self, id: Uuid) -> Result<Option<OutboxEvent>, StorageError>;

    /// 只 claim 這一列：`published_at IS NULL AND next_attempt_at <= now` 才拿得到。
    /// 成功時 `attempts+1`、`next_attempt_at = lease_until`。不碰其他列。
    async fn claim_outbox_by_id(
        &self,
        id: Uuid,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
    ) -> Result<Option<OutboxEvent>, StorageError>;

    /// 刪這一列（不論是否已送）。回 `true` 代表有刪到。
    async fn delete_outbox(&self, id: Uuid) -> Result<bool, StorageError>;

    /// 這個 correlation_id 的全部 outbox 列（含已送）。給 e2e 找自己寫的列。
    async fn list_outbox_by_correlation_id(
        &self,
        correlation_id: Uuid,
    ) -> Result<Vec<OutboxEvent>, StorageError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_outbox_limit_bounds() {
        assert_eq!(clamp_outbox_limit(0), 1);
        assert_eq!(clamp_outbox_limit(1), 1);
        assert_eq!(clamp_outbox_limit(50), 50);
        assert_eq!(clamp_outbox_limit(500), 500);
        assert_eq!(clamp_outbox_limit(501), 500);
        assert_eq!(clamp_outbox_limit(u32::MAX), 500);
    }
}
