//! V0.3 transactional outbox 郵差。
//!
//! 業務路徑只把列寫進 `event_outbox`（與業務資料同一個交易）。這裡負責
//! claim → 送到 Redpanda → 標記已送／失敗退避。broker 掛掉時列留在表裡，
//! 不會在 API handler 裡卡住 10 秒 Kafka timeout。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use core_config::EventOutboxSection;
use core_events::{EventEnvelope, EventError, EventProducer, SCHEMA_VERSION};
use core_observability::MetricsRegistry;
use storage_core::{CanonicalWriteStore, EventOutboxStore, OutboxEvent};
use tokio::sync::watch;

pub const METRIC_OUTBOX_PENDING: &str = "osint_event_outbox_pending";
pub const METRIC_OUTBOX_PUBLISHED: &str = "osint_event_outbox_published_total";
pub const METRIC_OUTBOX_PUBLISH_FAILURES: &str = "osint_event_outbox_publish_failures_total";
pub const METRIC_OUTBOX_OLDEST_AGE: &str = "osint_event_outbox_oldest_pending_age_secs";

/// 把 envelope 送到任意 topic。抽成 trait 是為了單元測試能注入失敗的假 publisher，
/// 不必連真實 broker。
#[async_trait]
pub trait OutboxPublisher: Send + Sync {
    async fn publish_envelope(
        &self,
        topic: &str,
        partition_key: Option<&str>,
        envelope: &EventEnvelope,
    ) -> Result<(), EventError>;
}

#[async_trait]
impl OutboxPublisher for EventProducer {
    async fn publish_envelope(
        &self,
        topic: &str,
        partition_key: Option<&str>,
        envelope: &EventEnvelope,
    ) -> Result<(), EventError> {
        self.publish_to_topic(topic, partition_key, envelope).await
    }
}

#[async_trait]
impl OutboxPublisher for Arc<EventProducer> {
    async fn publish_envelope(
        &self,
        topic: &str,
        partition_key: Option<&str>,
        envelope: &EventEnvelope,
    ) -> Result<(), EventError> {
        EventProducer::publish_to_topic(self, topic, partition_key, envelope).await
    }
}

/// 從 outbox 列組 envelope。`id` 用列的 id，重試同一列必須送出同一個 envelope id。
#[must_use]
pub fn envelope_from_outbox(row: &OutboxEvent) -> EventEnvelope {
    EventEnvelope {
        id: row.id,
        event_type: row.topic.clone(),
        schema_version: SCHEMA_VERSION.to_string(),
        source_service: row.source_service.clone(),
        timestamp: row.created_at,
        correlation_id: row.correlation_id,
        payload: row.payload.clone(),
    }
}

/// 第一次失敗等 `base`，之後倍增，上限 `max`。`attempts` 是 claim 之後的值
/// （第一次失敗時已經是 1）。
#[must_use]
pub fn backoff_delay(attempts: i32, base_ms: u64, max_ms: u64) -> Duration {
    let exp = u32::try_from(attempts.saturating_sub(1).max(0))
        .unwrap_or(u32::MAX)
        .min(20);
    let ms = base_ms.saturating_mul(1u64.checked_shl(exp).unwrap_or(u64::MAX));
    Duration::from_millis(ms.min(max_ms))
}

/// 背景迴圈：claim → publish → 標記，直到 shutdown。
///
/// 關閉訊號到了會等**當輪** `relay_once` 跑完再離開，不會在 batch 中途丟列。
/// 單則 produce 逾時 10s（`message.timeout.ms`），所以最久約 `batch_size × 10s`。
pub async fn event_outbox_loop<S, P>(
    store: S,
    publisher: P,
    config: EventOutboxSection,
    metrics: MetricsRegistry,
    mut shutdown: watch::Receiver<bool>,
) where
    S: CanonicalWriteStore,
    P: OutboxPublisher,
{
    let interval = Duration::from_millis(config.poll_interval_ms);
    tracing::info!(
        poll_interval_ms = config.poll_interval_ms,
        batch_size = config.batch_size,
        "event outbox 郵差已啟動"
    );
    loop {
        if *shutdown.borrow() {
            tracing::info!("event outbox 郵差收到關閉訊號，結束");
            return;
        }
        if let Err(err) = relay_once(&store, &publisher, &config, &metrics).await {
            tracing::warn!(
                error = %err,
                "event outbox 本輪失敗，下一輪再試。列仍在表裡，不會丟"
            );
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => {
                tracing::info!("event outbox 郵差收到關閉訊號，結束");
                return;
            }
        }
    }
}

/// 跑一輪：claim、送、標記、更新指標、依設定清已送列。
pub async fn relay_once<S, P>(
    store: &S,
    publisher: &P,
    config: &EventOutboxSection,
    metrics: &MetricsRegistry,
) -> Result<RelayStats, storage_core::StorageError>
where
    S: EventOutboxStore,
    P: OutboxPublisher,
{
    let now = Utc::now();
    let lease_until = now + chrono::Duration::milliseconds(config.lease_ms as i64);
    let claimed = store
        .claim_pending_outbox(config.batch_size, now, lease_until)
        .await?;

    let mut stats = RelayStats {
        claimed: claimed.len() as u64,
        published: 0,
        failed: 0,
        purged: 0,
    };

    for row in claimed {
        let envelope = envelope_from_outbox(&row);
        match publisher
            .publish_envelope(&row.topic, Some(&row.partition_key), &envelope)
            .await
        {
            Ok(()) => {
                store.mark_outbox_published(row.id, Utc::now()).await?;
                stats.published += 1;
                metrics.inc(METRIC_OUTBOX_PUBLISHED, 1);
            }
            Err(err) => {
                let delay =
                    backoff_delay(row.attempts, config.backoff_base_ms, config.backoff_max_ms);
                let next = Utc::now()
                    + chrono::Duration::from_std(delay).unwrap_or(chrono::Duration::seconds(1));
                store
                    .mark_outbox_failed(row.id, &err.to_string(), next)
                    .await?;
                stats.failed += 1;
                metrics.inc(METRIC_OUTBOX_PUBLISH_FAILURES, 1);
                tracing::warn!(
                    outbox_id = %row.id,
                    topic = %row.topic,
                    attempts = row.attempts,
                    next_attempt_at = %next,
                    error = %err,
                    "event outbox 發送失敗，已排下次。請確認 Redpanda 在跑"
                );
            }
        }
    }

    if config.published_retention_hours > 0 {
        let cutoff =
            Utc::now() - chrono::Duration::hours(i64::from(config.published_retention_hours));
        stats.purged = store
            .purge_published_outbox(cutoff, config.batch_size)
            .await?;
    }

    let pending = store.count_pending_outbox().await?;
    metrics.set_gauge(METRIC_OUTBOX_PENDING, pending);
    let oldest = store.oldest_pending_outbox_age_secs(Utc::now()).await?;
    metrics.set_gauge(
        METRIC_OUTBOX_OLDEST_AGE,
        oldest.and_then(|v| u64::try_from(v).ok()).unwrap_or(0),
    );

    Ok(stats)
}

/// 一輪郵差的計數，給測試用。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelayStats {
    pub claimed: u64,
    pub published: u64,
    pub failed: u64,
    pub purged: u64,
}

/// 測試用：把收到的 envelope 記下來，可設定下 N 次失敗。
#[cfg(test)]
pub struct RecordingPublisher {
    pub envelopes: std::sync::Mutex<Vec<(String, Option<String>, EventEnvelope)>>,
    fail_remaining: std::sync::atomic::AtomicU32,
}

#[cfg(test)]
impl RecordingPublisher {
    fn new() -> Self {
        Self {
            envelopes: std::sync::Mutex::new(Vec::new()),
            fail_remaining: std::sync::atomic::AtomicU32::new(0),
        }
    }

    fn fail_next(&self, n: u32) {
        self.fail_remaining
            .store(n, std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
#[async_trait]
impl OutboxPublisher for RecordingPublisher {
    async fn publish_envelope(
        &self,
        topic: &str,
        partition_key: Option<&str>,
        envelope: &EventEnvelope,
    ) -> Result<(), EventError> {
        let remaining = self
            .fail_remaining
            .load(std::sync::atomic::Ordering::SeqCst);
        if remaining > 0 {
            self.fail_remaining
                .store(remaining - 1, std::sync::atomic::Ordering::SeqCst);
            return Err(EventError::Produce {
                topic: topic.to_string(),
                message: "測試用：模擬 broker 失敗".into(),
            });
        }
        self.envelopes.lock().expect("publisher mutex").push((
            topic.to_string(),
            partition_key.map(str::to_string),
            envelope.clone(),
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_observability::MetricsRegistry;
    use serde_json::json;
    use std::path::PathBuf;
    use storage_core::conformance::find_workspace_root;
    use storage_core::{EventOutboxStore, OutboxEvent};
    use storage_sqlite::SqliteEmbeddedStore;
    use uuid::Uuid;

    async fn temp_store(label: &str) -> (SqliteEmbeddedStore, PathBuf) {
        let root = find_workspace_root().expect("workspace root");
        let path: PathBuf = root.join(format!(
            "var/osint-outbox-relay-{label}-{}.sqlite",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let store = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite");
        store.migrate().await.expect("migrate");
        (store, path)
    }

    fn cleanup(path: &PathBuf, store: SqliteEmbeddedStore) {
        drop(store);
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn config() -> EventOutboxSection {
        EventOutboxSection {
            enabled: true,
            poll_interval_ms: 100,
            batch_size: 50,
            backoff_base_ms: 1_000,
            backoff_max_ms: 60_000,
            lease_ms: 30_000,
            published_retention_hours: 24,
        }
    }

    #[test]
    fn backoff_grows_then_caps() {
        assert_eq!(
            backoff_delay(1, 1_000, 60_000),
            Duration::from_millis(1_000)
        );
        assert_eq!(
            backoff_delay(2, 1_000, 60_000),
            Duration::from_millis(2_000)
        );
        assert_eq!(
            backoff_delay(3, 1_000, 60_000),
            Duration::from_millis(4_000)
        );
        assert_eq!(
            backoff_delay(20, 1_000, 60_000),
            Duration::from_millis(60_000)
        );
    }

    #[tokio::test]
    async fn relay_publishes_once_and_marks() {
        let (store, path) = temp_store("once").await;
        let row = OutboxEvent::pending(
            "seed.created",
            "pk-1",
            Some(Uuid::now_v7()),
            json!({"n": 1}),
            "core-api",
        );
        let id = row.id;
        store.enqueue_outbox(&row).await.expect("enqueue");
        let publisher = RecordingPublisher::new();
        let metrics = MetricsRegistry::new();
        let stats = relay_once(&store, &publisher, &config(), &metrics)
            .await
            .expect("relay");
        assert_eq!(stats.published, 1);
        assert_eq!(stats.failed, 0);
        assert_eq!(store.count_pending_outbox().await.expect("count"), 0);
        let sent = publisher.envelopes.lock().expect("lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].2.id, id);
        assert_eq!(sent[0].0, "seed.created");
        cleanup(&path, store);
    }

    #[tokio::test]
    async fn relay_failure_keeps_row_and_retries_same_envelope_id() {
        let (store, path) = temp_store("retry").await;
        let row = OutboxEvent::pending(
            "candidate.approved",
            "pk-2",
            None,
            json!({"n": 2}),
            "core-api",
        );
        let id = row.id;
        store.enqueue_outbox(&row).await.expect("enqueue");
        let publisher = RecordingPublisher::new();
        publisher.fail_next(1);
        let metrics = MetricsRegistry::new();
        let stats = relay_once(&store, &publisher, &config(), &metrics)
            .await
            .expect("relay fail");
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.published, 0);
        assert_eq!(store.count_pending_outbox().await.expect("count"), 1);

        // 還沒到期，再跑一輪不該重送。
        let stats = relay_once(&store, &publisher, &config(), &metrics)
            .await
            .expect("relay not due");
        assert_eq!(stats.claimed, 0);

        // 把 next_attempt_at 拉回「現在之前」，模擬退避到期。不要寫死日期：
        // 寫死未來時間會讓 claim 永遠看不到這列。
        store
            .mark_outbox_failed(
                id,
                "測試：強制到期",
                Utc::now() - chrono::Duration::seconds(1),
            )
            .await
            .expect("due");
        let stats = relay_once(&store, &publisher, &config(), &metrics)
            .await
            .expect("relay retry");
        assert_eq!(stats.published, 1);
        let sent = publisher.envelopes.lock().expect("lock");
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].2.id, id);
        cleanup(&path, store);
    }

    #[tokio::test]
    async fn relay_does_not_see_rolled_back_row() {
        // SQLite 單一寫入者：開著交易時另一條路徑 claim 會鎖住（busy timeout），
        // 所以這裡只驗 rollback 之後列不可見。開著交易時的隔離由 postgres
        // conformance `assert_event_outbox_contract` 覆蓋。
        let (store, path) = temp_store("tx").await;
        let publisher = RecordingPublisher::new();
        let metrics = MetricsRegistry::new();
        let tx = storage_core::TransactionalStore::begin(&store)
            .await
            .expect("begin");
        let row = OutboxEvent::pending("ai.requested", "pk-3", None, json!({"n": 3}), "resolver");
        tx.outbox()
            .enqueue_outbox(&row)
            .await
            .expect("enqueue in tx");
        tx.rollback().await.expect("rollback");
        let stats = relay_once(&store, &publisher, &config(), &metrics)
            .await
            .expect("relay after rollback");
        assert_eq!(stats.claimed, 0);
        cleanup(&path, store);
    }
}
