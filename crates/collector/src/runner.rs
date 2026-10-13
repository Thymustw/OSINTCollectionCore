//! 一次收集：組裝 connector、落地、發事件、更新 checkpoint／Job。

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use connector_rest_api::RestApiConnector;
use connector_rss::RssConnector;
use connector_sdk::{
    CollectContext, CollectResult, ConnectorCheckpoint, ConnectorTrait, DomainRateLimiter,
    EvidenceSink, GuardedFetcher, RateLimitConfig, RelationalCheckpointStore, SourcePolicy,
    SsrfGuard, StoreEvidenceSink, SystemResolver,
};
use connector_static_web::StaticWebConnector;
use core_events::{EventProducer, EventTopic};
use core_jobs::JobService;
use core_model::{Connector, JobStatus, Source};
use core_observability::MetricsRegistry;
use core_security::MemoryAuditLog;
use serde_json::json;
use storage_core::RelationalStore;
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::task::JoinSet;
use url::Url;

use crate::bounds::RunBounds;
use crate::error::CollectorError;
use crate::registry::{KnownConnectorKind, classify_connector_type, is_import_only_type};
use crate::schedule::{is_due, is_unscheduled};

/// 一次收集的結果（測試可直接呼叫，不必等 cron）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectOutcome {
    /// 抓到新 body 並寫入 RawEvidence。
    Collected { raw_evidence_id: uuid::Uuid },
    /// 304／無新內容，已更新 checkpoint。
    Unchanged,
    /// 未知 connector_type，已記 log，沒有 panic。
    SkippedUnknownType { connector_type: String },
    /// 匯入／上傳專用類型，collector 不排程。
    SkippedImportType { connector_type: String },
    /// `schedule` 為空，只能手動觸發（目前沒有公開的手動觸發 API）。
    SkippedUnscheduled,
    /// cron 字串有值但解析失敗。
    SkippedInvalidSchedule,
    /// 有合法 schedule 但尚未到期。
    NotDue,
}

/// 生產用 runner：Postgres + 物件儲存 + Redpanda。
#[derive(Clone)]
pub struct CollectorRunner {
    store: PostgresCanonicalStore,
    objects: S3ObjectStore,
    producer: Arc<EventProducer>,
    jobs: Arc<JobService<PostgresCanonicalStore>>,
    bounds: RunBounds,
    metrics: MetricsRegistry,
    audit: Arc<MemoryAuditLog>,
    /// tick 摘要的上次計數與上次印出時間。容量固定（兩個欄位），不隨 connector 數成長。
    summary: Arc<Mutex<SummaryGate>>,
}

impl CollectorRunner {
    #[must_use]
    pub fn new(
        store: PostgresCanonicalStore,
        objects: S3ObjectStore,
        producer: Arc<EventProducer>,
        jobs: Arc<JobService<PostgresCanonicalStore>>,
        bounds: RunBounds,
        metrics: MetricsRegistry,
    ) -> Self {
        Self {
            store,
            objects,
            producer,
            jobs,
            bounds,
            metrics,
            audit: Arc::new(MemoryAuditLog::new()),
            summary: Arc::new(Mutex::new(SummaryGate::default())),
        }
    }

    #[must_use]
    pub fn bounds(&self) -> &RunBounds {
        &self.bounds
    }

    #[must_use]
    pub fn store(&self) -> &PostgresCanonicalStore {
        &self.store
    }

    /// 讀 enabled connector，到期的才跑。單一失敗不中斷迴圈。
    ///
    /// log 量不隨 connector 數線性放大：匯入類型／空 schedule 靜默略過；真正的錯誤
    /// 只在狀態改變時 warn／error，其餘降成 debug；每 tick 最多一行摘要。
    pub async fn tick(&self, now: chrono::DateTime<Utc>) -> Vec<(uuid::Uuid, CollectOutcome)> {
        let connectors = match self.store.list_enabled_connectors().await {
            Ok(list) => list,
            Err(err) => {
                tracing::error!(error = %err, "列出 enabled connector 失敗。請確認 Postgres 在跑");
                return Vec::new();
            }
        };
        let mut results = Vec::new();
        let mut due = Vec::new();
        let mut counts = TickCounts::default();
        for connector in connectors {
            let plan = plan_tick(&connector, now);
            log_tick_plan(&plan, &connector);
            match plan {
                TickPlan::SkipImport => {
                    counts.import_skipped += 1;
                    results.push((
                        connector.id,
                        CollectOutcome::SkippedImportType {
                            connector_type: connector.connector_type,
                        },
                    ));
                }
                TickPlan::SkipUnscheduled => {
                    counts.unscheduled += 1;
                    results.push((connector.id, CollectOutcome::SkippedUnscheduled));
                }
                TickPlan::SkipUnknownType => {
                    counts.unknown_type += 1;
                    let message = unknown_type_message(&connector.connector_type);
                    self.persist_skip_error(&connector, &message).await;
                    results.push((
                        connector.id,
                        CollectOutcome::SkippedUnknownType {
                            connector_type: connector.connector_type,
                        },
                    ));
                }
                TickPlan::SkipInvalidSchedule { message } => {
                    counts.invalid_schedule += 1;
                    self.persist_skip_error(&connector, &message).await;
                    results.push((connector.id, CollectOutcome::SkippedInvalidSchedule));
                }
                TickPlan::NotDue => results.push((connector.id, CollectOutcome::NotDue)),
                TickPlan::Run => due.push(connector),
            }
        }

        // spawn 數量夾在 global_inflight：不可依外部 connector 列無界 tokio::spawn。
        let cap = self.bounds.global_limit().max(1) as usize;
        let mut set = JoinSet::new();
        let mut due = due.into_iter();
        loop {
            while set.len() < cap {
                let Some(connector) = due.next() else {
                    break;
                };
                let runner = self.clone();
                set.spawn(async move {
                    let id = connector.id;
                    let recovered_from_error = connector.status == "error";
                    let outcome = runner.run_connector(connector).await;
                    (id, recovered_from_error, outcome)
                });
            }
            let Some(joined) = set.join_next().await else {
                break;
            };
            match joined {
                Ok((id, recovered_from_error, Ok(outcome))) => {
                    if recovered_from_error {
                        counts.recovered += 1;
                    }
                    results.push((id, outcome));
                }
                Ok((_id, _, Err(_err))) => {
                    // 細節已在 run_known 依狀態變化記過，這裡只累計摘要。
                    counts.collect_failed += 1;
                }
                Err(err) => tracing::error!(error = %err, "收集 task join 失敗"),
            }
        }
        self.emit_tick_summary(&counts);
        results
    }

    /// 第一次把錯誤寫進 `checkpoint.last_error`；同一內容不重寫，避免 error_count 空轉。
    async fn persist_skip_error(&self, connector: &Connector, message: &str) {
        if is_repeat_error(connector, message) {
            return;
        }
        if let Err(err) = mark_failure(&self.store, connector, message).await {
            tracing::debug!(
                connector_id = %connector.id,
                error = %err,
                "記錄 skip 原因到 connector 失敗"
            );
        }
    }

    fn emit_tick_summary(&self, counts: &TickCounts) {
        emit_tick_summary_with(self.summary.as_ref(), counts, Instant::now());
    }

    /// 測試與手動觸發：不管 cron 是否到期。
    pub async fn run_connector(
        &self,
        connector: Connector,
    ) -> Result<CollectOutcome, CollectorError> {
        if is_import_only_type(&connector.connector_type) {
            // tick() 不會走到這裡；測試仍依 SkippedUnknownType 斷言。
            tracing::debug!(
                connector_id = %connector.id,
                connector_type = %connector.connector_type,
                "匯入專用 connector_type，手動觸發也不會收集"
            );
            return Ok(CollectOutcome::SkippedUnknownType {
                connector_type: connector.connector_type,
            });
        }
        match classify_connector_type(&connector.connector_type) {
            Some(kind) => self.run_known(kind, connector).await,
            None => {
                let message = unknown_type_message(&connector.connector_type);
                emit_connector_problem(
                    &connector,
                    &message,
                    "未知 connector_type，跳過（尚未實作這個種類）",
                );
                Ok(CollectOutcome::SkippedUnknownType {
                    connector_type: connector.connector_type,
                })
            }
        }
    }

    async fn run_known(
        &self,
        kind: KnownConnectorKind,
        connector: Connector,
    ) -> Result<CollectOutcome, CollectorError> {
        let source = self
            .store
            .get_source(connector.source_id)
            .await?
            .ok_or_else(|| CollectorError::SourceMissing {
                id: connector.source_id.to_string(),
            })?;
        let domain = domain_of(&source);
        let _permit = self.bounds.acquire(&domain).await;

        let job = self
            .jobs
            .create("collect", Some(connector.id), None)
            .await?;
        if let Err(err) = self.jobs.transition(job.id, JobStatus::Running, None).await {
            tracing::warn!(error = %err, job_id = %job.id, "job 轉 running 失敗，仍繼續收集");
        }

        match self.collect_known(kind, &source, &connector).await {
            Ok(outcome) => {
                if connector.status == "error" {
                    tracing::info!(
                        connector_id = %connector.id,
                        "收集恢復成功"
                    );
                }
                if let Err(err) = self
                    .jobs
                    .transition(job.id, JobStatus::Completed, None)
                    .await
                {
                    tracing::warn!(error = %err, job_id = %job.id, "job 轉 completed 失敗");
                }
                Ok(outcome)
            }
            Err(err) => {
                let message = err.to_string();
                emit_collect_failure(&connector, &err);
                if let Err(job_err) = self
                    .jobs
                    .transition(job.id, JobStatus::Failed, Some(message.clone()))
                    .await
                {
                    tracing::warn!(error = %job_err, job_id = %job.id, "job 轉 failed 失敗");
                }
                if let Err(mark_err) = mark_failure(&self.store, &connector, &message).await {
                    tracing::warn!(error = %mark_err, "更新 connector.error_count 失敗");
                }
                self.metrics.inc_connector_errors(1);
                self.metrics.inc_failed_jobs(1);
                if let Err(pub_err) = self
                    .producer
                    .publish(
                        EventTopic::RawFailed,
                        Some(&source.id.to_string()),
                        Some(connector.id),
                        json!({
                            "connector_id": connector.id,
                            "source_id": source.id,
                            "error": message,
                        }),
                    )
                    .await
                {
                    tracing::warn!(error = %pub_err, "publish raw.failed 失敗");
                }
                Err(err)
            }
        }
    }

    async fn collect_known(
        &self,
        kind: KnownConnectorKind,
        source: &Source,
        connector: &Connector,
    ) -> Result<CollectOutcome, CollectorError> {
        let now = Utc::now();
        let mut running = connector.clone();
        running.status = "running".into();
        running.last_run = Some(now);
        self.store.put_connector(&running).await?;

        let rules = self.store.list_network_rules(source.id).await?;
        let policy = policy_from_source(source);
        let limiter = DomainRateLimiter::new(rate_limit_from_connector(connector, &policy));
        let guard = SsrfGuard::new(
            source.id,
            policy,
            rules,
            Arc::new(SystemResolver),
            self.audit.clone(),
        );
        let fetcher = GuardedFetcher::new(guard, limiter);
        let sink = StoreEvidenceSink::new(self.store.clone(), self.objects.clone());
        let checkpoints = RelationalCheckpointStore::new(self.store.clone());
        // 寫入端把每個來源／連接器卡在 50 個集合（core-api
        // `MAX_COLLECTIONS_PER_SOURCE`／`MAX_COLLECTIONS_PER_CONNECTOR`），
        // 來源∪連接器最多 100，剛好是這邊 `limit=100` 能一次讀完的量。
        // 不要改成翻頁：超過 100 是寫入端的 bug，不是讀取端該消化的。
        let mut collection_ids = self
            .store
            .list_collections_by_source(source.id, 100)
            .await?;
        collection_ids.extend(
            self.store
                .list_collections_by_connector(connector.id, 100)
                .await?,
        );
        collection_ids.sort();
        collection_ids.dedup();
        let ctx = CollectContext {
            source: source.clone(),
            connector: running.clone(),
            collection_ids,
            checkpoint: ConnectorCheckpoint::from_value(&connector.checkpoint),
            now,
        };

        let collected = match kind {
            KnownConnectorKind::RssOrAtom => {
                let rss = RssConnector::new(fetcher, sink, checkpoints);
                let collected = rss.collect(&ctx).await?;
                rss.update_checkpoint(&running, &collected.checkpoint)
                    .await?;
                collected
            }
            KnownConnectorKind::StaticWeb => {
                let web = StaticWebConnector::new(fetcher, sink, checkpoints);
                let collected = web.collect(&ctx).await?;
                web.update_checkpoint(&running, &collected.checkpoint)
                    .await?;
                collected
            }
            KnownConnectorKind::RestApi => {
                let rest = RestApiConnector::new(fetcher, sink, checkpoints);
                let collected = rest.collect(&ctx).await?;
                rest.update_checkpoint(&running, &collected.checkpoint)
                    .await?;
                collected
            }
        };

        self.finish_collect(source, connector, &running, now, collected)
            .await
    }

    async fn finish_collect(
        &self,
        source: &Source,
        connector: &Connector,
        running: &Connector,
        now: chrono::DateTime<Utc>,
        collected: CollectResult,
    ) -> Result<CollectOutcome, CollectorError> {
        if !collected.fetched {
            mark_success(&self.store, running, now, None).await?;
            return Ok(CollectOutcome::Unchanged);
        }

        let evidence = collected
            .evidence
            .ok_or_else(|| CollectorError::Configuration {
                message: "collect 回 fetched=true 但沒有 evidence。這是 connector 實作錯誤，請回報"
                    .into(),
            })?;
        let bytes = evidence.body.len() as u64;
        let sink = StoreEvidenceSink::new(self.store.clone(), self.objects.clone());
        let stored = sink.persist(evidence).await?;
        mark_success(&self.store, running, now, Some(&stored)).await?;

        self.metrics.inc_collected(1);
        self.metrics.add_raw_bytes(bytes);
        self.producer
            .publish(
                EventTopic::RawCollected,
                Some(&source.id.to_string()),
                Some(connector.id),
                json!({
                    "raw_evidence_id": stored.id,
                    "source_id": stored.source_id,
                    "connector_id": stored.connector_id,
                    "content_type": stored.content_type,
                    "sha256": stored.sha256,
                    "storage_path": stored.storage_path,
                }),
            )
            .await?;
        Ok(CollectOutcome::Collected {
            raw_evidence_id: stored.id,
        })
    }
}

fn domain_of(source: &Source) -> String {
    source
        .base_url
        .as_deref()
        .and_then(|raw| Url::parse(raw).ok())
        .and_then(|url| url.host_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".into())
}

fn policy_from_source(source: &Source) -> SourcePolicy {
    serde_json::from_value(source.collection_policy.clone()).unwrap_or_default()
}

fn rate_limit_from_connector(connector: &Connector, policy: &SourcePolicy) -> RateLimitConfig {
    serde_json::from_value(connector.rate_limit.clone()).unwrap_or(policy.rate_limit)
}

async fn mark_success<S: RelationalStore>(
    store: &S,
    connector: &Connector,
    now: chrono::DateTime<Utc>,
    evidence: Option<&core_model::RawEvidence>,
) -> Result<(), CollectorError> {
    let mut next = store
        .get_connector(connector.id)
        .await?
        .unwrap_or_else(|| connector.clone());
    next.last_run = Some(now);
    next.last_success = Some(now);
    next.status = "idle".into();
    next.error_count = 0;
    if let Some(ev) = evidence {
        if !next.checkpoint.is_object() {
            next.checkpoint = json!({});
        }
        if let Some(obj) = next.checkpoint.as_object_mut() {
            obj.insert("last_raw_evidence_id".into(), json!(ev.id));
        }
    }
    store.put_connector(&next).await?;
    Ok(())
}

async fn mark_failure<S: RelationalStore>(
    store: &S,
    connector: &Connector,
    message: &str,
) -> Result<(), CollectorError> {
    let mut next = store
        .get_connector(connector.id)
        .await?
        .unwrap_or_else(|| connector.clone());
    next.last_run = Some(Utc::now());
    next.status = "error".into();
    next.error_count = next.error_count.saturating_add(1);
    let mut checkpoint = next.checkpoint.clone();
    if let Some(obj) = checkpoint.as_object_mut() {
        obj.insert("last_error".into(), json!(message));
    }
    next.checkpoint = checkpoint;
    store.put_connector(&next).await?;
    Ok(())
}

/// tick 對單一 connector 的處置。抽成純函式，單元測試不必連 Postgres。
#[derive(Debug, Clone, PartialEq, Eq)]
enum TickPlan {
    SkipImport,
    SkipUnscheduled,
    SkipUnknownType,
    SkipInvalidSchedule { message: String },
    NotDue,
    Run,
}

fn plan_tick(connector: &Connector, now: chrono::DateTime<Utc>) -> TickPlan {
    if is_import_only_type(&connector.connector_type) {
        return TickPlan::SkipImport;
    }
    if classify_connector_type(&connector.connector_type).is_none() {
        return TickPlan::SkipUnknownType;
    }
    if is_unscheduled(connector.schedule.as_deref()) {
        return TickPlan::SkipUnscheduled;
    }
    match is_due(
        connector.schedule.as_deref().unwrap_or(""),
        connector.last_run,
        now,
    ) {
        Ok(true) => TickPlan::Run,
        Ok(false) => TickPlan::NotDue,
        Err(err) => TickPlan::SkipInvalidSchedule {
            message: err.to_string(),
        },
    }
}

fn log_tick_plan(plan: &TickPlan, connector: &Connector) {
    match plan {
        // 匯入類型與空 schedule 連 debug 都不打：殘留數千筆時，debug 一樣會把磁碟吃光。
        // 計數走 tick 摘要。
        TickPlan::SkipImport | TickPlan::SkipUnscheduled | TickPlan::NotDue | TickPlan::Run => {}
        TickPlan::SkipUnknownType => {
            let message = unknown_type_message(&connector.connector_type);
            emit_connector_problem(
                connector,
                &message,
                "未知 connector_type，跳過（尚未實作這個種類）",
            );
        }
        TickPlan::SkipInvalidSchedule { message } => {
            emit_connector_problem(connector, message, "跳過：schedule 無法解析");
        }
    }
}

fn unknown_type_message(connector_type: &str) -> String {
    format!("未知 connector_type：{connector_type}")
}

fn recorded_error(connector: &Connector) -> Option<&str> {
    connector
        .checkpoint
        .get("last_error")
        .and_then(|v| v.as_str())
}

/// 同一 connector、同一錯誤內容、且目前已是 error：視為重複，不該再打 warn。
fn is_repeat_error(connector: &Connector, message: &str) -> bool {
    connector.status == "error" && recorded_error(connector) == Some(message)
}

fn emit_connector_problem(connector: &Connector, message: &str, text: &str) {
    if is_repeat_error(connector, message) {
        tracing::debug!(
            connector_id = %connector.id,
            error = %message,
            "{text}（與上次相同，已降成 debug）"
        );
    } else {
        tracing::warn!(
            connector_id = %connector.id,
            error = %message,
            "{text}"
        );
    }
}

fn emit_collect_failure(connector: &Connector, err: &CollectorError) {
    if is_repeat_error(connector, &err.to_string()) {
        tracing::debug!(
            connector_id = %connector.id,
            error = %err,
            "收集失敗（與上次相同，已降成 debug）"
        );
    } else {
        tracing::error!(
            connector_id = %connector.id,
            error = %err,
            "收集失敗，繼續下一筆"
        );
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct TickCounts {
    import_skipped: usize,
    unscheduled: usize,
    unknown_type: usize,
    invalid_schedule: usize,
    collect_failed: usize,
    recovered: usize,
}

impl TickCounts {
    fn notable(&self) -> bool {
        self.import_skipped
            + self.unscheduled
            + self.unknown_type
            + self.invalid_schedule
            + self.collect_failed
            + self.recovered
            > 0
    }
}

#[derive(Debug, Default)]
struct SummaryGate {
    last_counts: TickCounts,
    last_emitted: Option<Instant>,
}

/// 計數變化時立刻印；穩定殘留每 5 分鐘一筆心跳。
///
/// tick 預設 5 秒一次。若每 tick 印一行摘要，一天 17,280 行，殘留數千個
/// connector 時仍是無意義的心跳。變化時立刻印，操作者看得到「剛修好／剛壞掉」；
/// 穩定殘留改成 5 分鐘一筆，避免誤以為 collector 沒在跑。
const SUMMARY_INTERVAL: Duration = Duration::from_secs(300);

fn emit_tick_summary_with(gate: &Mutex<SummaryGate>, counts: &TickCounts, now: Instant) {
    if !counts.notable() {
        return;
    }
    let mut gate = gate.lock().unwrap_or_else(|err| err.into_inner());
    let changed = gate.last_counts != *counts;
    let due = gate
        .last_emitted
        .is_none_or(|t| now.saturating_duration_since(t) >= SUMMARY_INTERVAL);
    if !changed && !due {
        return;
    }
    if counts.collect_failed + counts.invalid_schedule + counts.unknown_type > 0 {
        tracing::warn!(
            import_skipped = counts.import_skipped,
            unscheduled = counts.unscheduled,
            unknown_type = counts.unknown_type,
            invalid_schedule = counts.invalid_schedule,
            collect_failed = counts.collect_failed,
            recovered = counts.recovered,
            "本輪 tick 略過／失敗摘要"
        );
    } else {
        tracing::info!(
            import_skipped = counts.import_skipped,
            unscheduled = counts.unscheduled,
            recovered = counts.recovered,
            "本輪 tick 略過摘要"
        );
    }
    gate.last_counts = counts.clone();
    gate.last_emitted = Some(now);
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn ts(y: i32, m: u32, d: u32, h: u32, min: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    fn sample_connector(
        connector_type: &str,
        schedule: Option<&str>,
        status: &str,
        last_error: Option<&str>,
    ) -> Connector {
        let checkpoint = match last_error {
            Some(msg) => json!({ "last_error": msg }),
            None => json!({}),
        };
        Connector {
            id: Uuid::nil(),
            source_id: Uuid::nil(),
            name: "test".into(),
            connector_type: connector_type.into(),
            version: "0.1.0".into(),
            enabled: true,
            configuration: json!({}),
            credential_reference: None,
            schedule: schedule.map(str::to_string),
            rate_limit: json!({}),
            timeout: json!({}),
            proxy_reference: None,
            checkpoint,
            last_run: None,
            last_success: None,
            status: status.into(),
            error_count: i32::from(last_error.is_some()),
        }
    }

    #[derive(Clone, Default)]
    struct LogBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for LogBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().expect("log buf").extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
        type Writer = LogBuf;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    impl LogBuf {
        fn into_string(self) -> String {
            String::from_utf8(self.0.lock().expect("log buf").clone()).unwrap_or_default()
        }
    }

    fn capture_logs(f: impl FnOnce()) -> String {
        let buf = LogBuf::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(buf.clone())
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        buf.into_string()
    }

    fn has_warn_or_above(logs: &str) -> bool {
        logs.lines()
            .any(|line| line.contains("WARN") || line.contains("ERROR"))
    }

    #[test]
    fn import_types_and_empty_schedule_are_silent_skips() {
        let now = ts(2026, 10, 13, 12, 0);
        for ty in [
            "manual_upload",
            "manual",
            "json_import",
            "csv_import",
            "stix_import",
        ] {
            let c = sample_connector(ty, Some("*/15 * * * *"), "idle", None);
            assert_eq!(plan_tick(&c, now), TickPlan::SkipImport, "{ty}");
            let logs = capture_logs(|| log_tick_plan(&TickPlan::SkipImport, &c));
            assert!(
                !has_warn_or_above(&logs),
                "匯入類型不該打 warn 以上：{logs}"
            );
        }
        let empty = sample_connector("rss", None, "idle", None);
        assert_eq!(plan_tick(&empty, now), TickPlan::SkipUnscheduled);
        let logs = capture_logs(|| log_tick_plan(&TickPlan::SkipUnscheduled, &empty));
        assert!(
            !has_warn_or_above(&logs),
            "空 schedule 不該打 warn 以上：{logs}"
        );
        let blank = sample_connector("rss", Some("  "), "idle", None);
        assert_eq!(plan_tick(&blank, now), TickPlan::SkipUnscheduled);
    }

    #[test]
    fn same_error_on_second_tick_is_debug_not_warn() {
        let message = "cron `???` 無效：測試。請用 5 欄（分 時 日 月 週），例如 `*/15 * * * *`";
        let first = sample_connector("rss", Some("???"), "idle", None);
        let logs1 = capture_logs(|| {
            emit_connector_problem(&first, message, "跳過：schedule 無法解析");
        });
        assert!(logs1.contains("WARN"), "第一次應打 warn：{logs1}");
        assert!(!logs1.contains("已降成 debug"), "{logs1}");

        let second = sample_connector("rss", Some("???"), "error", Some(message));
        let logs2 = capture_logs(|| {
            emit_connector_problem(&second, message, "跳過：schedule 無法解析");
        });
        assert!(
            !has_warn_or_above(&logs2),
            "同一錯誤第二次不該打 warn 以上：{logs2}"
        );
        assert!(
            logs2.contains("DEBUG") || logs2.contains("debug"),
            "第二次應降成 debug：{logs2}"
        );
    }

    #[test]
    fn error_content_change_warns_again() {
        let previous = sample_connector("rss", Some("???"), "error", Some("舊的錯誤"));
        let logs = capture_logs(|| {
            emit_connector_problem(&previous, "新的錯誤", "跳過：schedule 無法解析");
        });
        assert!(logs.contains("WARN"), "錯誤內容變了要再打 warn：{logs}");
    }

    #[test]
    fn collect_failure_repeat_is_debug() {
        let err = CollectorError::Configuration {
            message: "Source.base_url 是空的".into(),
        };
        let first = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        let logs1 = capture_logs(|| emit_collect_failure(&first, &err));
        assert!(logs1.contains("ERROR"), "第一次收集失敗應打 error：{logs1}");

        let second = sample_connector("rss", Some("*/15 * * * *"), "error", Some(&err.to_string()));
        let logs2 = capture_logs(|| emit_collect_failure(&second, &err));
        assert!(
            !has_warn_or_above(&logs2),
            "同一收集錯誤第二次不該打 error：{logs2}"
        );
    }

    #[test]
    fn known_type_with_valid_cron_is_run_when_never_collected() {
        let c = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        assert_eq!(plan_tick(&c, ts(2026, 10, 13, 12, 0)), TickPlan::Run);
    }

    #[test]
    fn invalid_cron_is_plan_error_not_unscheduled() {
        let c = sample_connector("rss", Some("not a cron"), "idle", None);
        match plan_tick(&c, ts(2026, 10, 13, 12, 0)) {
            TickPlan::SkipInvalidSchedule { message } => {
                assert!(message.contains("5 欄"), "{message}");
            }
            other => panic!("應為 SkipInvalidSchedule，實際 {other:?}"),
        }
    }

    #[test]
    fn summary_emits_on_change_but_not_every_tick() {
        let gate = Mutex::new(SummaryGate::default());
        let t0 = Instant::now();
        let counts = TickCounts {
            unscheduled: 10,
            ..TickCounts::default()
        };
        let logs1 = capture_logs(|| emit_tick_summary_with(&gate, &counts, t0));
        assert!(
            logs1.contains("略過摘要") || logs1.contains("INFO"),
            "第一次應印摘要：{logs1}"
        );

        let logs2 =
            capture_logs(|| emit_tick_summary_with(&gate, &counts, t0 + Duration::from_secs(5)));
        assert!(logs2.is_empty(), "計數沒變且未滿 5 分鐘不該再印：{logs2}");

        let changed = TickCounts {
            unscheduled: 3,
            ..TickCounts::default()
        };
        let logs3 =
            capture_logs(|| emit_tick_summary_with(&gate, &changed, t0 + Duration::from_secs(6)));
        assert!(!logs3.is_empty(), "計數變了要立刻印：{logs3}");
    }
}
