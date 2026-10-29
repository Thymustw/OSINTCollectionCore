//! 一次收集：組裝 connector、落地、發事件、更新 checkpoint／Job。

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::Utc;
use connector_rest_api::RestApiConnector;
use connector_rss::RssConnector;
use connector_sdk::{
    BackoffConfig, CollectContext, CollectResult, ConnectorCheckpoint, ConnectorTrait,
    DomainRateLimiter, EvidenceSink, GuardedFetcher, RateLimitConfig, RelationalCheckpointStore,
    SourcePolicy, SsrfGuard, StoreEvidenceSink, SystemResolver, is_backing_off, next_attempt_at,
};
use connector_static_web::StaticWebConnector;
use core_events::{EventProducer, EventTopic};
use core_jobs::JobService;
use core_model::{Connector, JobStatus, Source};
use core_observability::MetricsRegistry;
use core_security::MemoryAuditLog;
use serde_json::json;
use storage_core::{CheckpointUpdate, ConnectorRunStateUpdate, RelationalStore, StorageError};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::backpressure::{BackpressureGate, BackpressureState};
use crate::bounds::RunBounds;
use crate::error::CollectorError;
use crate::registry::{KnownConnectorKind, classify_connector_type, is_import_only_type};
use crate::schedule::{is_due, is_unscheduled};

/// abort 收尾寫進 `jobs.error` 與 `checkpoint.last_error`。被中斷不是來源的錯。
pub const ABORT_JOB_ERROR: &str = "collector 停止時中斷";

/// 與 `[collector].shutdown_drain_secs` 預設值對齊。
const DEFAULT_SHUTDOWN_DRAIN_SECS: u64 = 20;

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
    /// cron 已到期，但仍在失敗退避期內，本輪不派。
    BackingOff,
    /// 本輪失敗重試名額已滿（`max_retry_per_tick`），不派、不改 last_run／error_count。
    RetryDeferred,
    /// 下游 lag 達到 pause 門檻，本輪不派、不改 last_run／error_count／status。
    BackpressureSkipped,
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
    backoff: BackoffConfig,
    max_retry_per_tick: u32,
    shutdown_drain_secs: u64,
    backpressure: BackpressureGate,
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
        let backpressure = BackpressureGate::disabled(metrics.clone());
        Self {
            store,
            objects,
            producer,
            jobs,
            bounds,
            metrics,
            audit: Arc::new(MemoryAuditLog::new()),
            summary: Arc::new(Mutex::new(SummaryGate::default())),
            backoff: BackoffConfig::default(),
            max_retry_per_tick: DEFAULT_MAX_RETRY_PER_TICK,
            shutdown_drain_secs: DEFAULT_SHUTDOWN_DRAIN_SECS,
            backpressure,
        }
    }

    /// 覆寫退避與每輪失敗重試上限。生產路徑從 `[collector]` 讀進來。
    #[must_use]
    pub fn with_backoff(mut self, backoff: BackoffConfig, max_retry_per_tick: u32) -> Self {
        self.backoff = backoff;
        self.max_retry_per_tick = max_retry_per_tick.max(1);
        self
    }

    /// 覆寫停止時等待 in-flight 的最長秒數。測試可把 drain 縮到 1 秒。
    #[must_use]
    pub fn with_shutdown_drain_secs(mut self, secs: u64) -> Self {
        self.shutdown_drain_secs = secs.max(1);
        self
    }

    /// 覆寫下游 lag 背壓閘門。生產路徑從 `[collector.backpressure]` 組裝。
    #[must_use]
    pub fn with_backpressure(mut self, gate: BackpressureGate) -> Self {
        self.backpressure = gate;
        self
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
    /// `token` 已取消時立刻回空 vec、**不** list——共用開發 DB 可能有數千個
    /// enabled connector，測試取消路徑不可掃全表去派工。
    ///
    /// log 量不隨 connector 數線性放大：匯入類型／空 schedule／退避中靜默略過；真正的錯誤
    /// 只在狀態改變時 warn／error，其餘降成 debug；每 tick 最多一行摘要。
    pub async fn tick(
        &self,
        now: chrono::DateTime<Utc>,
        token: &CancellationToken,
    ) -> Vec<(uuid::Uuid, CollectOutcome)> {
        if token.is_cancelled() {
            return Vec::new();
        }
        let connectors = match self.store.list_enabled_connectors().await {
            Ok(list) => list,
            Err(err) => {
                tracing::error!(error = %err, "列出 enabled connector 失敗。請確認 Postgres 在跑");
                return Vec::new();
            }
        };
        self.tick_connectors(connectors, now, token).await
    }

    /// 對指定 connector 清單做一輪排程。測試餵單一 connector，避免掃共用 DB。
    pub async fn tick_connectors(
        &self,
        connectors: Vec<Connector>,
        now: chrono::DateTime<Utc>,
        token: &CancellationToken,
    ) -> Vec<(uuid::Uuid, CollectOutcome)> {
        let started = Instant::now();
        let pressure = self.backpressure.evaluate().await;
        let mut results = Vec::new();
        let mut due = Vec::new();
        let mut counts = TickCounts::default();
        for connector in connectors {
            let plan = plan_tick(&connector, now, &self.backoff);
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
                TickPlan::BackingOff { .. } => {
                    counts.backing_off += 1;
                    results.push((connector.id, CollectOutcome::BackingOff));
                }
                TickPlan::Run => due.push(connector),
            }
        }

        let (dispatch, deferred) = take_fair_dispatch(due, self.max_retry_per_tick);
        for connector in deferred {
            counts.retry_deferred += 1;
            results.push((connector.id, CollectOutcome::RetryDeferred));
        }

        let dispatch = if pressure.state == BackpressureState::Pause {
            let n = dispatch.len() as u64;
            for connector in dispatch {
                counts.backpressure_skipped += 1;
                results.push((connector.id, CollectOutcome::BackpressureSkipped));
            }
            if n > 0 {
                self.metrics
                    .inc("osint_collector_backpressure_skipped_total", n);
            }
            Vec::new()
        } else {
            dispatch
        };

        let cap = pressure.state.dispatch_cap(self.bounds.global_limit());
        let mut set = JoinSet::new();
        let mut dispatch = dispatch.into_iter();
        let mut inflight: HashMap<uuid::Uuid, uuid::Uuid> = HashMap::new();
        let mut draining = token.is_cancelled();
        let mut drain_deadline = if draining {
            Some(tokio::time::Instant::now() + Duration::from_secs(self.shutdown_drain_secs))
        } else {
            None
        };

        loop {
            while !draining && set.len() < cap {
                if token.is_cancelled() {
                    draining = true;
                    drain_deadline = Some(
                        tokio::time::Instant::now() + Duration::from_secs(self.shutdown_drain_secs),
                    );
                    let n = set.len();
                    tracing::info!(
                        in_flight = n,
                        drain_secs = self.shutdown_drain_secs,
                        "開始 drain（in-flight={n}，期限 {}s）",
                        self.shutdown_drain_secs
                    );
                    break;
                }
                let Some(connector) = dispatch.next() else {
                    break;
                };
                let Some(kind) = classify_connector_type(&connector.connector_type) else {
                    continue;
                };
                match self.jobs.create("collect", Some(connector.id), None).await {
                    Ok(job) => {
                        if let Err(err) =
                            self.jobs.transition(job.id, JobStatus::Running, None).await
                        {
                            tracing::warn!(
                                error = %err,
                                job_id = %job.id,
                                "job 轉 running 失敗，仍繼續收集"
                            );
                        }
                        inflight.insert(connector.id, job.id);
                        let runner = self.clone();
                        let job_id = job.id;
                        set.spawn(async move {
                            let id = connector.id;
                            let recovered_from_error = connector.status == "error";
                            let outcome = runner.execute_collect(kind, connector, job_id).await;
                            (id, recovered_from_error, outcome)
                        });
                    }
                    Err(err) => {
                        tracing::error!(
                            error = %err,
                            connector_id = %connector.id,
                            "建 collect job 失敗，本輪不派。請確認 Postgres 在跑"
                        );
                    }
                }
            }

            if set.is_empty() {
                break;
            }

            if draining {
                let deadline = drain_deadline.unwrap_or_else(tokio::time::Instant::now);
                tokio::select! {
                    joined = set.join_next() => {
                        let Some(joined) = joined else { break };
                        Self::record_join(joined, &mut inflight, &mut results, &mut counts);
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        let n = inflight.len();
                        tracing::warn!(in_flight = n, "drain 逾時，中斷 {n} 筆");
                        set.abort_all();
                        while let Some(joined) = set.join_next().await {
                            Self::record_join(joined, &mut inflight, &mut results, &mut counts);
                        }
                        break;
                    }
                }
            } else {
                tokio::select! {
                    joined = set.join_next() => {
                        let Some(joined) = joined else { break };
                        Self::record_join(joined, &mut inflight, &mut results, &mut counts);
                    }
                    _ = token.cancelled() => {
                        draining = true;
                        let n = set.len();
                        tracing::info!(
                            in_flight = n,
                            drain_secs = self.shutdown_drain_secs,
                            "開始 drain（in-flight={n}，期限 {}s）",
                            self.shutdown_drain_secs
                        );
                        drain_deadline = Some(
                            tokio::time::Instant::now()
                                + Duration::from_secs(self.shutdown_drain_secs),
                        );
                    }
                }
            }
        }

        if draining {
            if inflight.is_empty() {
                tracing::info!("drain 完成，全部自然結束");
            } else {
                let n = inflight.len();
                self.finish_aborted(&inflight).await;
                self.metrics
                    .inc("osint_collector_shutdown_cancelled_total", n as u64);
            }
        }

        self.metrics.set_gauge(
            "osint_collector_connectors_backing_off",
            counts.backing_off as u64,
        );
        if counts.backing_off > 0 {
            self.metrics.inc(
                "osint_collector_backoff_skipped_total",
                counts.backing_off as u64,
            );
        }
        self.metrics.set_gauge(
            "osint_collector_tick_duration_ms",
            started.elapsed().as_millis() as u64,
        );
        self.emit_tick_summary(&counts);
        results
    }

    fn record_join(
        joined: Result<(uuid::Uuid, bool, Result<CollectOutcome, CollectorError>), JoinError>,
        inflight: &mut HashMap<uuid::Uuid, uuid::Uuid>,
        results: &mut Vec<(uuid::Uuid, CollectOutcome)>,
        counts: &mut TickCounts,
    ) {
        match joined {
            Ok((id, recovered_from_error, Ok(outcome))) => {
                inflight.remove(&id);
                if recovered_from_error {
                    counts.recovered += 1;
                }
                results.push((id, outcome));
            }
            Ok((id, _, Err(_err))) => {
                inflight.remove(&id);
                counts.collect_failed += 1;
            }
            Err(err) if err.is_cancelled() => {}
            Err(err) => tracing::error!(error = %err, "收集 task join 失敗"),
        }
    }

    async fn finish_aborted(&self, inflight: &HashMap<uuid::Uuid, uuid::Uuid>) {
        for (connector_id, job_id) in inflight {
            if let Err(err) = self
                .jobs
                .transition(
                    *job_id,
                    JobStatus::Cancelled,
                    Some(ABORT_JOB_ERROR.to_string()),
                )
                .await
            {
                tracing::warn!(error = %err, job_id = %job_id, "job 轉 cancelled 失敗");
            }
            if let Err(err) = self
                .store
                .update_connector_run_state(
                    *connector_id,
                    &ConnectorRunStateUpdate {
                        status: Some("idle".into()),
                        checkpoint: CheckpointUpdate::Merge(json!({"last_error": ABORT_JOB_ERROR})),
                        ..ConnectorRunStateUpdate::default()
                    },
                )
                .await
            {
                tracing::warn!(
                    error = %err,
                    connector_id = %connector_id,
                    "中斷後把 connector 設 idle 失敗"
                );
            }
        }
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
        let job = self
            .jobs
            .create("collect", Some(connector.id), None)
            .await?;
        if let Err(err) = self.jobs.transition(job.id, JobStatus::Running, None).await {
            tracing::warn!(error = %err, job_id = %job.id, "job 轉 running 失敗，仍繼續收集");
        }
        self.execute_collect(kind, connector, job.id).await
    }

    /// 已有 job（spawn 前建立，或 `run_known` 剛建的）之後真正去收集。
    async fn execute_collect(
        &self,
        kind: KnownConnectorKind,
        connector: Connector,
        job_id: uuid::Uuid,
    ) -> Result<CollectOutcome, CollectorError> {
        let source = match resolve_source(&self.store, &connector).await {
            Ok(source) => source,
            Err(err) => {
                if let Err(job_err) = self
                    .jobs
                    .transition(job_id, JobStatus::Failed, Some(err.to_string()))
                    .await
                {
                    tracing::warn!(error = %job_err, job_id = %job_id, "job 轉 failed 失敗");
                }
                return Err(err);
            }
        };
        let domain = domain_of(&source);
        let _permit = self.bounds.acquire(&domain).await;

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
                    .transition(job_id, JobStatus::Completed, None)
                    .await
                {
                    tracing::warn!(error = %err, job_id = %job_id, "job 轉 completed 失敗");
                }
                Ok(outcome)
            }
            Err(err) => {
                let message = err.to_string();
                emit_collect_failure(&connector, &err);
                if let Err(job_err) = self
                    .jobs
                    .transition(job_id, JobStatus::Failed, Some(message.clone()))
                    .await
                {
                    tracing::warn!(error = %job_err, job_id = %job_id, "job 轉 failed 失敗");
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
        // `running` 只是傳給 connector 與 mark_success 的記憶體副本（status／last_run
        // 對齊 DB）。它是 tick 當下的快照，**不能**再整列寫回——收集期間 operator 可能
        // 已 PATCH enabled／schedule。所有寫回都走 update_connector_run_state。
        let mut running = connector.clone();
        running.status = "running".into();
        running.last_run = Some(now);
        let marked = self
            .store
            .update_connector_run_state(
                connector.id,
                &ConnectorRunStateUpdate {
                    status: Some("running".into()),
                    last_run: Some(now),
                    ..ConnectorRunStateUpdate::default()
                },
            )
            .await?;
        if !marked {
            return Err(CollectorError::Storage(StorageError::NotFound {
                message: format!(
                    "找不到 connector `{}`，無法標記為 running。請確認尚未被刪除",
                    connector.id
                ),
            }));
        }

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

/// 建 job 前就失敗（source 不存在、讀 source 失敗）也要留下執行狀態，
/// 否則 last_run／error_count 不前進，後面的退避管不到它。
async fn resolve_source<S: RelationalStore>(
    store: &S,
    connector: &Connector,
) -> Result<Source, CollectorError> {
    match store.get_source(connector.source_id).await {
        Ok(Some(source)) => Ok(source),
        Ok(None) => {
            let err = CollectorError::SourceMissing {
                id: connector.source_id.to_string(),
            };
            if let Err(mark_err) = mark_failure(store, connector, &err.to_string()).await {
                tracing::warn!(
                    error = %mark_err,
                    connector_id = %connector.id,
                    "source 不存在，更新 connector.error_count 也失敗"
                );
            }
            Err(err)
        }
        Err(storage_err) => {
            let err = CollectorError::from(storage_err);
            if let Err(mark_err) = mark_failure(store, connector, &err.to_string()).await {
                tracing::warn!(
                    error = %mark_err,
                    connector_id = %connector.id,
                    "讀 source 失敗，更新 connector.error_count 也失敗"
                );
            }
            Err(err)
        }
    }
}

async fn mark_success<S: RelationalStore>(
    store: &S,
    connector: &Connector,
    now: chrono::DateTime<Utc>,
    evidence: Option<&core_model::RawEvidence>,
) -> Result<(), CollectorError> {
    let checkpoint = evidence
        .map(|ev| CheckpointUpdate::Merge(json!({"last_raw_evidence_id": ev.id})))
        .unwrap_or(CheckpointUpdate::Unchanged);
    let marked = store
        .update_connector_run_state(
            connector.id,
            &ConnectorRunStateUpdate {
                status: Some("idle".into()),
                last_run: Some(now),
                last_success: Some(now),
                error_count: Some(0),
                checkpoint,
            },
        )
        .await?;
    if !marked {
        tracing::warn!(
            connector_id = %connector.id,
            "找不到 connector，略過成功紀錄。請確認尚未被刪除"
        );
    }
    Ok(())
}

async fn mark_failure<S: RelationalStore>(
    store: &S,
    connector: &Connector,
    message: &str,
) -> Result<(), CollectorError> {
    let current_count = match store.get_connector(connector.id).await? {
        Some(current) => current.error_count,
        None => connector.error_count,
    };
    let marked = store
        .update_connector_run_state(
            connector.id,
            &ConnectorRunStateUpdate {
                status: Some("error".into()),
                last_run: Some(Utc::now()),
                error_count: Some(current_count.saturating_add(1)),
                checkpoint: CheckpointUpdate::Merge(json!({"last_error": message})),
                ..ConnectorRunStateUpdate::default()
            },
        )
        .await?;
    if !marked {
        tracing::warn!(
            connector_id = %connector.id,
            "找不到 connector，略過失敗紀錄。請確認尚未被刪除"
        );
    }
    Ok(())
}

/// 與 `[collector].max_retry_per_tick` 預設值對齊。
const DEFAULT_MAX_RETRY_PER_TICK: u32 = 8;

/// tick 對單一 connector 的處置。抽成純函式，單元測試不必連 Postgres。
#[derive(Debug, Clone, PartialEq, Eq)]
enum TickPlan {
    SkipImport,
    SkipUnscheduled,
    SkipUnknownType,
    SkipInvalidSchedule { message: String },
    NotDue,
    BackingOff { until: chrono::DateTime<Utc> },
    Run,
}

fn plan_tick(connector: &Connector, now: chrono::DateTime<Utc>, cfg: &BackoffConfig) -> TickPlan {
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
        Ok(true) if is_backing_off(connector, now, cfg) => TickPlan::BackingOff {
            until: next_attempt_at(connector, now, cfg).unwrap_or(now),
        },
        Ok(true) => TickPlan::Run,
        Ok(false) => TickPlan::NotDue,
        Err(err) => TickPlan::SkipInvalidSchedule {
            message: err.to_string(),
        },
    }
}

/// 到期清單的公平性排序：失敗少的先、最近成功過的先、從未成功的最後，再比 id。
fn fair_order(a: &Connector, b: &Connector) -> Ordering {
    a.error_count
        .cmp(&b.error_count)
        .then_with(|| match (a.last_success, b.last_success) {
            (Some(x), Some(y)) => y.cmp(&x),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
        .then_with(|| a.id.cmp(&b.id))
}

/// 從到期清單挑出本輪要派的，超出 `max_retry_per_tick` 的失敗連接器延後。
fn take_fair_dispatch(
    mut due: Vec<Connector>,
    max_retry_per_tick: u32,
) -> (Vec<Connector>, Vec<Connector>) {
    due.sort_by(fair_order);
    let cap = max_retry_per_tick.max(1);
    let mut dispatch = Vec::new();
    let mut deferred = Vec::new();
    let mut retries = 0u32;
    for connector in due {
        if connector.error_count > 0 {
            if retries >= cap {
                deferred.push(connector);
                continue;
            }
            retries += 1;
        }
        dispatch.push(connector);
    }
    (dispatch, deferred)
}

fn log_tick_plan(plan: &TickPlan, connector: &Connector) {
    match plan {
        // 匯入類型與空 schedule 連 debug 都不打：殘留數千筆時，debug 一樣會把磁碟吃光。
        // 計數走 tick 摘要。
        TickPlan::SkipImport
        | TickPlan::SkipUnscheduled
        | TickPlan::NotDue
        | TickPlan::Run
        | TickPlan::BackingOff { .. } => {}
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
    backing_off: usize,
    retry_deferred: usize,
    backpressure_skipped: usize,
}

impl TickCounts {
    fn notable(&self) -> bool {
        self.import_skipped
            + self.unscheduled
            + self.unknown_type
            + self.invalid_schedule
            + self.collect_failed
            + self.recovered
            + self.backing_off
            + self.retry_deferred
            + self.backpressure_skipped
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
            backing_off = counts.backing_off,
            retry_deferred = counts.retry_deferred,
            backpressure_skipped = counts.backpressure_skipped,
            "本輪 tick 略過／失敗摘要"
        );
    } else {
        tracing::info!(
            import_skipped = counts.import_skipped,
            unscheduled = counts.unscheduled,
            recovered = counts.recovered,
            backing_off = counts.backing_off,
            retry_deferred = counts.retry_deferred,
            backpressure_skipped = counts.backpressure_skipped,
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
    use connector_sdk::CheckpointStore;
    use serde_json::json;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    fn ts(y: i32, m: u32, d: u32, h: u32, min: u32) -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, min, 0).unwrap()
    }

    /// jitter = 0 的 id：`as_u128() % 2001 == 1000`。
    fn zero_jitter_id() -> Uuid {
        Uuid::from_u128(1000)
    }

    fn default_backoff() -> BackoffConfig {
        BackoffConfig::default()
    }

    fn plan(connector: &Connector, now: chrono::DateTime<Utc>) -> TickPlan {
        plan_tick(connector, now, &default_backoff())
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
            assert_eq!(plan(&c, now), TickPlan::SkipImport, "{ty}");
            let logs = capture_logs(|| log_tick_plan(&TickPlan::SkipImport, &c));
            assert!(
                !has_warn_or_above(&logs),
                "匯入類型不該打 warn 以上：{logs}"
            );
        }
        let empty = sample_connector("rss", None, "idle", None);
        assert_eq!(plan(&empty, now), TickPlan::SkipUnscheduled);
        let logs = capture_logs(|| log_tick_plan(&TickPlan::SkipUnscheduled, &empty));
        assert!(
            !has_warn_or_above(&logs),
            "空 schedule 不該打 warn 以上：{logs}"
        );
        let blank = sample_connector("rss", Some("  "), "idle", None);
        assert_eq!(plan(&blank, now), TickPlan::SkipUnscheduled);
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
        assert_eq!(plan(&c, ts(2026, 10, 13, 12, 0)), TickPlan::Run);
    }

    #[test]
    fn invalid_cron_is_plan_error_not_unscheduled() {
        let c = sample_connector("rss", Some("not a cron"), "idle", None);
        match plan(&c, ts(2026, 10, 13, 12, 0)) {
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

    fn seeded_store(connector: &Connector) -> crate::test_store::MemoryConnectorStore {
        let store = crate::test_store::MemoryConnectorStore::new();
        store.seed_connector(connector.clone());
        store
    }

    #[tokio::test]
    async fn source_missing_increments_error_count_and_advances_last_run() {
        let connector = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        let store = seeded_store(&connector);
        let before = Utc::now();
        let err = resolve_source(&store, &connector)
            .await
            .expect_err("沒有 source 應失敗");
        assert!(matches!(err, CollectorError::SourceMissing { .. }), "{err}");
        let got = store
            .get_connector(connector.id)
            .await
            .expect("get")
            .expect("connector 還在");
        assert_eq!(got.error_count, 1, "SourceMissing 要讓 error_count +1");
        assert!(
            got.last_run.is_some_and(|ts| ts >= before),
            "last_run 要前進，實際 {:?}",
            got.last_run
        );
        assert_eq!(got.status, "error");
        let expected_error = err.to_string();
        assert_eq!(
            got.checkpoint.get("last_error").and_then(|v| v.as_str()),
            Some(expected_error.as_str())
        );
        assert_eq!(
            store.put_connector_call_count(),
            0,
            "失敗紀錄不該走整列 put_connector"
        );
    }

    #[tokio::test]
    async fn mark_success_does_not_clobber_disabled_connector() {
        let mut connector = sample_connector("rss", Some("*/15 * * * *"), "running", None);
        connector.enabled = true;
        connector.schedule = Some("*/15 * * * *".into());
        let store = seeded_store(&connector);

        // 模擬收集期間 operator PATCH：enabled=false、換 schedule。
        let mut patched = connector.clone();
        patched.enabled = false;
        patched.schedule = Some("0 * * * *".into());
        store.put_connector(&patched).await.expect("patch");
        let puts_after_patch = store.put_connector_call_count();

        let now = ts(2026, 10, 28, 12, 0);
        mark_success(&store, &connector, now, None)
            .await
            .expect("mark_success");

        let got = store
            .get_connector(connector.id)
            .await
            .expect("get")
            .expect("connector");
        assert!(!got.enabled, "mark_success 不該把 enabled 蓋回 true");
        assert_eq!(got.schedule.as_deref(), Some("0 * * * *"));
        assert_eq!(got.status, "idle");
        assert_eq!(got.error_count, 0);
        assert_eq!(got.last_success, Some(now));
        assert_eq!(
            store.put_connector_call_count(),
            puts_after_patch,
            "成功紀錄不該再走整列 put_connector"
        );
    }

    #[tokio::test]
    async fn checkpoint_save_does_not_clobber_disabled_connector() {
        let mut connector = sample_connector("rss", Some("*/15 * * * *"), "running", None);
        connector.checkpoint = json!({"etag": "old", "last_error": "先前失敗"});
        let store = seeded_store(&connector);
        let mut patched = connector.clone();
        patched.enabled = false;
        store.put_connector(&patched).await.expect("patch");
        let puts_after_patch = store.put_connector_call_count();

        let checkpoints = RelationalCheckpointStore::new(store.clone());
        checkpoints
            .save(
                &connector,
                &ConnectorCheckpoint {
                    etag: Some("new".into()),
                    ..ConnectorCheckpoint::default()
                },
            )
            .await
            .expect("save");

        let got = store
            .get_connector(connector.id)
            .await
            .expect("get")
            .expect("connector");
        assert!(!got.enabled, "save checkpoint 不該把 enabled 蓋回 true");
        assert_eq!(got.checkpoint["etag"], json!("new"));
        assert_eq!(
            got.checkpoint["last_error"],
            json!("先前失敗"),
            "Merge 應保留 last_error"
        );
        assert_eq!(
            store.put_connector_call_count(),
            puts_after_patch,
            "checkpoint save 不該走整列 put_connector"
        );
    }

    #[test]
    fn due_but_backing_off_is_backing_off_plan() {
        let mut c = sample_connector("rss", Some("*/15 * * * *"), "error", Some("失敗"));
        c.id = zero_jitter_id();
        c.last_run = Some(ts(2026, 1, 1, 12, 0));
        c.error_count = 1;
        // n=1 → 退避 5 分鐘到 12:05；cron 下一個是 12:15。12:16 已到期但仍在...
        // 不對：12:16 已過 12:05，不是 backing off。用 12:03（cron 還沒到期）會是 NotDue。
        // 用 n=4（40 分鐘）讓 12:16 到期且仍在退避。
        c.error_count = 4;
        match plan(&c, ts(2026, 1, 1, 12, 16)) {
            TickPlan::BackingOff { until } => {
                assert_eq!(
                    until,
                    ts(2026, 1, 1, 12, 40),
                    "until 應為 last_run + 40 分鐘"
                );
            }
            other => panic!("應為 BackingOff，實際 {other:?}"),
        }
        let logs = capture_logs(|| {
            log_tick_plan(
                &TickPlan::BackingOff {
                    until: ts(2026, 1, 1, 12, 40),
                },
                &c,
            );
        });
        assert!(
            !has_warn_or_above(&logs),
            "BackingOff 不該打 warn 以上：{logs}"
        );
    }

    #[test]
    fn backoff_expired_is_run() {
        let mut c = sample_connector("rss", Some("*/15 * * * *"), "error", Some("失敗"));
        c.id = zero_jitter_id();
        c.last_run = Some(ts(2026, 1, 1, 12, 0));
        c.error_count = 1;
        // n=1 退避到 12:05；12:15 cron 到期且退避已過。
        assert_eq!(plan(&c, ts(2026, 1, 1, 12, 15)), TickPlan::Run);
    }

    #[test]
    fn error_count_zero_never_backs_off() {
        let mut c = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        c.id = zero_jitter_id();
        c.last_run = Some(ts(2026, 1, 1, 12, 0));
        c.error_count = 0;
        assert_eq!(plan(&c, ts(2026, 1, 1, 12, 15)), TickPlan::Run);
    }

    #[test]
    fn fair_order_healthy_before_failing_and_null_success_last() {
        let older = ts(2026, 1, 1, 10, 0);
        let newer = ts(2026, 1, 1, 11, 0);
        let mut healthy_old = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        healthy_old.id = Uuid::from_u128(1);
        healthy_old.error_count = 0;
        healthy_old.last_success = Some(older);
        let mut healthy_new = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        healthy_new.id = Uuid::from_u128(2);
        healthy_new.error_count = 0;
        healthy_new.last_success = Some(newer);
        let mut never = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        never.id = Uuid::from_u128(3);
        never.error_count = 0;
        never.last_success = None;
        let mut fail_low = sample_connector("rss", Some("*/15 * * * *"), "error", Some("x"));
        fail_low.id = Uuid::from_u128(4);
        fail_low.error_count = 1;
        let mut fail_high = sample_connector("rss", Some("*/15 * * * *"), "error", Some("x"));
        fail_high.id = Uuid::from_u128(5);
        fail_high.error_count = 9;

        let mut all = [
            fail_high.clone(),
            never.clone(),
            healthy_old.clone(),
            fail_low.clone(),
            healthy_new.clone(),
        ];
        all.sort_by(fair_order);
        let ids: Vec<_> = all.iter().map(|c| c.id).collect();
        assert_eq!(
            ids,
            vec![
                healthy_new.id,
                healthy_old.id,
                never.id,
                fail_low.id,
                fail_high.id
            ],
            "error_count 升冪、last_success 新到舊且 NULL 最後"
        );
    }

    #[test]
    fn max_retry_per_tick_keeps_healthy_and_caps_retries() {
        let mut healthy = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        healthy.id = Uuid::from_u128(1);
        healthy.error_count = 0;
        let mut a = sample_connector("rss", Some("*/15 * * * *"), "error", Some("x"));
        a.id = Uuid::from_u128(2);
        a.error_count = 1;
        let mut b = sample_connector("rss", Some("*/15 * * * *"), "error", Some("x"));
        b.id = Uuid::from_u128(3);
        b.error_count = 1;
        let mut c = sample_connector("rss", Some("*/15 * * * *"), "error", Some("x"));
        c.id = Uuid::from_u128(4);
        c.error_count = 5;
        let (dispatch, deferred) =
            take_fair_dispatch(vec![c.clone(), b.clone(), healthy.clone(), a.clone()], 2);
        let dispatch_ids: Vec<_> = dispatch.iter().map(|x| x.id).collect();
        let deferred_ids: Vec<_> = deferred.iter().map(|x| x.id).collect();
        assert_eq!(dispatch_ids, vec![healthy.id, a.id, b.id]);
        assert_eq!(deferred_ids, vec![c.id]);
        assert_eq!(
            deferred[0].error_count, 5,
            "延後的連接器 error_count 不可被改"
        );
        assert_eq!(deferred[0].last_run, c.last_run);
    }

    #[test]
    fn simulated_24h_backoff_caps_retries() {
        let cfg = default_backoff();
        let mut connector = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        connector.id = zero_jitter_id();
        let start = ts(2026, 1, 1, 0, 0);
        let end = start + chrono::Duration::hours(24);
        let mut now = start;
        let mut runs = 0usize;
        while now < end {
            match plan_tick(&connector, now, &cfg) {
                TickPlan::Run => {
                    runs += 1;
                    connector.error_count = connector.error_count.saturating_add(1);
                    connector.last_run = Some(now);
                    connector.status = "error".into();
                }
                TickPlan::BackingOff { .. } | TickPlan::NotDue => {}
                other => panic!("24h 模擬不該出現 {other:?} at {now}"),
            }
            now += chrono::Duration::seconds(5);
        }
        assert_eq!(
            runs, 9,
            "zero-jitter、base=300、*/15、每次當失敗：24h 應剛好 9 次，實際 {runs}"
        );
    }

    #[test]
    fn simulated_24h_without_backoff_is_every_cron_slot() {
        let cfg = BackoffConfig::from_secs(0, 43_200);
        let mut connector = sample_connector("rss", Some("*/15 * * * *"), "idle", None);
        connector.id = zero_jitter_id();
        let start = ts(2026, 1, 1, 0, 0);
        let end = start + chrono::Duration::hours(24);
        let mut now = start;
        let mut runs = 0usize;
        while now < end {
            match plan_tick(&connector, now, &cfg) {
                TickPlan::Run => {
                    runs += 1;
                    connector.error_count = connector.error_count.saturating_add(1);
                    connector.last_run = Some(now);
                    connector.status = "error".into();
                }
                TickPlan::BackingOff { .. } | TickPlan::NotDue => {}
                other => panic!("無退避模擬不該出現 {other:?} at {now}"),
            }
            now += chrono::Duration::seconds(5);
        }
        assert_eq!(
            runs, 96,
            "關閉退避時 */15 在 24h 內（不含終點 24:00）應為 96 次，實際 {runs}"
        );
    }

    #[test]
    fn summary_with_only_backing_off_is_info_not_warn() {
        let gate = Mutex::new(SummaryGate::default());
        let counts = TickCounts {
            backing_off: 12,
            ..TickCounts::default()
        };
        let logs = capture_logs(|| emit_tick_summary_with(&gate, &counts, Instant::now()));
        assert!(
            logs.contains("INFO") || logs.contains("略過摘要"),
            "純 backing_off 應走 info：{logs}"
        );
        assert!(
            !logs.contains("WARN") && !logs.contains("ERROR"),
            "純 backing_off 不該 warn：{logs}"
        );
    }
}
