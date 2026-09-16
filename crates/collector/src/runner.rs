//! 一次收集：組裝 connector、落地、發事件、更新 checkpoint／Job。

use std::sync::Arc;

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
use crate::registry::{KnownConnectorKind, classify_connector_type};

/// 一次收集的結果（測試可直接呼叫，不必等 cron）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectOutcome {
    /// 抓到新 body 並寫入 RawEvidence。
    Collected { raw_evidence_id: uuid::Uuid },
    /// 304／無新內容，已更新 checkpoint。
    Unchanged,
    /// 未知 connector_type，已記 log，沒有 panic。
    SkippedUnknownType { connector_type: String },
    /// 沒有 schedule 或尚未到期。
    NotDue,
}

/// 生產用 runner：Postgres + MinIO + Redpanda。
#[derive(Clone)]
pub struct CollectorRunner {
    store: PostgresCanonicalStore,
    objects: S3ObjectStore,
    producer: Arc<EventProducer>,
    jobs: Arc<JobService<PostgresCanonicalStore>>,
    bounds: RunBounds,
    metrics: MetricsRegistry,
    audit: Arc<MemoryAuditLog>,
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
        for connector in connectors {
            match crate::schedule::is_due(
                connector.schedule.as_deref().unwrap_or(""),
                connector.last_run,
                now,
            ) {
                Ok(true) => due.push(connector),
                Ok(false) => results.push((connector.id, CollectOutcome::NotDue)),
                Err(err) => {
                    tracing::warn!(
                        connector_id = %connector.id,
                        error = %err,
                        "跳過：schedule 無法解析"
                    );
                    results.push((connector.id, CollectOutcome::NotDue));
                }
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
                    let outcome = runner.run_connector(connector).await;
                    (id, outcome)
                });
            }
            let Some(joined) = set.join_next().await else {
                break;
            };
            match joined {
                Ok((id, Ok(outcome))) => results.push((id, outcome)),
                Ok((id, Err(err))) => {
                    tracing::error!(connector_id = %id, error = %err, "收集失敗，繼續下一筆");
                }
                Err(err) => tracing::error!(error = %err, "收集 task join 失敗"),
            }
        }
        results
    }

    /// 測試與手動觸發：不管 cron 是否到期。
    pub async fn run_connector(
        &self,
        connector: Connector,
    ) -> Result<CollectOutcome, CollectorError> {
        match classify_connector_type(&connector.connector_type) {
            Some(kind) => self.run_known(kind, connector).await,
            None => {
                tracing::warn!(
                    connector_id = %connector.id,
                    connector_type = %connector.connector_type,
                    "未知 connector_type，跳過（尚未實作這個種類）"
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
        let ctx = CollectContext {
            source: source.clone(),
            connector: running.clone(),
            collection_id: None,
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
