//! `job.dispatched` → `stix_import` / `stix_export` 的執行調度。
//!
//! `stix_import` 與 `stix_export` 的執行本體是這裡的 `run_import`（本檔）與
//! `export.rs` 的 `run_export`。這個模組負責 job 狀態轉移（Running →
//! Completed/Failed）與 metrics。
//!
//! # 為什麼 JobService 不塞進 [`StixWorker`]
//!
//! 與 graph-worker 同一理由：這個 struct 的職責是「處理一份 STIX job」，
//! job 狀態轉移是呼叫端的事。方法放在這裡是為了讓 e2e 能直接呼叫
//! [`StixWorker::process_dispatched_job`]，不必接 Kafka。
//!
//! # offset 一律由呼叫端提交
//!
//! 這個方法**永遠回 [`JobDispatchOutcome`]**——「job 跑失敗」不是「消費事件失敗」。
//! 執行完（不管成敗）就該 commit，理由見 graph-worker 的 consume_loop 註解。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ai_gateway::LlmProvider;
use chrono::{DateTime, Utc};
use core_events::{EventProducer, EventTopic};
use core_jobs::{JobError, JobService};
use core_model::{Entity, EntityIdentifier, JobStatus, Provenance, Relationship, ResolutionStatus};
use core_observability::MetricsRegistry;
use entity_worker::{entity_id, identifier_id, relationship_id};
use resolver::{
    AutoApprovalConfig, AutoApprovalEvaluator, AutoApprovalOutcome, ResolverService,
    group_candidates_for_auto_approval,
};
use serde_json::{Value, json};
use stix_adapter::{
    StixBundle, StixId, StixObject, stix_object_to_entity, stix_relationship_to_core,
};
use storage_core::{
    EmbeddingProvider, ObjectStore, RelationalStore, StorageError, TransactionalStore,
};
use uuid::Uuid;

use crate::error::ImportError;

/// `job.dispatched` 上 stix-worker 認得的 `stix_import` job type。
pub const JOB_TYPE_IMPORT: &str = "stix_import";

/// `job.dispatched` 上 stix-worker 認得的 `stix_export` job type。
pub const JOB_TYPE_EXPORT: &str = "stix_export";

pub const PROCESSOR: &str = "stix-worker";

/// provenance 的動作名。每次匯入寫新的 claim（id = UUID v7），不做冪等擋下。
///
/// 重跑同一份 bundle 要能留下「又匯入一次」的紀錄；Entity／Relationship 的冪等
/// 靠 UUID v5 自然鍵，不靠這條 claim。
pub const ACTION_STIX_IMPORTED: &str = "stix_imported";

/// STIX 匯入寫入 Entity／Relationship 時的預設信心。
pub const STIX_DEFAULT_CONFIDENCE: f64 = 0.8;

const STIX_IDENTIFIER_NAMESPACE: &str = "stix";
const AUTO_APPROVAL_SOURCE: &str = "stix_import";
const PENDING_CANDIDATE_LIMIT: u32 = 100;

/// 一則 `job.dispatched` 處理完的結果。給呼叫端記 log 與 metrics。
///
/// 與 graph-worker 的同名 enum **語意相同、型別獨立**：stix-worker 不依賴
/// graph-worker crate。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobDispatchOutcome {
    /// `job_type` 不是 `stix_import`。已忽略，不是錯誤。
    Ignored { job_type: String },
    /// 匯入跑完且成功。Job 已轉 `Completed`。
    Completed { job_id: Uuid },
    /// 匯入回 `Err`。Job 已轉 `Failed`（錯誤訊息寫進 job.error）。
    Failed { job_id: Uuid },
    /// `JobService::transition` 本身失敗（例如 job_id 在 Postgres 查不到）。
    /// 重送也不會讓那個 id 出現，呼叫端仍應 commit。
    TransitionFailed { job_id: Option<Uuid> },
}

impl JobDispatchOutcome {
    fn metric_name(&self) -> &'static str {
        match self {
            Self::Ignored { .. } => "osint_stix_worker_job_ignored_total",
            Self::Completed { .. } => "osint_stix_worker_job_completed_total",
            Self::Failed { .. } | Self::TransitionFailed { .. } => {
                "osint_stix_worker_job_failed_total"
            }
        }
    }
}

/// 建構 [`StixWorker`] 時一次塞齊的可調參數。
#[derive(Debug, Clone)]
pub struct StixWorkerOptions {
    /// 單次交易最多寫入幾個**去重後**的 Entity。超過整批 Failed，不拆交易。
    pub max_objects_per_tx: usize,
    /// `stix_export` 匯出符合條件的 Entity 數量上限，來自 `[stix].max_objects`
    /// （跟 import 驗證 bundle 物件數共用同一個設定值，語意對稱：
    /// 一邊擋「進來太多」，一邊擋「一次要撈出去太多」）。
    pub max_export_objects: usize,
    /// 整個 bundle 共用的自動合併上限，來自 `[auto_approval].max_auto_merges_per_resolve`。
    pub max_auto_merges_per_resolve: u32,
    pub auto_approval: AutoApprovalConfig,
}

/// 生產用 stix-worker。泛型是為了測試能注入 mock embedder／LLM 與真實 S3。
pub struct StixWorker<S, E, L, O>
where
    S: TransactionalStore + Clone,
    E: EmbeddingProvider,
    L: LlmProvider,
    O: ObjectStore + Clone,
{
    pub(crate) store: S,
    pub(crate) objects: O,
    resolver: ResolverService<S, E>,
    auto_approval: AutoApprovalEvaluator<S, L>,
    producer: Option<Arc<EventProducer>>,
    metrics: MetricsRegistry,
    max_objects_per_tx: usize,
    pub(crate) max_export_objects: usize,
    max_auto_merges_per_resolve: u32,
}

impl<S, E, L, O> StixWorker<S, E, L, O>
where
    S: TransactionalStore + Clone,
    E: EmbeddingProvider,
    L: LlmProvider,
    O: ObjectStore + Clone,
{
    #[must_use]
    pub fn new(
        store: S,
        objects: O,
        embedder: E,
        llm: L,
        producer: Option<Arc<EventProducer>>,
        metrics: MetricsRegistry,
        options: StixWorkerOptions,
    ) -> Self {
        let resolver = ResolverService::new(store.clone(), embedder);
        let auto_approval =
            AutoApprovalEvaluator::new(store.clone(), producer.clone(), llm, options.auto_approval);
        Self {
            store,
            objects,
            resolver,
            auto_approval,
            producer,
            metrics,
            max_objects_per_tx: options.max_objects_per_tx,
            max_export_objects: options.max_export_objects,
            max_auto_merges_per_resolve: options.max_auto_merges_per_resolve,
        }
    }

    #[must_use]
    pub fn store(&self) -> &S {
        &self.store
    }

    /// 處理一則 `job.dispatched` payload。
    ///
    /// # offset 一律由呼叫端提交
    ///
    /// 這個方法永遠回 [`JobDispatchOutcome`]——「job 跑失敗」不是「消費事件失敗」。
    pub async fn process_dispatched_job(
        &self,
        jobs: &JobService<S>,
        payload: &Value,
    ) -> JobDispatchOutcome {
        let outcome = self.process_dispatched_job_inner(jobs, payload).await;
        self.metrics.inc(outcome.metric_name(), 1);
        if matches!(
            outcome,
            JobDispatchOutcome::Failed { .. } | JobDispatchOutcome::TransitionFailed { .. }
        ) {
            self.metrics.inc_failed_jobs(1);
        }
        outcome
    }

    async fn process_dispatched_job_inner(
        &self,
        jobs: &JobService<S>,
        payload: &Value,
    ) -> JobDispatchOutcome {
        let job_type = payload
            .get("job_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        match job_type {
            JOB_TYPE_IMPORT => {
                let Some(job_id) = payload
                    .get("job_id")
                    .and_then(Value::as_str)
                    .and_then(|s| Uuid::parse_str(s).ok())
                else {
                    tracing::error!(
                        payload = %payload,
                        "stix_import job.dispatched 缺少合法 job_id。重送也不會讓它出現在 Postgres，提交 offset"
                    );
                    return JobDispatchOutcome::TransitionFailed { job_id: None };
                };

                tracing::info!(%job_id, "收到 stix_import job，開始讀 bundle");

                if let Err(err) = jobs.transition(job_id, JobStatus::Running, None).await {
                    log_job_transition_error(job_id, "Running", &err);
                    return JobDispatchOutcome::TransitionFailed {
                        job_id: Some(job_id),
                    };
                }

                match self.run_import(jobs, job_id).await {
                    Ok(report) => {
                        if let Err(err) = jobs.transition(job_id, JobStatus::Completed, None).await
                        {
                            log_job_transition_error(job_id, "Completed", &err);
                            return JobDispatchOutcome::TransitionFailed {
                                job_id: Some(job_id),
                            };
                        }
                        self.metrics.inc(
                            "osint_stix_worker_entities_total",
                            report.entity_count as u64,
                        );
                        self.metrics.inc(
                            "osint_stix_worker_relationships_total",
                            report.relationship_count as u64,
                        );
                        self.metrics.inc(
                            "osint_stix_worker_skipped_total",
                            (report.skipped_objects + report.skipped_relationships) as u64,
                        );
                        self.metrics.inc(
                            "osint_stix_worker_auto_merges_total",
                            u64::from(report.auto_merges),
                        );
                        tracing::info!(
                            %job_id,
                            entity_count = report.entity_count,
                            relationship_count = report.relationship_count,
                            skipped_objects = report.skipped_objects,
                            skipped_relationships = report.skipped_relationships,
                            auto_merges = report.auto_merges,
                            "stix_import job 完成"
                        );
                        JobDispatchOutcome::Completed { job_id }
                    }
                    Err(err) => {
                        if let Err(trans_err) = jobs
                            .transition(job_id, JobStatus::Failed, Some(err.to_string()))
                            .await
                        {
                            tracing::error!(
                                error = %trans_err,
                                import_error = %err,
                                %job_id,
                                "stix_import 失敗，且標記 Failed 也失敗。重送不會讓 already-failed 的 job 重跑，提交 offset"
                            );
                            return JobDispatchOutcome::TransitionFailed {
                                job_id: Some(job_id),
                            };
                        }
                        tracing::error!(error = %err, %job_id, "stix_import job 失敗，已標記 Failed");
                        JobDispatchOutcome::Failed { job_id }
                    }
                }
            }
            JOB_TYPE_EXPORT => self.process_export(jobs, payload).await,
            _ => {
                tracing::debug!(
                    job_type,
                    "job.dispatched 的 job_type 既不是 stix_import 也不是 stix_export，stix-worker 忽略"
                );
                JobDispatchOutcome::Ignored {
                    job_type: job_type.to_string(),
                }
            }
        }
    }

    /// `stix_export` Job 的執行流程：Running → `run_export` → 成功時先
    /// `merge_parameters` 把 `result_object_key` 寫回 `job.parameters` 才
    /// `Completed`；失敗 `Failed`。metrics 沿用 [`JobDispatchOutcome::metric_name`]
    /// （依 outcome 計算，不只依 job_type），另加 export 專屬計數器。
    async fn process_export(&self, jobs: &JobService<S>, payload: &Value) -> JobDispatchOutcome {
        let Some(job_id) = payload
            .get("job_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
        else {
            tracing::error!(
                payload = %payload,
                "stix_export job.dispatched 缺少合法 job_id。重送也不會讓它出現在 Postgres，提交 offset"
            );
            return JobDispatchOutcome::TransitionFailed { job_id: None };
        };

        tracing::info!(%job_id, "收到 stix_export job，開始組 bundle");

        if let Err(err) = jobs.transition(job_id, JobStatus::Running, None).await {
            log_job_transition_error(job_id, "Running", &err);
            return JobDispatchOutcome::TransitionFailed {
                job_id: Some(job_id),
            };
        }

        match self.run_export(jobs, job_id).await {
            Ok(report) => {
                // 先回填 result_object_key 再 Completed：`GET /jobs/{id}/result` 判斷
                // 「completed 但缺 key」是 500——順序反過來中間會有一個時間窗使用者
                // 查到 completed 卻拿到「缺 key」的 500。
                if let Err(err) = jobs
                    .merge_parameters(
                        job_id,
                        json!({
                            "result_object_key": report.result_object_key,
                        }),
                    )
                    .await
                {
                    tracing::error!(
                        error = %err,
                        %job_id,
                        object_key = %report.result_object_key,
                        "stix_export bundle 已寫進物件儲存，但回填 Job parameters 失敗，整個 Job 標記 Failed"
                    );
                    if let Err(trans_err) = jobs
                        .transition(job_id, JobStatus::Failed, Some(err.to_string()))
                        .await
                    {
                        tracing::error!(
                            error = %trans_err,
                            %job_id,
                            "stix_export 回填 parameters 失敗後標記 Failed 又失敗"
                        );
                        return JobDispatchOutcome::TransitionFailed {
                            job_id: Some(job_id),
                        };
                    }
                    return JobDispatchOutcome::Failed { job_id };
                }
                if let Err(trans_err) = jobs.transition(job_id, JobStatus::Completed, None).await {
                    tracing::error!(
                        error = %trans_err,
                        %job_id,
                        "stix_export 完成但狀態轉 Completed 失敗"
                    );
                    return JobDispatchOutcome::TransitionFailed {
                        job_id: Some(job_id),
                    };
                }
                self.metrics.inc(
                    "osint_stix_worker_export_entities_total",
                    report.entity_count as u64,
                );
                self.metrics.inc(
                    "osint_stix_worker_export_relationships_total",
                    report.relationship_count as u64,
                );
                tracing::info!(
                    %job_id,
                    entity_count = report.entity_count,
                    relationship_count = report.relationship_count,
                    result_object_key = %report.result_object_key,
                    "stix_export job 完成"
                );
                JobDispatchOutcome::Completed { job_id }
            }
            Err(err) => {
                if let Err(trans_err) = jobs
                    .transition(job_id, JobStatus::Failed, Some(err.to_string()))
                    .await
                {
                    tracing::error!(
                        error = %trans_err,
                        import_error = %err,
                        %job_id,
                        "stix_export 失敗，且標記 Failed 也失敗。提交 offset"
                    );
                    return JobDispatchOutcome::TransitionFailed {
                        job_id: Some(job_id),
                    };
                }
                tracing::error!(error = %err, %job_id, "stix_export job 失敗，已標記 Failed");
                JobDispatchOutcome::Failed { job_id }
            }
        }
    }

    async fn run_import(
        &self,
        jobs: &JobService<S>,
        job_id: Uuid,
    ) -> Result<ImportReport, ImportError> {
        let job = jobs.get(job_id).await.map_err(|err| {
            ImportError::Storage(StorageError::Unknown {
                backend: "jobs",
                message: format!("讀取 stix_import Job `{job_id}` 失敗：{err}"),
            })
        })?;
        let params = job
            .parameters
            .as_ref()
            .ok_or_else(|| ImportError::MissingParameter {
                field: "parameters".into(),
            })?;
        let source_id = uuid_param(params, "source_id")?;
        let raw_evidence_id = uuid_param(params, "raw_evidence_id")?;

        let evidence = self.store.get_raw_evidence(raw_evidence_id).await?.ok_or(
            ImportError::EvidenceNotFound {
                id: raw_evidence_id,
            },
        )?;
        if evidence.source_id != source_id {
            tracing::warn!(
                %job_id,
                parameter_source_id = %source_id,
                evidence_source_id = %evidence.source_id,
                %raw_evidence_id,
                "Job 參數 source_id 與 RawEvidence.source_id 不一致，改用證據上的 source_id"
            );
        }
        let source_id = evidence.source_id;

        let blob = self
            .objects
            .get(&evidence.storage_path)
            .await?
            .ok_or_else(|| ImportError::BlobMissing {
                raw_evidence_id,
                path: evidence.storage_path.clone(),
            })?;

        let bundle: StixBundle =
            serde_json::from_slice(&blob).map_err(|err| ImportError::BundleParse {
                message: err.to_string(),
            })?;

        let planned = plan_bundle(&bundle, self.max_objects_per_tx)?;
        let now = Utc::now();

        let tx = self.store.begin().await?;
        let db = tx.store();
        let mut written_entities: Vec<Uuid> = Vec::new();
        let mut seen_entities: HashSet<Uuid> = HashSet::new();
        let mut stix_to_entity: HashMap<StixId, Uuid> = HashMap::new();
        let mut relationships: Vec<RelationshipFact> = Vec::new();

        for mapped in &planned.entities {
            let persisted = upsert_entity(db, mapped, now).await?;
            write_stix_identifier(db, &persisted, mapped, source_id).await?;
            write_entity_provenance(db, persisted.id, mapped, raw_evidence_id, now).await?;
            stix_to_entity.insert(mapped.stix_id.clone(), persisted.id);
            if seen_entities.insert(persisted.id) {
                written_entities.push(persisted.id);
            }
        }

        let mut skipped_relationships = 0usize;
        for rel in &planned.relationships {
            let Some(&source_object_id) = stix_to_entity.get(&rel.source_ref) else {
                tracing::warn!(
                    source_ref = %rel.source_ref,
                    target_ref = %rel.target_ref,
                    relationship_type = %rel.stix_relationship_type,
                    "STIX Relationship 的 source_ref 不在本批 Entity 對應表，跳過這條邊，不中斷整批"
                );
                skipped_relationships += 1;
                continue;
            };
            let Some(&target_object_id) = stix_to_entity.get(&rel.target_ref) else {
                tracing::warn!(
                    source_ref = %rel.source_ref,
                    target_ref = %rel.target_ref,
                    relationship_type = %rel.stix_relationship_type,
                    "STIX Relationship 的 target_ref 不在本批 Entity 對應表，跳過這條邊，不中斷整批"
                );
                skipped_relationships += 1;
                continue;
            };
            let rel_id = upsert_relationship(
                db,
                source_object_id,
                rel.relationship_type,
                target_object_id,
                now,
            )
            .await?;
            write_relationship_provenance(db, rel_id, rel, raw_evidence_id, now).await?;
            relationships.push(RelationshipFact {
                relationship_id: rel_id,
                source_object_id,
                target_object_id,
                relationship_type: rel.relationship_type,
            });
        }

        tx.commit().await?;

        publish_relationship_changes(self.producer.as_deref(), &relationships).await;

        let auto_merges = self.resolve_and_auto_approve(&written_entities).await;

        Ok(ImportReport {
            entity_count: written_entities.len(),
            relationship_count: relationships.len(),
            skipped_objects: planned.skipped_objects,
            skipped_relationships,
            auto_merges,
        })
    }

    /// 交易提交後才跑。失敗只記 log，不把已匯入成功的 Job 改成 Failed。
    ///
    /// `max_auto_merges_per_resolve` 是**整個 bundle 共用**一個計數器，不是每個
    /// Entity 各算一次。resolve 本身不受這個上限影響——只停自動合併。
    async fn resolve_and_auto_approve(&self, entity_ids: &[Uuid]) -> u32 {
        let mut auto_merges = 0u32;
        for &entity_id in entity_ids {
            if let Err(err) = self.resolver.resolve_entity(entity_id).await {
                tracing::error!(
                    error = %err,
                    %entity_id,
                    "stix_import 後 resolve_entity 失敗。Entity 已落地，這次略過自動核准；\
                     請之後用 POST /entities/{{id}}/resolve 補跑"
                );
                continue;
            }
            if auto_merges >= self.max_auto_merges_per_resolve {
                continue;
            }
            let pending = match self
                .store
                .list_resolution_candidates_by_entity(
                    entity_id,
                    Some(ResolutionStatus::Pending),
                    None,
                    PENDING_CANDIDATE_LIMIT,
                )
                .await
            {
                Ok(rows) => rows,
                Err(err) => {
                    tracing::error!(
                        error = %err,
                        %entity_id,
                        "列出 Pending 候選失敗，這個 Entity 略過自動核准"
                    );
                    continue;
                }
            };
            let groups = group_candidates_for_auto_approval(entity_id, &pending);
            for (other_id, group) in groups {
                if auto_merges >= self.max_auto_merges_per_resolve {
                    break;
                }
                match self
                    .auto_approval
                    .evaluate_pair(entity_id, other_id, &group, AUTO_APPROVAL_SOURCE)
                    .await
                {
                    AutoApprovalOutcome::Merged { merge_history_id } => {
                        auto_merges += 1;
                        tracing::info!(
                            survivor_id = %entity_id,
                            merged_id = %other_id,
                            %merge_history_id,
                            "stix_import 自動核准已合併"
                        );
                    }
                    AutoApprovalOutcome::Pending { reason } => {
                        tracing::debug!(
                            survivor_id = %entity_id,
                            other_id = %other_id,
                            %reason,
                            "stix_import 自動核准維持 Pending"
                        );
                    }
                    AutoApprovalOutcome::Disabled => {}
                }
            }
        }
        auto_merges
    }
}

struct ImportReport {
    entity_count: usize,
    relationship_count: usize,
    skipped_objects: usize,
    skipped_relationships: usize,
    auto_merges: u32,
}

#[derive(Debug)]
struct PlannedBundle {
    entities: Vec<stix_adapter::MappedEntity>,
    relationships: Vec<stix_adapter::MappedRelationship>,
    skipped_objects: usize,
}

fn plan_bundle(
    bundle: &StixBundle,
    max_objects_per_tx: usize,
) -> Result<PlannedBundle, ImportError> {
    let mut entities = Vec::new();
    let mut relationships = Vec::new();
    let mut skipped_objects = 0usize;
    let mut unique_entity_ids: HashSet<Uuid> = HashSet::new();

    for object in &bundle.objects {
        if let StixObject::Relationship(rel) = object {
            relationships.push(stix_relationship_to_core(rel));
            continue;
        }
        match stix_object_to_entity(object) {
            Some(mapped) => {
                unique_entity_ids.insert(entity_id(mapped.entity_type, &mapped.normalized_name));
                entities.push(mapped);
            }
            None => skipped_objects += 1,
        }
    }

    if unique_entity_ids.len() > max_objects_per_tx {
        return Err(ImportError::TooManyObjects {
            mapped: unique_entity_ids.len(),
            max: max_objects_per_tx,
        });
    }

    Ok(PlannedBundle {
        entities,
        relationships,
        skipped_objects,
    })
}

async fn upsert_entity(
    db: &dyn RelationalStore,
    mapped: &stix_adapter::MappedEntity,
    now: DateTime<Utc>,
) -> Result<Entity, ImportError> {
    let existing = db
        .find_entity_by_normalized_name(mapped.entity_type, &mapped.normalized_name)
        .await?;

    let mut attributes = serde_json::Map::new();
    if let Some(existing) = &existing {
        if let Some(map) = existing.attributes.as_object() {
            attributes.extend(map.clone());
        }
    }
    if let Some(map) = mapped.attributes.as_object() {
        for (key, value) in map {
            attributes.insert(key.clone(), value.clone());
        }
    }

    let entity = Entity {
        id: existing.as_ref().map_or_else(
            || entity_id(mapped.entity_type, &mapped.normalized_name),
            |e| e.id,
        ),
        entity_type: mapped.entity_type,
        name: existing
            .as_ref()
            .map_or_else(|| mapped.name.clone(), |e| e.name.clone()),
        normalized_name: mapped.normalized_name.clone(),
        description: existing
            .as_ref()
            .and_then(|e| e.description.clone())
            .or_else(|| mapped.description.clone()),
        confidence: existing.as_ref().map_or(STIX_DEFAULT_CONFIDENCE, |e| {
            e.confidence.max(STIX_DEFAULT_CONFIDENCE)
        }),
        first_seen: existing.as_ref().map_or(now, |e| e.first_seen),
        last_seen: now,
        merged_into: existing.as_ref().and_then(|e| e.merged_into),
        attributes: Value::Object(attributes),
    };

    match db.put_entity(&entity).await {
        Ok(()) => Ok(entity),
        Err(StorageError::Conflict { .. }) => db
            .find_entity_by_normalized_name(mapped.entity_type, &mapped.normalized_name)
            .await?
            .ok_or_else(|| {
                ImportError::Storage(StorageError::Conflict {
                    message: format!(
                        "Entity `{:?}` / `{}` 寫入衝突後重查不到。請查 idx_entities_natural_key",
                        mapped.entity_type, mapped.normalized_name
                    ),
                })
            }),
        Err(err) => Err(err.into()),
    }
}

/// STIX identifier 已有 owner（不論是自己還是別人）就跳過，不中斷交易。
async fn write_stix_identifier(
    db: &dyn RelationalStore,
    entity: &Entity,
    mapped: &stix_adapter::MappedEntity,
    source_id: Uuid,
) -> Result<(), ImportError> {
    let fields = mapped.stix_identifier_fields();
    if let Some(owner) = db
        .find_entity_identifier_owner(&fields.namespace, &fields.normalized_value)
        .await?
    {
        if owner.entity_id != entity.id {
            tracing::info!(
                entity_id = %entity.id,
                owner_entity_id = %owner.entity_id,
                stix_id = %fields.value,
                "STIX identifier 已被另一個 Entity 佔走，略過寫入，不中斷這批匯入"
            );
        }
        return Ok(());
    }

    let identifier = EntityIdentifier {
        id: identifier_id(
            STIX_IDENTIFIER_NAMESPACE,
            entity.id,
            &fields.normalized_value,
        ),
        entity_id: entity.id,
        namespace: fields.namespace,
        value: fields.value,
        normalized_value: fields.normalized_value.clone(),
        confidence: entity.confidence,
        source_id: Some(source_id),
        first_seen: entity.first_seen,
        last_seen: entity.last_seen,
    };
    match db.put_entity_identifier(&identifier).await {
        Ok(()) => Ok(()),
        Err(StorageError::Conflict { message }) => {
            tracing::info!(
                entity_id = %entity.id,
                stix_id = %fields.normalized_value,
                %message,
                "寫入 STIX identifier 撞 UNIQUE，略過，不中斷這批匯入"
            );
            Ok(())
        }
        Err(err) => Err(err.into()),
    }
}

async fn write_entity_provenance(
    db: &dyn RelationalStore,
    subject_id: Uuid,
    mapped: &stix_adapter::MappedEntity,
    raw_evidence_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), ImportError> {
    let claim = Provenance {
        id: Uuid::now_v7(),
        subject_id,
        action: ACTION_STIX_IMPORTED.into(),
        parent_id: Some(raw_evidence_id),
        raw_evidence_id: Some(raw_evidence_id),
        processor: PROCESSOR.into(),
        processor_version: env!("CARGO_PKG_VERSION").into(),
        timestamp: now,
        metadata: json!({
            "stix_id": mapped.stix_id.to_string(),
            "entity_type": mapped.entity_type,
        }),
    };
    db.put_provenance(&claim).await?;
    Ok(())
}

async fn upsert_relationship(
    db: &dyn RelationalStore,
    source_object_id: Uuid,
    relationship_type: core_model::RelationshipType,
    target_object_id: Uuid,
    now: DateTime<Utc>,
) -> Result<Uuid, ImportError> {
    let id = relationship_id(source_object_id, relationship_type, target_object_id);
    let existing = db.get_relationship(id).await?;
    let relationship = Relationship {
        id,
        source_object_id,
        relationship_type,
        target_object_id,
        confidence: existing.as_ref().map_or(STIX_DEFAULT_CONFIDENCE, |r| {
            r.confidence.max(STIX_DEFAULT_CONFIDENCE)
        }),
        first_seen: existing.as_ref().map_or(now, |r| r.first_seen),
        last_seen: now,
        evidence_count: existing.as_ref().map_or(1, |r| r.evidence_count),
        created_at: existing.as_ref().map_or(now, |r| r.created_at),
        updated_at: now,
    };
    db.put_relationship(&relationship).await?;
    Ok(id)
}

async fn write_relationship_provenance(
    db: &dyn RelationalStore,
    subject_id: Uuid,
    rel: &stix_adapter::MappedRelationship,
    raw_evidence_id: Uuid,
    now: DateTime<Utc>,
) -> Result<(), ImportError> {
    let claim = Provenance {
        id: Uuid::now_v7(),
        subject_id,
        action: ACTION_STIX_IMPORTED.into(),
        parent_id: Some(raw_evidence_id),
        raw_evidence_id: Some(raw_evidence_id),
        processor: PROCESSOR.into(),
        processor_version: env!("CARGO_PKG_VERSION").into(),
        timestamp: now,
        metadata: json!({
            "stix_relationship_type": rel.stix_relationship_type,
            "source_ref": rel.source_ref.to_string(),
            "target_ref": rel.target_ref.to_string(),
        }),
    };
    db.put_provenance(&claim).await?;
    Ok(())
}

struct RelationshipFact {
    relationship_id: Uuid,
    source_object_id: Uuid,
    target_object_id: Uuid,
    relationship_type: core_model::RelationshipType,
}

async fn publish_relationship_changes(
    producer: Option<&EventProducer>,
    relationships: &[RelationshipFact],
) {
    let Some(producer) = producer else {
        return;
    };
    for fact in relationships {
        let key = fact.relationship_id.to_string();
        let payload = json!({
            "relationship_id": fact.relationship_id,
            "source_object_id": fact.source_object_id,
            "target_object_id": fact.target_object_id,
            "relationship_type": fact.relationship_type,
            "change_kind": "upserted",
        });
        if let Err(err) = producer
            .publish(
                EventTopic::RelationshipChanged,
                Some(&key),
                Some(fact.relationship_id),
                payload,
            )
            .await
        {
            tracing::error!(
                error = %err,
                relationship_id = %fact.relationship_id,
                "stix_import 發 relationship.changed 失敗。邊已在 Postgres，圖投影這次不會即時更新；\
                 請之後跑 graph-worker --rebuild，或等下一次同一條邊的變更事件"
            );
        }
    }
}

fn uuid_param(params: &Value, field: &str) -> Result<Uuid, ImportError> {
    let value = params
        .get(field)
        .ok_or_else(|| ImportError::MissingParameter {
            field: field.to_string(),
        })?;
    if let Some(s) = value.as_str() {
        return Uuid::parse_str(s).map_err(|_| ImportError::MissingParameter {
            field: field.to_string(),
        });
    }
    Err(ImportError::MissingParameter {
        field: field.to_string(),
    })
}

fn log_job_transition_error(job_id: Uuid, to: &str, err: &JobError) {
    tracing::error!(
        error = %err,
        %job_id,
        to,
        "stix_import job 狀態轉移失敗。重送也不會讓這個 job_id 變成可轉移，提交 offset"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_model::RelationshipType;
    use stix_adapter::StixId;

    fn bundle_json(objects: Value) -> Value {
        json!({
            "type": "bundle",
            "id": format!("bundle--{}", Uuid::nil()),
            "objects": objects,
        })
    }

    #[test]
    fn uuid_param_reads_string() {
        let id = Uuid::now_v7();
        let params = json!({ "source_id": id.to_string() });
        assert_eq!(uuid_param(&params, "source_id").unwrap(), id);
    }

    #[test]
    fn uuid_param_missing_is_explicit() {
        let err = uuid_param(&json!({}), "raw_evidence_id").unwrap_err();
        assert!(err.to_string().contains("raw_evidence_id"), "{err}");
        assert!(err.to_string().contains("不會重試"), "{err}");
    }

    #[test]
    fn plan_bundle_skips_unknown_and_counts_unique_entities() {
        let org = Uuid::now_v7();
        let actor = Uuid::now_v7();
        let campaign = Uuid::now_v7();
        let rel = Uuid::now_v7();
        let objects = json!([
            {
                "type": "identity",
                "id": format!("identity--{org}"),
                "identity_class": "organization",
                "name": "Acme"
            },
            {
                "type": "threat-actor",
                "id": format!("threat-actor--{actor}"),
                "name": "APT-X"
            },
            {
                "type": "campaign",
                "id": format!("campaign--{campaign}"),
                "name": "ignored"
            },
            {
                "type": "relationship",
                "id": format!("relationship--{rel}"),
                "relationship_type": "attributed-to",
                "source_ref": format!("threat-actor--{actor}"),
                "target_ref": format!("identity--{org}")
            }
        ]);
        let bundle: StixBundle = serde_json::from_value(bundle_json(objects)).unwrap();
        let planned = plan_bundle(&bundle, 10_000).unwrap();
        assert_eq!(planned.entities.len(), 2);
        assert_eq!(planned.relationships.len(), 1);
        assert_eq!(planned.skipped_objects, 1);
        assert_eq!(
            planned.relationships[0].relationship_type,
            RelationshipType::AttributedTo
        );
    }

    #[test]
    fn plan_bundle_rejects_over_max_unique_entities() {
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        let objects = json!([
            {
                "type": "identity",
                "id": format!("identity--{a}"),
                "identity_class": "organization",
                "name": "One"
            },
            {
                "type": "identity",
                "id": format!("identity--{b}"),
                "identity_class": "organization",
                "name": "Two"
            }
        ]);
        let bundle: StixBundle = serde_json::from_value(bundle_json(objects)).unwrap();
        let err = plan_bundle(&bundle, 1).unwrap_err();
        match err {
            ImportError::TooManyObjects { mapped, max } => {
                assert_eq!(mapped, 2);
                assert_eq!(max, 1);
            }
            other => panic!("expected TooManyObjects, got {other}"),
        }
    }

    #[test]
    fn stix_id_round_trips_in_map_key() {
        let id = StixId::parse(format!("identity--{}", Uuid::now_v7())).unwrap();
        let mut map = HashMap::new();
        map.insert(id.clone(), Uuid::now_v7());
        assert!(map.contains_key(&id));
    }
}
