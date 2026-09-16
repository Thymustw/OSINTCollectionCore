//! stix-worker e2e：對本機 Docker（Postgres／MinIO）真跑，**不接 Kafka**。
//!
//! 直接呼叫 [`stix_worker::StixWorker::process_dispatched_job`]。少一個
//! 非確定性來源（消費者何時收到）就少一類 flaky。
//!
//! 每個測試的 Entity 名稱含 run-specific UUID，避免與前幾次跑的資料共用列。
//! 測完刪掉這次寫進 MinIO 的 key。

use chrono::Utc;
use core_jobs::JobService;
use core_model::{
    Connector, Entity, EntityAlias, EntityType, Job, JobStatus, Provenance, RawEvidence,
    RelationshipType, Source, SourceType,
};
use core_observability::MetricsRegistry;
use entity_worker::entity_id;
use resolver::AutoApprovalConfig;
use serde_json::{Value, json};
use stix_worker::service::StixWorkerOptions;
use stix_worker::{
    ACTION_STIX_IMPORTED, JOB_TYPE_IMPORT, JobDispatchOutcome, PROCESSOR, StixWorker,
};
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::mock::MockEmbeddingProvider;
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use uuid::Uuid;

struct Stack {
    pg: PostgresCanonicalStore,
    s3: S3ObjectStore,
}

async fn connect_stack() -> Stack {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "e2e 只連本機 Postgres"
    );
    let endpoint = required_env("S3_ENDPOINT").expect("S3_ENDPOINT");
    let _ = verify_not_opencti_s3(&endpoint).expect("S3 埠隔離");
    let bucket = required_env("S3_BUCKET").unwrap_or_else(|_| "raw-evidence".into());
    let access = required_env("MINIO_ROOT_USER").expect("MINIO_ROOT_USER");
    let secret = required_env("MINIO_ROOT_PASSWORD").expect("MINIO_ROOT_PASSWORD");

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");
    let s3 = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("s3");
    s3.ensure_bucket().await.expect("bucket");
    Stack { pg, s3 }
}

fn disabled_auto() -> AutoApprovalConfig {
    AutoApprovalConfig {
        enabled: false,
        auto_confirm_score: 0.95,
        llm_review_score: 0.70,
        llm_model: "unused".into(),
        llm_temperature: 0.0,
        llm_max_tokens: 16,
    }
}

fn alias_auto() -> AutoApprovalConfig {
    AutoApprovalConfig {
        enabled: true,
        // alias 分數是 0.55；門檻降到 0.50 走高信心路徑，不打 LLM。
        auto_confirm_score: 0.50,
        llm_review_score: 0.50,
        llm_model: "unused".into(),
        llm_temperature: 0.0,
        llm_max_tokens: 16,
    }
}

type TestWorker = StixWorker<
    PostgresCanonicalStore,
    MockEmbeddingProvider,
    ai_gateway::MockLlmProvider,
    S3ObjectStore,
>;

fn worker(stack: &Stack) -> TestWorker {
    worker_with(stack, disabled_auto(), 3)
}

fn worker_with(stack: &Stack, auto: AutoApprovalConfig, max_merges: u32) -> TestWorker {
    StixWorker::new(
        stack.pg.clone(),
        stack.s3.clone(),
        MockEmbeddingProvider::unsupported(),
        ai_gateway::MockLlmProvider::always_same_entity(true),
        None,
        MetricsRegistry::new(),
        StixWorkerOptions {
            max_objects_per_tx: 10_000,
            max_auto_merges_per_resolve: max_merges,
            auto_approval: auto,
        },
    )
}

fn jobs(stack: &Stack) -> JobService<PostgresCanonicalStore> {
    JobService::new(stack.pg.clone(), None)
}

fn dispatch_payload(job: &Job) -> Value {
    json!({
        "job_id": job.id,
        "job_type": job.job_type,
        "status": job.status,
        "retry_count": job.retry_count,
        "parameters": job.parameters,
    })
}

async fn seed_source(pg: &PostgresCanonicalStore) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("stix-e2e-{}", Uuid::now_v7()),
        source_type: SourceType::StixImport,
        platform: Some("stix-e2e".into()),
        base_url: None,
        description: Some("stix-worker e2e fixture".into()),
        language: Some("zh-Hant".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    pg.put_source(&source).await.expect("source");
    source
}

async fn seed_connector(pg: &PostgresCanonicalStore, source: &Source) -> Connector {
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("stix-e2e-connector-{}", Uuid::now_v7()),
        connector_type: "stix_import".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({}),
        credential_reference: None,
        schedule: None,
        rate_limit: json!({}),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    pg.put_connector(&connector).await.expect("connector");
    connector
}

struct SeededImport {
    job: Job,
    storage_path: String,
}

async fn seed_import(
    stack: &Stack,
    source: &Source,
    connector: &Connector,
    bundle: &Value,
) -> SeededImport {
    let now = Utc::now();
    let raw_id = Uuid::now_v7();
    let bytes = serde_json::to_vec(bundle).expect("bundle json");
    let storage_path = format!("stix-e2e/{}/{raw_id}.json", source.id);
    stack
        .s3
        .put(&storage_path, &bytes, Some("application/json"))
        .await
        .expect("s3 put");
    let evidence = RawEvidence {
        id: raw_id,
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: Some(raw_id.to_string()),
        source_url: format!("http://127.0.0.1/stix-e2e/{raw_id}"),
        retrieved_at: now,
        content_type: Some("application/json".into()),
        mime_type: Some("application/json".into()),
        content_length: Some(bytes.len() as i64),
        sha256: format!("{}{}", raw_id.simple(), raw_id.simple()),
        storage_path: storage_path.clone(),
        http_status: Some(200),
        http_headers: json!({}),
        metadata: json!({ "import_spec": "stix_bundle" }),
        collector_version: "stix-worker-e2e/0.1.0".into(),
    };
    stack
        .pg
        .insert_raw_evidence(&evidence)
        .await
        .expect("raw evidence");
    let job = jobs(stack)
        .create(
            JOB_TYPE_IMPORT,
            Some(raw_id),
            Some(json!({
                "source_id": source.id,
                "raw_evidence_id": raw_id,
            })),
        )
        .await
        .expect("job");
    SeededImport { job, storage_path }
}

async fn cleanup_s3(stack: &Stack, path: &str) {
    let _ = stack.s3.delete(path).await;
}

fn org_actor_bundle(org_name: &str, actor_name: &str, org_desc: Option<&str>) -> Value {
    let org = Uuid::now_v7();
    let actor = Uuid::now_v7();
    let rel = Uuid::now_v7();
    let mut identity = json!({
        "type": "identity",
        "spec_version": "2.1",
        "id": format!("identity--{org}"),
        "identity_class": "organization",
        "name": org_name,
    });
    if let Some(desc) = org_desc {
        identity["description"] = json!(desc);
    }
    json!({
        "type": "bundle",
        "id": format!("bundle--{}", Uuid::now_v7()),
        "objects": [
            identity,
            {
                "type": "threat-actor",
                "spec_version": "2.1",
                "id": format!("threat-actor--{actor}"),
                "name": actor_name,
            },
            {
                "type": "relationship",
                "spec_version": "2.1",
                "id": format!("relationship--{rel}"),
                "relationship_type": "attributed-to",
                "source_ref": format!("threat-actor--{actor}"),
                "target_ref": format!("identity--{org}"),
            }
        ]
    })
}

fn normalize_org(name: &str) -> String {
    name.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

async fn require_entity(
    pg: &PostgresCanonicalStore,
    entity_type: EntityType,
    name: &str,
) -> Entity {
    let normalized = match entity_type {
        EntityType::Organization | EntityType::ThreatActor | EntityType::Person => {
            normalize_org(name)
        }
        _ => name.to_lowercase(),
    };
    pg.find_entity_by_normalized_name(entity_type, &normalized)
        .await
        .expect("find entity")
        .unwrap_or_else(|| panic!("找不到 {entity_type:?} / {normalized}"))
}

fn has_stix_imported(rows: &[Provenance]) -> bool {
    rows.iter()
        .any(|p| p.action == ACTION_STIX_IMPORTED && p.processor == PROCESSOR)
}

#[tokio::test]
async fn import_identity_and_threat_actor_writes_graph() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let org_name = format!("Acme Org {run}");
    let actor_name = format!("APT-{run}");
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let bundle = org_actor_bundle(&org_name, &actor_name, Some("first seen"));
    let seeded = seed_import(&stack, &source, &connector, &bundle).await;
    let service = worker(&stack);
    let jobs = jobs(&stack);

    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&seeded.job))
        .await;
    assert_eq!(
        outcome,
        JobDispatchOutcome::Completed {
            job_id: seeded.job.id
        }
    );
    let job = jobs.get(seeded.job.id).await.expect("job");
    assert_eq!(job.status, JobStatus::Completed);

    let org = require_entity(&stack.pg, EntityType::Organization, &org_name).await;
    let actor = require_entity(&stack.pg, EntityType::ThreatActor, &actor_name).await;
    assert_eq!(
        org.id,
        entity_id(EntityType::Organization, &org.normalized_name)
    );
    assert_eq!(org.description.as_deref(), Some("first seen"));
    assert!((org.confidence - 0.8).abs() < f64::EPSILON);

    let ids = stack
        .pg
        .list_entity_identifiers_by_entity(org.id, 100)
        .await
        .expect("identifiers");
    assert!(
        ids.iter().any(|i| i.namespace == "stix"),
        "Organization 應有 namespace=stix 的 identifier：{ids:?}"
    );

    let rels = stack
        .pg
        .list_relationships_by_object(actor.id, 100)
        .await
        .expect("rels");
    let edge = rels
        .iter()
        .find(|r| {
            r.relationship_type == RelationshipType::AttributedTo
                && r.source_object_id == actor.id
                && r.target_object_id == org.id
        })
        .expect("attributed-to 邊");
    assert_eq!(edge.evidence_count, 1);

    let org_prov = stack
        .pg
        .list_provenance_by_subject(org.id)
        .await
        .expect("org provenance");
    let rel_prov = stack
        .pg
        .list_provenance_by_subject(edge.id)
        .await
        .expect("rel provenance");
    assert!(has_stix_imported(&org_prov), "{org_prov:?}");
    assert!(has_stix_imported(&rel_prov), "{rel_prov:?}");

    cleanup_s3(&stack, &seeded.storage_path).await;
}

#[tokio::test]
async fn reimport_same_names_reuses_entity_and_keeps_first_seen() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let org_name = format!("Reimport Org {run}");
    let actor_name = format!("Reimport-APT-{run}");
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let first = org_actor_bundle(&org_name, &actor_name, Some("keep me"));
    let second = org_actor_bundle(&org_name, &actor_name, Some("should not overwrite"));
    let seeded1 = seed_import(&stack, &source, &connector, &first).await;
    let seeded2 = seed_import(&stack, &source, &connector, &second).await;
    let service = worker(&stack);
    let jobs = jobs(&stack);

    let o1 = service
        .process_dispatched_job(&jobs, &dispatch_payload(&seeded1.job))
        .await;
    let o2 = service
        .process_dispatched_job(&jobs, &dispatch_payload(&seeded2.job))
        .await;
    assert!(matches!(o1, JobDispatchOutcome::Completed { .. }));
    assert!(matches!(o2, JobDispatchOutcome::Completed { .. }));

    let org = require_entity(&stack.pg, EntityType::Organization, &org_name).await;
    assert_eq!(org.description.as_deref(), Some("keep me"));
    let actor = require_entity(&stack.pg, EntityType::ThreatActor, &actor_name).await;
    let rels = stack
        .pg
        .list_relationships_by_object(actor.id, 100)
        .await
        .expect("rels");
    let matching: Vec<_> = rels
        .iter()
        .filter(|r| {
            r.relationship_type == RelationshipType::AttributedTo
                && r.source_object_id == actor.id
                && r.target_object_id == org.id
        })
        .collect();
    assert_eq!(matching.len(), 1, "重匯入不得長出第二條邊：{matching:?}");
    assert_eq!(matching[0].evidence_count, 1);

    let org_prov = stack
        .pg
        .list_provenance_by_subject(org.id)
        .await
        .expect("prov");
    let imported = org_prov
        .iter()
        .filter(|p| p.action == ACTION_STIX_IMPORTED)
        .count();
    assert!(
        imported >= 2,
        "每次匯入應新寫一筆 stix_imported claim，實際 {imported}"
    );

    cleanup_s3(&stack, &seeded1.storage_path).await;
    cleanup_s3(&stack, &seeded2.storage_path).await;
}

#[tokio::test]
async fn unknown_campaign_is_skipped_and_rest_succeeds() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let org_name = format!("Skip Org {run}");
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let org = Uuid::now_v7();
    let campaign = Uuid::now_v7();
    let campaign_name = format!("ignored-campaign-{run}");
    let bundle = json!({
        "type": "bundle",
        "id": format!("bundle--{}", Uuid::now_v7()),
        "objects": [
            {
                "type": "identity",
                "id": format!("identity--{org}"),
                "identity_class": "organization",
                "name": org_name,
            },
            {
                "type": "campaign",
                "id": format!("campaign--{campaign}"),
                "name": campaign_name,
            }
        ]
    });
    let seeded = seed_import(&stack, &source, &connector, &bundle).await;
    let service = worker(&stack);
    let jobs = jobs(&stack);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&seeded.job))
        .await;
    assert!(matches!(outcome, JobDispatchOutcome::Completed { .. }));
    let _ = require_entity(&stack.pg, EntityType::Organization, &org_name).await;
    let missing = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Organization, &normalize_org(&campaign_name))
        .await
        .expect("lookup");
    assert!(missing.is_none(), "campaign 不該被映成 Entity");
    cleanup_s3(&stack, &seeded.storage_path).await;
}

#[tokio::test]
async fn graph_rebuild_job_is_ignored() {
    let stack = connect_stack().await;
    let jobs = jobs(&stack);
    let job = jobs
        .create("graph_rebuild", None, Some(json!({})))
        .await
        .expect("job");
    let service = worker(&stack);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&job))
        .await;
    assert_eq!(
        outcome,
        JobDispatchOutcome::Ignored {
            job_type: "graph_rebuild".into()
        }
    );
    let still = jobs.get(job.id).await.expect("job");
    assert_eq!(still.status, JobStatus::Queued);
}

#[tokio::test]
async fn stix_export_job_is_ignored() {
    let stack = connect_stack().await;
    let jobs = jobs(&stack);
    let job = jobs
        .create("stix_export", None, Some(json!({ "filter": {} })))
        .await
        .expect("job");
    let service = worker(&stack);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&job))
        .await;
    assert_eq!(
        outcome,
        JobDispatchOutcome::Ignored {
            job_type: "stix_export".into()
        }
    );
}

#[tokio::test]
async fn missing_parameters_marks_job_failed() {
    let stack = connect_stack().await;
    let jobs = jobs(&stack);
    let job = jobs
        .create(
            JOB_TYPE_IMPORT,
            None,
            Some(json!({ "source_id": Uuid::now_v7() })),
        )
        .await
        .expect("job");
    let service = worker(&stack);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&job))
        .await;
    assert_eq!(outcome, JobDispatchOutcome::Failed { job_id: job.id });
    let failed = jobs.get(job.id).await.expect("job");
    assert_eq!(failed.status, JobStatus::Failed);
    let err = failed.error.expect("error");
    assert!(err.contains("raw_evidence_id"), "{err}");
    assert!(err.contains("不會重試"), "{err}");
}

#[tokio::test]
async fn alias_auto_approval_merges_preseeded_pair() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let org_a_name = format!("Auto A {run}");
    let org_b_name = format!("Auto B {run}");
    let alias = format!("shared-{run}");
    let now = Utc::now();

    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;

    let entity_a = Entity {
        id: entity_id(EntityType::Organization, &normalize_org(&org_a_name)),
        entity_type: EntityType::Organization,
        name: org_a_name.clone(),
        normalized_name: normalize_org(&org_a_name),
        description: None,
        confidence: 0.8,
        first_seen: now,
        last_seen: now,
        merged_into: None,
        attributes: json!({}),
    };
    stack.pg.put_entity(&entity_a).await.expect("entity a");
    let entity_b = Entity {
        id: entity_id(EntityType::Organization, &normalize_org(&org_b_name)),
        entity_type: EntityType::Organization,
        name: org_b_name.clone(),
        normalized_name: normalize_org(&org_b_name),
        description: None,
        confidence: 0.8,
        first_seen: now,
        last_seen: now,
        merged_into: None,
        attributes: json!({}),
    };
    stack.pg.put_entity(&entity_b).await.expect("entity b");
    for (entity_id, tag) in [(entity_a.id, "a"), (entity_b.id, "b")] {
        stack
            .pg
            .put_entity_alias(&EntityAlias {
                id: Uuid::now_v7(),
                entity_id,
                alias: alias.clone(),
                alias_type: "aka".into(),
                source_id: Some(source.id),
                confidence: 0.9,
                first_seen: now,
                last_seen: now,
            })
            .await
            .unwrap_or_else(|err| panic!("alias {tag}: {err}"));
    }

    let org = Uuid::now_v7();
    let bundle = json!({
        "type": "bundle",
        "id": format!("bundle--{}", Uuid::now_v7()),
        "objects": [{
            "type": "identity",
            "id": format!("identity--{org}"),
            "identity_class": "organization",
            "name": org_b_name,
        }]
    });
    let seeded = seed_import(&stack, &source, &connector, &bundle).await;
    let service = worker_with(&stack, alias_auto(), 3);
    let jobs = jobs(&stack);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&seeded.job))
        .await;
    assert!(
        matches!(outcome, JobDispatchOutcome::Completed { .. }),
        "{outcome:?}"
    );

    let history = stack
        .pg
        .list_merge_history_by_entity(entity_b.id, 100)
        .await
        .expect("history");
    assert!(
        history.iter().any(|h| {
            h.undone_at.is_none()
                && ((h.survivor_id == entity_b.id && h.merged_id == entity_a.id)
                    || (h.survivor_id == entity_a.id && h.merged_id == entity_b.id))
                && h.operator.contains("stix_import")
        }),
        "應有 stix_import 自動合併紀錄：{history:?}"
    );
    let a_after = stack
        .pg
        .get_entity(entity_a.id)
        .await
        .expect("a")
        .expect("a");
    let b_after = stack
        .pg
        .get_entity(entity_b.id)
        .await
        .expect("b")
        .expect("b");
    assert!(
        a_after.merged_into == Some(entity_b.id) || b_after.merged_into == Some(entity_a.id),
        "其中一端應被併掉：a.merged_into={:?} b.merged_into={:?}",
        a_after.merged_into,
        b_after.merged_into
    );

    cleanup_s3(&stack, &seeded.storage_path).await;
}
