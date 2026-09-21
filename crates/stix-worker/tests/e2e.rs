//! stix-worker e2e：對本機 Docker（Postgres／MinIO）真跑，**不接 Kafka**。
//!
//! 直接呼叫 [`stix_worker::StixWorker::process_dispatched_job`]。少一個
//! 非確定性來源（消費者何時收到）就少一類 flaky。
//!
//! 每個測試的 Entity 名稱含 run-specific UUID，避免與前幾次跑的資料共用列。
//! 測完刪掉這次寫進 MinIO 的 key。

use chrono::{TimeZone, Utc};
use core_jobs::JobService;
use core_model::{
    Connector, Entity, EntityAlias, EntityType, Job, JobStatus, Provenance, RawEvidence,
    Relationship, RelationshipType, Source, SourceType,
};
use core_observability::MetricsRegistry;
use entity_worker::{entity_id, relationship_id};
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
        llm_model_version: "unused".into(),
        llm_temperature: 0.0,
        llm_max_tokens: 16,
        enable_reasoning: false,
    }
}

fn alias_auto() -> AutoApprovalConfig {
    AutoApprovalConfig {
        enabled: true,
        // alias 分數是 0.55；門檻降到 0.50 走高信心路徑，不打 LLM。
        auto_confirm_score: 0.50,
        llm_review_score: 0.50,
        llm_model: "unused".into(),
        llm_model_version: "unused".into(),
        llm_temperature: 0.0,
        llm_max_tokens: 16,
        enable_reasoning: false,
    }
}

type TestWorker = StixWorker<
    PostgresCanonicalStore,
    MockEmbeddingProvider,
    ai_gateway::MockLlmProvider,
    S3ObjectStore,
>;

fn worker(stack: &Stack) -> TestWorker {
    worker_with(stack, disabled_auto(), 3, 10_000)
}

fn worker_with_max_export(stack: &Stack, max_export_objects: usize) -> TestWorker {
    worker_with(stack, disabled_auto(), 3, max_export_objects)
}

fn worker_with(
    stack: &Stack,
    auto: AutoApprovalConfig,
    max_merges: u32,
    max_export_objects: usize,
) -> TestWorker {
    StixWorker::new(
        stack.pg.clone(),
        stack.s3.clone(),
        MockEmbeddingProvider::unsupported(),
        ai_gateway::MockLlmProvider::always_same_entity(true),
        None,
        MetricsRegistry::new(),
        StixWorkerOptions {
            max_objects_per_tx: 10_000,
            max_export_objects,
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

fn make_entity(
    entity_type: EntityType,
    name: &str,
    first_seen: chrono::DateTime<chrono::Utc>,
) -> Entity {
    let normalized = name.to_lowercase();
    Entity {
        id: entity_id(entity_type, &normalized),
        entity_type,
        name: name.into(),
        normalized_name: normalized,
        description: None,
        confidence: 0.8,
        first_seen,
        last_seen: first_seen,
        merged_into: None,
        attributes: json!({}),
    }
}

fn make_relationship(
    source: &Uuid,
    rel_type: RelationshipType,
    target: &Uuid,
    first_seen: chrono::DateTime<chrono::Utc>,
) -> Relationship {
    Relationship {
        id: relationship_id(*source, rel_type, *target),
        source_object_id: *source,
        relationship_type: rel_type,
        target_object_id: *target,
        confidence: 0.8,
        first_seen,
        last_seen: first_seen,
        evidence_count: 1,
        created_at: first_seen,
        updated_at: first_seen,
    }
}

/// 從物件儲存讀回匯出結果 bundle，解析成 Value 供斷言。
async fn fetch_export_bundle(stack: &Stack, job_id: Uuid) -> Value {
    let key = stix_adapter::export_result_object_key(job_id);
    let bytes = stack
        .s3
        .get(&key)
        .await
        .expect("s3 get")
        .unwrap_or_else(|| panic!("找不到匯出 key `{key}`"));
    serde_json::from_slice(&bytes).expect("bundle json")
}

fn bundle_objects(bundle: &Value) -> &Vec<Value> {
    bundle["objects"].as_array().expect("objects array")
}

/// 建立一個 `stix_export` Job 並回傳。
async fn create_export_job(stack: &Stack, filter: Value) -> Job {
    let jobs = jobs(stack);
    jobs.create("stix_export", None, Some(json!({ "filter": filter })))
        .await
        .expect("job")
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
async fn unknown_job_type_is_ignored() {
    let stack = connect_stack().await;
    let jobs = jobs(&stack);
    let job = jobs
        .create("unknown_job_type", None, Some(json!({})))
        .await
        .expect("job");
    let service = worker(&stack);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&job))
        .await;
    assert_eq!(
        outcome,
        JobDispatchOutcome::Ignored {
            job_type: "unknown_job_type".into()
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
    let service = worker_with(&stack, alias_auto(), 3, 10_000);
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

#[tokio::test]
async fn export_entity_ids_precise() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let a = make_entity(
        EntityType::Organization,
        &format!("Export Org A {}", Uuid::now_v7()),
        now,
    );
    let b = make_entity(
        EntityType::Person,
        &format!("Export Person B {}", Uuid::now_v7()),
        now,
    );
    stack.pg.put_entity(&a).await.expect("entity a");
    stack.pg.put_entity(&b).await.expect("entity b");
    let rel = make_relationship(&b.id, RelationshipType::MemberOf, &a.id, now);
    stack.pg.put_relationship(&rel).await.expect("relationship");

    let job = create_export_job(&stack, json!({ "entity_ids": [a.id, b.id] })).await;
    let service = worker(&stack);
    let outcome = service
        .process_dispatched_job(&jobs(&stack), &dispatch_payload(&job))
        .await;
    assert_eq!(outcome, JobDispatchOutcome::Completed { job_id: job.id });

    let bundle = fetch_export_bundle(&stack, job.id).await;
    assert_eq!(bundle["type"], "bundle");
    let objects = bundle_objects(&bundle);
    let kinds: Vec<&str> = objects
        .iter()
        .map(|o| o["type"].as_str().unwrap())
        .collect();
    assert_eq!(objects.len(), 3, "應含兩物件一關係：{objects:#?}");
    assert!(
        kinds.contains(&"identity"),
        "Organization → identity：{kinds:?}"
    );
    assert!(kinds.contains(&"relationship"), "{kinds:?}");

    // parameters 應保留 filter 並新增 result_object_key
    let after = jobs(&stack).get(job.id).await.expect("job");
    let params = after.parameters.expect("parameters");
    assert!(params.get("filter").is_some(), "filter 應保留：{params}");
    assert_eq!(
        params["result_object_key"],
        json!(stix_adapter::export_result_object_key(job.id))
    );

    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job.id)).await;
}

#[tokio::test]
async fn export_entity_types_filters_output() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let org = make_entity(
        EntityType::Organization,
        &format!("Filter Org {}", Uuid::now_v7()),
        now,
    );
    let actor = make_entity(
        EntityType::ThreatActor,
        &format!("Filter Actor {}", Uuid::now_v7()),
        now,
    );
    let domain = make_entity(
        EntityType::Domain,
        &format!("Filter Domain {}.example", Uuid::now_v7()),
        now,
    );
    for e in [&org, &actor, &domain] {
        stack.pg.put_entity(e).await.expect("put entity");
    }

    let job = create_export_job(&stack, json!({ "entity_types": ["threat_actor"] })).await;
    let service = worker(&stack);
    service
        .process_dispatched_job(&jobs(&stack), &dispatch_payload(&job))
        .await;
    let bundle = fetch_export_bundle(&stack, job.id).await;
    let objects = bundle_objects(&bundle);
    // 這個情境沒有 entity_ids，走全表掃描，共用的開發用 Postgres 上可能還有
    // 其他測試留下的 threat_actor——不能斷言總數，只能斷言「型別只有
    // threat-actor」且「這次造的 actor 有進去、org／domain 沒有」。
    assert!(
        objects.iter().all(|o| o["type"] == "threat-actor"),
        "entity_types 過濾後不該有其他型別：{objects:#?}"
    );
    let names: Vec<&str> = objects.iter().filter_map(|o| o["name"].as_str()).collect();
    assert!(
        names.contains(&actor.name.as_str()),
        "應含這次造的 actor：{names:?}"
    );
    assert!(!names.contains(&org.name.as_str()), "不該含 org：{names:?}");
    assert!(
        !names.contains(&domain.name.as_str()),
        "不該含 domain：{names:?}"
    );

    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job.id)).await;
}

#[tokio::test]
async fn export_depth_expands_one_level_then_two() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let a = make_entity(
        EntityType::Person,
        &format!("Depth A {}", Uuid::now_v7()),
        now,
    );
    let b = make_entity(
        EntityType::Person,
        &format!("Depth B {}", Uuid::now_v7()),
        now,
    );
    let c = make_entity(
        EntityType::Person,
        &format!("Depth C {}", Uuid::now_v7()),
        now,
    );
    for e in [&a, &b, &c] {
        stack.pg.put_entity(e).await.expect("put entity");
    }
    // A→B→C 鏈
    stack
        .pg
        .put_relationship(&make_relationship(
            &a.id,
            RelationshipType::Mentions,
            &b.id,
            now,
        ))
        .await
        .expect("rel a-b");
    stack
        .pg
        .put_relationship(&make_relationship(
            &b.id,
            RelationshipType::Mentions,
            &c.id,
            now,
        ))
        .await
        .expect("rel b-c");

    // depth=1：只有 A、B 與 A-B 的關係，不含 C
    let job1 = create_export_job(&stack, json!({ "entity_ids": [a.id], "depth": 1 })).await;
    service_process(&stack, &job1).await;
    let bundle1 = fetch_export_bundle(&stack, job1.id).await;
    let names1: Vec<String> = bundle_objects(&bundle1)
        .iter()
        .filter(|o| o["type"] != "relationship")
        .map(|o| o["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(names1.contains(&a.name), "深度1含 A：{names1:?}");
    assert!(names1.contains(&b.name), "深度1含 B：{names1:?}");
    assert!(!names1.contains(&c.name), "深度1不含 C：{names1:?}");
    assert!(
        bundle_objects(&bundle1)
            .iter()
            .any(|o| o["type"] == "relationship"),
        "深度1含 A-B 關係：{bundle1:#?}"
    );

    // depth=2：含 C
    let job2 = create_export_job(&stack, json!({ "entity_ids": [a.id], "depth": 2 })).await;
    service_process(&stack, &job2).await;
    let bundle2 = fetch_export_bundle(&stack, job2.id).await;
    let names2: Vec<String> = bundle_objects(&bundle2)
        .iter()
        .filter(|o| o["type"] != "relationship")
        .map(|o| o["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(names2.contains(&c.name), "深度2含 C：{names2:?}");

    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job1.id)).await;
    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job2.id)).await;
}

#[tokio::test]
async fn export_excludes_merged_entities() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let a = make_entity(
        EntityType::Organization,
        &format!("Merged Survivor {}", Uuid::now_v7()),
        now,
    );
    let b = make_entity(
        EntityType::Organization,
        &format!("Merged Away {}", Uuid::now_v7()),
        now,
    );
    // 把 B 標記成被 A 併掉
    let mut b = b;
    b.merged_into = Some(a.id);
    stack.pg.put_entity(&a).await.expect("entity a");
    stack.pg.put_entity(&b).await.expect("entity b");

    let job = create_export_job(&stack, json!({ "entity_ids": [a.id, b.id] })).await;
    service_process(&stack, &job).await;
    let bundle = fetch_export_bundle(&stack, job.id).await;
    let names: Vec<String> = bundle_objects(&bundle)
        .iter()
        .map(|o| o["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(names.contains(&a.name), "survivor 要匯出：{names:?}");
    assert!(!names.contains(&b.name), "被併掉的 B 不該匯出：{names:?}");

    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job.id)).await;
}

#[tokio::test]
async fn export_missing_entity_id_is_skipped_and_job_completes() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let a = make_entity(
        EntityType::Domain,
        &format!("Real Domain {}.example", Uuid::now_v7()),
        now,
    );
    stack.pg.put_entity(&a).await.expect("entity a");
    let ghost = Uuid::now_v7();

    let job = create_export_job(&stack, json!({ "entity_ids": [a.id, ghost] })).await;
    let service = worker(&stack);
    let outcome = service
        .process_dispatched_job(&jobs(&stack), &dispatch_payload(&job))
        .await;
    assert_eq!(outcome, JobDispatchOutcome::Completed { job_id: job.id });
    let bundle = fetch_export_bundle(&stack, job.id).await;
    let objects = bundle_objects(&bundle);
    assert_eq!(objects.len(), 1, "只有真實的 A：{objects:#?}");
    assert_eq!(objects[0]["type"], "domain-name");

    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job.id)).await;
}

#[tokio::test]
async fn export_time_range_filters_relationship() {
    let stack = connect_stack().await;
    let dawn = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let a = make_entity(
        EntityType::Person,
        &format!("TR A {}", Uuid::now_v7()),
        dawn,
    );
    let b = make_entity(
        EntityType::Organization,
        &format!("TR B {}", Uuid::now_v7()),
        dawn,
    );
    for e in [&a, &b] {
        stack.pg.put_entity(e).await.expect("put entity");
    }
    // 窗內（2 月）與窗外（12 月）各一條
    let in_rel = make_relationship(
        &a.id,
        RelationshipType::BelongsTo,
        &b.id,
        Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap(),
    );
    let out_rel = make_relationship(
        &a.id,
        RelationshipType::Targets,
        &b.id,
        Utc.with_ymd_and_hms(2026, 12, 1, 0, 0, 0).unwrap(),
    );
    stack.pg.put_relationship(&in_rel).await.expect("in rel");
    stack.pg.put_relationship(&out_rel).await.expect("out rel");

    // entity_ids 指定兩端，time_range 只罩 2026-01-15..2026-03-15
    let job = create_export_job(
        &stack,
        json!({
            "entity_ids": [a.id, b.id],
            "time_range": {
                "from": "2026-01-15T00:00:00Z",
                "to": "2026-03-15T00:00:00Z"
            }
        }),
    )
    .await;
    service_process(&stack, &job).await;
    let bundle = fetch_export_bundle(&stack, job.id).await;
    let rels: Vec<&Value> = bundle_objects(&bundle)
        .iter()
        .filter(|o| o["type"] == "relationship")
        .collect();
    assert_eq!(rels.len(), 1, "只該有窗內的關係：{rels:#?}");
    assert_eq!(rels[0]["relationship_type"], "belongs-to");

    cleanup_s3(&stack, &stix_adapter::export_result_object_key(job.id)).await;
}

#[tokio::test]
async fn export_exceeds_max_export_objects_is_failed() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let a = make_entity(
        EntityType::Organization,
        &format!("MO A {}", Uuid::now_v7()),
        now,
    );
    let b = make_entity(
        EntityType::Organization,
        &format!("MO B {}", Uuid::now_v7()),
        now,
    );
    let c = make_entity(
        EntityType::Organization,
        &format!("MO C {}", Uuid::now_v7()),
        now,
    );
    for e in [&a, &b, &c] {
        stack.pg.put_entity(e).await.expect("put entity");
    }
    // max_export_objects = 1，掃描會命中 3 個
    let service = worker_with_max_export(&stack, 1);
    let job = create_export_job(&stack, json!({})).await;
    let outcome = service
        .process_dispatched_job(&jobs(&stack), &dispatch_payload(&job))
        .await;
    assert_eq!(outcome, JobDispatchOutcome::Failed { job_id: job.id });
    let failed = jobs(&stack).get(job.id).await.expect("job");
    assert_eq!(failed.status, JobStatus::Failed);
    let err = failed.error.expect("error");
    assert!(
        err.contains("max_objects") || err.contains("上限"),
        "錯誤訊息應指出 max_objects：{err}"
    );
}

async fn service_process(stack: &Stack, job: &Job) {
    let service = worker(stack);
    let outcome = service
        .process_dispatched_job(&jobs(stack), &dispatch_payload(job))
        .await;
    assert!(
        matches!(outcome, JobDispatchOutcome::Completed { .. }),
        "{outcome:?}"
    );
}
