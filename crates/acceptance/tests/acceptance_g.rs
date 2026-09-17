//! SPEC_V0.2 §23 Acceptance G：**Import → Core → Export 不遺失主要 semantic relationship**。
//! （SPEC_V0.1 §26 只有 A-F，沒有 G——這裡標的一直是 V0.2 §23 的驗收項目，
//! 早先的檔頭寫成「SPEC §26」是筆誤。）
//!
//! 這支測試把 STIX 2.1 匯出／匯入**完整走一遍真正的 HTTP**：
//!
//! 1. `POST /api/v1/import/stix`（operator）→ 202 + `stix_import` Job
//! 2. 直接呼叫 [`stix_worker::StixWorker::process_dispatched_job`]
//!    （不接 Kafka，模擬 worker 剛好消費到這則 job.dispatched）把 bundle 寫進 Core
//! 3. 反查 Entity（Identity→Organization、ThreatActor）確認真的寫進去了
//! 4. `POST /api/v1/export/stix`（operator）→ 202 + `stix_export` Job
//! 5. 用同一個 worker 匯出成 bundle，`GET /jobs/{id}/result`（viewer）讀回來
//! 6. **關鍵斷言**：匯出 bundle 裡的那條 `attributed-to` Relationship，
//!    其 `source_ref`／`target_ref` 真的指向匯出 bundle 裡對應的 threat-actor／identity
//!    物件（不是只斷言「存在一條 attributed-to 關係」）——這才是
//!    「不遺失主要 semantic relationship」的實質意義。
//!
//! 需要本機 Docker：Postgres／MinIO。不打外網。
//!
//! # 為什麼命名分段跨越整個 HTTP 邊界
//!
//! `stix-worker/tests/e2e.rs` 已經各自驗過 import 端與 export 端單邊的行為，
//! 但沒有一支測試把「HTTP 匯入 → worker 落地 → Core → worker 匯出 → HTTP 讀回」
//! 串成一整圈。`raw_evidence_id`、Job.parameters、Entity 決定的 id（UUID v5）、
//! Relationship 兩端的 STIX id——任一個環節接錯，都會在這支測試的關鍵斷言上炸開。

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use common::*;
use connector_sdk::StoreEvidenceSink;
use core_api::{AppState, AuthState, ImportState, RateLimiter, ready_always, router};
use core_events::EventProducer;
use core_jobs::JobService;
use core_model::{EntityType, Job, JobStatus, Source, SourceType};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use resolver::AutoApprovalConfig;
use serde_json::{Value, json};
use stix_worker::{JobDispatchOutcome, StixWorker, StixWorkerOptions};
use storage_core::mock::MockEmbeddingProvider;
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tower::ServiceExt;
use uuid::Uuid;

/// 沖出這條 attributed-to 邊的 run 標記。所有 Entity／Relationship 名稱都含它，
/// 只命中這次 run 的資料，不會比對到前幾次測試留下的 Entity。
#[tokio::test]
async fn acceptance_g_import_core_export_round_trip() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let org_name = format!("AccG-Org-{run}");
    let actor_name = format!("AccG-Actor-{run}");

    // ---- 1. STIX import 專用 Source（enabled）----
    let source = seed_stix_source(&stack.pg).await;

    // ---- 2. 組 bundle：Identity(organization) + ThreatActor + attributed-to ----
    let org_uuid = Uuid::now_v7();
    let actor_uuid = Uuid::now_v7();
    let rel_uuid = Uuid::now_v7();
    let bundle = json!({
        "type": "bundle",
        "id": format!("bundle--{}", Uuid::now_v7()),
        "objects": [
            {
                "type": "identity",
                "spec_version": "2.1",
                "id": format!("identity--{org_uuid}"),
                "identity_class": "organization",
                "name": org_name,
            },
            {
                "type": "threat-actor",
                "spec_version": "2.1",
                "id": format!("threat-actor--{actor_uuid}"),
                "name": actor_name,
            },
            {
                "type": "relationship",
                "spec_version": "2.1",
                "id": format!("relationship--{rel_uuid}"),
                "relationship_type": "attributed-to",
                "source_ref": format!("threat-actor--{actor_uuid}"),
                "target_ref": format!("identity--{org_uuid}"),
            }
        ]
    });

    // ---- 3. 起 operator／viewer token 的測試 API ----
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "acceptance-g").expect("producer"));
    let api = build_api(&stack, producer);

    // ---- 4. 走真正的 HTTP 匯入 ----
    let (status, body) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/import/stix",
            &api.operator,
            &json!({"source_id": source.id, "bundle": bundle}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["type"], "stix_import");
    assert_eq!(body["status"], "queued");
    let import_job = stack
        .pg
        .get_job(parse_uuid(&body["id"]))
        .await
        .expect("get job")
        .expect("stix_import Job 應寫進 Postgres");

    // ---- 5. 直接跑 worker（不接 Kafka）處理這則 job.dispatched ----
    let service = build_worker(&stack);
    let jobs = JobService::new(stack.pg.clone(), None);
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&import_job))
        .await;
    assert_eq!(
        outcome,
        JobDispatchOutcome::Completed {
            job_id: import_job.id
        },
        "匯入 job 應 Completed"
    );
    // 確認匯入真的轉成 Completed（不是 worker 說 done 但 job 卡住／失敗）。
    let stored_import = jobs.get(import_job.id).await.expect("job");
    assert_eq!(stored_import.status, JobStatus::Completed);

    // ---- 6. 反查兩筆 Entity，確認匯入真的寫進 Core ----
    let org = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Organization, &normalize(&org_name))
        .await
        .expect("query org")
        .unwrap_or_else(|| panic!("匯入後找不到 Organization `{org_name}`——匯入沒寫進預期資料"));
    let actor = stack
        .pg
        .find_entity_by_normalized_name(EntityType::ThreatActor, &normalize(&actor_name))
        .await
        .expect("query actor")
        .unwrap_or_else(|| panic!("匯入後找不到 ThreatActor `{actor_name}`——匯入沒寫進預期資料"));

    // ---- 7. 走真正的 HTTP 匯出，指定這兩筆 Entity ----
    let (status, body) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/export/stix",
            &api.operator,
            &json!({"filter": {"entity_ids": [org.id, actor.id]}}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["type"], "stix_export");
    assert_eq!(body["status"], "queued");
    let export_job = stack
        .pg
        .get_job(parse_uuid(&body["id"]))
        .await
        .expect("get job")
        .expect("stix_export Job 應寫進 Postgres");

    // ---- 8. 用同一個 worker 處理 export job ----
    let outcome = service
        .process_dispatched_job(&jobs, &dispatch_payload(&export_job))
        .await;
    assert_eq!(
        outcome,
        JobDispatchOutcome::Completed {
            job_id: export_job.id
        },
        "匯出 job 應 Completed"
    );

    // ---- 9. operator 匯出後，viewer 走 HTTP 讀回 bundle ----
    let (status, exported) = send(
        &api.app,
        get(
            &format!("/api/v1/jobs/{}/result", export_job.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{exported}");
    assert_eq!(exported["type"], "bundle");

    // ---- 10. 關鍵斷言：主要 semantic relationship 沒遺失 ----
    let objects = exported["objects"].as_array().expect("objects 必須是陣列");

    // 匯出端會為 Core Entity 重新組 STIX id（compose_stix_id），所以必須
    // 用 name 反查這次匯出 bundle 裡的 identity／threat-actor 物件，再拿它們
    // 的 id 去對 relationship 的兩端——不能直接用 import 時的原生 STIX id。
    let identity_obj = objects
        .iter()
        .find(|o| o["type"] == "identity" && o["name"] == org_name.as_str())
        .unwrap_or_else(|| panic!("匯出 bundle 缺 identity `{org_name}`：{objects:#?}"));
    let identity_id = identity_obj["id"].as_str().expect("identity id");

    let actor_obj = objects
        .iter()
        .find(|o| o["type"] == "threat-actor" && o["name"] == actor_name.as_str())
        .unwrap_or_else(|| panic!("匯出 bundle 缺 threat-actor `{actor_name}`：{objects:#?}"));
    let actor_id = actor_obj["id"].as_str().expect("actor id");

    // 找到 this attributed-to relationship，並證明它的 source_ref／target_ref
    // 真的指向上面那兩個物件的 id（不是只斷言「有一條 attributed-to 存在」）。
    let rel = objects
        .iter()
        .find(|o| o["type"] == "relationship" && o["relationship_type"] == "attributed-to");
    let Some(rel) = rel else {
        panic!("匯出 bundle 缺 attributed-to relationship：{objects:#?}");
    };
    assert_eq!(
        rel["source_ref"].as_str(),
        Some(actor_id),
        "attributed-to 的 source_ref 應指向 threat-actor 物件的 id：{rel:#?}"
    );
    assert_eq!(
        rel["target_ref"].as_str(),
        Some(identity_id),
        "attributed-to 的 target_ref 應指向 identity 物件的 id：{rel:#?}"
    );
    assert_ne!(
        actor_id, identity_id,
        "threat-actor 與 identity 的 id 不該相同——source/target 都等於同一個 id 的斷言是自欺欺人"
    );

    // ---- 11. 清理 ----
    let _ = stack
        .s3
        .delete(&stix_adapter::export_result_object_key(export_job.id))
        .await;
}

// ---------------------------------------------------------------------------
// 測試自己的小工具
// ---------------------------------------------------------------------------

/// STIX import 專用的 Source，欄位齊全、enabled。
pub async fn seed_stix_source(pg: &PostgresCanonicalStore) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("acceptance-g-{}", Uuid::now_v7()),
        source_type: SourceType::StixImport,
        platform: Some("acceptance-g".into()),
        base_url: None,
        description: Some("acceptance g fixture".into()),
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

/// 從 `body["id"]` 的 JSON 字串解出 UUID。
fn parse_uuid(v: &Value) -> Uuid {
    Uuid::parse_str(v.as_str().expect("uuid 字串")).expect("合法 UUID")
}

fn normalize(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

// ---------- core-api 測試 app（比照 stix_api_e2e.rs::build_api）----------

struct TestApi {
    app: Router,
    viewer: String,
    operator: String,
}

fn build_api(stack: &Stack, producer: Arc<EventProducer>) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let viewer = jwt.issue("acc-g-viewer", Role::Viewer).expect("issue");
    let operator = jwt.issue("acc-g-operator", Role::Operator).expect("issue");
    let sink = StoreEvidenceSink::new(stack.pg.clone(), stack.s3.clone());
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        store: Some(Arc::new(stack.pg.clone())),
        objects: Some(Arc::new(stack.s3.clone())),
        jobs: Some(Arc::new(JobService::new(
            stack.pg.clone(),
            Some(producer.clone()),
        ))),
        merge: None,
        resolver: None,
        auto_approval: None,
        graph_resolver: None,
        graph: None,
        graph_projection: None,
        import: Some(Arc::new(ImportState {
            store: Arc::new(stack.pg.clone()),
            sink: Arc::new(sink),
            producer: Some(producer),
        })),
        search: None,
        semantic_search: None,
        hybrid_weights: core_config::HybridSearchSection::default(),
        ready: ready_always(),
        backends: core_api::ReadyProbe::new(Vec::new()),
        queues: None,
        backends_missing: Vec::new(),
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config: core_config::ImportSection::default(),
        stix_config: core_config::StixSection::default(),
        object_bucket: String::new(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        viewer,
        operator,
    }
}

fn with_peer(mut request: Request<Body>) -> Request<Body> {
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
        4_242,
    )));
    request
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.expect("call api");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

fn json_request(method: &str, uri: &str, token: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

// ---------- stix-worker（比照 stix-worker/tests/e2e.rs 的 worker）----------

type TestWorker = StixWorker<
    PostgresCanonicalStore,
    MockEmbeddingProvider,
    ai_gateway::MockLlmProvider,
    S3ObjectStore,
>;

/// 驗「匯入匯出」，不是驗重複判定：auto_approval 整個關掉，
/// 不讓 resolver／LLM 路徑進入這次測試。
fn build_worker(stack: &Stack) -> TestWorker {
    StixWorker::new(
        stack.pg.clone(),
        stack.s3.clone(),
        MockEmbeddingProvider::unsupported(),
        ai_gateway::MockLlmProvider::always_same_entity(true),
        None,
        MetricsRegistry::new(),
        StixWorkerOptions {
            max_objects_per_tx: 10_000,
            max_export_objects: 10_000,
            max_auto_merges_per_resolve: 0,
            auto_approval: AutoApprovalConfig {
                enabled: false,
                auto_confirm_score: 0.95,
                llm_review_score: 0.70,
                llm_model: "unused".into(),
                llm_temperature: 0.0,
                llm_max_tokens: 16,
            },
        },
    )
}

/// 組 worker 用的 `job.dispatched` payload（比照 stix-worker/tests/e2e.rs）。
fn dispatch_payload(job: &Job) -> Value {
    json!({
        "job_id": job.id,
        "job_type": job.job_type,
        "status": job.status,
        "retry_count": job.retry_count,
        "parameters": job.parameters,
    })
}
