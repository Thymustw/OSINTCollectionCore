//! Discovery 資料層 REST endpoint 的 e2e 測試（SPEC_V0.3 §17 Seed／Candidate／AI Run）。
//!
//! 只連本機 Docker Postgres（`STORAGE_*` 那批 storage 層 conformance 測試已驗證過
//! 底層行為，這裡驗的是「真的打 HTTP 走 router」的那一層）。建置方式比照
//! `tests/timeline_api_e2e.rs`——這份沒有 `mod common;`，因為 CI 的共用 harness
//! 沒有連 canonical store。
//!
//! Candidate／AiRun 沒有建立 endpoint，fixture 直接走 storage 層 `put_candidate`／
//! `put_ai_run` 塞進去（跟 `POST /api/v1/seeds` 只建立 Seed 的設計一致）。

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use core_api::{AppState, AuthState, RateLimiter, ready_always, router};
use core_config::ImportSection;
use core_model::{AiRun, Candidate, CandidateEvidence, CandidateStatus, CandidateType, Collection};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use tower::ServiceExt;
use uuid::Uuid;

const MAX_BODY_BYTES: u64 = 4_096;

struct Stack {
    pg: PostgresCanonicalStore,
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
    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");
    Stack { pg }
}

struct TestApi {
    app: Router,
    viewer: String,
    operator: String,
    audit: MemoryAuditLog,
}

fn build_api(stack: &Stack) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let viewer = jwt.issue("e2e-ds-viewer", Role::Viewer).expect("issue");
    let operator = jwt.issue("e2e-ds-operator", Role::Operator).expect("issue");
    let audit = MemoryAuditLog::new();
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(audit.clone()),
        store: Some(Arc::new(stack.pg.clone())),
        objects: None,
        jobs: None,
        merge: None,
        resolver: None,
        auto_approval: None,
        graph_resolver: None,
        graph: None,
        graph_projection: None,
        import: None,
        search: None,
        semantic_search: None,
        hybrid_weights: core_config::HybridSearchSection::default(),
        ready: ready_always(),
        backends: core_api::ReadyProbe::new(Vec::new()),
        queues: None,
        backends_missing: Vec::new(),
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config: ImportSection {
            max_upload_bytes: MAX_BODY_BYTES,
            ..ImportSection::default()
        },
        stix_config: core_config::StixSection::default(),
        object_bucket: "raw-evidence".into(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        viewer,
        operator,
        audit,
    }
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

fn get(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn post_json(uri: &str, token: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

// ---------------------------------------------------------------- fixtures

async fn put_candidate_fixture(stack: &Stack, collection_id: Option<Uuid>) -> Candidate {
    let now = Utc::now();
    let candidate = Candidate {
        id: Uuid::now_v7(),
        candidate_type: CandidateType::Account,
        value: format!("ds-e2e-candidate-{}", Uuid::now_v7().simple()),
        normalized_value: "ds-e2e-candidate".into(),
        collection_id,
        discovered_by: "ds-e2e-seed".into(),
        discovery_method: "account_expansion".into(),
        confidence: 0.85,
        score: 0.9,
        status: CandidateStatus::Pending,
        depth: 1,
        created_at: now,
        reviewed_at: None,
    };
    stack
        .pg
        .put_candidate(&candidate)
        .await
        .expect("seed candidate");
    candidate
}

async fn put_evidence_fixture(stack: &Stack, candidate_id: Uuid) -> CandidateEvidence {
    let evidence = CandidateEvidence {
        id: Uuid::now_v7(),
        candidate_id,
        object_id: None,
        entity_id: None,
        relationship_id: None,
        raw_evidence_id: None,
        reason: format!("ds-e2e reason via seed {}", Uuid::now_v7().simple()),
        weight: 0.8,
        created_at: Utc::now(),
    };
    stack
        .pg
        .put_candidate_evidence(&evidence)
        .await
        .expect("seed evidence");
    evidence
}

async fn put_ai_run_fixture(stack: &Stack, task_type: &str) -> AiRun {
    let run = AiRun {
        id: Uuid::now_v7(),
        task_type: task_type.into(),
        provider: "llamacpp".into(),
        model: "Qwen3.8-27B-UD-Q4_K_XL".into(),
        model_version: "1".into(),
        prompt_version: "v1".into(),
        input_reference: json!({ "seed_id": Uuid::now_v7().to_string() }),
        output: json!({ "score": 0.87 }),
        confidence: 0.7,
        tokens: 120,
        estimated_cost: 0.0012,
        duration_ms: 850,
        created_at: Utc::now(),
    };
    stack.pg.put_ai_run(&run).await.expect("seed ai run");
    run
}

// ---------------------------------------------------------------- seeds

#[tokio::test]
async fn create_seed_returns_201_with_full_seed() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let body = json!({
        "seed_type": "keyword",
        "value": "APT-42",
        "origin": "manual",
    });
    let (status, resp) = send(&api.app, post_json("/api/v1/seeds", &api.operator, &body)).await;
    assert_eq!(status, StatusCode::CREATED, "{resp}");
    assert!(resp["id"].as_str().is_some(), "{resp}");
    assert_eq!(resp["seed_type"], json!("keyword"), "{resp}");
    assert_eq!(resp["value"], json!("APT-42"), "{resp}");
    assert_eq!(resp["origin"], json!("manual"), "{resp}");
    // 伺服器補齊的預設值。
    assert_eq!(resp["status"], json!("pending"), "{resp}");
    assert_eq!(resp["priority"], json!(0), "{resp}");
    assert!(resp["created_at"].as_str().is_some(), "{resp}");
    // 稽核：seed.create 成功一筆。
    let entries = api.audit.entries();
    let seed_action = entries
        .iter()
        .find(|e| e.action == core_api::AUDIT_SEED_CREATE)
        .expect("seed.create audit");
    assert_eq!(seed_action.outcome, "success");
    assert_eq!(
        seed_action.resource_id.as_deref(),
        Some(resp["id"].as_str().unwrap())
    );
}

#[tokio::test]
async fn create_seed_with_duplicate_id_is_409() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let id = Uuid::now_v7();
    let body = json!({
        "id": id.to_string(),
        "seed_type": "domain",
        "value": "example.invalid",
        "origin": "manual",
    });
    let (status, _) = send(&api.app, post_json("/api/v1/seeds", &api.operator, &body)).await;
    assert_eq!(status, StatusCode::CREATED);
    // 同一把 id 再建立 → 409。
    let (status2, resp2) = send(&api.app, post_json("/api/v1/seeds", &api.operator, &body)).await;
    assert_eq!(status2, StatusCode::CONFLICT, "{resp2}");
    assert!(
        resp2["message"].as_str().unwrap().contains("已經存在"),
        "{resp2}"
    );
    // 稽核：衝突那筆要記 rejected。
    let entries = api.audit.entries();
    let seed_actions: Vec<_> = entries
        .iter()
        .filter(|e| e.action == core_api::AUDIT_SEED_CREATE)
        .collect();
    assert_eq!(seed_actions.len(), 2);
    assert_eq!(seed_actions[1].outcome, "rejected");
}

#[tokio::test]
async fn list_seeds_filters_by_status() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let body = json!({
        "seed_type": "url",
        "value": "https://example.invalid/news",
        "origin": "connector",
    });
    let (status, _) = send(&api.app, post_json("/api/v1/seeds", &api.operator, &body)).await;
    assert_eq!(status, StatusCode::CREATED);

    let (pending, pending_body) =
        send(&api.app, get("/api/v1/seeds?status=pending", &api.viewer)).await;
    assert_eq!(pending, StatusCode::OK, "{pending_body}");
    let pending_ids = pending_body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["value"].as_str().unwrap())
        .collect::<Vec<&str>>();
    assert!(
        pending_ids.contains(&"https://example.invalid/news"),
        "{pending_body}"
    );

    let (none, none_body) =
        send(&api.app, get("/api/v1/seeds?status=confirmed", &api.viewer)).await;
    assert_eq!(none, StatusCode::OK);
    assert!(
        !none_body["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["value"] == json!("https://example.invalid/news")),
        "{none_body}"
    );
}

// ---------------------------------------------------------------- candidates

#[tokio::test]
async fn approve_candidate_updates_status_and_audits() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let candidate = put_candidate_fixture(&stack, None).await;

    let (status, resp) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", candidate.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["status"], json!("approved"), "{resp}");

    // 再次 GET 確認狀態真的改了。
    let (get_status, get_resp) = send(
        &api.app,
        get(&format!("/api/v1/candidates/{}", candidate.id), &api.viewer),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_resp["status"], json!("approved"), "{get_resp}");

    // 稽核：candidate.approve 一筆 success。
    let entries = api.audit.entries();
    let approve = entries
        .iter()
        .find(|e| e.action == core_api::AUDIT_CANDIDATE_APPROVE)
        .expect("candidate.approve audit");
    assert_eq!(approve.outcome, "success");
    assert_eq!(
        approve.resource_id.as_deref(),
        Some(candidate.id.to_string()).as_deref()
    );
}

#[tokio::test]
async fn approve_missing_candidate_is_404() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let (status, resp) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", Uuid::now_v7()),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{resp}");
    // 稽核：404 那次也要記 rejected。
    let entries = api.audit.entries();
    let approve = entries
        .iter()
        .find(|e| e.action == core_api::AUDIT_CANDIDATE_APPROVE)
        .expect("candidate.approve audit");
    assert_eq!(approve.outcome, "rejected", "{entries:?}");
}

#[tokio::test]
async fn reject_candidate_audits_reject_action() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let candidate = put_candidate_fixture(&stack, None).await;
    let (status, resp) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/reject", candidate.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["status"], json!("rejected"), "{resp}");
    let entries = api.audit.entries();
    let reject = entries
        .iter()
        .find(|e| e.action == core_api::AUDIT_CANDIDATE_REJECT)
        .expect("candidate.reject audit");
    assert_eq!(reject.outcome, "success");
}

#[tokio::test]
async fn get_candidate_detail_includes_evidence() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let candidate = put_candidate_fixture(&stack, None).await;
    let evidence = put_evidence_fixture(&stack, candidate.id).await;

    let (status, resp) = send(
        &api.app,
        get(&format!("/api/v1/candidates/{}", candidate.id), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    // Acceptance C：evidence 回答「why was this discovered」。
    let evidence_ids = resp["evidence"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect::<Vec<&str>>();
    assert!(
        evidence_ids.iter().any(|id| *id == evidence.id.to_string()),
        "{resp}"
    );
    // evidence_truncated：只有一筆證據，不該被截斷。
    assert_eq!(resp["evidence_truncated"], json!(false), "{resp}");
}

#[tokio::test]
async fn collection_discovery_lists_only_that_collection() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    // candidates.collection_id 有外鍵約束，先建一筆真的 Collection 才能引用它。
    let now = Utc::now();
    let collection = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: "ds-e2e-collection".into(),
        description: None,
        status: "active".into(),
        priority: 3,
        created_at: now,
        updated_at: now,
    };
    stack
        .pg
        .put_collection(&collection)
        .await
        .expect("seed collection");
    let collection_id = collection.id;
    let in_collection = put_candidate_fixture(&stack, Some(collection_id)).await;
    let orphan = put_candidate_fixture(&stack, None).await;

    let (status, resp) = send(
        &api.app,
        get(
            &format!("/api/v1/collections/{}/discovery", collection_id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    let ids = resp["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect::<Vec<&str>>();
    assert!(
        ids.iter().any(|id| *id == in_collection.id.to_string()),
        "{resp}"
    );
    assert!(!ids.iter().any(|id| *id == orphan.id.to_string()), "{resp}");
}

// ---------------------------------------------------------------- ai runs

#[tokio::test]
async fn list_and_get_ai_runs() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let scoring = put_ai_run_fixture(&stack, "candidate_scoring").await;
    let summarization = put_ai_run_fixture(&stack, "summarization").await;

    let (list_status, list_resp) = send(
        &api.app,
        get("/api/v1/ai/runs?task_type=candidate_scoring", &api.viewer),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK, "{list_resp}");
    let list_ids = list_resp["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["id"].as_str().unwrap())
        .collect::<Vec<&str>>();
    assert!(
        list_ids.iter().any(|id| *id == scoring.id.to_string()),
        "{list_resp}"
    );
    assert!(
        !list_ids
            .iter()
            .any(|id| *id == summarization.id.to_string()),
        "{list_resp}"
    );

    let (get_status, get_resp) = send(
        &api.app,
        get(&format!("/api/v1/ai/runs/{}", scoring.id), &api.viewer),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK, "{get_resp}");
    assert_eq!(get_resp["id"], json!(scoring.id.to_string()));
    assert_eq!(get_resp["task_type"], json!("candidate_scoring"));
}

// ---------------------------------------------------------------- rbac

#[tokio::test]
async fn viewer_cannot_create_seed() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let body = json!({
        "seed_type": "keyword",
        "value": "viewer-blocked",
        "origin": "manual",
    });
    let (status, resp) = send(&api.app, post_json("/api/v1/seeds", &api.viewer, &body)).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{resp}");
    // 被擋的建立不該寫 success 稽核。
    let entries = api.audit.entries();
    assert!(
        !entries
            .iter()
            .any(|e| e.action == core_api::AUDIT_SEED_CREATE),
        "{entries:?}"
    );
}

/// 一般 POST request（`post_json` 的簡化版，用在沒有 body 的 approve/reject）。
fn post(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}
