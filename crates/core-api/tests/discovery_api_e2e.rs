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
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use core_api::{AppState, AuthState, RateLimiter, envelope_from_outbox, ready_always, router};
use core_config::ImportSection;
use core_events::{EventConsumer, EventProducer};
use core_jobs::JobService;
use core_model::{AiRun, Candidate, CandidateEvidence, CandidateStatus, CandidateType, Collection};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{EventOutboxStore, EventOutboxTestSupport, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use tower::ServiceExt;
use uuid::Uuid;

const MAX_BODY_BYTES: u64 = 4_096;

struct Stack {
    pg: PostgresCanonicalStore,
    dsn: String,
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
    Stack { pg, dsn }
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
        // JobService 用 `None` producer：`dispatch` 對沒有 producer 時只跳過
        // `job.dispatched` 的 publish、照樣回 queued Job——Discovery Run 的
        // e2e 只要驗「建立並派工 Job」這一段，不需要真的起 worker 或 broker。
        jobs: Some(Arc::new(JobService::new(stack.pg.clone(), None))),
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
        auto_approval_max_concurrent: 2,
        object_bucket: "raw-evidence".into(),
        collector: core_config::CollectorSection::default(),
        retention_jobs: core_config::RetentionJobsConfig::default(),
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

/// 讀 `candidate_type: Url` 的 fixture，`value` 由呼叫端指定（approve 前
/// 的 URL 格式驗證只在這種 candidate 上觸發）。
async fn put_url_candidate_fixture(stack: &Stack, value: String) -> Candidate {
    let now = Utc::now();
    let candidate = Candidate {
        id: Uuid::now_v7(),
        candidate_type: CandidateType::Url,
        value: value.clone(),
        normalized_value: value,
        collection_id: None,
        discovered_by: "ds-e2e-seed".into(),
        discovery_method: "link_expansion".into(),
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
        .expect("seed url candidate");
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
    let seed_id = resp["id"].as_str().and_then(|s| Uuid::parse_str(s).ok());
    let _cleanup = seed_id.map(|id| E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).seed(id));
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
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).seed(id);
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
    let (status, resp) = send(&api.app, post_json("/api/v1/seeds", &api.operator, &body)).await;
    let seed_id = resp["id"].as_str().and_then(|s| Uuid::parse_str(s).ok());
    let _cleanup = seed_id.map(|id| E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).seed(id));
    assert_eq!(status, StatusCode::CREATED, "{resp}");

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
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);

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
async fn approve_candidate_twice_does_not_reemit_outbox() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let candidate = put_candidate_fixture(&stack, None).await;
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);

    let (first_status, first) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", candidate.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(first_status, StatusCode::OK, "{first}");
    assert_eq!(first["status"], json!("approved"), "{first}");
    let after_first =
        count_outbox_for_payload(&stack, "candidate.approved", "candidate_id", candidate.id).await;
    assert_eq!(after_first, 1, "第一次核准應寫一列 candidate.approved");

    let (second_status, second) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", candidate.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(second_status, StatusCode::OK, "{second}");
    assert_eq!(second["status"], json!("approved"), "{second}");
    let after_second =
        count_outbox_for_payload(&stack, "candidate.approved", "candidate_id", candidate.id).await;
    assert_eq!(after_second, 1, "重複核准不可再寫 candidate.approved");
}

/// 真實 Redpanda：建立 seed、核准 candidate，郵差送出後從正式 topic 消費。
/// 測完刪自己的 outbox 列；正式 V0.3 topic 留下。
#[tokio::test]
async fn outbox_relay_publishes_seed_created_and_candidate_approved() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let brokers = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        brokers.contains("127.0.0.1") || brokers.contains("localhost"),
        "e2e 只連本機 Redpanda，實際 brokers={brokers}"
    );

    let seed_value = format!("outbox-e2e-{}", Uuid::now_v7().simple());
    let (seed_status, seed_resp) = send(
        &api.app,
        post_json(
            "/api/v1/seeds",
            &api.operator,
            &json!({
                "seed_type": "keyword",
                "value": seed_value,
                "origin": "manual",
            }),
        ),
    )
    .await;
    assert_eq!(seed_status, StatusCode::CREATED, "{seed_resp}");
    let seed_id = Uuid::parse_str(seed_resp["id"].as_str().expect("seed id")).expect("uuid");
    let mut cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).seed(seed_id);

    let candidate = put_candidate_fixture(&stack, None).await;
    cleanup.track_candidate(candidate.id);
    let _cleanup = cleanup;
    let (approve_status, approve_resp) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", candidate.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(approve_status, StatusCode::OK, "{approve_resp}");

    let group = format!("osint-outbox-e2e-{}", Uuid::now_v7());
    let consumer =
        EventConsumer::connect(&brokers, &group, &["seed.created", "candidate.approved"])
            .expect("建立 consumer。請確認 osint-core-redpanda-1 在跑（埠 9092）");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer = EventProducer::connect(&brokers, "core-api-outbox-e2e")
        .expect("建立 producer。請確認 Redpanda healthy");
    relay_own_outbox_rows(&stack, &producer, &[seed_id, candidate.id]).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let seed_id_text = seed_id.to_string();
    let candidate_id_text = candidate.id.to_string();
    let mut saw_seed = false;
    let mut saw_approved = false;
    while tokio::time::Instant::now() < deadline && !(saw_seed && saw_approved) {
        if let Ok(envelope) = consumer.next_envelope(Duration::from_secs(2)).await {
            if envelope.event_type == "seed.created"
                && envelope.payload.get("seed_id").and_then(Value::as_str)
                    == Some(seed_id_text.as_str())
            {
                saw_seed = true;
            }
            if envelope.event_type == "candidate.approved"
                && envelope.payload.get("candidate_id").and_then(Value::as_str)
                    == Some(candidate_id_text.as_str())
            {
                saw_approved = true;
            }
        }
    }
    assert!(
        saw_seed,
        "30 秒內沒收到 seed.created（seed_id={seed_id}）。請確認 Redpanda 與 event_outbox 列"
    );
    assert!(
        saw_approved,
        "30 秒內沒收到 candidate.approved（candidate_id={}）。請確認 Redpanda 與 event_outbox 列",
        candidate.id
    );

    drop(consumer);
}

/// 打不存在的 id：沒有 candidate 列、handler 在 enqueue 前就 404，不寫 outbox。
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
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);
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

// §26「unsafe model-generated URL/action」：approve 一筆 Url candidate 之前，
// value 必須能被 `url_norm::canonicalize` 正規化成絕對 URL，否則 400 拒絕。
// reject 不受這道驗證影響——拒絕一個格式不合格的 candidate 永遠放行。
#[tokio::test]
async fn approve_url_candidate_with_non_absolute_value_is_400() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    // 沒有 scheme、無法解析成絕對 URL——`canonicalize` 回 `None` → 400。
    // （`javascript:` 這類 opaque URL 是第二道 scheme 白名單擋的，見下面
    // `approve_url_candidate_with_non_http_scheme_is_400`。）
    let candidate = put_url_candidate_fixture(&stack, "path-without-scheme".into()).await;
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);

    let (status, resp) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", candidate.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{resp}");
    assert!(
        resp["message"].as_str().unwrap().contains("不是合法 URL"),
        "{resp}"
    );

    // 被拒後狀態不該被改動，仍是 pending。
    let (get_status, get_resp) = send(
        &api.app,
        get(&format!("/api/v1/candidates/{}", candidate.id), &api.viewer),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(get_resp["status"], json!("pending"), "{get_resp}");
}

#[tokio::test]
async fn approve_url_candidate_with_valid_absolute_url_succeeds() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let candidate = put_url_candidate_fixture(&stack, "https://example.com/path".into()).await;
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);

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
}

/// `url` crate 把這些都當成合法 URL（canonicalize 回 `Some`），所以只靠格式檢查
/// 會被核准。scheme 白名單（只收 http/https）必須把它們全部擋成 400、狀態不變。
#[tokio::test]
async fn approve_url_candidate_with_non_http_scheme_is_400() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let mut cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone());
    for value in [
        "javascript:alert(1)",
        "data:text/html,x",
        "mailto:a@b.c",
        "ftp://x",
    ] {
        let candidate = put_url_candidate_fixture(&stack, value.into()).await;
        cleanup.track_candidate(candidate.id);
        let (status, resp) = send(
            &api.app,
            post(
                &format!("/api/v1/candidates/{}/approve", candidate.id),
                &api.operator,
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "`{value}`：{resp}");
        assert!(
            resp["message"]
                .as_str()
                .is_some_and(|m| m.contains("http/https")),
            "訊息要講清楚只接受 http/https（`{value}`）：{resp}"
        );
        let (_, get_resp) = send(
            &api.app,
            get(&format!("/api/v1/candidates/{}", candidate.id), &api.viewer),
        )
        .await;
        assert_eq!(
            get_resp["status"],
            json!("pending"),
            "被拒後不可改狀態（`{value}`）：{get_resp}"
        );
    }
    drop(cleanup);
}

#[tokio::test]
async fn reject_url_candidate_ignores_url_validation() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    // 跟前一筆 approve 測試相同的不合法 URL：reject 必須照樣成功，不被擋住。
    let candidate = put_url_candidate_fixture(&stack, "path-without-scheme".into()).await;
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);

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
}

#[tokio::test]
async fn get_candidate_detail_includes_evidence() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let candidate = put_candidate_fixture(&stack, None).await;
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(candidate.id);
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
    let _cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone())
        .candidate(in_collection.id)
        .candidate(orphan.id);

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

/// 打不存在的 collection：沒有 candidate／outbox 列可刪。
#[tokio::test]
async fn collection_discovery_missing_collection_is_404() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let missing = Uuid::now_v7();
    let (status, resp) = send(
        &api.app,
        get(
            &format!("/api/v1/collections/{missing}/discovery"),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{resp}");
    assert!(
        resp["message"]
            .as_str()
            .is_some_and(|m| m.contains("找不到 Collection")),
        "打錯 id 不該看起來像「這個調查還沒有候選」：{resp}"
    );
}

// ---------------------------------------------------------------- ai runs

/// 直接 `put_ai_run`，不經 8 個 send point，不寫 outbox。ai_runs 列不在本次清理範圍。
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

// ---------------------------------------------------------------- ops discovery

/// `GET /ops/discovery` 回設定值、backlog 近似計數、最近 AI Run。
/// viewer 即可讀（Read 權限，不是 Write）。
#[tokio::test]
async fn ops_discovery_returns_backlog_and_recent_ai_runs() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let pending = put_candidate_fixture(&stack, None).await;
    let mut cleanup = E2eDiscoveryRowsOnDrop::from_dsn(stack.dsn.clone()).candidate(pending.id);
    let (approve_status, _) = send(
        &api.app,
        post(
            &format!("/api/v1/candidates/{}/approve", pending.id),
            &api.operator,
        ),
    )
    .await;
    assert_eq!(approve_status, StatusCode::OK);
    let still_pending = put_candidate_fixture(&stack, None).await;
    cleanup.track_candidate(still_pending.id);
    let run = put_ai_run_fixture(&stack, "candidate_scoring").await;

    let (status, resp) = send(&api.app, get("/api/v1/ops/discovery", &api.viewer)).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert_eq!(resp["ai_max_concurrent_requests"], json!(2), "{resp}");
    assert!(
        resp["candidate_backlog"]["pending"].as_u64().unwrap() >= 1,
        "剛塞的 pending candidate {} 應被計入：{resp}",
        still_pending.id
    );
    assert!(
        resp["candidate_backlog"]["approved"].as_u64().unwrap() >= 1,
        "剛核准的 candidate {} 應被計入：{resp}",
        pending.id
    );
    // 不能斷言 saturated == false：candidates 是共用開發 DB 上的全域計數，
    // 每跑一次這個檔就累積十幾筆 pending，跑幾次就必然碰到 100 的上限
    // （2026-10-15 實測 pending 185 筆時這裡確定性失敗）。改斷言 saturated
    // 與計數一致：任何一個狀態數到上限 100 時必須亮，否則不能亮。
    let backlog = &resp["candidate_backlog"];
    let any_at_cap = [
        "pending",
        "approved",
        "auto_approved",
        "rejected",
        "expired",
    ]
    .iter()
    .any(|k| backlog[*k].as_u64() == Some(100));
    assert_eq!(backlog["saturated"], json!(any_at_cap), "{resp}");
    let recent_ids: Vec<&str> = resp["ai_run_recent"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert!(
        recent_ids.iter().any(|id| *id == run.id.to_string()),
        "剛寫入的 AI Run {} 應出現在最近歷史：{resp}",
        run.id
    );
    let note = resp["note"].as_str().unwrap();
    assert!(
        note.contains("ObservedPressure"),
        "note 要講清楚 Admission／壓力訊號已落地、這個端點仍無佇列快照：{resp}"
    );
    assert!(note.contains("P0"), "note 要講 P0 刻意不看壓力訊號：{resp}");
    assert!(
        resp["collection_daily_usage"].is_array(),
        "今天有用量的 collection 清單必須存在：{resp}"
    );
    assert!(
        resp["collection_daily_usage_truncated"].is_boolean(),
        "截斷旗標必須存在：{resp}"
    );
    assert!(
        resp["event_outbox_pending"].as_u64().is_some(),
        "event_outbox 待送列數必須存在：{resp}"
    );
    assert!(
        note.contains("event_outbox"),
        "note 要提到 event_outbox 待送列數：{resp}"
    );
    drop(cleanup);
}

/// `GET /ops/discovery` 列出今天有用量的 collection。測完刪自己寫的列，
/// 避免污染共用開發 DB（今天的用量會出現在後續呼叫裡）。
#[tokio::test]
async fn ops_discovery_lists_today_usage_then_cleans_up() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let now = Utc::now();
    let collection = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: "ds-e2e-daily-usage".into(),
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
    let today = now.date_naive();
    let ai = stack
        .pg
        .try_consume_daily_ai_budget(collection.id, today, 3, 500)
        .await
        .expect("consume ai");
    assert!(ai.allowed, "{ai:?}");
    let req = stack
        .pg
        .try_consume_daily_request_budget(collection.id, today, 2, 5_000)
        .await
        .expect("consume requests");
    assert!(req.allowed, "{req:?}");

    let (status, resp) = send(&api.app, get("/api/v1/ops/discovery", &api.viewer)).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    let rows = resp["collection_daily_usage"]
        .as_array()
        .unwrap_or_else(|| panic!("collection_daily_usage 必須是陣列：{resp}"));
    let collection_id = collection.id.to_string();
    let found = rows
        .iter()
        .find(|row| row["collection_id"].as_str() == Some(collection_id.as_str()));
    assert!(
        found.is_some(),
        "剛消耗的 collection {collection_id} 應出現在今日用量：{resp}"
    );
    let found = found.unwrap();
    assert_eq!(found["ai_calls_used"], json!(3), "{resp}");
    assert_eq!(found["requests_used"], json!(2), "{resp}");
    let today_text = today.to_string();
    assert_eq!(found["date"].as_str(), Some(today_text.as_str()), "{resp}");

    sqlx::query("DELETE FROM discovery_daily_usage WHERE collection_id = $1 AND usage_date = $2")
        .bind(collection.id)
        .bind(today)
        .execute(stack.pg.pool())
        .await
        .expect("cleanup today's usage row");
}

/// viewer 讀 `/ops/discovery` 是 Read 權限，不是 403。
/// 只讀 ops，不建立 seed／candidate／outbox。
#[tokio::test]
async fn viewer_may_read_ops_discovery() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let (status, resp) = send(&api.app, get("/api/v1/ops/discovery", &api.viewer)).await;
    assert_eq!(status, StatusCode::OK, "{resp}");
    assert!(resp["candidate_backlog"].is_object(), "{resp}");
}

// ---------------------------------------------------------------- rbac

/// viewer 403：沒寫 seed 列，也不寫 outbox。
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

// ---------------------------------------------------------------- discovery run

/// 只建立 queued Job；`discovery.completed` 是 worker 寫終態時才進 outbox。
#[tokio::test]
async fn run_discovery_creates_and_dispatches_job() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let body = json!({
        "collection_id": Uuid::now_v7().to_string(),
        "entity_id": Uuid::now_v7().to_string(),
        "depth": 0,
    });
    let (status, resp) = send(
        &api.app,
        post_json("/api/v1/discovery/run", &api.operator, &body),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{resp}");
    // 建立並派工 `discovery_run` Job，停在 queued（discovery-worker 沒起，
    // 不會被消費成 running）。
    assert_eq!(resp["type"], json!("discovery_run"), "{resp}");
    assert_eq!(resp["status"], json!("queued"), "{resp}");
    assert!(resp["id"].as_str().is_some(), "{resp}");
    // 派工 parameters 記載來源與深度——discovery-worker 靠它開跑。
    assert_eq!(
        resp["parameters"]["collection_id"], body["collection_id"],
        "{resp}"
    );
    assert_eq!(resp["parameters"]["entity_id"], body["entity_id"], "{resp}");
    assert_eq!(resp["parameters"]["depth"], json!(0), "{resp}");
}

/// 只建立 queued Job；不寫 `discovery.completed` outbox。
#[tokio::test]
async fn run_discovery_writes_success_audit() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let body = json!({
        "collection_id": Uuid::now_v7().to_string(),
        "entity_id": Uuid::now_v7().to_string(),
        "depth": 0,
    });
    let (status, resp) = send(
        &api.app,
        post_json("/api/v1/discovery/run", &api.operator, &body),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{resp}");
    let entries = api.audit.entries();
    let run = entries
        .iter()
        .find(|e| e.action == core_api::AUDIT_DISCOVERY_RUN)
        .expect("discovery.run audit");
    assert_eq!(run.outcome, "success");
    assert_eq!(
        run.resource_id.as_deref(),
        Some(resp["id"].as_str().unwrap())
    );
}

/// 只建立 queued Job；不寫 `discovery.completed` outbox。
#[tokio::test]
async fn discover_from_entity_creates_job_with_path_entity_id() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let entity_id = Uuid::now_v7();
    // body 只帶 collection_id（entity_id 從路徑帶入，不該放進 body）。
    let body = json!({
        "collection_id": Uuid::now_v7().to_string(),
        "depth": 0,
    });
    let (status, resp) = send(
        &api.app,
        post_json(
            &format!("/api/v1/entities/{entity_id}/discover"),
            &api.operator,
            &body,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{resp}");
    assert_eq!(resp["type"], json!("discovery_run"), "{resp}");
    assert_eq!(resp["status"], json!("queued"), "{resp}");
    // entity_id 從路徑進 parameters。
    assert_eq!(
        resp["parameters"]["entity_id"],
        json!(entity_id.to_string()),
        "{resp}"
    );
    assert_eq!(
        resp["parameters"]["collection_id"], body["collection_id"],
        "{resp}"
    );
    assert_eq!(resp["parameters"]["depth"], json!(0), "{resp}");
}

/// viewer 403：沒建立 Job，也不寫 outbox。
#[tokio::test]
async fn viewer_cannot_run_discovery() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let body = json!({
        "collection_id": Uuid::now_v7().to_string(),
        "entity_id": Uuid::now_v7().to_string(),
        "depth": 0,
    });
    let (status, resp) = send(
        &api.app,
        post_json("/api/v1/discovery/run", &api.viewer, &body),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{resp}");
    // 被擋的派工不該寫 success 稽核。
    let entries = api.audit.entries();
    assert!(
        !entries
            .iter()
            .any(|e| e.action == core_api::AUDIT_DISCOVERY_RUN),
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

async fn count_outbox_for_payload(stack: &Stack, topic: &str, payload_key: &str, id: Uuid) -> i64 {
    let id_text = id.to_string();
    let (count,): (i64,) = match payload_key {
        "candidate_id" => sqlx::query_as(
            "SELECT count(*) FROM event_outbox WHERE topic = $1 AND payload->>'candidate_id' = $2",
        )
        .bind(topic)
        .bind(&id_text)
        .fetch_one(stack.pg.pool())
        .await
        .expect("count event_outbox"),
        "seed_id" => sqlx::query_as(
            "SELECT count(*) FROM event_outbox WHERE topic = $1 AND payload->>'seed_id' = $2",
        )
        .bind(topic)
        .bind(&id_text)
        .fetch_one(stack.pg.pool())
        .await
        .expect("count event_outbox"),
        other => panic!("count_outbox_for_payload 不認得 payload_key `{other}`"),
    };
    count
}

/// 只 claim／發送這幾個 correlation_id 的列，不掃共用表。
async fn relay_own_outbox_rows(stack: &Stack, producer: &EventProducer, correlation_ids: &[Uuid]) {
    let now = Utc::now();
    let lease = now + chrono::Duration::seconds(30);
    for correlation_id in correlation_ids {
        let rows = stack
            .pg
            .list_outbox_by_correlation_id(*correlation_id)
            .await
            .expect("list own outbox");
        for row in rows {
            if row.published_at.is_some() {
                continue;
            }
            let claimed = stack
                .pg
                .claim_outbox_by_id(row.id, now, lease)
                .await
                .expect("claim own outbox");
            let Some(claimed) = claimed else {
                continue;
            };
            let envelope = envelope_from_outbox(&claimed);
            producer
                .publish_to_topic(&claimed.topic, Some(&claimed.partition_key), &envelope)
                .await
                .expect("publish own outbox");
            stack
                .pg
                .mark_outbox_published(claimed.id, Utc::now())
                .await
                .expect("mark own outbox published");
        }
    }
}

/// e2e 結束（含 panic）時刪自己的 outbox／seed／candidate／candidate_evidence。
///
/// `seed_ids`／`candidate_ids` 可只填一邊。刪 candidate 前先刪
/// `candidate_evidence`（FK 沒有 ON DELETE CASCADE）。
#[must_use = "不綁成變數會立刻 drop，測試列還沒寫就被刪"]
struct E2eDiscoveryRowsOnDrop {
    dsn: String,
    seed_ids: Vec<Uuid>,
    candidate_ids: Vec<Uuid>,
}

impl E2eDiscoveryRowsOnDrop {
    fn from_dsn(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            seed_ids: Vec::new(),
            candidate_ids: Vec::new(),
        }
    }

    fn seed(mut self, id: Uuid) -> Self {
        self.seed_ids.push(id);
        self
    }

    fn candidate(mut self, id: Uuid) -> Self {
        self.candidate_ids.push(id);
        self
    }

    fn track_candidate(&mut self, id: Uuid) {
        self.candidate_ids.push(id);
    }
}

impl Drop for E2eDiscoveryRowsOnDrop {
    fn drop(&mut self) {
        let dsn = self.dsn.clone();
        let seed_ids = std::mem::take(&mut self.seed_ids);
        let candidate_ids = std::mem::take(&mut self.candidate_ids);
        if seed_ids.is_empty() && candidate_ids.is_empty() {
            return;
        }
        let joined = std::thread::spawn(move || {
            delete_e2e_discovery_rows_blocking(&dsn, &seed_ids, &candidate_ids)
        })
        .join();
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                eprintln!(
                    "E2eDiscoveryRowsOnDrop: 刪測試列失敗：{message}。\
                     請手動刪這次測試的 seed／candidate 與其 outbox 列，\
                     否則會污染共用開發 DB"
                );
            }
            Err(_) => {
                eprintln!(
                    "E2eDiscoveryRowsOnDrop: 刪測試列的執行緒 panic。\
                     請檢查開發 Postgres 是否留下 ds-e2e-seed candidate 或測試 seed"
                );
            }
        }
    }
}

fn delete_e2e_discovery_rows_blocking(
    dsn: &str,
    seed_ids: &[Uuid],
    candidate_ids: &[Uuid],
) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("建立 tokio runtime 失敗：{err}"))?;
    runtime.block_on(async {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(dsn)
            .await
            .map_err(|err| format!("連 Postgres 失敗：{err}"))?;
        for id in seed_ids {
            let seed_text = id.to_string();
            sqlx::query("DELETE FROM event_outbox WHERE payload->>'seed_id' = $1")
                .bind(&seed_text)
                .execute(&pool)
                .await
                .map_err(|err| format!("DELETE event_outbox seed `{id}` 失敗：{err}"))?;
            sqlx::query("DELETE FROM seeds WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .map_err(|err| format!("DELETE seeds `{id}` 失敗：{err}"))?;
        }
        for id in candidate_ids {
            let candidate_text = id.to_string();
            sqlx::query("DELETE FROM event_outbox WHERE payload->>'candidate_id' = $1")
                .bind(&candidate_text)
                .execute(&pool)
                .await
                .map_err(|err| format!("DELETE event_outbox candidate `{id}` 失敗：{err}"))?;
            sqlx::query("DELETE FROM candidate_evidence WHERE candidate_id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .map_err(|err| format!("DELETE candidate_evidence `{id}` 失敗：{err}"))?;
            sqlx::query("DELETE FROM candidates WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .map_err(|err| format!("DELETE candidates `{id}` 失敗：{err}"))?;
        }
        pool.close().await;
        Ok(())
    })
}
