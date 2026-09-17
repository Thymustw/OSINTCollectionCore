//! STIX 匯入／匯出需要真實 Postgres／MinIO 的那一半：
//! 202 + Job.parameters、source 檢查、不發 `raw.collected`、
//! `GET /jobs/{id}/result` 的 404／409／500 骨架。
//!
//! 只連本機 Docker 服務。

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use connector_sdk::StoreEvidenceSink;
use core_api::{AppState, AuthState, ImportState, RateLimiter, ready_always, router};
use core_events::{EventConsumer, EventError, EventProducer, EventTopic};
use core_jobs::JobService;
use core_model::{Source, SourceType};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tower::ServiceExt;
use uuid::Uuid;

const UUID: &str = "2152fbe0-4471-4d43-8b64-0b907d186c23";

struct Stack {
    pg: PostgresCanonicalStore,
    s3: S3ObjectStore,
    brokers: String,
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
    let brokers = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        brokers.contains("127.0.0.1") || brokers.contains("localhost"),
        "e2e 只連本機 Redpanda，實際 brokers={brokers}"
    );

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");
    let s3 = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("s3");
    s3.ensure_bucket().await.expect("bucket");
    Stack { pg, s3, brokers }
}

async fn seed_source(pg: &PostgresCanonicalStore, source_type: SourceType) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("e2e-stix-{}", Uuid::now_v7()),
        source_type,
        platform: None,
        base_url: None,
        description: Some("stix e2e".into()),
        language: Some("zh".into()),
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

struct TestApi {
    app: Router,
    viewer: String,
    operator: String,
}

fn build_api(stack: &Stack, producer: Arc<EventProducer>) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let viewer = jwt.issue("e2e-viewer", Role::Viewer).expect("issue");
    let operator = jwt.issue("e2e-operator", Role::Operator).expect("issue");
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

fn ok_bundle() -> Value {
    json!({
        "type": "bundle",
        "id": format!("bundle--{UUID}"),
        "objects": [
            {
                "type": "identity",
                "id": format!("identity--{UUID}"),
                "name": "Alice",
                "identity_class": "individual"
            }
        ]
    })
}

#[tokio::test]
async fn import_stix_creates_job_without_publishing_raw_collected() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::StixImport).await;
    let group = format!("osint-e2e-stix-{}", Uuid::now_v7());
    let consumer =
        EventConsumer::connect(&stack.brokers, &group, &[EventTopic::RawCollected.as_str()])
            .expect("raw.collected consumer");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "core-api-stix-e2e").expect("producer"));
    let api = build_api(&stack, producer);

    let (status, body) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/import/stix",
            &api.operator,
            &json!({"source_id": source.id, "bundle": ok_bundle()}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["type"], "stix_import");
    assert_eq!(body["status"], "queued");
    let job_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let raw_evidence_id: Uuid = body["parameters"]["raw_evidence_id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(body["parameters"]["source_id"], source.id.to_string());
    assert_eq!(body["correlation_id"], raw_evidence_id.to_string());

    let stored = stack
        .pg
        .get_job(job_id)
        .await
        .expect("get job")
        .expect("Job 應寫進 Postgres");
    assert_eq!(stored.job_type, "stix_import");
    assert_eq!(
        stored.parameters.as_ref().unwrap()["raw_evidence_id"],
        json!(raw_evidence_id)
    );
    assert_eq!(
        stored.parameters.as_ref().unwrap()["source_id"],
        json!(source.id)
    );

    let evidence = stack
        .pg
        .get_raw_evidence(raw_evidence_id)
        .await
        .expect("get evidence")
        .expect("RawEvidence 應寫進 Postgres");
    assert_eq!(evidence.source_id, source.id);
    assert_eq!(evidence.metadata["import_spec"], "stix_bundle");
    assert_eq!(evidence.metadata["object_count"], 1);

    // STIX import 不得發 raw.collected。等一小段確認這筆 id 沒出現。
    let want = raw_evidence_id.to_string();
    match consumer.next_envelope(Duration::from_secs(2)).await {
        Err(EventError::ConsumeTimeout { .. }) => {}
        Ok(envelope) => {
            let got = envelope
                .payload
                .get("raw_evidence_id")
                .and_then(Value::as_str);
            assert_ne!(
                got,
                Some(want.as_str()),
                "STIX import 不該 publish raw.collected：{envelope:?}"
            );
        }
        Err(err) => panic!("讀 raw.collected 失敗：{err}"),
    }
}

#[tokio::test]
async fn import_stix_missing_source_is_404() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-stix-e2e-404").expect("producer"),
    );
    let api = build_api(&stack, producer);
    let (status, body) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/import/stix",
            &api.operator,
            &json!({"source_id": Uuid::now_v7(), "bundle": ok_bundle()}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn import_stix_wrong_source_type_is_400() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::Rss).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-stix-e2e-400").expect("producer"),
    );
    let api = build_api(&stack, producer);
    let (status, body) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/import/stix",
            &api.operator,
            &json!({"source_id": source.id, "bundle": ok_bundle()}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("stix_import"), "{message}");
}

#[tokio::test]
async fn export_stix_creates_queued_job() {
    let stack = connect_stack().await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "core-api-stix-export").expect("producer"));
    let api = build_api(&stack, producer);
    let (status, body) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/export/stix",
            &api.operator,
            &json!({"filter": {"entity_types": ["person"]}}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{body}");
    assert_eq!(body["type"], "stix_export");
    assert_eq!(body["status"], "queued");
    assert_eq!(
        body["parameters"]["filter"]["entity_types"],
        json!(["person"])
    );

    let job_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let (status, result) = send(
        &api.app,
        get(&format!("/api/v1/jobs/{job_id}/result"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{result}");
    let message = result["message"].as_str().unwrap();
    assert!(message.contains("queued"), "{message}");
}

#[tokio::test]
async fn job_result_unknown_id_is_404() {
    let stack = connect_stack().await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "core-api-stix-result").expect("producer"));
    let api = build_api(&stack, producer);
    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/jobs/{}/result", Uuid::now_v7()),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn job_result_wrong_type_is_404() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-stix-wrong-type").expect("producer"),
    );
    let api = build_api(&stack, producer);
    let (status, job) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/jobs",
            &api.operator,
            &json!({"type": "collect", "dispatch": false}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{job}");
    let id = job["id"].as_str().unwrap();
    let (status, body) = send(
        &api.app,
        get(&format!("/api/v1/jobs/{id}/result"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn job_result_completed_without_key_is_500() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-stix-completed").expect("producer"),
    );
    let api = build_api(&stack, producer);
    let (status, job) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/export/stix",
            &api.operator,
            &json!({"filter": {}}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{job}");
    let id: Uuid = job["id"].as_str().unwrap().parse().unwrap();

    let (status, _) = send(
        &api.app,
        json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/transition"),
            &api.operator,
            &json!({"status": "running"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &api.app,
        json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/transition"),
            &api.operator,
            &json!({"status": "completed"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        &api.app,
        get(&format!("/api/v1/jobs/{id}/result"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("result_object_key"), "{message}");
}

/// 這個 e2e 測試環境沒有真的 stix-worker 在跑，所以要模擬 worker 做完的狀態：
/// 直接把一份合法 bundle 寫進物件儲存，直接改 `Job.parameters` 補
/// `result_object_key`（沒有 HTTP 端點能改 `parameters`，比照其他測試直接用
/// store handle），走 HTTP 轉成 `completed`，驗證 `GET /jobs/{id}/result`
/// 真的把物件儲存裡的 bundle 讀回來，而不是 500 骨架。
#[tokio::test]
async fn job_result_completed_with_object_returns_bundle() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-stix-result-ok").expect("producer"),
    );
    let api = build_api(&stack, producer);
    let (status, job) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/export/stix",
            &api.operator,
            &json!({"filter": {}}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED, "{job}");
    let id: Uuid = job["id"].as_str().unwrap().parse().unwrap();

    // 模擬 worker：把 bundle 寫進物件儲存。
    let key = stix_adapter::export_result_object_key(id);
    let bundle = ok_bundle();
    stack
        .s3
        .put(
            &key,
            &serde_json::to_vec(&bundle).expect("serialize bundle"),
            Some("application/stix+json"),
        )
        .await
        .expect("s3 put");

    // 模擬 worker：把 result_object_key 補進 Job.parameters（保留既有的 filter）。
    let mut stored = stack
        .pg
        .get_job(id)
        .await
        .expect("get job")
        .expect("job exists");
    let mut params = stored
        .parameters
        .take()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    params.insert("result_object_key".into(), json!(key));
    stored.parameters = Some(Value::Object(params));
    stack.pg.put_job(&stored).await.expect("put job");

    let (status, _) = send(
        &api.app,
        json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/transition"),
            &api.operator,
            &json!({"status": "running"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = send(
        &api.app,
        json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/transition"),
            &api.operator,
            &json!({"status": "completed"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, body) = send(
        &api.app,
        get(&format!("/api/v1/jobs/{id}/result"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["type"], "bundle");
    assert_eq!(body, bundle);

    let _ = stack.s3.delete(&key).await;
}
