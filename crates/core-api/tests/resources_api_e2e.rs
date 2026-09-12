//! SPEC §19 資源 endpoint 的**需要真實資料**的那一半：200 的回應形狀、404、
//! PATCH 的 If-Match（412／428）、POST 的 409／422／400、`GET /raw/{id}?body=true`、
//! `POST /jobs/{id}/retry`、`/api/v1/ops/health` 對真實後端。
//!
//! 只連本機 Docker 服務（Postgres／MinIO／Redpanda），不打外部網路；
//! 埠隔離沿用 storage-core conformance（本機 8080／9000 可能是別的系統）。
//!
//! 認證／RBAC／501 那一半在 `tests/resources_api.rs`，那支不需要資料庫。
//!
//! ⚠️ **list 類斷言一律用 cursor 錨定自己的 id**（見 `find_by_cursor`），不用筆數、
//! 也不翻頁找。這個資料庫是共用的（其他 e2e 也在寫），斷言「總共 2 筆」或
//! 「一定在前幾頁」都會隨著既有資料量隨機失敗。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use core_api::{AppState, AuthState, RateLimiter, ReadyProbe, ready_always, router};
use core_config::ImportSection;
use core_events::EventProducer;
use core_jobs::JobService;
use core_model::{
    Connector, Document, DocumentType, DuplicateGroup, Entity, EntityAlias, EntityExtraction,
    EntityIdentifier, EntityType, Provenance, RawEvidence, Relationship, RelationshipEvidence,
    RelationshipType, Source, SourceType,
};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tower::ServiceExt;
use uuid::Uuid;

/// `?body=true` 的回傳上限。測試用小值：不需要為了驗 413 真的搬 10 MiB。
const MAX_BODY_BYTES: u64 = 4_096;

struct Stack {
    pg: PostgresCanonicalStore,
    s3: S3ObjectStore,
    bucket: String,
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
    Stack {
        pg,
        s3,
        bucket,
        brokers,
    }
}

struct TestApi {
    app: Router,
    viewer: String,
    operator: String,
    audit: MemoryAuditLog,
}

fn build_api(stack: &Stack, producer: Option<Arc<EventProducer>>) -> TestApi {
    build_api_with_backends(stack, producer, Vec::new(), Vec::new())
}

fn build_api_with_backends(
    stack: &Stack,
    producer: Option<Arc<EventProducer>>,
    checks: Vec<Arc<dyn core_api::ReadyCheck>>,
    missing: Vec<&'static str>,
) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let viewer = jwt.issue("e2e-viewer", Role::Viewer).expect("issue");
    let operator = jwt.issue("e2e-operator", Role::Operator).expect("issue");
    let audit = MemoryAuditLog::new();
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(audit.clone()),
        store: Some(Arc::new(stack.pg.clone())),
        objects: Some(Arc::new(stack.s3.clone())),
        jobs: producer
            .clone()
            .map(|p| Arc::new(JobService::new(stack.pg.clone(), Some(p)))),
        merge: Some(Arc::new(merge::MergeService::new(
            stack.pg.clone(),
            producer.clone(),
        ))),
        resolver: Some(Arc::new(resolver::ResolverService::new(
            stack.pg.clone(),
            storage_core::mock::MockEmbeddingProvider::unsupported(),
            storage_core::mock::MockGraphStore::new(),
        ))),
        import: None,
        search: None,
        ready: ready_always(),
        backends: ReadyProbe::new(checks),
        // 真的接本機 Redpanda：`/ops/queues` 要驗的正是「查得到 group lag」，
        // 給 None 只會驗到 503 那條路徑。
        queues: core_events::GroupLagProbe::new(&stack.brokers, std::time::Duration::from_secs(3))
            .ok()
            .map(|probe| {
                Arc::new(core_api::QueueInspector {
                    probe,
                    bindings: vec![core_api::QueueBinding {
                        service: "normalizer",
                        group: "osint-normalizer".into(),
                        topic: core_events::EventTopic::RawCollected.as_str(),
                    }],
                })
            }),
        backends_missing: missing,
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config: ImportSection {
            max_upload_bytes: MAX_BODY_BYTES,
            ..ImportSection::default()
        },
        object_bucket: stack.bucket.clone(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        viewer,
        operator,
        audit,
    }
}

// ---------------------------------------------------------------- HTTP helpers

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value, Option<String>) {
    let response = app.clone().oneshot(request).await.expect("call api");
    let status = response.status();
    let etag = response
        .headers()
        .get(axum::http::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body, etag)
}

fn get(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
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

fn patch_with_etag(uri: &str, token: &str, etag: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("PATCH")
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .header("If-Match", etag)
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// 帶上假的 TCP peer，讓稽核的 `ip` 有值（`ConnectInfo` 平常由 axum 的
/// `into_make_service_with_connect_info` 插入，`oneshot` 沒有）。
fn with_peer(mut request: Request<Body>) -> Request<Body> {
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 9)),
        4_242,
    )));
    request
}

/// 在 list 裡找一個 id：命中回那一筆，沒命中回 `None`。
///
/// # 為什麼用 cursor 錨定而不是翻前幾頁
///
/// list 一律依 `id` 遞減。**但 id 不全是 UUID v7**：entity-worker 寫的 Entity 與
/// Relationship 是 UUID v5（由自然鍵推導，為了冪等），沒有時間序，而且 v5 的高位
/// 基本上是亂數——絕大多數 v5 id 會排在 v7 之上。也就是說「剛建立的東西在第一頁」
/// 這個直覺在這兩張表上是錯的，翻幾頁找會隨著資料庫既有資料量隨機失敗
/// （第一次寫這支測試時就是這樣紅的）。
///
/// cursor 的語意是「嚴格小於」，所以把 cursor 設成 `id + 1` 再取 1 筆，
/// 回來的第一筆若通過過濾條件就一定是它自己，與資料庫裡有多少別的資料無關。
async fn find_by_cursor(app: &Router, token: &str, base: &str, id: Uuid) -> Option<Value> {
    let anchor = Uuid::from_u128(id.as_u128() + 1);
    let sep = if base.contains('?') { '&' } else { '?' };
    let uri = format!("{base}{sep}limit=1&cursor={anchor}");
    let (status, body, _) = send(app, get(&uri, token)).await;
    assert_eq!(status, StatusCode::OK, "GET {uri}：{body}");
    body["items"]
        .as_array()
        .and_then(|items| items.first())
        .filter(|item| item["id"] == id.to_string())
        .cloned()
}

// ---------------------------------------------------------------- seed helpers

fn new_source(source_type: SourceType) -> Source {
    let now = Utc::now();
    Source {
        id: Uuid::now_v7(),
        name: format!("e2e-resource-{}", Uuid::now_v7()),
        source_type,
        platform: None,
        base_url: None,
        description: None,
        language: Some("zh".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    }
}

fn new_connector(source_id: Uuid, enabled: bool) -> Connector {
    Connector {
        id: Uuid::now_v7(),
        source_id,
        name: format!("e2e-connector-{}", Uuid::now_v7()),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled,
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
    }
}

fn new_document(object_type: DocumentType, duplicate_of: Option<Uuid>) -> Document {
    let now = Utc::now();
    Document {
        id: Uuid::now_v7(),
        object_type,
        schema_version: "0.1".into(),
        title: Some("e2e 文件".into()),
        body: Some("內文".into()),
        summary: None,
        language: Some("zh".into()),
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: Some("https://example.invalid/a".into()),
        canonical_url: Some("https://example.invalid/a".into()),
        normalized_content_hash: None,
        confidence: 0.9,
        labels: vec![],
        attributes: json!({}),
        external_key: None,
        simhash: None,
        duplicate_of,
    }
}

fn new_raw_evidence(
    source_id: Uuid,
    connector_id: Uuid,
    storage_path: &str,
    len: i64,
) -> RawEvidence {
    RawEvidence {
        id: Uuid::now_v7(),
        source_id,
        connector_id,
        collection_id: None,
        external_id: None,
        source_url: "https://example.invalid/raw".into(),
        retrieved_at: Utc::now(),
        content_type: Some("text/plain".into()),
        mime_type: Some("text/plain".into()),
        content_length: Some(len),
        sha256: "0".repeat(64),
        storage_path: storage_path.to_string(),
        http_status: Some(200),
        http_headers: json!({}),
        metadata: json!({}),
        collector_version: "e2e/0.1".into(),
    }
}

// ---------------------------------------------------------------- sources

/// Source 的完整生命週期：建立 → 讀 → merge patch → 清除欄位 → 併發保護 → 409／404。
#[tokio::test]
async fn source_lifecycle_including_optimistic_locking() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    // --- POST 201 ---
    let (status, created, etag) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/sources",
            &api.operator,
            &json!({
                "name": "e2e 來源",
                "source_type": "rss",
                "description": "會被清掉",
                "language": "zh",
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(created["enabled"], true, "預設 enabled");
    assert_eq!(created["collection_policy"], json!({}));
    let etag = etag.expect("POST 必須回 ETag，否則呼叫端沒有東西能放進 If-Match");

    // 建立要留稽核，而且要有 IP。
    let entry = api
        .audit
        .entries()
        .into_iter()
        .find(|e| {
            e.action == core_api::AUDIT_SOURCE_CREATE && e.resource_id == Some(id.to_string())
        })
        .expect("source.create 稽核");
    assert_eq!(entry.outcome, "success");
    assert_eq!(entry.ip.as_deref(), Some("203.0.113.9"));

    // --- GET 200 + ETag ---
    let (status, got, get_etag) =
        send(&api.app, get(&format!("/api/v1/sources/{id}"), &api.viewer)).await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(got["name"], "e2e 來源");
    assert_eq!(
        get_etag.as_deref(),
        Some(etag.as_str()),
        "POST 與 GET 的 ETag 必須一致，否則呼叫端要多做一次 GET 才敢 PATCH"
    );

    // --- list 找得到 ---
    assert!(
        find_by_cursor(&api.app, &api.viewer, "/api/v1/sources", id)
            .await
            .is_some(),
        "剛建立的 Source 必須出現在 GET /api/v1/sources"
    );

    // --- PATCH 沒帶 If-Match → 428 ---
    let (status, body, _) = send(
        &api.app,
        json_request(
            "PATCH",
            &format!("/api/v1/sources/{id}"),
            &api.operator,
            &json!({"name": "x"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_REQUIRED, "{body}");
    assert_eq!(body["error"], "precondition_required");

    // --- PATCH 帶錯的 If-Match → 412 ---
    let (status, body, _) = send(
        &api.app,
        patch_with_etag(
            &format!("/api/v1/sources/{id}"),
            &api.operator,
            "\"2000-01-01T00:00:00.000000Z\"",
            &json!({"name": "x"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PRECONDITION_FAILED, "{body}");

    // --- merge patch：只改指定欄位，null 清除 ---
    let (status, patched, new_etag) = send(
        &api.app,
        with_peer(patch_with_etag(
            &format!("/api/v1/sources/{id}"),
            &api.operator,
            &etag,
            &json!({"platform": "example", "description": null}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["platform"], "example");
    assert_eq!(patched["description"], Value::Null, "null 必須真的清除");
    assert_eq!(patched["name"], "e2e 來源", "沒提到的欄位不可以被動到");
    assert_eq!(patched["language"], "zh");
    let new_etag = new_etag.expect("PATCH 要回新的 ETag");
    assert_ne!(new_etag, etag, "改完 updated_at 必須變，否則樂觀鎖等於沒有");

    // --- 舊 ETag 立刻失效（lost update 防護）---
    let (status, body, _) = send(
        &api.app,
        patch_with_etag(
            &format!("/api/v1/sources/{id}"),
            &api.operator,
            &etag,
            &json!({"name": "後來的寫入"}),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PRECONDITION_FAILED,
        "舊版本的 If-Match 必須被擋下來：{body}"
    );

    // --- 不可為空的欄位傳 null → 400 ---
    let (status, body, _) = send(
        &api.app,
        patch_with_etag(
            &format!("/api/v1/sources/{id}"),
            &api.operator,
            &new_etag,
            &json!({"name": null}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // --- 重複 id → 409 ---
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            "/api/v1/sources",
            &api.operator,
            &json!({"id": id, "name": "撞號", "source_type": "rss"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("PATCH"),
        "{body}"
    );

    // --- 不存在的 id → 404 ---
    let (status, body, _) = send(
        &api.app,
        get(&format!("/api/v1/sources/{}", Uuid::now_v7()), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

// ---------------------------------------------------------------- connectors

#[tokio::test]
async fn connector_create_validates_source_and_credentials() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);
    let source = new_source(SourceType::Rss);
    stack.pg.put_source(&source).await.expect("seed source");

    // source 不存在 → 422（不是 400：JSON 沒問題，是引用的資源不存在）
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            "/api/v1/connectors",
            &api.operator,
            &json!({"source_id": Uuid::now_v7(), "name": "c", "type": "rss"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");

    // 明文密碼 → 400，而且訊息不可回顯那個值
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            "/api/v1/connectors",
            &api.operator,
            &json!({
                "source_id": source.id,
                "name": "c",
                "type": "rss",
                "credential_reference": "hunter2",
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        !body["message"].as_str().unwrap().contains("hunter2"),
        "錯誤訊息不可回顯疑似密鑰的值：{body}"
    );

    // 合法 SecretRef → 201
    let (status, created, etag) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/connectors",
            &api.operator,
            &json!({
                "source_id": source.id,
                "name": "e2e connector",
                "type": "rss",
                "credential_reference": "env:E2E_TOKEN",
                "enabled": false,
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(created["credential_reference"], "env:E2E_TOKEN");
    assert_eq!(created["version"], "0.1.0", "沒給 version 要有預設值");
    assert_eq!(created["status"], "idle");
    let etag = etag.expect("ETag");

    // 重複 id → 409
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            "/api/v1/connectors",
            &api.operator,
            &json!({"id": id, "source_id": source.id, "name": "c", "type": "rss"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // GET 單筆
    let (status, got, _) = send(
        &api.app,
        get(&format!("/api/v1/connectors/{id}"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{got}");
    assert_eq!(
        got["type"], "rss",
        "回應的欄位名是 type，不是 connector_type"
    );

    // PATCH：改 enabled，舊 ETag 失效
    let (status, patched, new_etag) = send(
        &api.app,
        with_peer(patch_with_etag(
            &format!("/api/v1/connectors/{id}"),
            &api.operator,
            &etag,
            &json!({"enabled": true, "credential_reference": null}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    assert_eq!(patched["enabled"], true);
    assert_eq!(patched["credential_reference"], Value::Null);
    assert_eq!(patched["name"], "e2e connector", "沒提到的欄位不可被動到");
    assert_ne!(new_etag.unwrap(), etag, "指紋必須跟著內容改變");

    // PATCH 明文密碼 → 400
    let (status, body, current_etag) = send(
        &api.app,
        get(&format!("/api/v1/connectors/{id}"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, body, _) = send(
        &api.app,
        patch_with_etag(
            &format!("/api/v1/connectors/{id}"),
            &api.operator,
            &current_etag.unwrap(),
            &json!({"credential_reference": "postgres://u:p@127.0.0.1/db"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    // 404
    let (status, _, _) = send(
        &api.app,
        get(
            &format!("/api/v1/connectors/{}", Uuid::now_v7()),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// `?enabled=` 的過濾必須在 SQL 做：停用的 connector 不能因為「不在最新一頁」而消失。
#[tokio::test]
async fn connector_enabled_filter_is_applied_in_sql() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);
    let source = new_source(SourceType::Rss);
    stack.pg.put_source(&source).await.expect("seed source");
    let enabled = new_connector(source.id, true);
    let disabled = new_connector(source.id, false);
    stack.pg.put_connector(&enabled).await.expect("seed");
    stack.pg.put_connector(&disabled).await.expect("seed");

    // 預設：兩個都看得到。
    assert!(
        find_by_cursor(&api.app, &api.viewer, "/api/v1/connectors", disabled.id)
            .await
            .is_some(),
        "預設不過濾——停用的 connector 藏起來只會讓「我的 connector 不見了」更難查"
    );

    let found = find_by_cursor(
        &api.app,
        &api.viewer,
        "/api/v1/connectors?enabled=false",
        disabled.id,
    )
    .await;
    assert!(found.is_some(), "enabled=false 要找得到停用的");
    assert_eq!(found.unwrap()["enabled"], false);

    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/connectors?enabled=true",
            disabled.id
        )
        .await
        .is_none(),
        "enabled=true 不該出現停用的"
    );
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/connectors?enabled=true",
            enabled.id
        )
        .await
        .is_some()
    );
}

// ---------------------------------------------------------------- collections

#[tokio::test]
async fn collection_create_links_and_detail() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);
    let source = new_source(SourceType::Rss);
    stack.pg.put_source(&source).await.expect("seed");
    let connector = new_connector(source.id, true);
    stack.pg.put_connector(&connector).await.expect("seed");

    // 關聯不存在 → 422，而且不可以留下半成品。
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            "/api/v1/collections",
            &api.operator,
            &json!({"name": "壞的", "source_ids": [Uuid::now_v7()]}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("沒有建立任何東西"),
        "{body}"
    );

    let (status, created, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/collections",
            &api.operator,
            &json!({
                "name": "e2e collection",
                "source_ids": [source.id],
                "connector_ids": [connector.id],
                "priority": 3,
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id: Uuid = created["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(created["status"], "active", "沒給 status 要有預設值");
    assert_eq!(created["priority"], 3);
    assert_eq!(created["source_ids"], json!([source.id]));

    // 掛一份 object 上去，明細要看得到。
    let document = new_document(DocumentType::Article, None);
    stack.pg.put_document(&document).await.expect("seed doc");
    stack
        .pg
        .link_collection_object(id, document.id)
        .await
        .expect("link object");

    let (status, detail, _) = send(
        &api.app,
        get(&format!("/api/v1/collections/{id}"), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(
        detail["name"], "e2e collection",
        "collection 欄位要 flatten 在頂層"
    );
    assert_eq!(detail["source_ids"], json!([source.id]));
    assert_eq!(detail["connector_ids"], json!([connector.id]));
    assert_eq!(detail["object_ids"], json!([document.id]));
    assert_eq!(detail["objects_truncated"], false);

    assert!(
        find_by_cursor(&api.app, &api.viewer, "/api/v1/collections", id)
            .await
            .is_some()
    );

    let (status, _, _) = send(
        &api.app,
        get(
            &format!("/api/v1/collections/{}", Uuid::now_v7()),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------- objects

/// 預設排除重複；`include_duplicates=true` 才含。
#[tokio::test]
async fn objects_exclude_duplicates_by_default() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let canonical = new_document(DocumentType::Article, None);
    stack.pg.put_document(&canonical).await.expect("seed");
    let duplicate = new_document(DocumentType::Article, Some(canonical.id));
    stack.pg.put_document(&duplicate).await.expect("seed");

    assert!(
        find_by_cursor(&api.app, &api.viewer, "/api/v1/objects", canonical.id)
            .await
            .is_some(),
        "canonical 一定要出現"
    );
    assert!(
        find_by_cursor(&api.app, &api.viewer, "/api/v1/objects", duplicate.id)
            .await
            .is_none(),
        "預設列表不該出現重複文件（SPEC §15／§16）"
    );
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/objects?include_duplicates=true",
            duplicate.id
        )
        .await
        .is_some(),
        "include_duplicates=true 要看得到"
    );

    // object_type 過濾
    let advisory = new_document(DocumentType::Advisory, None);
    stack.pg.put_document(&advisory).await.expect("seed");
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/objects?object_type=advisory",
            advisory.id
        )
        .await
        .is_some()
    );
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/objects?object_type=advisory",
            canonical.id
        )
        .await
        .is_none(),
        "object_type 過濾要真的濾掉別的型別"
    );
}

/// `GET /objects/{id}` 要串出 SPEC §14 的可追溯鏈與去重、抽取結果。
#[tokio::test]
async fn object_detail_carries_the_provenance_chain() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let source = new_source(SourceType::Rss);
    stack.pg.put_source(&source).await.expect("seed");
    let connector = new_connector(source.id, true);
    stack.pg.put_connector(&connector).await.expect("seed");
    let key = format!("raw/e2e/{}", Uuid::now_v7());
    let evidence = new_raw_evidence(source.id, connector.id, &key, 5);
    stack
        .pg
        .insert_raw_evidence(&evidence)
        .await
        .expect("seed raw");

    let canonical = new_document(DocumentType::Article, None);
    stack.pg.put_document(&canonical).await.expect("seed");
    let duplicate = new_document(DocumentType::Article, Some(canonical.id));
    stack.pg.put_document(&duplicate).await.expect("seed");

    stack
        .pg
        .put_provenance(&Provenance {
            id: Uuid::now_v7(),
            subject_id: canonical.id,
            action: "normalized".into(),
            parent_id: None,
            raw_evidence_id: Some(evidence.id),
            processor: "e2e".into(),
            processor_version: "0.1".into(),
            timestamp: Utc::now(),
            metadata: json!({}),
        })
        .await
        .expect("seed provenance");

    stack
        .pg
        .put_duplicate_group(&DuplicateGroup {
            id: Uuid::now_v7(),
            canonical_object_id: canonical.id,
            member_object_id: Some(duplicate.id),
            member_raw_evidence_id: None,
            method: "content_hash".into(),
            similarity: 1.0,
            first_seen: Utc::now(),
        })
        .await
        .expect("seed group");

    let entity = Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Vulnerability,
        name: "CVE-2026-0001".into(),
        normalized_name: format!("cve-2026-{}", Uuid::now_v7().simple()),
        description: None,
        confidence: 0.9,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        merged_into: None,
        attributes: json!({}),
    };
    stack.pg.put_entity(&entity).await.expect("seed entity");
    stack
        .pg
        .put_entity_extraction(&EntityExtraction {
            id: Uuid::now_v7(),
            object_id: canonical.id,
            entity_id: entity.id,
            extractor: "regex".into(),
            extractor_version: "0.1".into(),
            confidence: 0.8,
            text_offset: Some(10),
            excerpt: Some("CVE-2026-0001".into()),
        })
        .await
        .expect("seed extraction");

    let (status, detail, _) = send(
        &api.app,
        get(&format!("/api/v1/objects/{}", canonical.id), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(
        detail["id"],
        canonical.id.to_string(),
        "document 要 flatten"
    );
    assert_eq!(detail["provenance"].as_array().unwrap().len(), 1);
    assert_eq!(
        detail["raw_evidence"][0]["id"],
        evidence.id.to_string(),
        "可追溯鏈的下一站是 RawEvidence：{detail}"
    );
    assert_eq!(
        detail["duplicate_group"],
        Value::Null,
        "canonical 不是誰的重複"
    );
    assert_eq!(detail["duplicates"].as_array().unwrap().len(), 1);
    assert_eq!(detail["duplicates"][0]["method"], "content_hash");
    assert_eq!(detail["entities"][0]["entity"]["id"], entity.id.to_string());
    assert_eq!(detail["entities"][0]["extraction"]["text_offset"], 10);

    // 反過來：重複的那一份要指得出 canonical。
    let (status, detail, _) = send(
        &api.app,
        get(&format!("/api/v1/objects/{}", duplicate.id), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(
        detail["duplicate_group"]["canonical_object_id"],
        canonical.id.to_string()
    );

    let (status, _, _) = send(
        &api.app,
        get(&format!("/api/v1/objects/{}", Uuid::now_v7()), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------- entities / relationships

#[tokio::test]
async fn entity_and_relationship_detail() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let document = new_document(DocumentType::Article, None);
    stack.pg.put_document(&document).await.expect("seed");
    let entity = Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Ip,
        name: "203.0.113.5".into(),
        normalized_name: format!("203.0.113.5-{}", Uuid::now_v7().simple()),
        description: None,
        confidence: 0.7,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        merged_into: None,
        attributes: json!({}),
    };
    stack.pg.put_entity(&entity).await.expect("seed entity");
    stack
        .pg
        .put_entity_extraction(&EntityExtraction {
            id: Uuid::now_v7(),
            object_id: document.id,
            entity_id: entity.id,
            extractor: "regex".into(),
            extractor_version: "0.1".into(),
            confidence: 0.8,
            text_offset: None,
            excerpt: None,
        })
        .await
        .expect("seed extraction");

    let relationship = Relationship {
        id: Uuid::now_v7(),
        source_object_id: document.id,
        relationship_type: RelationshipType::Mentions,
        target_object_id: entity.id,
        confidence: 0.8,
        first_seen: Utc::now(),
        last_seen: Utc::now(),
        evidence_count: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    stack
        .pg
        .put_relationship(&relationship)
        .await
        .expect("seed relationship");
    stack
        .pg
        .put_relationship_evidence(&RelationshipEvidence {
            id: Uuid::now_v7(),
            relationship_id: relationship.id,
            object_id: document.id,
            raw_evidence_id: None,
            excerpt: Some("提到 203.0.113.5".into()),
            confidence: 0.8,
            created_at: Utc::now(),
        })
        .await
        .expect("seed evidence");

    // --- entity 明細 ---
    let (status, detail, _) = send(
        &api.app,
        get(&format!("/api/v1/entities/{}", entity.id), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["entity_type"], "ip", "entity 欄位要 flatten");
    assert_eq!(detail["relationship_count"], 1);
    assert_eq!(detail["relationships_truncated"], false);
    assert_eq!(detail["recent_extractions"].as_array().unwrap().len(), 1);

    // entity_type 過濾
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/entities?entity_type=ip",
            entity.id
        )
        .await
        .is_some()
    );
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/entities?entity_type=person",
            entity.id
        )
        .await
        .is_none(),
        "entity_type 過濾要真的濾掉別的型別"
    );

    // --- relationship 明細：SPEC §12 一定要回得出 evidence ---
    let (status, detail, _) = send(
        &api.app,
        get(
            &format!("/api/v1/relationships/{}", relationship.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["relationship_type"], "mentions");
    assert_eq!(detail["evidence"].as_array().unwrap().len(), 1);
    assert_eq!(detail["evidence"][0]["excerpt"], "提到 203.0.113.5");

    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/relationships?type=mentions",
            relationship.id
        )
        .await
        .is_some()
    );
    assert!(
        find_by_cursor(
            &api.app,
            &api.viewer,
            "/api/v1/relationships?type=affects",
            relationship.id
        )
        .await
        .is_none()
    );

    for path in ["entities", "relationships"] {
        let (status, _, _) = send(
            &api.app,
            get(&format!("/api/v1/{path}/{}", Uuid::now_v7()), &api.viewer),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path} 的 404");
    }
}

// ---------------------------------------------------------------- events

/// events 表在 V0.1 沒有寫入者（SPEC §13）。空清單與 404 是**正確行為**。
#[tokio::test]
async fn events_endpoints_are_valid_even_with_no_writer() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let (status, body, _) = send(&api.app, get("/api/v1/events", &api.viewer)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["items"].is_array(), "要回合法的分頁結構：{body}");

    let (status, body, _) = send(
        &api.app,
        get(&format!("/api/v1/events/{}", Uuid::now_v7()), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["message"].as_str().unwrap().contains("SPEC §13"),
        "404 要說明「這是正常的」，否則會被當成故障查：{body}"
    );
}

// ---------------------------------------------------------------- raw

#[tokio::test]
async fn raw_metadata_and_body() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);
    let source = new_source(SourceType::Rss);
    stack.pg.put_source(&source).await.expect("seed");
    let connector = new_connector(source.id, true);
    stack.pg.put_connector(&connector).await.expect("seed");

    // --- UTF-8 ---
    let text = "漏洞公告：CVE-2026-0001";
    let key = format!("raw/e2e/{}", Uuid::now_v7());
    stack
        .s3
        .put(&key, text.as_bytes(), Some("text/plain"))
        .await
        .expect("put object");
    let evidence = new_raw_evidence(source.id, connector.id, &key, text.len() as i64);
    stack.pg.insert_raw_evidence(&evidence).await.expect("seed");

    // 預設只回 metadata。
    let (status, body, _) = send(
        &api.app,
        get(&format!("/api/v1/raw/{}", evidence.id), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["source_id"], source.id.to_string());
    assert_eq!(body["storage_path"], key);
    assert!(
        body.get("body").is_none(),
        "沒要 body 就不該去物件儲存拉內容：{body}"
    );

    let (status, body, _) = send(
        &api.app,
        get(
            &format!("/api/v1/raw/{}?body=true", evidence.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["body"]["content_encoding"], "utf8");
    assert_eq!(body["body"]["content"], text);
    assert_eq!(body["body"]["bytes"], text.len());

    // --- 二進位 → base64 ---
    let binary: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x0d, 0x0a, 0x1a, 0x0a, 0xff, 0xfe];
    let key = format!("raw/e2e/{}", Uuid::now_v7());
    stack
        .s3
        .put(&key, &binary, Some("image/png"))
        .await
        .expect("put object");
    let evidence = new_raw_evidence(source.id, connector.id, &key, binary.len() as i64);
    stack.pg.insert_raw_evidence(&evidence).await.expect("seed");

    let (status, body, _) = send(
        &api.app,
        get(
            &format!("/api/v1/raw/{}?body=true", evidence.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["body"]["content_encoding"], "base64");
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(body["body"]["content"].as_str().unwrap())
        .expect("base64");
    assert_eq!(decoded, binary, "base64 解回來必須位元相同");

    // --- 超過上限 → 413（metadata 仍可取得）---
    let big = vec![b'x'; (MAX_BODY_BYTES as usize) + 1];
    let key = format!("raw/e2e/{}", Uuid::now_v7());
    stack.s3.put(&key, &big, None).await.expect("put object");
    let evidence = new_raw_evidence(source.id, connector.id, &key, big.len() as i64);
    stack.pg.insert_raw_evidence(&evidence).await.expect("seed");

    let (status, body, _) = send(
        &api.app,
        get(
            &format!("/api/v1/raw/{}?body=true", evidence.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("?body=true"),
        "413 要告訴使用者「metadata 還是拿得到」：{body}"
    );

    // content_length 是 NULL 的舊資料也要擋得住（改從實際長度判斷）。
    let key = format!("raw/e2e/{}", Uuid::now_v7());
    stack.s3.put(&key, &big, None).await.expect("put object");
    let mut evidence = new_raw_evidence(source.id, connector.id, &key, 0);
    evidence.content_length = None;
    stack.pg.insert_raw_evidence(&evidence).await.expect("seed");
    let (status, _, _) = send(
        &api.app,
        get(
            &format!("/api/v1/raw/{}?body=true", evidence.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::PAYLOAD_TOO_LARGE,
        "content_length 沒填也不能讓上限失效"
    );

    // --- 404 ---
    let (status, _, _) = send(
        &api.app,
        get(&format!("/api/v1/raw/{}", Uuid::now_v7()), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------- jobs retry

/// SPEC §31：operator 可以重試失敗的 job，動作要留稽核，viewer 不可以。
#[tokio::test]
async fn job_retry_only_from_failed_and_is_audited() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-e2e").expect("Redpanda producer"),
    );
    let api = build_api(&stack, Some(producer));

    // 建一個 job（不 dispatch，避免依賴 broker 往返）。
    let (status, job, _) = send(
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
    let id: Uuid = job["id"].as_str().unwrap().parse().unwrap();

    // queued 的 job 不能 retry → 409
    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/retry"),
            &api.operator,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    // queued → running
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/transition"),
            &api.operator,
            &json!({"status": "running"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    // running 的 job 不能 retry → 409
    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/retry"),
            &api.operator,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("failed"),
        "{body}"
    );

    // running → failed
    let (status, body, _) = send(
        &api.app,
        json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/transition"),
            &api.operator,
            &json!({"status": "failed", "error": "e2e 故意失敗"}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "failed");

    // viewer 不可以 retry → 403
    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/retry"),
            &api.viewer,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    // operator retry → 200，狀態進 retrying、retry_count +1
    let (status, retried, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/jobs/{id}/retry"),
            &api.operator,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retried}");
    assert_eq!(
        retried["status"], "retrying",
        "狀態機沒有 failed→queued；retry_count 是在進 retrying 時累加的：{retried}"
    );
    assert_eq!(retried["retry_count"], 1);

    // 稽核：SPEC §31「retry action is audited」。成功與被拒都要在，而且要有 IP。
    let retries: Vec<_> = api
        .audit
        .entries()
        .into_iter()
        .filter(|e| e.action == core_api::AUDIT_JOB_RETRY && e.resource_id == Some(id.to_string()))
        .collect();
    assert_eq!(retries.len(), 3, "兩次 409 + 一次成功都要留痕：{retries:?}");
    assert!(
        retries
            .iter()
            .all(|e| e.ip.as_deref() == Some("203.0.113.9"))
    );
    assert_eq!(retries.iter().filter(|e| e.outcome == "success").count(), 1);

    // 403 那次由 middleware 記 authz.denied（handler 根本沒被呼叫）。
    assert!(
        api.audit
            .entries()
            .iter()
            .any(|e| e.action == core_api::AUDIT_AUTHZ_DENIED),
        "viewer 被擋下來也要留痕（SPEC §31：read-only role cannot execute retry）"
    );

    // `?status=failed` 走 SQL 過濾。
    let (status, body, _) = send(
        &api.app,
        get("/api/v1/jobs?status=failed&limit=100", &api.operator),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|j| j["status"] == "failed"),
        "status 過濾回來的不可以有別的狀態：{body}"
    );
}

// ---------------------------------------------------------------- ops health

/// 把 Redis 指到一個沒有人在聽的埠：`/ops/health` 要回 503 並指出是 redis。
#[tokio::test]
async fn ops_health_reports_a_broken_redis() {
    let stack = connect_stack().await;

    let good = storage_redis::RedisKeyValueStore::connect(
        &required_env("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:16379".into()),
    )
    .expect("redis client");
    // 埠 1 沒有服務在聽。用真的 client 而不是假的檢查——這裡要驗的正是
    // 「連不上時我們回的是 down，而不是讓整個 endpoint 500 或卡住」。
    let broken = storage_redis::RedisKeyValueStore::connect("redis://127.0.0.1:1")
        .expect("redis client（連線是 lazy 的，建立本身不會失敗）");

    let api_ok = build_api_with_backends(
        &stack,
        None,
        vec![
            Arc::new(core_api::BackendCheck::new(
                "postgres",
                Arc::new(stack.pg.clone()),
            )),
            Arc::new(core_api::BackendCheck::new(
                "object_store",
                Arc::new(stack.s3.clone()),
            )),
            Arc::new(core_api::BackendCheck::new("redis", Arc::new(good))),
        ],
        Vec::new(),
    );
    let (status, body, _) = send(&api_ok.app, get("/api/v1/ops/health", &api_ok.viewer)).await;
    assert_eq!(status, StatusCode::OK, "全部活著時要 200：{body}");
    assert_eq!(body["healthy"], true, "{body}");

    let api_bad = build_api_with_backends(
        &stack,
        None,
        vec![
            Arc::new(core_api::BackendCheck::new(
                "postgres",
                Arc::new(stack.pg.clone()),
            )),
            Arc::new(core_api::BackendCheck::new("redis", Arc::new(broken))),
        ],
        Vec::new(),
    );
    let (status, body, _) = send(&api_bad.app, get("/api/v1/ops/health", &api_bad.viewer)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(
        body["unhealthy"],
        json!(["redis"]),
        "要指出是哪一個：{body}"
    );
    let postgres = body["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "postgres")
        .expect("postgres 檢查");
    assert_eq!(
        postgres["healthy"], true,
        "一個壞掉不該讓其他檢查也被標成壞的：{body}"
    );
}

// ---------------------------------------------------------------- resolve / merge

fn new_entity(entity_type: EntityType, name: &str, normalized_name: &str) -> Entity {
    let now = Utc::now();
    Entity {
        id: Uuid::now_v7(),
        entity_type,
        name: name.into(),
        normalized_name: normalized_name.into(),
        description: None,
        confidence: 0.8,
        first_seen: now,
        last_seen: now,
        merged_into: None,
        attributes: json!({}),
    }
}

/// 跨 type 同名 → `POST /entities/{id}/resolve` 產出 `normalized_name` 候選，
/// `GET /entities/{id}/resolution-candidates` 查得到同一筆。
#[tokio::test]
async fn resolve_entity_hits_normalized_name() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let token = Uuid::now_v7().simple().to_string();
    let name = format!("acme-{token}");
    let person = new_entity(EntityType::Person, &name, &name);
    let org = new_entity(EntityType::Organization, &name, &name);
    stack.pg.put_entity(&person).await.expect("seed person");
    stack.pg.put_entity(&org).await.expect("seed org");

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/entities/{}/resolve", person.id),
            &api.operator,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let candidates = body.as_array().expect("resolve 回陣列");
    let hit = candidates.iter().find(|c| {
        c["method"] == "normalized_name"
            && (c["entity_a_id"] == person.id.to_string()
                || c["entity_b_id"] == person.id.to_string())
            && (c["entity_a_id"] == org.id.to_string() || c["entity_b_id"] == org.id.to_string())
    });
    assert!(
        hit.is_some(),
        "應含跨 type 同名的 normalized_name 候選：{body}"
    );
    let candidate_id: Uuid = hit.unwrap()["id"].as_str().unwrap().parse().unwrap();

    let listed = find_by_cursor(
        &api.app,
        &api.viewer,
        &format!("/api/v1/entities/{}/resolution-candidates", person.id),
        candidate_id,
    )
    .await;
    assert!(
        listed.is_some(),
        "GET resolution-candidates 應查得到剛 resolve 寫入的候選"
    );
}

/// 同 type merge 成功：survivor 吃到 alias／identifier，merged 被標 `merged_into`。
#[tokio::test]
async fn merge_entities_absorbs_alias_and_identifier() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let run = Uuid::now_v7().simple().to_string();
    let survivor = new_entity(
        EntityType::Person,
        &format!("survivor-{run}"),
        &format!("survivor-{run}"),
    );
    let merged = new_entity(
        EntityType::Person,
        &format!("merged-{run}"),
        &format!("merged-{run}"),
    );
    stack.pg.put_entity(&survivor).await.expect("seed survivor");
    stack.pg.put_entity(&merged).await.expect("seed merged");

    let now = Utc::now();
    stack
        .pg
        .put_entity_alias(&EntityAlias {
            id: Uuid::now_v7(),
            entity_id: merged.id,
            alias: format!("aka-{run}"),
            alias_type: "aka".into(),
            source_id: None,
            confidence: 0.7,
            first_seen: now,
            last_seen: now,
        })
        .await
        .expect("seed alias");
    stack
        .pg
        .put_entity_identifier(&EntityIdentifier {
            id: Uuid::now_v7(),
            entity_id: merged.id,
            namespace: format!("e2e-{run}"),
            value: format!("id-{run}"),
            normalized_value: format!("id-{run}"),
            confidence: 0.9,
            source_id: None,
            first_seen: now,
            last_seen: now,
        })
        .await
        .expect("seed identifier");

    let (status, history, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/entities/merge",
            &api.operator,
            &json!({
                "survivor_id": survivor.id,
                "merged_id": merged.id,
                "reason": "e2e 確認是同一個人",
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(history["survivor_id"], survivor.id.to_string());
    assert_eq!(history["merged_id"], merged.id.to_string());
    let history_id: Uuid = history["id"].as_str().unwrap().parse().unwrap();

    let aliases = stack
        .pg
        .list_entity_aliases_by_entity(survivor.id, 100)
        .await
        .expect("list aliases");
    assert!(
        aliases.iter().any(|a| a.alias == format!("aka-{run}")),
        "survivor 應吃到 merged 的 alias：{aliases:?}"
    );
    let identifiers = stack
        .pg
        .list_entity_identifiers_by_entity(survivor.id, 100)
        .await
        .expect("list identifiers");
    assert!(
        identifiers
            .iter()
            .any(|i| i.normalized_value == format!("id-{run}")),
        "survivor 應吃到 merged 的 identifier：{identifiers:?}"
    );

    let (status, detail, _) = send(
        &api.app,
        get(&format!("/api/v1/entities/{}", merged.id), &api.viewer),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["merged_into"], survivor.id.to_string());

    let (status, listed, _) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/merge-history", survivor.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{listed}");
    assert!(
        listed
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h["id"] == history_id.to_string()),
        "GET merge-history 應含剛寫入的歷史：{listed}"
    );

    let entry = api
        .audit
        .entries()
        .into_iter()
        .find(|e| {
            e.action == core_api::AUDIT_ENTITY_MERGE
                && e.resource_id == Some(history_id.to_string())
        })
        .expect("entity.merge 稽核");
    assert_eq!(entry.outcome, "success");
    assert_eq!(entry.ip.as_deref(), Some("203.0.113.9"));
}

#[tokio::test]
async fn merge_rejects_type_mismatch_and_empty_reason() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let run = Uuid::now_v7().simple().to_string();
    let person = new_entity(EntityType::Person, &format!("p-{run}"), &format!("p-{run}"));
    let org = new_entity(
        EntityType::Organization,
        &format!("o-{run}"),
        &format!("o-{run}"),
    );
    stack.pg.put_entity(&person).await.expect("seed person");
    stack.pg.put_entity(&org).await.expect("seed org");

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/entities/merge",
            &api.operator,
            &json!({
                "survivor_id": person.id,
                "merged_id": org.id,
                "reason": "型別不同不該過",
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/entities/merge",
            &api.operator,
            &json!({
                "survivor_id": person.id,
                "merged_id": org.id,
                "reason": "   ",
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("reason"),
        "空 reason 的訊息要指出是 reason：{body}"
    );
}

#[tokio::test]
async fn undo_merge_twice_is_409() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);

    let run = Uuid::now_v7().simple().to_string();
    let survivor = new_entity(
        EntityType::Domain,
        &format!("surv-{run}"),
        &format!("surv-{run}"),
    );
    let merged = new_entity(
        EntityType::Domain,
        &format!("src-{run}"),
        &format!("src-{run}"),
    );
    stack.pg.put_entity(&survivor).await.expect("seed survivor");
    stack.pg.put_entity(&merged).await.expect("seed merged");

    let (status, history, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/entities/merge",
            &api.operator,
            &json!({
                "survivor_id": survivor.id,
                "merged_id": merged.id,
                "reason": "e2e undo",
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    let history_id = history["id"].as_str().unwrap();

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/merge-history/{history_id}/undo"),
            &api.operator,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{body}");

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/merge-history/{history_id}/undo"),
            &api.operator,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
}

#[tokio::test]
async fn viewer_cannot_resolve_or_merge() {
    let stack = connect_stack().await;
    let api = build_api(&stack, None);
    let id = Uuid::now_v7();

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            &format!("/api/v1/entities/{id}/resolve"),
            &api.viewer,
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body, _) = send(
        &api.app,
        with_peer(json_request(
            "POST",
            "/api/v1/entities/merge",
            &api.viewer,
            &json!({
                "survivor_id": id,
                "merged_id": Uuid::now_v7(),
                "reason": "viewer 不該過",
            }),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}
