//! `GET /api/v1/entities/{id}/timeline` 的**需要真實資料**的那一半（SPEC_V0.2 §10）。
//!
//! 只連本機 Docker 服務（Postgres），不打外部網路；建置方式比照
//! `tests/resources_api_e2e.rs`。這些測試需要 entity／document／entity_extraction，
//! `tests/common/mod.rs` 的最小 app 沒有連 canonical store，所以不 `mod common;`。

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{TimeZone, Utc};
use core_api::{AppState, AuthState, RateLimiter, ready_always, router};
use core_config::ImportSection;
use core_model::{Document, DocumentType, Entity, EntityExtraction, EntityType};
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
}

fn build_api(stack: &Stack) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let viewer = jwt.issue("e2e-tl-viewer", Role::Viewer).expect("issue");
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
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
        auto_approval_max_concurrent: 2,
        object_bucket: "raw-evidence".into(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        viewer,
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

fn new_document(object_type: DocumentType, duplicate_of: Option<Uuid>) -> Document {
    let now = Utc::now();
    Document {
        id: Uuid::now_v7(),
        object_type,
        schema_version: "0.1".into(),
        title: Some("timeline e2e 文件".into()),
        body: Some("內文".into()),
        summary: None,
        language: Some("zh".into()),
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: Some("https://example.invalid/tl".into()),
        canonical_url: Some("https://example.invalid/tl".into()),
        normalized_content_hash: None,
        confidence: 0.9,
        labels: vec![],
        attributes: json!({}),
        external_key: None,
        simhash: None,
        duplicate_of,
    }
}

fn new_entity(name: &str) -> Entity {
    let now = Utc::now();
    Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Ip,
        name: name.into(),
        normalized_name: format!("tl-{}", Uuid::now_v7().simple()),
        description: None,
        confidence: 0.8,
        first_seen: now,
        last_seen: now,
        merged_into: None,
        attributes: json!({}),
    }
}

async fn put_extraction(stack: &Stack, object_id: Uuid, entity_id: Uuid) {
    stack
        .pg
        .put_entity_extraction(&EntityExtraction {
            id: Uuid::now_v7(),
            object_id,
            entity_id,
            extractor: "regex".into(),
            extractor_version: "0.1".into(),
            confidence: 0.8,
            text_offset: Some(10),
            excerpt: Some("IOC".into()),
        })
        .await
        .expect("seed extraction");
}

// ---------------------------------------------------------------- timeline

#[tokio::test]
async fn timeline_entity_not_found_is_404() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/timeline", Uuid::now_v7()),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
}

#[tokio::test]
async fn timeline_empty_when_no_extractions() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let entity = new_entity("203.0.113.1");
    stack.pg.put_entity(&entity).await.expect("seed entity");

    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/timeline", entity.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["entries"], json!([]));
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn timeline_sorts_and_marks_time_source() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let entity = new_entity("203.0.113.2");

    // 有 published_at 的 Document，時間較早。
    let mut with_published = new_document(DocumentType::Article, None);
    with_published.published_at = Some(Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap());
    stack
        .pg
        .put_document(&with_published)
        .await
        .expect("seed doc");

    // 沒有 published_at、只有 observed_at 的 Document，時間較晚。
    let mut with_observed = new_document(DocumentType::Article, None);
    with_observed.observed_at = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
    stack
        .pg
        .put_document(&with_observed)
        .await
        .expect("seed doc");

    stack.pg.put_entity(&entity).await.expect("seed entity");
    put_extraction(&stack, with_published.id, entity.id).await;
    put_extraction(&stack, with_observed.id, entity.id).await;

    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/timeline", entity.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    // observed_at（9/1）較新，應排在最前。
    assert_eq!(entries[0]["document_id"], with_observed.id.to_string());
    assert_eq!(entries[0]["time_source"], "observed_at");
    assert_eq!(entries[1]["document_id"], with_published.id.to_string());
    assert_eq!(entries[1]["time_source"], "published_at");
    assert_eq!(body["truncated"], false);
}

#[tokio::test]
async fn timeline_skips_duplicate_document() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let entity = new_entity("203.0.113.3");

    // 一份是 canonical，一份是它的 duplicate。
    let canonical = new_document(DocumentType::Article, None);
    let duplicate = new_document(DocumentType::Article, Some(canonical.id));
    stack
        .pg
        .put_document(&canonical)
        .await
        .expect("seed canonical");
    stack
        .pg
        .put_document(&duplicate)
        .await
        .expect("seed duplicate");

    stack.pg.put_entity(&entity).await.expect("seed entity");
    // 兩筆 extraction 都指向同一 entity：canonical 一份、duplicate 一份。
    put_extraction(&stack, canonical.id, entity.id).await;
    put_extraction(&stack, duplicate.id, entity.id).await;

    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/timeline", entity.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let entries = body["entries"].as_array().unwrap();
    // duplicate 那份不該出現在結果裡，即使它有一筆 extraction。
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["document_id"], canonical.id.to_string());
}

#[tokio::test]
async fn timeline_truncated_when_hitting_limit() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let entity = new_entity("203.0.113.4");

    let mut docs: Vec<Document> = Vec::new();
    for _ in 0..3 {
        let d = new_document(DocumentType::Article, None);
        stack.pg.put_document(&d).await.expect("seed doc");
        docs.push(d);
    }
    stack.pg.put_entity(&entity).await.expect("seed entity");
    for d in &docs {
        put_extraction(&stack, d.id, entity.id).await;
    }

    // limit=2：只回 2 筆，且 truncated=true。
    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/timeline?limit=2", entity.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["entries"].as_array().unwrap().len(), 2);
    assert_eq!(body["truncated"], true);
}

#[tokio::test]
async fn timeline_rbac() {
    let stack = connect_stack().await;
    let api = build_api(&stack);
    let entity = new_entity("203.0.113.5");
    stack.pg.put_entity(&entity).await.expect("seed entity");

    // 沒帶 token → 401。
    let (status, _) = send(
        &api.app,
        get(&format!("/api/v1/entities/{}/timeline", entity.id), ""),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // viewer → 200。
    let (status, body) = send(
        &api.app,
        get(
            &format!("/api/v1/entities/{}/timeline", entity.id),
            &api.viewer,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["entries"], json!([]));
}
