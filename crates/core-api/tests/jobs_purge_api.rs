//! `POST /api/v1/ops/jobs/purge` 與 `GET /ops/dlq?type=`。
//!
//! operator 403 不需要 Postgres。真正刪列的案例連本機 Postgres，
//! 用獨一無二的 job_type 避免碰到共用 DB 的 leftover collect job。

mod common;

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{TimeZone, Utc};
use common::{issue_test_jwt, test_app};
use core_api::{
    AUDIT_JOBS_PURGE, AppState, AuthState, RateLimiter, ReadyProbe, ready_always, router,
};
use core_jobs::JobService;
use core_model::{Job, JobStatus};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use storage_postgres::PostgresCanonicalStore;
use tower::ServiceExt;
use uuid::Uuid;

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

fn bearer(method: &str, uri: &str, token: &str, body: Option<Value>) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"));
    if body.is_some() {
        builder = builder.header("Content-Type", "application/json");
    }
    builder
        .body(Body::from(body.map(|v| v.to_string()).unwrap_or_default()))
        .unwrap()
}

#[tokio::test]
async fn operator_cannot_purge_jobs() {
    let (jwt, operator) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let (status, body) = send(
        &app,
        bearer("POST", "/api/v1/ops/jobs/purge", &operator, Some(json!({}))),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

struct PgApi {
    app: Router,
    admin: String,
    viewer: String,
    audit: MemoryAuditLog,
    store: PostgresCanonicalStore,
}

async fn pg_api() -> PgApi {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "只連本機 Postgres"
    );
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres。跑此測試前請停 osint-core 應用容器");
    store.migrate().await.expect("migrate");

    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let admin = jwt.issue("purge-admin", Role::Admin).expect("issue");
    let viewer = jwt.issue("purge-viewer", Role::Viewer).expect("issue");
    let audit = MemoryAuditLog::new();
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(audit.clone()),
        store: Some(Arc::new(store.clone())),
        objects: None,
        jobs: Some(Arc::new(JobService::new(store.clone(), None))),
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
        backends: ReadyProbe::new(Vec::new()),
        queues: None,
        backends_missing: Vec::new(),
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config: core_config::ImportSection::default(),
        stix_config: core_config::StixSection::default(),
        auto_approval_max_concurrent: 2,
        object_bucket: String::new(),
        collector: core_config::CollectorSection::default(),
        retention_jobs: core_config::RetentionJobsConfig::default(),
        rate_limiter: RateLimiter::new(1_000),
    };
    PgApi {
        app: router(state),
        admin,
        viewer,
        audit,
        store,
    }
}

fn old_job(job_type: &str, status: JobStatus) -> Job {
    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    Job {
        id: Uuid::now_v7(),
        job_type: job_type.into(),
        status,
        correlation_id: None,
        created_at: old,
        started_at: Some(old),
        completed_at: Some(old),
        retry_count: 0,
        error: None,
        parameters: None,
    }
}

#[tokio::test]
async fn admin_custom_purge_deletes_and_writes_audit() {
    let api = pg_api().await;
    let job_type = format!("purge_api_{}", Uuid::now_v7());
    let target = old_job(&job_type, JobStatus::Failed);
    let keep = old_job("collect", JobStatus::Failed);
    api.store.put_job(&target).await.expect("put target");
    api.store.put_job(&keep).await.expect("put keep");

    let (status, body) = send(
        &api.app,
        bearer(
            "POST",
            "/api/v1/ops/jobs/purge",
            &api.admin,
            Some(json!({
                "job_type": job_type,
                "statuses": ["failed"],
                "older_than_days": 30
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["deleted"], json!(1), "{body}");
    assert_eq!(body["dry_run"], json!(false));

    assert!(api.store.get_job(target.id).await.expect("get").is_none());
    assert!(
        api.store.get_job(keep.id).await.expect("get").is_some(),
        "不該動到其他 type"
    );

    let entries = api.audit.entries();
    assert!(
        entries.iter().any(|e| e.action == AUDIT_JOBS_PURGE
            && e.actor == "purge-admin"
            && e.outcome == "success"),
        "應寫 jobs.purge 稽核，實際 {entries:?}"
    );

    let _ = api.store.delete_job(keep.id).await;
}

#[tokio::test]
async fn dry_run_does_not_delete() {
    let api = pg_api().await;
    let job_type = format!("purge_api_dry_{}", Uuid::now_v7());
    let target = old_job(&job_type, JobStatus::Failed);
    api.store.put_job(&target).await.expect("put");

    let (status, body) = send(
        &api.app,
        bearer(
            "POST",
            "/api/v1/ops/jobs/purge",
            &api.admin,
            Some(json!({
                "dry_run": true,
                "job_type": job_type,
                "statuses": ["failed"],
                "older_than_days": 30
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["dry_run"], json!(true));
    assert_eq!(body["deleted"], json!(0), "{body}");
    assert!(body["counted"].as_u64().unwrap_or(0) >= 1, "{body}");
    assert!(api.store.get_job(target.id).await.expect("get").is_some());
    let _ = api.store.delete_job(target.id).await;
}

#[tokio::test]
async fn non_terminal_status_returns_422() {
    let api = pg_api().await;
    let (status, body) = send(
        &api.app,
        bearer(
            "POST",
            "/api/v1/ops/jobs/purge",
            &api.admin,
            Some(json!({
                "job_type": "collect",
                "statuses": ["running"],
                "older_than_days": 7
            })),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert_eq!(body["error"], "unprocessable");
    assert!(
        body["message"].as_str().unwrap_or("").contains("running"),
        "{body}"
    );
}

#[tokio::test]
async fn dlq_type_filter_only_returns_matching_jobs() {
    let api = pg_api().await;
    let job_type = format!("dlq_type_{}", Uuid::now_v7());
    let matching = old_job(&job_type, JobStatus::Failed);
    let other = old_job("collect", JobStatus::Failed);
    api.store.put_job(&matching).await.expect("put matching");
    api.store.put_job(&other).await.expect("put other");

    let (status, body) = send(
        &api.app,
        bearer(
            "GET",
            &format!("/api/v1/ops/dlq?type={job_type}"),
            &api.viewer,
            None,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let jobs = body["failed_jobs"].as_array().expect("failed_jobs");
    assert!(
        jobs.iter().any(|j| j["id"] == matching.id.to_string()),
        "應含本次 failed job，實際 {body}"
    );
    assert!(
        jobs.iter().all(|j| j["type"] == job_type),
        "不該混進其他 type，實際 {body}"
    );
    assert!(
        body["note"]
            .as_str()
            .unwrap_or("")
            .contains("?type=collect"),
        "note 應提到 type 過濾：{body}"
    );

    let _ = api.store.delete_job(matching.id).await;
    let _ = api.store.delete_job(other.id).await;
}
