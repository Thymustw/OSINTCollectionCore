//! 假 REST JSON → connector 抓取 → RawEvidence 寫入 MinIO + PostgreSQL。
//! 不連真實外網。憑證走 SecretRef（env:），不寫進 configuration。

use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::Utc;
use connector_rest_api::RestApiConnector;
use connector_sdk::{
    CollectContext, ConnectorCheckpoint, ConnectorTrait, DomainRateLimiter, GuardedFetcher,
    MapResolver, RelationalCheckpointStore, SourcePolicy, SsrfGuard, StoreEvidenceSink, sha256_hex,
};
use core_model::{Collection, Connector, NetworkRule, Source, SourceType};
use core_security::MemoryAuditLog;
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use uuid::Uuid;

const TOKEN: &str = "e2e-rest-token";
const JSON_BODY: &str = r#"{"items":[{"id":"CVE-2026-0001"}],"next":null}"#;

async fn require_bearer(req: Request, next: Next) -> Response {
    let ok = req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        == Some("Bearer e2e-rest-token");
    if ok {
        next.run(req).await
    } else {
        StatusCode::UNAUTHORIZED.into_response()
    }
}

async fn serve_api() -> String {
    let app = Router::new()
        .route(
            "/v1/items",
            get(|| async {
                let mut headers = HeaderMap::new();
                headers.insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                (headers, JSON_BODY).into_response()
            }),
        )
        .layer(middleware::from_fn(require_bearer));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}", addr.port())
}

fn ts() -> chrono::DateTime<Utc> {
    Utc::now()
}

async fn stack() -> (PostgresCanonicalStore, S3ObjectStore) {
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
    (pg, s3)
}

fn make_rest(
    pg: PostgresCanonicalStore,
    s3: S3ObjectStore,
    source_id: uuid::Uuid,
    rule: NetworkRule,
) -> RestApiConnector<
    StoreEvidenceSink<PostgresCanonicalStore, S3ObjectStore>,
    RelationalCheckpointStore<PostgresCanonicalStore>,
> {
    let audit = Arc::new(MemoryAuditLog::new());
    let guard = SsrfGuard::new(
        source_id,
        SourcePolicy::default(),
        vec![rule],
        Arc::new(MapResolver::default()),
        audit,
    );
    let fetcher = GuardedFetcher::new(
        guard,
        DomainRateLimiter::new(SourcePolicy::default().rate_limit),
    );
    let sink = StoreEvidenceSink::new(pg.clone(), s3);
    let checkpoints = RelationalCheckpointStore::new(pg);
    RestApiConnector::new(fetcher, sink, checkpoints)
}

#[tokio::test]
async fn rest_api_to_raw_evidence_round_trip() {
    unsafe {
        std::env::set_var("OSINT_E2E_REST_TOKEN", TOKEN);
    }
    let (pg, s3) = stack().await;
    let base = serve_api().await;
    let now = ts();
    let source = Source {
        id: Uuid::now_v7(),
        name: "e2e-rest".into(),
        source_type: SourceType::RestApi,
        platform: None,
        base_url: Some(base.clone()),
        description: Some("local fixture".into()),
        language: Some("en".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    pg.put_source(&source).await.expect("source");
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "e2e 本機假 REST".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "e2e-rest-connector".into(),
        connector_type: "rest_api".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({
            "path": "/v1/items",
            "method": "GET",
            "auth": { "header": "Authorization", "prefix": "Bearer " },
            "pagination": { "max_pages": 1, "next_url_pointer": "/next" }
        }),
        credential_reference: Some("env:OSINT_E2E_REST_TOKEN".into()),
        schedule: None,
        rate_limit: json!({ "per_second": 10 }),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    pg.put_connector(&connector).await.expect("connector");
    let collection = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: "e2e-rest-collection".into(),
        description: None,
        status: "active".into(),
        priority: 0,
        created_at: now,
        updated_at: now,
    };
    pg.put_collection(&collection).await.expect("collection");

    let rest = make_rest(pg.clone(), s3.clone(), source.id, rule);
    let ctx = CollectContext {
        source: source.clone(),
        connector: connector.clone(),
        collection_id: Some(collection.id),
        checkpoint: ConnectorCheckpoint::default(),
        now,
    };
    let collected = rest.collect(&ctx).await.expect("collect");
    assert!(collected.fetched, "應抓到 JSON body");
    let new_ev = collected.evidence.expect("evidence");
    assert_eq!(new_ev.body, JSON_BODY.as_bytes());
    assert_eq!(new_ev.mime_type.as_deref(), Some("application/json"));

    let items = rest.parse(&new_ev.body).await.expect("parse");
    assert!(items.is_empty(), "V0.1 parse 不拆 JSON 成 Document 項目");

    let stored = rest.create_raw_evidence(new_ev).await.expect("persist");
    rest.update_checkpoint(&connector, &collected.checkpoint)
        .await
        .expect("checkpoint");

    let meta = pg
        .get_raw_evidence(stored.id)
        .await
        .expect("get meta")
        .expect("raw evidence 應存在 Postgres");
    assert_eq!(meta.sha256, sha256_hex(JSON_BODY.as_bytes()));
    assert_eq!(meta.http_status, Some(200));

    let blob = s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("MinIO 應有 body");
    assert_eq!(blob, JSON_BODY.as_bytes());
}

#[tokio::test]
async fn rest_api_rejects_inline_authorization_header() {
    let (pg, s3) = stack().await;
    let now = ts();
    let source = Source {
        id: Uuid::now_v7(),
        name: "e2e-rest-bad".into(),
        source_type: SourceType::RestApi,
        platform: None,
        base_url: Some("http://127.0.0.1:1/v1".into()),
        description: None,
        language: None,
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "e2e-rest-bad-connector".into(),
        connector_type: "rest_api".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({
            "path": "/v1/items",
            "headers": { "Authorization": "Bearer leaked" }
        }),
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
    let dummy_rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "e2e 本機假 REST".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    let rest = make_rest(pg, s3, source.id, dummy_rule);
    let ctx = CollectContext {
        source,
        connector,
        collection_id: None,
        checkpoint: ConnectorCheckpoint::default(),
        now,
    };
    let err = rest
        .collect(&ctx)
        .await
        .expect_err("明文 Authorization 必須被拒");
    let msg = err.to_string();
    assert!(
        msg.contains("credential_reference"),
        "應提示改用 SecretRef，實際：{msg}"
    );
}
