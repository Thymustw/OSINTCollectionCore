//! 假 HTML → connector 抓取 → RawEvidence 寫入 MinIO + PostgreSQL → 讀回核對。
//! 不連真實外網。本機 OpenCTI 埠隔離沿用 storage-core conformance。

use std::sync::Arc;

use axum::Router;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use chrono::Utc;
use connector_sdk::{
    CollectContext, ConnectorCheckpoint, ConnectorTrait, DomainRateLimiter, GuardedFetcher,
    MapResolver, RelationalCheckpointStore, SourcePolicy, SsrfGuard, StoreEvidenceSink, sha256_hex,
};
use connector_static_web::StaticWebConnector;
use core_model::{Collection, Connector, NetworkRule, Source, SourceType};
use core_security::MemoryAuditLog;
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use uuid::Uuid;

const HTML: &str = r#"<!doctype html>
<html>
<head>
  <title>CVE-2026-0001 advisory</title>
  <meta name="description" content="fixture page">
</head>
<body>
  <script>alert(1)</script>
  <article>
    <h1>CVE-2026-0001</h1>
    <p>This is the main body text.</p>
  </article>
</body>
</html>
"#;

async fn serve_html() -> String {
    let app = Router::new().route(
        "/page.html",
        get(|| async {
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            (headers, HTML).into_response()
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/page.html", addr.port())
}

async fn serve_html_with_etag() -> String {
    let app = Router::new().route(
        "/page.html",
        get(|headers: HeaderMap| async move {
            if headers.get("if-none-match").and_then(|v| v.to_str().ok()) == Some("\"page-1\"") {
                return StatusCode::NOT_MODIFIED.into_response();
            }
            let mut out = HeaderMap::new();
            out.insert(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            );
            out.insert(
                axum::http::header::ETAG,
                HeaderValue::from_static("\"page-1\""),
            );
            (out, HTML).into_response()
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/page.html", addr.port())
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

fn seed_source_connector(
    page_url: &str,
    now: chrono::DateTime<Utc>,
) -> (Source, NetworkRule, Connector, Collection) {
    let source = Source {
        id: Uuid::now_v7(),
        name: "e2e-static-web".into(),
        source_type: SourceType::StaticWeb,
        platform: None,
        base_url: Some(page_url.to_string()),
        description: Some("local fixture".into()),
        language: Some("en".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "e2e 本機假 HTML".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "e2e-static-web-connector".into(),
        connector_type: "static_web".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({ "url": page_url }),
        credential_reference: None,
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
    let collection = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: "e2e-collection".into(),
        description: None,
        status: "active".into(),
        priority: 0,
        created_at: now,
        updated_at: now,
    };
    (source, rule, connector, collection)
}

#[tokio::test]
async fn static_web_to_raw_evidence_round_trip() {
    let (pg, s3) = stack().await;
    let page_url = serve_html().await;
    let now = ts();
    let (source, rule, connector, collection) = seed_source_connector(&page_url, now);
    pg.put_source(&source).await.expect("source");
    pg.put_network_rule(&rule).await.expect("rule");
    pg.put_connector(&connector).await.expect("connector");
    pg.put_collection(&collection).await.expect("collection");

    let audit = Arc::new(MemoryAuditLog::new());
    let guard = SsrfGuard::new(
        source.id,
        SourcePolicy::default(),
        vec![rule],
        Arc::new(MapResolver::default()),
        audit,
    );
    let fetcher = GuardedFetcher::new(
        guard,
        DomainRateLimiter::new(SourcePolicy::default().rate_limit),
    );
    let sink = StoreEvidenceSink::new(pg.clone(), s3.clone());
    let checkpoints = RelationalCheckpointStore::new(pg.clone());
    let web = StaticWebConnector::new(fetcher, sink, checkpoints);

    let discovered = web.discover(&source).await.expect("discover");
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].url, page_url);

    let ctx = CollectContext {
        source: source.clone(),
        connector: connector.clone(),
        collection_id: Some(collection.id),
        checkpoint: ConnectorCheckpoint::default(),
        now,
    };
    let collected = web.collect(&ctx).await.expect("collect");
    assert!(collected.fetched, "應抓到 HTML body");
    let new_ev = collected.evidence.expect("evidence");
    assert_eq!(new_ev.body, HTML.as_bytes());

    let items = web.parse(&new_ev.body).await.expect("parse");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].title.as_deref(), Some("CVE-2026-0001 advisory"));

    let stored = web.create_raw_evidence(new_ev).await.expect("persist");
    web.update_checkpoint(&connector, &collected.checkpoint)
        .await
        .expect("checkpoint");

    let meta = pg
        .get_raw_evidence(stored.id)
        .await
        .expect("get meta")
        .expect("raw evidence 應存在 Postgres");
    assert_eq!(meta.sha256, sha256_hex(HTML.as_bytes()));
    assert_eq!(meta.source_url, page_url);
    assert_eq!(meta.http_status, Some(200));
    assert_eq!(meta.content_type.as_deref(), Some("text/html"));

    let blob = s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("MinIO 應有 body");
    assert_eq!(blob, HTML.as_bytes());

    let reloaded = pg
        .get_connector(connector.id)
        .await
        .expect("reload")
        .expect("connector");
    let cp = ConnectorCheckpoint::from_value(&reloaded.checkpoint);
    assert!(cp.last_retrieved_at.is_some());
    assert_eq!(
        cp.content_sha256.as_deref(),
        Some(sha256_hex(HTML.as_bytes()).as_str())
    );
}

#[tokio::test]
async fn static_web_unchanged_on_etag_304() {
    let (pg, s3) = stack().await;
    let page_url = serve_html_with_etag().await;
    let now = ts();
    let (source, rule, connector, collection) = seed_source_connector(&page_url, now);
    pg.put_source(&source).await.expect("source");
    pg.put_network_rule(&rule).await.expect("rule");
    pg.put_connector(&connector).await.expect("connector");
    pg.put_collection(&collection).await.expect("collection");

    let audit = Arc::new(MemoryAuditLog::new());
    let guard = SsrfGuard::new(
        source.id,
        SourcePolicy::default(),
        vec![rule],
        Arc::new(MapResolver::default()),
        audit,
    );
    let fetcher = GuardedFetcher::new(
        guard,
        DomainRateLimiter::new(SourcePolicy::default().rate_limit),
    );
    let sink = StoreEvidenceSink::new(pg.clone(), s3.clone());
    let checkpoints = RelationalCheckpointStore::new(pg.clone());
    let web = StaticWebConnector::new(fetcher, sink, checkpoints);

    let ctx = CollectContext {
        source: source.clone(),
        connector: connector.clone(),
        collection_id: Some(collection.id),
        checkpoint: ConnectorCheckpoint::default(),
        now,
    };
    let first = web.collect(&ctx).await.expect("first");
    assert!(first.fetched);
    let etag = first.checkpoint.etag.clone();
    assert_eq!(etag.as_deref(), Some("\"page-1\""));

    let ctx2 = CollectContext {
        source,
        connector,
        collection_id: Some(collection.id),
        checkpoint: first.checkpoint,
        now,
    };
    let second = web.collect(&ctx2).await.expect("second");
    assert!(!second.fetched, "304 不該再寫 RawEvidence");
    assert!(second.evidence.is_none());
}
