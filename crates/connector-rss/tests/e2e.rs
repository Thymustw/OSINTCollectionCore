//! 假 RSS feed → connector 抓取 → RawEvidence 寫入 MinIO + PostgreSQL → 讀回核對。
//! 不連真實外網。本機 OpenCTI 埠隔離沿用 storage-core conformance。

use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use chrono::Utc;
use connector_rss::RssConnector;
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

const RSS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Local Fixture</title>
    <link>http://127.0.0.1/feed</link>
    <description>e2e</description>
    <item>
      <title>CVE-2026-0001</title>
      <link>http://127.0.0.1/cve</link>
      <guid>CVE-2026-0001</guid>
      <description>fixture item</description>
    </item>
  </channel>
</rss>
"#;

async fn serve_rss() -> String {
    let app = Router::new().route("/rss.xml", get(|| async { RSS }));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/rss.xml", addr.port())
}

fn ts() -> chrono::DateTime<Utc> {
    Utc::now()
}

#[tokio::test]
async fn rss_to_raw_evidence_round_trip() {
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

    let feed_url = serve_rss().await;
    let now = ts();
    let source = Source {
        id: Uuid::now_v7(),
        name: "e2e-rss".into(),
        source_type: SourceType::Rss,
        platform: None,
        base_url: Some(feed_url.clone()),
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
        reason: "e2e 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");

    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "e2e-rss-connector".into(),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({ "url": feed_url }),
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
    pg.put_connector(&connector).await.expect("connector");

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
    let rss = RssConnector::new(fetcher, sink, checkpoints);

    let discovered = rss.discover(&source).await.expect("discover");
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].url, feed_url);

    let ctx = CollectContext {
        source: source.clone(),
        connector: connector.clone(),
        collection_id: Some(collection.id),
        checkpoint: ConnectorCheckpoint::default(),
        now,
    };
    let collected = rss.collect(&ctx).await.expect("collect");
    assert!(collected.fetched, "應抓到 feed body，不是 304");
    let new_ev = collected.evidence.expect("evidence");
    assert_eq!(new_ev.body, RSS.as_bytes());

    let items = rss.parse(&new_ev.body).await.expect("parse");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].title.as_deref(), Some("CVE-2026-0001"));

    let stored = rss.create_raw_evidence(new_ev).await.expect("persist");
    rss.update_checkpoint(&connector, &collected.checkpoint)
        .await
        .expect("checkpoint");

    let meta = pg
        .get_raw_evidence(stored.id)
        .await
        .expect("get meta")
        .expect("raw evidence 應存在 Postgres");
    assert_eq!(meta.sha256, sha256_hex(RSS.as_bytes()));
    assert_eq!(meta.source_url, feed_url);
    assert_eq!(meta.http_status, Some(200));
    assert_eq!(meta.content_length, Some(RSS.len() as i64));

    let blob = s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("MinIO 應有 body");
    assert_eq!(blob, RSS.as_bytes(), "讀回的 body 必須與假 feed 完全一致");
    assert_eq!(sha256_hex(&blob), meta.sha256);

    let reloaded = pg
        .get_connector(connector.id)
        .await
        .expect("reload connector")
        .expect("connector");
    let cp = ConnectorCheckpoint::from_value(&reloaded.checkpoint);
    assert!(cp.last_retrieved_at.is_some());

    let health = rss.health().await;
    assert!(health.healthy, "{}", health.message);
}
