//! 假 RSS feed → connector 抓取 → RawEvidence 寫入物件儲存 + PostgreSQL → 讀回核對。
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
use core_model::{
    Collection, Connector, EntityType, NetworkRule, RelationshipType, Source, SourceType,
};
use core_observability::MetricsRegistry;
use core_security::MemoryAuditLog;
use entity_worker::{EntityWorker, ExtractOutcome, ExtractionBounds};
use normalizer::{NormalizeOutcome, Normalizer};
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use uuid::Uuid;

fn rss_body(feed_title: &str, guid: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>{feed_title}</title>
    <link>http://127.0.0.1/feed</link>
    <description>e2e</description>
    <item>
      <title>CVE-2026-0001</title>
      <link>http://127.0.0.1/cve</link>
      <guid>{guid}</guid>
      <description>fixture item</description>
    </item>
  </channel>
</rss>
"#
    )
}

async fn serve_rss(body: String) -> String {
    let app = Router::new().route(
        "/rss.xml",
        get({
            let body = body.clone();
            move || {
                let body = body.clone();
                async move { body }
            }
        }),
    );
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
    let access = required_env("S3_ACCESS_KEY").expect("S3_ACCESS_KEY");
    let secret = required_env("S3_SECRET_KEY").expect("S3_SECRET_KEY");

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");
    let s3 = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("s3");
    s3.ensure_bucket().await.expect("bucket");

    let run = Uuid::now_v7();
    let feed_title = format!("Rss Org {run}");
    let feed_xml = rss_body(&feed_title, &format!("guid-{run}"));
    let feed_url = serve_rss(feed_xml.clone()).await;
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
        collection_ids: vec![collection.id],
        checkpoint: ConnectorCheckpoint::default(),
        now,
    };
    let collected = rss.collect(&ctx).await.expect("collect");
    assert!(collected.fetched, "應抓到 feed body，不是 304");
    let new_ev = collected.evidence.expect("evidence");
    assert_eq!(new_ev.body, feed_xml.as_bytes());

    let items = rss.parse(&new_ev.body).await.expect("parse");
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].title.as_deref(), Some("CVE-2026-0001"));
    assert_eq!(
        items[0].attributes["feed_title"],
        json!(feed_title),
        "parse 必須帶出 channel title：{}",
        items[0].attributes
    );

    let stored = rss.create_raw_evidence(new_ev).await.expect("persist");
    rss.update_checkpoint(&connector, &collected.checkpoint)
        .await
        .expect("checkpoint");

    let meta = pg
        .get_raw_evidence(stored.id)
        .await
        .expect("get meta")
        .expect("raw evidence 應存在 Postgres");
    assert_eq!(meta.sha256, sha256_hex(feed_xml.as_bytes()));
    assert_eq!(meta.source_url, feed_url);
    assert_eq!(meta.http_status, Some(200));
    assert_eq!(meta.content_length, Some(feed_xml.len() as i64));
    let linked = pg
        .list_collections_by_raw_evidence(stored.id, 100)
        .await
        .expect("list collections");
    assert_eq!(
        linked,
        vec![collection.id],
        "CollectContext.collection_ids 必須寫進 raw_evidence_collections"
    );

    let blob = s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("物件儲存應有 body");
    assert_eq!(
        blob,
        feed_xml.as_bytes(),
        "讀回的 body 必須與假 feed 完全一致"
    );
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

    // feed 標題要一路走到 Organization，不能只停在 parse。
    let outcome = Normalizer::new(pg.clone(), s3.clone(), None, MetricsRegistry::new())
        .normalize_raw(stored.id)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = outcome else {
        panic!("預期 Created，得到 {outcome:?}");
    };
    assert_eq!(document_ids.len(), 1);
    let document_id = document_ids[0];
    let doc = pg
        .get_document(document_id)
        .await
        .expect("get document")
        .expect("Document");
    assert_eq!(
        doc.attributes["feed_title"],
        json!(feed_title),
        "normalizer 必須把 feed_title 寫進 attributes：{}",
        doc.attributes
    );
    let objs = pg
        .list_collection_objects(collection.id, 100)
        .await
        .expect("objects");
    assert!(
        objs.contains(&document_id),
        "文件應繼承原始證據的集合，實際 {objs:?}"
    );

    let extract = EntityWorker::new(
        pg.clone(),
        None,
        MetricsRegistry::new(),
        ExtractionBounds::default(),
    )
    .extract_document(document_id)
    .await
    .expect("extract");
    assert!(
        matches!(extract, ExtractOutcome::Extracted { .. }),
        "預期 Extracted，實際 {extract:?}"
    );
    let organization = pg
        .find_entity_by_normalized_name(EntityType::Organization, &feed_title.to_lowercase())
        .await
        .expect("query org")
        .expect("attributes.feed_title 必須產生 Organization");
    let org_rels = pg
        .list_relationships_by_object(organization.id, 100)
        .await
        .expect("org rels");
    assert!(
        org_rels.iter().any(|r| r.source_object_id == document_id
            && r.relationship_type == RelationshipType::PublishedBy),
        "Document → Organization 必須是 published_by，實際 {org_rels:?}"
    );
}
