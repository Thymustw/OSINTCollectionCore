//! 假 RSS → collector → RawEvidence → Redpanda `raw.collected` → normalizer → Document。
//! 不連真實外網；本機 OpenCTI 埠隔離沿用 storage-core conformance。

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use chrono::Utc;
use collector::{CollectOutcome, CollectorRunner, RunBounds};
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_jobs::JobService;
use core_model::{Connector, NetworkRule, Source, SourceType};
use core_observability::MetricsRegistry;
use normalizer::{NormalizeOutcome, Normalizer};
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

async fn serve_rss() -> String {
    let app = Router::new().route("/rss.xml", get(|| async { RSS }));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/rss.xml", addr.port())
}

async fn seed_rss_connector(
    pg: &PostgresCanonicalStore,
    feed_url: &str,
    connector_type: &str,
) -> Connector {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("e2e-rss-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: None,
        base_url: Some(feed_url.to_string()),
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
        name: format!("e2e-rss-connector-{}", Uuid::now_v7()),
        connector_type: connector_type.into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({ "url": feed_url }),
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({ "per_second": 10.0, "burst": 10.0 }),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    pg.put_connector(&connector).await.expect("connector");
    connector
}

fn runner(stack: &Stack, producer: Arc<EventProducer>) -> CollectorRunner {
    let jobs = Arc::new(JobService::new(stack.pg.clone(), Some(producer.clone())));
    CollectorRunner::new(
        stack.pg.clone(),
        stack.s3.clone(),
        producer,
        jobs,
        RunBounds::new(2, 1),
        MetricsRegistry::new(),
    )
}

async fn wait_for_event(
    consumer: &EventConsumer,
    want_field: &str,
    want_value: &str,
) -> core_events::EventEnvelope {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "等到 {want_field}={want_value} 逾時。請確認 Redpanda 在跑、topic 可自動建立"
        );
        let envelope = consumer
            .next_envelope(remaining)
            .await
            .expect("consume 失敗。請確認 osint-core-redpanda-1 在跑（埠 9092）");
        let got = envelope
            .payload
            .get(want_field)
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if got == want_value {
            let _ = consumer.commit_last();
            return envelope;
        }
    }
}

fn derived_count(rows: &[core_model::Provenance]) -> usize {
    rows.iter().filter(|p| p.action == "derived_from").count()
}

fn normalized_count(rows: &[core_model::Provenance]) -> usize {
    rows.iter().filter(|p| p.action == "normalized").count()
}

#[tokio::test]
async fn rss_collect_publish_normalize_round_trip() {
    let stack = connect_stack().await;
    let feed_url = serve_rss().await;
    let connector = seed_rss_connector(&stack.pg, &feed_url, "rss").await;

    let probe = Uuid::now_v7();
    let raw_group = format!("osint-e2e-raw-{probe}");
    let obj_group = format!("osint-e2e-obj-{probe}");
    let raw_consumer = EventConsumer::connect(
        &stack.brokers,
        &raw_group,
        &[EventTopic::RawCollected.as_str()],
    )
    .expect("raw.collected consumer");
    let obj_consumer = EventConsumer::connect(
        &stack.brokers,
        &obj_group,
        &[EventTopic::ObjectNormalized.as_str()],
    )
    .expect("object.normalized consumer");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "collector-e2e").expect("producer"));
    let collector = runner(&stack, producer.clone());
    let outcome = collector
        .run_connector(connector.clone())
        .await
        .expect("collect");
    let CollectOutcome::Collected { raw_evidence_id } = outcome else {
        panic!("預期 Collected，得到 {outcome:?}");
    };

    let meta = stack
        .pg
        .get_raw_evidence(raw_evidence_id)
        .await
        .expect("get meta")
        .expect("RawEvidence 應寫進 Postgres");
    assert_eq!(meta.source_url, feed_url);
    assert_eq!(meta.http_status, Some(200));
    let blob = stack
        .s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("MinIO 應有 body");
    assert_eq!(blob, RSS.as_bytes());

    let reloaded = stack
        .pg
        .get_connector(connector.id)
        .await
        .expect("reload")
        .expect("connector");
    assert_eq!(reloaded.error_count, 0);
    assert!(reloaded.last_success.is_some());
    assert_eq!(reloaded.status, "idle");

    let collected = wait_for_event(
        &raw_consumer,
        "raw_evidence_id",
        &raw_evidence_id.to_string(),
    )
    .await;
    assert_eq!(collected.event_type, "raw.collected");
    assert_eq!(collected.source_service, "collector-e2e");

    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        Some(producer),
        MetricsRegistry::new(),
    );
    let created = normalizer
        .handle_payload(&collected.payload)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = created else {
        panic!("預期 Created，得到 {created:?}");
    };
    assert_eq!(document_ids.len(), 1, "這份 fixture 只有一則 item");

    let doc = stack
        .pg
        .get_document(document_ids[0])
        .await
        .expect("get document")
        .expect("Document 應寫進 Postgres");
    assert_eq!(doc.title.as_deref(), Some("CVE-2026-0001"));
    assert_eq!(doc.object_type, core_model::DocumentType::Article);
    assert_eq!(doc.summary.as_deref(), Some("fixture item"));
    assert_eq!(doc.attributes["raw_evidence_id"], json!(raw_evidence_id));

    let normalized = wait_for_event(
        &obj_consumer,
        "raw_evidence_id",
        &raw_evidence_id.to_string(),
    )
    .await;
    assert_eq!(normalized.event_type, "object.normalized");
    assert_eq!(normalized.payload["count"], 1);
}

#[tokio::test]
async fn normalize_same_raw_evidence_twice_is_idempotent() {
    let stack = connect_stack().await;
    let feed_url = serve_rss().await;
    let connector = seed_rss_connector(&stack.pg, &feed_url, "rss").await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "collector-e2e-idem").expect("producer"));
    let collector = runner(&stack, producer);
    let outcome = collector.run_connector(connector).await.expect("collect");
    let CollectOutcome::Collected { raw_evidence_id } = outcome else {
        panic!("預期 Collected，得到 {outcome:?}");
    };

    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );
    let first = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("first");
    let NormalizeOutcome::Created { document_ids } = first else {
        panic!("第一次應 Created，得到 {first:?}");
    };
    assert_eq!(document_ids.len(), 1);

    let second = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("second");
    match second {
        NormalizeOutcome::AlreadyDone {
            document_ids: again,
        } => {
            assert_eq!(again, document_ids, "第二次應回同一組 Document id");
        }
        other => panic!("第二次應 AlreadyDone，得到 {other:?}"),
    }

    let rows = stack
        .pg
        .list_provenance_by_raw_evidence(raw_evidence_id)
        .await
        .expect("list provenance");
    assert_eq!(normalized_count(&rows), 1, "normalized claim 只能有一列");
    assert_eq!(derived_count(&rows), 1, "不該因重跑多寫 derived_from");
    let stored = stack
        .pg
        .get_document(document_ids[0])
        .await
        .expect("get")
        .expect("原 Document 還在");
    assert_eq!(stored.title.as_deref(), Some("CVE-2026-0001"));
}

#[tokio::test]
async fn concurrent_normalize_hits_conflict_and_keeps_one_document() {
    let stack = connect_stack().await;
    let feed_url = serve_rss().await;
    let connector = seed_rss_connector(&stack.pg, &feed_url, "rss").await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "collector-e2e-race").expect("producer"));
    let collector = runner(&stack, producer);
    let outcome = collector.run_connector(connector).await.expect("collect");
    let CollectOutcome::Collected { raw_evidence_id } = outcome else {
        panic!("預期 Collected，得到 {outcome:?}");
    };

    let a = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );
    let b = a.clone();
    let left = tokio::spawn(async move { a.normalize_raw(raw_evidence_id).await });
    let right = tokio::spawn(async move { b.normalize_raw(raw_evidence_id).await });
    let left = left.await.expect("join left").expect("normalize left");
    let right = right.await.expect("join right").expect("normalize right");

    let outcomes = [left, right];
    let created = outcomes
        .iter()
        .filter(|o| matches!(o, NormalizeOutcome::Created { .. }))
        .count();
    let already = outcomes
        .iter()
        .filter(|o| matches!(o, NormalizeOutcome::AlreadyDone { .. }))
        .count();
    assert_eq!(created, 1, "並發兩次應只有一次 Created，實際 {outcomes:?}");
    assert_eq!(
        already, 1,
        "輸家應走 unique index Conflict → AlreadyDone，實際 {outcomes:?}"
    );

    let rows = stack
        .pg
        .list_provenance_by_raw_evidence(raw_evidence_id)
        .await
        .expect("list provenance");
    assert_eq!(normalized_count(&rows), 1);
    assert_eq!(derived_count(&rows), 1, "並發輸家不可再寫 Document");
}

#[tokio::test]
async fn unknown_connector_type_is_skipped() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "collector-e2e-unknown").expect("producer"),
    );
    let collector = runner(&stack, producer);
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: "e2e-unknown-source".into(),
        source_type: SourceType::ManualUpload,
        platform: None,
        base_url: Some("http://127.0.0.1/file".into()),
        description: None,
        language: None,
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    stack.pg.put_source(&source).await.expect("source");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "e2e-manual-upload".into(),
        connector_type: "manual_upload".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({}),
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({}),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    stack.pg.put_connector(&connector).await.expect("connector");
    let outcome = collector
        .run_connector(connector)
        .await
        .expect("未知種類不可 panic");
    assert_eq!(
        outcome,
        CollectOutcome::SkippedUnknownType {
            connector_type: "manual_upload".into()
        }
    );
}

#[tokio::test]
async fn unknown_content_type_is_skipped_not_panic() {
    let stack = connect_stack().await;
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: "e2e-pdf-source".into(),
        source_type: SourceType::ManualUpload,
        platform: None,
        base_url: None,
        description: None,
        language: None,
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    stack.pg.put_source(&source).await.expect("source");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "e2e-pdf-connector".into(),
        connector_type: "manual".into(),
        version: "0.1.0".into(),
        enabled: true,
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
    };
    stack.pg.put_connector(&connector).await.expect("connector");

    let id = Uuid::now_v7();
    let path = format!("raw/{}/{}/{}", source.id, now.format("%Y/%m/%d"), id);
    let body = b"%PDF-1.4 fixture";
    stack
        .s3
        .put(&path, body, Some("application/pdf"))
        .await
        .expect("put pdf");
    let evidence = core_model::RawEvidence {
        id,
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: Some("pdf-1".into()),
        source_url: "http://127.0.0.1/file.pdf".into(),
        retrieved_at: now,
        content_type: Some("application/pdf".into()),
        mime_type: Some("application/pdf".into()),
        content_length: Some(body.len() as i64),
        sha256: "ab".repeat(32),
        storage_path: path,
        http_status: Some(200),
        http_headers: json!({}),
        metadata: json!({}),
        collector_version: "0.1.0".into(),
    };
    stack
        .pg
        .insert_raw_evidence(&evidence)
        .await
        .expect("insert evidence");

    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );
    let outcome = normalizer
        .normalize_raw(id)
        .await
        .expect("未知 content type 不可 panic");
    assert_eq!(
        outcome,
        NormalizeOutcome::SkippedUnsupported {
            content_type: Some("application/pdf".into())
        }
    );
    let rows = stack
        .pg
        .list_provenance_by_raw_evidence(id)
        .await
        .expect("list");
    assert!(
        rows.is_empty(),
        "跳過時不該寫 provenance／Document，實際 {rows:?}"
    );
}

const HTML: &str = r#"<!doctype html>
<html>
<head>
  <title>CVE-2026-0001 advisory</title>
  <meta name="description" content="fixture page">
</head>
<body>
  <article>
    <p>This is the main body text.</p>
  </article>
</body>
</html>
"#;

const JSON_API: &str = r#"{"items":[{"id":"CVE-2026-0001"}]}"#;

async fn serve_html() -> String {
    let app = Router::new().route(
        "/page.html",
        get(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "text/html; charset=utf-8")],
                HTML,
            )
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/page.html", addr.port())
}

async fn serve_json() -> String {
    let app = Router::new().route(
        "/v1/items",
        get(|| async {
            (
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                JSON_API,
            )
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/v1/items", addr.port())
}

async fn seed_http_connector(
    pg: &PostgresCanonicalStore,
    url: &str,
    source_type: SourceType,
    connector_type: &str,
    configuration: serde_json::Value,
) -> Connector {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("e2e-{connector_type}-{}", Uuid::now_v7()),
        source_type,
        platform: None,
        base_url: Some(url.to_string()),
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
        reason: "e2e 本機假 HTTP".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("e2e-{connector_type}-connector-{}", Uuid::now_v7()),
        connector_type: connector_type.into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration,
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({ "per_second": 10.0, "burst": 10.0 }),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    pg.put_connector(&connector).await.expect("connector");
    connector
}

#[tokio::test]
async fn static_web_collect_normalize_webpage() {
    let stack = connect_stack().await;
    let page_url = serve_html().await;
    let connector = seed_http_connector(
        &stack.pg,
        &page_url,
        SourceType::StaticWeb,
        "static_web",
        json!({ "url": page_url }),
    )
    .await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "collector-e2e-web").expect("producer"));
    let collector = runner(&stack, producer);
    let outcome = collector.run_connector(connector).await.expect("collect");
    let CollectOutcome::Collected { raw_evidence_id } = outcome else {
        panic!("預期 Collected，得到 {outcome:?}");
    };

    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );
    let created = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = created else {
        panic!("預期 Created，得到 {created:?}");
    };
    assert_eq!(document_ids.len(), 1);
    let doc = stack
        .pg
        .get_document(document_ids[0])
        .await
        .expect("get document")
        .expect("Document 應寫進 Postgres");
    assert_eq!(doc.object_type, core_model::DocumentType::WebPage);
    assert_eq!(doc.title.as_deref(), Some("CVE-2026-0001 advisory"));
    assert_eq!(doc.summary.as_deref(), Some("fixture page"));
    assert!(
        doc.body
            .as_deref()
            .is_some_and(|b| b.contains("main body text")),
        "正文應抽出，實際 {:?}",
        doc.body
    );
}

#[tokio::test]
async fn rest_api_collect_normalize_skips_json() {
    let stack = connect_stack().await;
    let api_url = serve_json().await;
    let connector = seed_http_connector(
        &stack.pg,
        &api_url,
        SourceType::RestApi,
        "rest_api",
        json!({ "url": api_url, "method": "GET" }),
    )
    .await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "collector-e2e-rest").expect("producer"));
    let collector = runner(&stack, producer);
    let outcome = collector.run_connector(connector).await.expect("collect");
    let CollectOutcome::Collected { raw_evidence_id } = outcome else {
        panic!("預期 Collected，得到 {outcome:?}");
    };
    let meta = stack
        .pg
        .get_raw_evidence(raw_evidence_id)
        .await
        .expect("get meta")
        .expect("RawEvidence 應寫進 Postgres");
    assert_eq!(meta.mime_type.as_deref(), Some("application/json"));

    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );
    let outcome = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("JSON 必須安全跳過，不可 panic");
    assert_eq!(
        outcome,
        NormalizeOutcome::SkippedUnsupported {
            content_type: Some("application/json".into())
        }
    );
    let rows = stack
        .pg
        .list_provenance_by_raw_evidence(raw_evidence_id)
        .await
        .expect("list");
    assert!(
        rows.is_empty(),
        "JSON 跳過時不該寫 provenance／Document，實際 {rows:?}"
    );
}
