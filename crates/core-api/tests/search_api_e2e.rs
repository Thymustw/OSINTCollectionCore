//! SPEC §26 Acceptance A 與 E 的完整版，從 `POST /api/v1/search` 這一端驗證。
//!
//! ```text
//! 假 RSS → collector → normalizer → deduplicator → entity-worker → indexer
//!        → POST /api/v1/search → hit.raw_evidence_id → RawEvidence → Source / Connector
//! ```
//!
//! # 為什麼要走完整管線而不是直接塞 Document
//!
//! Acceptance A／E 要證明的是「真實流程產生的資料可搜尋、可回溯」。
//! 用手工塞的 Document 驗證等於自己給自己出題——normalizer 沒寫
//! `attributes.raw_evidence_id` 這種問題就驗不出來，而那正好會讓 Acceptance E
//! 在第一步就斷掉。
//!
//! 只連本機 Docker（Postgres／MinIO／Redpanda／OpenSearch 19200），不打外部網路。

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Router, http};
use chrono::Utc;
use collector::{CollectOutcome, CollectorRunner, RunBounds};
use core_api::{AppState, AuthState, RateLimiter, SearchState, ready_always, router};
use core_events::EventProducer;
use core_jobs::JobService;
use core_model::{Connector, NetworkRule, Source, SourceType};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use deduplicator::{DedupBounds, DedupOutcome, Deduplicator};
use entity_worker::{EntityWorker, ExtractionBounds};
use http_body_util::BodyExt;
use indexer::{IndexBounds, Indexer, PrepareOutcome};
use normalizer::{NormalizeOutcome, Normalizer};
use serde_json::{Value, json};
use storage_core::RelationalStore;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_s3,
    verify_not_opencti_search,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use tower::ServiceExt;
use uuid::Uuid;

struct Stack {
    pg: PostgresCanonicalStore,
    s3: S3ObjectStore,
    os: OpenSearchStore,
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
    let search_url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    verify_not_opencti_search(&search_url).expect("OpenSearch 埠隔離");
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
    let os = OpenSearchStore::connect(&search_url)
        .expect("opensearch")
        .with_refresh_on_write(true);
    // 本機 9200 是 OpenCTI 的 Elasticsearch。這一步不是形式。
    assert_opensearch_identity(&os.cluster_info().await.expect("GET /"))
        .expect("必須是 OpenSearch");

    Stack {
        pg,
        s3,
        os,
        brokers,
    }
}

struct TestApi {
    app: Router,
    token: String,
}

fn build_api(stack: &Stack, index: &str, role: Role) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let token = jwt.issue("search-e2e", role).expect("issue");
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        // 這個 e2e 只驗搜尋路徑，不接 canonical store 與物件儲存。
        store: None,
        objects: None,
        jobs: None,
        merge: None,
        resolver: None,
        graph_resolver: None,
        import: None,
        search: Some(Arc::new(SearchState {
            store: stack.os.clone(),
            index: index.to_string(),
        })),
        ready: ready_always(),
        // 這支 e2e 不驗 ops health，給空清單。
        backends: core_api::ReadyProbe::new(Vec::new()),
        // 這支測試不驗 /ops/queues。
        queues: None,
        backends_missing: Vec::new(),
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config: core_config::ImportSection::default(),
        object_bucket: String::new(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        token,
    }
}

/// 送一次 `POST /api/v1/search`。`token` 為 `None` 時不帶 Authorization。
async fn post_search(api: &TestApi, body: Value, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/search")
        .header(http::header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let request = builder.body(Body::from(body.to_string())).unwrap();
    let response = api.app.clone().oneshot(request).await.expect("call api");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

// ---------------------------------------------------------------------------
// fixture：假 RSS server
// ---------------------------------------------------------------------------

fn rss_body(guid: &str, link: &str, title: &str, description: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Search Fixture</title>
    <link>http://127.0.0.1/feed</link>
    <description>e2e</description>
    <item>
      <title>{title}</title>
      <link>{link}</link>
      <guid>{guid}</guid>
      <description>{description}</description>
    </item>
  </channel>
</rss>
"#
    )
}

async fn serve_feed(body: String) -> String {
    let app = Router::new().route("/rss.xml", get(move || std::future::ready(body.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/rss.xml", addr.port())
}

async fn seed_source(pg: &PostgresCanonicalStore, feed_url: &str) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("search-e2e-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: Some("search-e2e".into()),
        base_url: Some(feed_url.to_string()),
        description: Some("search API e2e".into()),
        language: Some("en".into()),
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

async fn seed_connector(pg: &PostgresCanonicalStore, source: &Source, feed_url: &str) -> Connector {
    let now = Utc::now();
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "search API e2e 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("search-e2e-connector-{}", Uuid::now_v7()),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({ "url": feed_url }),
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({ "per_second": 20.0, "burst": 20.0 }),
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

// ---------------------------------------------------------------------------
// Acceptance A + E
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acceptance_a_and_e_rss_to_search_and_back_to_connector() {
    let stack = connect_stack().await;
    let index = format!("osint-documents-api-e2e-{}", Uuid::now_v7().simple());

    let run = Uuid::now_v7();
    let serial = run.as_u128() % 10_000_000;
    let cve = format!("CVE-2026-{serial:07}");
    let tag = format!("osintapi{}", run.simple());

    // ---- 0) 假 RSS server ----
    let feed = rss_body(
        &format!("guid-{run}"),
        &format!("http://127.0.0.1/advisory/{run}"),
        &format!("{tag} ransomware advisory"),
        &format!(
            "{tag} A lockbit ransomware campaign. Tracked as {cve}. Contact soc@t{serial:07}.example.com ."
        ),
    );
    let feed_url = serve_feed(feed).await;
    let source = seed_source(&stack.pg, &feed_url).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    // ---- 1) collector ----
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "search-e2e").expect("producer"));
    let jobs = Arc::new(JobService::new(stack.pg.clone(), Some(producer.clone())));
    let collector = CollectorRunner::new(
        stack.pg.clone(),
        stack.s3.clone(),
        producer,
        jobs,
        RunBounds::new(2, 1),
        MetricsRegistry::new(),
    );
    let CollectOutcome::Collected { raw_evidence_id } = collector
        .run_connector(connector.clone())
        .await
        .expect("collect")
    else {
        panic!("假 server 每次都回 200，應為 Collected");
    };

    // ---- 2) normalizer ----
    let NormalizeOutcome::Created { document_ids } = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    )
    .normalize_raw(raw_evidence_id)
    .await
    .expect("normalize") else {
        panic!("預期 Created");
    };
    let document_id = document_ids[0];

    // ---- 3) deduplicator ----
    let dedup_outcome = Deduplicator::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        DedupBounds::default(),
    )
    .dedup_document(document_id)
    .await
    .expect("dedup");
    assert!(
        matches!(dedup_outcome, DedupOutcome::Canonical { .. }),
        "這一份應是 canonical，實際 {dedup_outcome:?}"
    );

    // ---- 4) entity-worker ----
    EntityWorker::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        ExtractionBounds::default(),
    )
    .extract_document(document_id)
    .await
    .expect("extract");

    // ---- 5) indexer ----
    let service = Indexer::new(
        stack.pg.clone(),
        stack.os.clone(),
        None,
        MetricsRegistry::new(),
        index.clone(),
        IndexBounds::default(),
    );
    service.ensure_index().await.expect("ensure index");
    let PrepareOutcome::Ready(document) = service.prepare(document_id).await.expect("prepare")
    else {
        panic!("預期 Ready");
    };
    let report = service.flush(vec![*document]).await.expect("flush");
    assert_eq!(
        report.indexed, 1,
        "索引失敗：{:?}",
        report.permanent_failures
    );

    // ---- 6) POST /api/v1/search ----
    let api = build_api(&stack, &index, Role::Viewer);
    let (status, body) = post_search(
        &api,
        json!({ "query": "ransomware", "source_id": source.id }),
        Some(&api.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "搜尋回應：{body}");
    assert_eq!(body["total"], json!(1), "Acceptance A：應搜到一筆 {body}");

    let hit = &body["hits"][0];
    assert_eq!(hit["document_id"], json!(document_id.to_string()));
    assert!(
        hit["snippet"].as_str().is_some_and(|s| s.contains("<em>")),
        "全文查詢應有 highlight 片段，實際 {hit}"
    );
    assert!(
        hit["entities"]
            .as_array()
            .is_some_and(|list| list.iter().any(|e| e["normalized_name"]
                .as_str()
                .is_some_and(|n| n.eq_ignore_ascii_case(&cve)))),
        "hit 應帶抽出的 CVE：{hit}"
    );

    // ---- 7) Acceptance E：從 search result 一路反查 ----
    // 刻意只用 hit 裡的欄位，不使用上面已知的中間變數當捷徑。
    let traced_raw_id = hit["raw_evidence_id"]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .expect("步驟 1：search hit 必須帶 raw_evidence_id，否則鏈在第一步就斷");

    let traced_raw = stack
        .pg
        .get_raw_evidence(traced_raw_id)
        .await
        .expect("query")
        .expect("步驟 2：RawEvidence 必須查得到");
    assert_eq!(traced_raw.id, raw_evidence_id, "追回來的必須是同一筆證據");

    let traced_source = stack
        .pg
        .get_source(traced_raw.source_id)
        .await
        .expect("query")
        .expect("步驟 3：Source 必須查得到");
    assert_eq!(traced_source.id, source.id);

    let traced_connector = stack
        .pg
        .get_connector(traced_raw.connector_id)
        .await
        .expect("query")
        .expect("步驟 4：Connector 必須查得到");
    assert_eq!(traced_connector.id, connector.id);
    assert_eq!(traced_connector.source_id, traced_source.id);

    // hit 上的 source_id／connector_id 必須與回查結果一致——
    // 不一致代表投影寫錯了關聯，而搜尋結果看起來完全正常。
    assert_eq!(
        hit["source_id"].as_str(),
        Some(traced_source.id.to_string().as_str())
    );
    assert_eq!(
        hit["connector_id"].as_str(),
        Some(traced_connector.id.to_string().as_str())
    );

    // ---- 8) entity 過濾也走得通（SPEC §18） ----
    let (status, body) = post_search(
        &api,
        json!({
            "entity": { "type": "vulnerability", "name": cve },
            "source_id": source.id,
        }),
        Some(&api.token),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["total"], json!(1), "entity 過濾應命中同一份：{body}");
    assert_eq!(
        body["hits"][0]["document_id"],
        json!(document_id.to_string())
    );

    let _ = stack.os.delete_index(&index).await;
}

// ---------------------------------------------------------------------------
// RBAC 與請求驗證
// ---------------------------------------------------------------------------

#[tokio::test]
async fn search_requires_authentication_and_allows_viewer() {
    let stack = connect_stack().await;
    let index = format!("osint-documents-api-e2e-{}", Uuid::now_v7().simple());
    let api = build_api(&stack, &index, Role::Viewer);

    let (status, body) = post_search(&api, json!({ "query": "anything" }), None).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "沒有 token 必須 401，實際 {body}"
    );
    assert_eq!(body["error"], json!("unauthorized"));

    let (status, _) = post_search(&api, json!({ "query": "x" }), Some("Bearer-garbage")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "壞 token 也要 401");

    // viewer 是唯讀角色，必須查得到（搜尋不需要 write）。
    // index 還不存在時回 200 + 0 筆，不是 500。
    let (status, body) = post_search(&api, json!({ "query": "x" }), Some(&api.token)).await;
    assert_eq!(status, StatusCode::OK, "viewer 應可搜尋，實際 {body}");
    assert_eq!(body["total"], json!(0));
}

#[tokio::test]
async fn bad_requests_are_400_with_actionable_messages() {
    let stack = connect_stack().await;
    let index = format!("osint-documents-api-e2e-{}", Uuid::now_v7().simple());
    let api = build_api(&stack, &index, Role::Viewer);

    // 語法錯
    let (status, body) =
        post_search(&api, json!({ "query": "\"unclosed" }), Some(&api.token)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["message"].as_str().is_some_and(|m| m.contains("補上")),
        "錯誤訊息要說怎麼修：{body}"
    );

    // 日期區間顛倒
    let (status, body) = post_search(
        &api,
        json!({ "date_from": "2026-09-10T00:00:00Z", "date_to": "2026-09-01T00:00:00Z" }),
        Some(&api.token),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body["message"].as_str().is_some_and(|m| m.contains("對調")));

    // 打錯欄位名不該被靜默忽略——否則使用者以為自己過濾了，其實沒有。
    let (status, _) = post_search(&api, json!({ "langauge": "en" }), Some(&api.token)).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "未知欄位必須被拒絕（serde deny_unknown_fields）"
    );

    // limit 被夾住，不會變成 DoS。
    let (status, body) = post_search(&api, json!({ "limit": 100000 }), Some(&api.token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
}
