//! 端到端：`POST /api/v1/import` → RawEvidence（Postgres + 物件儲存）→ Redpanda
//! `raw.collected` → normalizer → Document。
//!
//! 只連本機 Docker 服務，不打外部網路；埠隔離沿用 storage-core conformance
//! （本機 8080／9000 可能是別的系統）。

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use connector_sdk::StoreEvidenceSink;
use core_api::{AppState, AuthState, ImportState, RateLimiter, ready_always, router};
use core_config::ImportSection;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_model::{Collection, EntityType, RelationshipType, Source, SourceType};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use entity_worker::{EntityWorker, ExtractOutcome, ExtractionBounds};
use http_body_util::BodyExt;
use normalizer::{NormalizeOutcome, Normalizer};
use serde_json::{Value, json};
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{ObjectStore, RelationalStore};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tower::ServiceExt;
use uuid::Uuid;

const BOUNDARY: &str = "X-OSINT-IMPORT-E2E";

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
    let access = required_env("S3_ACCESS_KEY").expect("S3_ACCESS_KEY");
    let secret = required_env("S3_SECRET_KEY").expect("S3_SECRET_KEY");
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

/// 建一個匯入用 Source。匯入不需要 base_url，也不需要 NetworkRule（不對外連線）。
async fn seed_source(pg: &PostgresCanonicalStore, source_type: SourceType) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("e2e-import-{}", Uuid::now_v7()),
        source_type,
        platform: None,
        base_url: None,
        description: Some("import e2e".into()),
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

async fn seed_collection(pg: &PostgresCanonicalStore, name: &str) -> Collection {
    let now = Utc::now();
    let collection = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: name.into(),
        description: None,
        status: "active".into(),
        priority: 0,
        created_at: now,
        updated_at: now,
    };
    pg.put_collection(&collection).await.expect("collection");
    collection
}

fn entity_worker(stack: &Stack) -> EntityWorker {
    EntityWorker::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        ExtractionBounds::default(),
    )
}

struct TestApi {
    app: Router,
    token: String,
    audit: MemoryAuditLog,
}

fn build_api(stack: &Stack, producer: Arc<EventProducer>, import_config: ImportSection) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let token = jwt.issue("e2e-operator", Role::Operator).expect("issue");
    let audit = MemoryAuditLog::new();
    let sink = StoreEvidenceSink::new(stack.pg.clone(), stack.s3.clone());
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(audit.clone()),
        store: Some(Arc::new(stack.pg.clone())),
        objects: Some(Arc::new(stack.s3.clone())),
        jobs: None,
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
        // 這個 e2e 只驗匯入路徑，不接搜尋投影。
        search: None,
        semantic_search: None,
        hybrid_weights: core_config::HybridSearchSection::default(),
        ready: ready_always(),
        // 這支 e2e 不驗 ops health，給空清單。
        backends: core_api::ReadyProbe::new(Vec::new()),
        // 這支測試不驗 /ops/queues。
        queues: None,
        backends_missing: Vec::new(),
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config,
        stix_config: core_config::StixSection::default(),
        auto_approval_max_concurrent: 2,
        object_bucket: String::new(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        token,
        audit,
    }
}

fn multipart_body(request_json: &str, filename: &str, file_bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"request\"\r\n\r\n");
    body.extend_from_slice(request_json.as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    // 刻意宣告一個錯的 Content-Type：解析方式只能由 request.kind 決定。
    body.extend_from_slice(b"Content-Type: text/html\r\n\r\n");
    body.extend_from_slice(file_bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn upload(
    api: &TestApi,
    request_json: &str,
    filename: &str,
    bytes: &[u8],
) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/import")
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .header("Authorization", format!("Bearer {}", api.token))
        .body(Body::from(multipart_body(request_json, filename, bytes)))
        .unwrap();
    let response = api.app.clone().oneshot(request).await.expect("call api");
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&body).unwrap_or(Value::Null);
    (status, json)
}

async fn wait_for_event(consumer: &EventConsumer, want: &str) -> core_events::EventEnvelope {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "等 raw.collected（raw_evidence_id={want}）逾時。請確認 Redpanda 在跑"
        );
        let envelope = consumer
            .next_envelope(remaining)
            .await
            .expect("consume 失敗。請確認 osint-core-redpanda-1 在跑（埠 9092）");
        if envelope
            .payload
            .get("raw_evidence_id")
            .and_then(Value::as_str)
            == Some(want)
        {
            let _ = consumer.commit_last();
            return envelope;
        }
    }
}

fn normalizer(stack: &Stack) -> Normalizer {
    Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    )
}

const JSON_PAYLOAD: &str = r#"[
  {"title":"CVE-2026-0001","description":"第一筆摘要","link":"http://127.0.0.1/a","published":"2026-09-10T00:00:00Z","id":"imp-1","author":"analyst"},
  {"title":"CVE-2026-0002","content":"第二筆內文","id":"imp-2"}
]"#;

const CSV_PAYLOAD: &str = "headline,summary,link,published_at,id\n\
CSV-0001,第一列,http://127.0.0.1/c1,2026-09-11,csv-1\n\
CSV-0002,第二列,http://127.0.0.1/c2,2026-09-12,csv-2\n";

#[tokio::test]
async fn json_import_round_trip_produces_documents() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::JsonImport).await;
    let group = format!("osint-e2e-import-json-{}", Uuid::now_v7());
    let consumer =
        EventConsumer::connect(&stack.brokers, &group, &[EventTopic::RawCollected.as_str()])
            .expect("raw.collected consumer");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "core-api-import-e2e").expect("producer"));
    let api = build_api(&stack, producer, ImportSection::default());

    let request = format!(
        r#"{{"source_id":"{}","kind":"json","title":"匯入測試","description":"e2e"}}"#,
        source.id
    );
    let (status, body) = upload(&api, &request, "payload.json", JSON_PAYLOAD.as_bytes()).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["record_count"], 2);
    assert_eq!(body["published"], true);
    assert_eq!(
        body["content_type"], "application/json",
        "part 宣告 text/html 也不能改變型別：{body}"
    );

    let raw_evidence_id: Uuid = body["raw_evidence_id"].as_str().unwrap().parse().unwrap();

    // 1. RawEvidence 在 Postgres。
    let meta = stack
        .pg
        .get_raw_evidence(raw_evidence_id)
        .await
        .expect("get meta")
        .expect("RawEvidence 應寫進 Postgres");
    assert_eq!(meta.source_id, source.id);
    assert_eq!(meta.content_type.as_deref(), Some("application/json"));
    assert_eq!(meta.http_status, None, "push 進來的資料沒有上游 HTTP 狀態");
    assert_eq!(meta.metadata["import"]["kind"], "json");
    assert_eq!(meta.metadata["upload"]["actor"], "e2e-operator");
    assert_eq!(meta.source_url, "import://json/payload.json");

    // 2. body 在物件儲存，且與上傳位元組完全一致。
    let blob = stack
        .s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("物件儲存應有 body");
    assert_eq!(blob, JSON_PAYLOAD.as_bytes());

    // 3. 自動配置的 connector 不會被 collector 撿去跑。
    let connector_id: Uuid = body["connector_id"].as_str().unwrap().parse().unwrap();
    let connector = stack
        .pg
        .get_connector(connector_id)
        .await
        .expect("get connector")
        .expect("connector 應自動建立");
    assert_eq!(connector.connector_type, "json_import");
    assert!(!connector.enabled, "匯入 connector 不可被排程");
    assert!(connector.schedule.is_none());

    // 4. raw.collected 真的發出去了。
    let envelope = wait_for_event(&consumer, &raw_evidence_id.to_string()).await;
    assert_eq!(envelope.event_type, "raw.collected");

    // 5. normalizer 接手，產出兩份 Document。
    let outcome = normalizer(&stack)
        .handle_payload(&envelope.payload)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = outcome else {
        panic!("預期 Created，得到 {outcome:?}");
    };
    assert_eq!(document_ids.len(), 2);
    let first = stack
        .pg
        .get_document(document_ids[0])
        .await
        .expect("get document")
        .expect("Document 應寫進 Postgres");
    assert_eq!(first.title.as_deref(), Some("CVE-2026-0001"));
    assert_eq!(first.summary.as_deref(), Some("第一筆摘要"));
    assert_eq!(first.author.as_deref(), Some("analyst"));
    assert_eq!(first.source_url.as_deref(), Some("http://127.0.0.1/a"));
    assert!(first.published_at.is_some());
    assert_eq!(first.object_type, core_model::DocumentType::Report);
    assert_eq!(first.attributes["import_kind"], "json");
    assert_eq!(first.attributes["external_id"], "imp-1");
    let second = stack
        .pg
        .get_document(document_ids[1])
        .await
        .expect("get")
        .expect("Document");
    assert_eq!(second.body.as_deref(), Some("第二筆內文"));

    // 6. 稽核紀錄。
    let entries = api.audit.entries();
    assert_eq!(entries.len(), 1, "{entries:?}");
    assert_eq!(entries[0].action, "import.upload");
    assert_eq!(entries[0].actor, "e2e-operator");
    assert_eq!(entries[0].outcome, "success");
    assert_eq!(
        entries[0].metadata["raw_evidence_id"],
        json!(raw_evidence_id)
    );
}

#[tokio::test]
async fn csv_import_with_explicit_mapping_produces_documents() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::CsvImport).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-csv").expect("producer"),
    );
    let api = build_api(&stack, producer, ImportSection::default());

    let request = format!(
        r#"{{"source_id":"{}","kind":"csv","object_type":"advisory","mapping":{{"title":"headline"}}}}"#,
        source.id
    );
    let (status, body) = upload(&api, &request, "table.csv", CSV_PAYLOAD.as_bytes()).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["record_count"], 2);
    assert_eq!(body["content_type"], "text/csv");

    let raw_evidence_id: Uuid = body["raw_evidence_id"].as_str().unwrap().parse().unwrap();
    let outcome = normalizer(&stack)
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = outcome else {
        panic!("預期 Created，得到 {outcome:?}");
    };
    assert_eq!(document_ids.len(), 2);
    let doc = stack
        .pg
        .get_document(document_ids[0])
        .await
        .expect("get")
        .expect("Document");
    assert_eq!(doc.title.as_deref(), Some("CSV-0001"));
    assert_eq!(doc.summary.as_deref(), Some("第一列"));
    assert_eq!(
        doc.object_type,
        core_model::DocumentType::Advisory,
        "object_type 應由上傳者指定"
    );
    assert_eq!(doc.attributes["import_kind"], "csv");
    assert!(doc.published_at.is_some());
}

#[tokio::test]
async fn manual_upload_is_stored_but_not_normalized() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::ManualUpload).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-manual").expect("producer"),
    );
    let api = build_api(&stack, producer, ImportSection::default());

    let pdf = b"%PDF-1.4 fixture body";
    let request = format!(
        r#"{{"source_id":"{}","kind":"manual","title":"手動上傳的報告","source_url":"http://127.0.0.1/report.pdf"}}"#,
        source.id
    );
    let (status, body) = upload(&api, &request, "../../etc/report.pdf", pdf).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(
        body["content_type"], "application/pdf",
        "未宣告 content_type 時依內容 sniff：{body}"
    );
    assert_eq!(body["record_count"], Value::Null, "manual 不拆紀錄");

    let raw_evidence_id: Uuid = body["raw_evidence_id"].as_str().unwrap().parse().unwrap();
    let meta = stack
        .pg
        .get_raw_evidence(raw_evidence_id)
        .await
        .expect("get")
        .expect("RawEvidence");
    assert_eq!(
        meta.metadata["upload"]["filename"], "report.pdf",
        "檔名只留最後一段：{}",
        meta.metadata
    );
    let blob = stack
        .s3
        .get(&meta.storage_path)
        .await
        .expect("get blob")
        .expect("物件儲存應有 body");
    assert_eq!(blob, pdf);

    let outcome = normalizer(&stack)
        .normalize_raw(raw_evidence_id)
        .await
        .expect("manual 不可 panic");
    assert_eq!(
        outcome,
        NormalizeOutcome::SkippedUnsupported {
            content_type: Some("application/pdf".into())
        }
    );
    let rows = stack
        .pg
        .list_provenance_by_raw_evidence(raw_evidence_id)
        .await
        .expect("list");
    assert!(rows.is_empty(), "跳過時不該寫 provenance：{rows:?}");
}

#[tokio::test]
async fn record_limit_rejects_before_anything_is_stored() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::JsonImport).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-limit").expect("producer"),
    );
    let api = build_api(
        &stack,
        producer,
        ImportSection {
            max_records: 1,
            ..ImportSection::default()
        },
    );

    let request = format!(r#"{{"source_id":"{}","kind":"json"}}"#, source.id);
    let (status, body) = upload(&api, &request, "payload.json", JSON_PAYLOAD.as_bytes()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("max_records"),
        "要指出是哪個上限：{message}"
    );
    assert!(
        !message.contains("raw/") && !message.contains("raw-evidence"),
        "錯誤訊息不可洩漏儲存路徑：{message}"
    );

    let entries = api.audit.entries();
    assert_eq!(entries[0].outcome, "rejected");
}

#[tokio::test]
async fn field_size_limit_is_enforced_on_upload() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::CsvImport).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-field").expect("producer"),
    );
    let api = build_api(
        &stack,
        producer,
        ImportSection {
            max_field_bytes: 32,
            ..ImportSection::default()
        },
    );

    let csv = format!("title,summary\n{},x\n", "y".repeat(200));
    let request = format!(r#"{{"source_id":"{}","kind":"csv"}}"#, source.id);
    let (status, body) = upload(&api, &request, "big.csv", csv.as_bytes()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    assert!(
        body["message"]
            .as_str()
            .unwrap()
            .contains("max_field_bytes"),
        "{body}"
    );
}

#[tokio::test]
async fn source_type_must_match_kind() {
    let stack = connect_stack().await;
    // RSS 的 Source 不可以拿來塞 CSV。
    let source = seed_source(&stack.pg, SourceType::Rss).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-mismatch").expect("producer"),
    );
    let api = build_api(&stack, producer, ImportSection::default());
    let request = format!(r#"{{"source_id":"{}","kind":"csv"}}"#, source.id);
    let (status, body) = upload(&api, &request, "a.csv", CSV_PAYLOAD.as_bytes()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("csv_import"),
        "{body}"
    );
}

#[tokio::test]
async fn repeated_uploads_reuse_the_same_import_connector() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::JsonImport).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-reuse").expect("producer"),
    );
    let api = build_api(&stack, producer, ImportSection::default());
    let request = format!(r#"{{"source_id":"{}","kind":"json"}}"#, source.id);

    let (first_status, first) = upload(&api, &request, "a.json", JSON_PAYLOAD.as_bytes()).await;
    let (second_status, second) = upload(&api, &request, "b.json", JSON_PAYLOAD.as_bytes()).await;
    assert_eq!(first_status, StatusCode::CREATED, "{first}");
    assert_eq!(second_status, StatusCode::CREATED, "{second}");
    assert_eq!(
        first["connector_id"], second["connector_id"],
        "同一個 Source 的匯入要掛在同一個 connector 上"
    );
    assert_ne!(
        first["raw_evidence_id"], second["raw_evidence_id"],
        "每次上傳都是獨立的 RawEvidence（immutable）"
    );
}

/// 兩個集合共用一個來源 → 匯入一筆（不顯式給 collection_ids）→
/// 原始證據與文件同時屬於兩個集合；`publisher` 寫進 attributes 並抽出 Organization。
#[tokio::test]
async fn two_collections_and_publisher_flow_through_import() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::JsonImport).await;
    let collection_a = seed_collection(&stack.pg, "import-e2e-a").await;
    let collection_b = seed_collection(&stack.pg, "import-e2e-b").await;
    stack
        .pg
        .link_collection_source(collection_a.id, source.id)
        .await
        .expect("link a");
    stack
        .pg
        .link_collection_source(collection_b.id, source.id)
        .await
        .expect("link b");

    let run = Uuid::now_v7();
    let publisher = format!("Import Publisher {run}");
    let title = format!("Import CVE {run}");
    let payload = format!(
        r#"[{{"title":"{title}","description":"兩集合共用來源","publisher":"{publisher}","author":"Import Author {run}"}}]"#
    );

    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-collections")
            .expect("producer"),
    );
    let api = build_api(&stack, producer, ImportSection::default());
    // 不傳 collection_ids：應走來源反查，掛上兩個集合。
    let request = format!(r#"{{"source_id":"{}","kind":"json"}}"#, source.id);
    let (status, body) = upload(&api, &request, "payload.json", payload.as_bytes()).await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert_eq!(body["record_count"], 1);

    let raw_evidence_id: Uuid = body["raw_evidence_id"].as_str().unwrap().parse().unwrap();
    let mut linked = stack
        .pg
        .list_collections_by_raw_evidence(raw_evidence_id, 100)
        .await
        .expect("list evidence collections");
    let mut expected = vec![collection_a.id, collection_b.id];
    linked.sort();
    expected.sort();
    assert_eq!(
        linked, expected,
        "沒給 collection_ids 時應反查來源掛上的兩個集合"
    );

    let outcome = normalizer(&stack)
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = outcome else {
        panic!("預期 Created，得到 {outcome:?}");
    };
    assert_eq!(document_ids.len(), 1);
    let document_id = document_ids[0];
    let doc = stack
        .pg
        .get_document(document_id)
        .await
        .expect("get document")
        .expect("Document");
    assert_eq!(doc.title.as_deref(), Some(title.as_str()));
    assert_eq!(
        doc.attributes["publisher"],
        json!(publisher),
        "JSON publisher 必須寫進 Document.attributes：{}",
        doc.attributes
    );
    assert_eq!(
        doc.author.as_deref(),
        Some(format!("Import Author {run}").as_str())
    );

    let objs_a = stack
        .pg
        .list_collection_objects(collection_a.id, 100)
        .await
        .expect("objects a");
    let objs_b = stack
        .pg
        .list_collection_objects(collection_b.id, 100)
        .await
        .expect("objects b");
    assert!(
        objs_a.contains(&document_id),
        "文件應繼承集合 A，實際 {objs_a:?}"
    );
    assert!(
        objs_b.contains(&document_id),
        "文件應繼承集合 B，實際 {objs_b:?}"
    );

    let extract = entity_worker(&stack)
        .extract_document(document_id)
        .await
        .expect("extract");
    assert!(
        matches!(extract, ExtractOutcome::Extracted { .. }),
        "預期 Extracted，實際 {extract:?}"
    );

    let organization = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Organization, &publisher.to_lowercase())
        .await
        .expect("query org")
        .expect("attributes.publisher 必須產生 Organization");
    let org_rels = stack
        .pg
        .list_relationships_by_object(organization.id, 100)
        .await
        .expect("org rels");
    assert!(
        org_rels.iter().any(|r| r.source_object_id == document_id
            && r.relationship_type == RelationshipType::PublishedBy),
        "Document → Organization 必須是 published_by，實際 {org_rels:?}"
    );
}

/// 明確給不存在的 collection_id 必須 422，且不能留下半套 RawEvidence。
#[tokio::test]
async fn import_unknown_collection_ids_is_422_and_stores_nothing() {
    let stack = connect_stack().await;
    let source = seed_source(&stack.pg, SourceType::JsonImport).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "core-api-import-e2e-bad-col").expect("producer"),
    );
    let api = build_api(&stack, producer, ImportSection::default());
    let missing = Uuid::now_v7();
    let request = format!(
        r#"{{"source_id":"{}","kind":"json","collection_ids":["{missing}"]}}"#,
        source.id
    );
    let (status, body) = upload(&api, &request, "payload.json", JSON_PAYLOAD.as_bytes()).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains(&missing.to_string()),
        "422 要列出缺的 id：{message}"
    );
    assert!(
        message.contains("沒有寫入任何原始證據"),
        "要講清楚這次沒落地：{message}"
    );

    let leftover = stack
        .pg
        .list_raw_evidence_by_source(source.id, None, 100)
        .await
        .expect("list evidence");
    assert!(
        leftover.is_empty(),
        "422 之後這個來源底下不該有 RawEvidence：{leftover:?}"
    );
}
