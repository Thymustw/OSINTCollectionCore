//! entity-worker e2e：對本機 Docker（Postgres／MinIO／Redpanda）真跑，不連外網。
//!
//! 涵蓋 SPEC §26 Acceptance D（CVE／domain／IP／email／hash 均建立 Entity）
//! 與 E（一路反查到 RawEvidence／Source／Connector），加上冪等、重複文件跳過、
//! 跨 Document 的 Entity 重用與抽取上限。
//!
//! # 共用 DB 的鐵則（比 deduplicator 更嚴格）
//!
//! Entity 的自然鍵 `(entity_type, normalized_name)` 是**全域**唯一的：
//! 兩個測試都抽到 `CVE-2026-0001` 時，它們會共用同一個 Entity 列。
//! 所以：
//! 1. 每個測試的 CVE／domain／email／hash 都含一個 run-specific 的值；
//! 2. 對「數量」的斷言一律**限縮在本次的 document id 範圍內**，
//!    不要去數「資料庫裡總共有幾個 Entity」——那個數字包含前幾次跑的殘留。

use std::collections::HashSet;
use std::sync::Arc;

use axum::Router;
use axum::routing::get;
use chrono::Utc;
use collector::{CollectOutcome, CollectorRunner, RunBounds};
use core_events::EventProducer;
use core_jobs::JobService;
use core_model::{
    Connector, Document, DocumentType, Entity, EntityIdentifier, EntityType, NetworkRule,
    RawEvidence, RelationshipType, ResolutionStatus, Source, SourceType,
};
use core_observability::MetricsRegistry;
use deduplicator::{DedupBounds, DedupOutcome, Deduplicator};
use entity_worker::{
    ACTION_ENTITY_EXTRACTED, EntityWorker, ExtractOutcome, ExtractionBounds, entity_id,
    identifier_id,
};
use normalizer::{NormalizeOutcome, Normalizer};
use serde_json::json;
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use uuid::Uuid;

const PAGE: u32 = 100;

// ---------------------------------------------------------------------------
// run-specific 的測試值
// ---------------------------------------------------------------------------

/// 這一次測試專屬的一組 IOC。每個欄位都含 run 衍生值，避免與前幾次跑的資料共用 Entity。
struct Fixture {
    run: Uuid,
    cve: String,
    domain: String,
    email: String,
    ip: String,
    sha256: String,
}

impl Fixture {
    fn new() -> Self {
        let run = Uuid::now_v7();
        // CVE 的序號只能是數字，所以不能直接用 UUID 的十六進位。
        let serial = run.as_u128() % 10_000_000;
        let label = format!("t{serial:07}");
        Self {
            cve: format!("CVE-2026-{serial:07}"),
            domain: format!("{label}.example.com"),
            email: format!("soc@{label}.example.com"),
            // 203.0.113.0/24 是 RFC 5737 的文件用網段。只有 254 個值，跨 run 會重複——
            // 所以關於 IP 的斷言一律限縮在本次的 document 範圍內，不做全域計數。
            ip: format!("203.0.113.{}", (run.as_u128() % 254) + 1),
            // 64 個十六進位字元 = SHA256 的形狀。
            sha256: format!("{}{}", run.simple(), run.simple()),
            run,
        }
    }

    /// 一篇同時含五種 IOC 的文章（SPEC §26 Acceptance D 的測試內容）。
    fn article(&self) -> String {
        format!(
            "資安公告 {cve} 影響多個版本。受影響的網站為 {domain}，\
             觀察到的攻擊來源 IP 是 {ip}。\
             通報信箱 {email}。\
             樣本 SHA256 為 {sha256}。\
             詳細說明請見 https://{domain}/advisory/{run}?utm_source=news 。",
            cve = self.cve,
            domain = self.domain,
            ip = self.ip,
            email = self.email,
            sha256 = self.sha256,
            run = self.run,
        )
    }
}

// ---------------------------------------------------------------------------
// 基礎設施
// ---------------------------------------------------------------------------

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

/// 不接 broker 的 worker。單元層級的 e2e 不需要真的發事件，
/// 少一個非確定性來源（消費者何時收到）就少一類 flaky。
fn worker(stack: &Stack) -> EntityWorker {
    EntityWorker::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        ExtractionBounds::default(),
    )
}

fn worker_with_bounds(stack: &Stack, bounds: ExtractionBounds) -> EntityWorker {
    EntityWorker::new(stack.pg.clone(), None, MetricsRegistry::new(), bounds)
}

async fn seed_source(pg: &PostgresCanonicalStore, base_url: Option<&str>) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("entity-e2e-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: Some("entity-e2e-platform".into()),
        base_url: base_url.map(str::to_string),
        description: Some("entity-worker e2e fixture".into()),
        language: Some("zh-Hant".into()),
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
        reason: "entity-worker e2e 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("entity-e2e-connector-{}", Uuid::now_v7()),
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

/// 建一份 Document + 它的 RawEvidence，不經 collector／normalizer。
///
/// 抽取器的行為測試需要精確控制文字內容，走完整管線做不到。
/// Acceptance E 另外用真實管線跑（見 `acceptance_e_...`）。
#[allow(clippy::too_many_arguments)]
async fn seed_document(
    pg: &PostgresCanonicalStore,
    source: &Source,
    connector: &Connector,
    title: &str,
    body: &str,
    author: Option<&str>,
    attributes: serde_json::Value,
    duplicate_of: Option<Uuid>,
) -> Document {
    let now = Utc::now();
    let raw_id = Uuid::now_v7();
    let evidence = RawEvidence {
        id: raw_id,
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: Some(raw_id.to_string()),
        source_url: format!("http://127.0.0.1/entity-e2e/{raw_id}"),
        retrieved_at: now,
        content_type: Some("application/rss+xml".into()),
        mime_type: Some("application/rss+xml".into()),
        content_length: Some(body.len() as i64),
        sha256: format!("{}{}", raw_id.simple(), raw_id.simple()),
        storage_path: format!("raw/{}/{raw_id}", source.id),
        http_status: Some(200),
        http_headers: json!({}),
        metadata: json!({}),
        collector_version: "0.1.0".into(),
    };
    pg.insert_raw_evidence(&evidence).await.expect("raw");

    let mut attrs = attributes;
    attrs["raw_evidence_id"] = json!(raw_id);

    let document = Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::Article,
        schema_version: "1".into(),
        title: Some(title.into()),
        body: Some(body.into()),
        summary: None,
        language: Some("zh-Hant".into()),
        author: author.map(str::to_string),
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: Some(format!("http://127.0.0.1/entity-e2e/{raw_id}")),
        canonical_url: Some(format!("http://127.0.0.1/entity-e2e/{raw_id}")),
        normalized_content_hash: Some(core_model::content_hash(Some(title), None, Some(body))),
        confidence: 0.8,
        labels: Vec::new(),
        attributes: attrs,
        external_key: None,
        simhash: None,
        duplicate_of,
    };
    pg.put_document(&document).await.expect("document");
    document
}

/// 取出這次抽取產生的結果數字。
fn extracted(outcome: &ExtractOutcome) -> (usize, usize, usize) {
    match outcome {
        ExtractOutcome::Extracted {
            entity_count,
            extraction_count,
            relationship_count,
            ..
        } => (*entity_count, *extraction_count, *relationship_count),
        other => panic!("預期 Extracted，實際 {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SPEC §26 Acceptance D
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acceptance_d_cve_domain_ip_email_hash_all_become_entities() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("公告 {}", fixture.cve),
        &fixture.article(),
        None,
        json!({}),
        None,
    )
    .await;

    let outcome = worker(&stack)
        .extract_document(document.id)
        .await
        .expect("extract");
    let (entity_count, extraction_count, relationship_count) = extracted(&outcome);
    assert!(
        entity_count >= 5,
        "至少要有五種 Entity，實際 {entity_count}"
    );
    assert!(extraction_count >= entity_count);
    assert!(relationship_count >= entity_count);

    // 逐一確認五種型別的 Entity 都建立了，而且值是這次 fixture 的值。
    let expected: [(EntityType, String); 5] = [
        (EntityType::Vulnerability, fixture.cve.to_uppercase()),
        (EntityType::Domain, fixture.domain.clone()),
        (EntityType::Ip, fixture.ip.clone()),
        (EntityType::Email, fixture.email.clone()),
        (EntityType::Hash, fixture.sha256.clone()),
    ];

    for (kind, normalized) in &expected {
        let entity = stack
            .pg
            .find_entity_by_normalized_name(*kind, normalized)
            .await
            .expect("query")
            .unwrap_or_else(|| {
                panic!("SPEC §26 Acceptance D：{kind:?} `{normalized}` 必須建立 Entity")
            });
        assert_eq!(entity.entity_type, *kind);

        // 每個 Entity 都要有指向**這份 Document** 的 extraction。
        let extractions = stack
            .pg
            .list_entity_extractions_by_entity(entity.id, PAGE)
            .await
            .expect("extractions");
        assert!(
            extractions.iter().any(|e| e.object_id == document.id),
            "{kind:?} `{normalized}` 沒有指向本次 Document 的 EntityExtraction"
        );

        // 以及一條回到 Document 的 Relationship（SPEC §11）。
        let relationships = stack
            .pg
            .list_relationships_by_object(entity.id, PAGE)
            .await
            .expect("relationships");
        let to_document: Vec<_> = relationships
            .iter()
            .filter(|r| r.source_object_id == document.id && r.target_object_id == entity.id)
            .collect();
        assert!(
            !to_document.is_empty(),
            "{kind:?} `{normalized}` 沒有 Document → Entity 的 Relationship"
        );

        // 以及每條 Relationship 的 evidence（SPEC §12：必須能回查）。
        for relationship in to_document {
            let evidence = stack
                .pg
                .list_relationship_evidence(relationship.id, PAGE)
                .await
                .expect("evidence");
            assert!(
                !evidence.is_empty(),
                "SPEC §12：relationship {} 查不到任何 evidence",
                relationship.id
            );
            assert!(
                evidence.iter().all(|e| e.object_id == document.id),
                "evidence 的 object_id 必須指回產生它的 Document"
            );
            assert!(
                evidence.iter().any(|e| e.excerpt.is_some()),
                "evidence 要帶 excerpt，否則人看不出這條關聯是憑什麼建立的"
            );
        }
    }

    // Entity id 必須是決定性的 v5——冪等的基礎。
    let cve_entity = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Vulnerability, &fixture.cve.to_uppercase())
        .await
        .expect("query")
        .expect("cve entity");
    assert_eq!(
        cve_entity.id,
        entity_id(EntityType::Vulnerability, &fixture.cve.to_uppercase()),
        "Entity id 必須等於 UUID v5(namespace, type|normalized_name)"
    );

    // SPEC §14：provenance 要記下這次抽取。
    let provenance = stack
        .pg
        .list_provenance_by_subject(document.id)
        .await
        .expect("provenance");
    let claim = provenance
        .iter()
        .find(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .expect("必須寫一列 entity_extracted 的 provenance");
    assert_eq!(claim.processor, "entity-worker");
    assert_eq!(claim.metadata["entity_count"], json!(entity_count));
}

// ---------------------------------------------------------------------------
// SPEC §26 Acceptance E：整條可追溯鏈
// ---------------------------------------------------------------------------

fn rss_body(guid: &str, link: &str, title: &str, description: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Entity Fixture</title>
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

async fn serve_body(body: String) -> String {
    let app = Router::new().route("/rss.xml", get(move || std::future::ready(body.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/rss.xml", addr.port())
}

/// 從 Entity 一路反查到 Connector，每一步都 assert 查得到。
///
/// **這個測試刻意走完整管線**（collector → normalizer → deduplicator → entity-worker），
/// 而不是直接塞 Document：Acceptance E 要證明的是「真實流程產生的資料能回查」，
/// 用手工塞的資料驗證等於自己給自己出題。
#[tokio::test]
async fn acceptance_e_entity_traces_back_to_raw_evidence_source_and_connector() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();

    let feed = rss_body(
        &format!("guid-{}", fixture.run),
        &format!("http://127.0.0.1/advisory/{}", fixture.run),
        &format!("公告 {}", fixture.cve),
        &fixture.article(),
    );
    let feed_url = serve_body(feed).await;
    let source = seed_source(&stack.pg, Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "entity-e2e-e").expect("producer"));
    let jobs = Arc::new(JobService::new(stack.pg.clone(), Some(producer.clone())));
    let collector = CollectorRunner::new(
        stack.pg.clone(),
        stack.s3.clone(),
        producer,
        jobs,
        RunBounds::new(2, 1),
        MetricsRegistry::new(),
    );

    // 1) 採集
    let CollectOutcome::Collected { raw_evidence_id } = collector
        .run_connector(connector.clone())
        .await
        .expect("collect")
    else {
        panic!("假 server 每次都回 200，應為 Collected");
    };

    // 2) 正規化
    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );
    let NormalizeOutcome::Created { document_ids } = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize")
    else {
        panic!("預期 Created");
    };
    let document_id = document_ids[0];

    // 3) 去重（這一份是 canonical）
    let dedup = Deduplicator::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        DedupBounds::default(),
    );
    let dedup_outcome = dedup.dedup_document(document_id).await.expect("dedup");
    assert!(
        matches!(dedup_outcome, DedupOutcome::Canonical { .. }),
        "這一份應是 canonical，實際 {dedup_outcome:?}"
    );

    // 4) 抽取
    let outcome = worker(&stack)
        .extract_document(document_id)
        .await
        .expect("extract");
    let (entity_count, _, _) = extracted(&outcome);
    assert!(entity_count >= 5);

    // ---- 開始反查。從 Entity 出發，完全不使用上面已知的中間變數當捷徑。----
    let entity = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Vulnerability, &fixture.cve.to_uppercase())
        .await
        .expect("query")
        .expect("步驟 1：Entity 必須查得到");

    // Entity → Relationship
    let relationships = stack
        .pg
        .list_relationships_by_object(entity.id, PAGE)
        .await
        .expect("query");
    let relationship = relationships
        .iter()
        .find(|r| {
            r.target_object_id == entity.id && r.relationship_type == RelationshipType::Mentions
        })
        .expect("步驟 2：Entity 必須有一條指向它的 mentions Relationship");

    // Relationship → RelationshipEvidence（SPEC §12）
    let evidence = stack
        .pg
        .list_relationship_evidence(relationship.id, PAGE)
        .await
        .expect("query");
    let evidence = evidence
        .first()
        .expect("步驟 3：SPEC §12 要求任何 relationship 都能回查 evidence");

    // RelationshipEvidence → Document
    let traced_document_id = evidence.object_id;
    let traced_document = stack
        .pg
        .get_document(traced_document_id)
        .await
        .expect("query")
        .expect("步驟 4：evidence.object_id 必須指向存在的 Document");
    assert_eq!(traced_document.id, document_id, "追回來的必須是同一份");

    // RelationshipEvidence → RawEvidence
    let traced_raw_id = evidence
        .raw_evidence_id
        .expect("步驟 5：evidence 必須帶 raw_evidence_id，否則 Acceptance E 的鏈在這裡斷掉");
    let traced_raw = stack
        .pg
        .get_raw_evidence(traced_raw_id)
        .await
        .expect("query")
        .expect("步驟 6：RawEvidence 必須查得到");
    assert_eq!(traced_raw.id, raw_evidence_id);

    // RawEvidence → Source
    let traced_source = stack
        .pg
        .get_source(traced_raw.source_id)
        .await
        .expect("query")
        .expect("步驟 7：Source 必須查得到");
    assert_eq!(traced_source.id, source.id);

    // RawEvidence → Connector
    let traced_connector = stack
        .pg
        .get_connector(traced_raw.connector_id)
        .await
        .expect("query")
        .expect("步驟 8：Connector 必須查得到");
    assert_eq!(traced_connector.id, connector.id);
    assert_eq!(traced_connector.source_id, source.id);

    // 額外確認 provenance 也走得通（SPEC §14 的另一條路徑）。
    let provenance = stack
        .pg
        .list_provenance_by_subject(document_id)
        .await
        .expect("query");
    let claim = provenance
        .iter()
        .find(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .expect("entity_extracted claim");
    assert_eq!(
        claim.raw_evidence_id,
        Some(raw_evidence_id),
        "provenance 也要能指回 RawEvidence"
    );
}

// ---------------------------------------------------------------------------
// 跨 Document 的 Entity 重用
// ---------------------------------------------------------------------------

#[tokio::test]
async fn same_cve_in_two_documents_reuses_one_entity_and_updates_last_seen() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let normalized = fixture.cve.to_uppercase();

    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        "第一篇",
        &format!("第一篇報導提到 {}。", fixture.cve),
        None,
        json!({}),
        None,
    )
    .await;
    let service = worker(&stack);
    service.extract_document(first.id).await.expect("first");

    let after_first = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Vulnerability, &normalized)
        .await
        .expect("query")
        .expect("entity");

    // 讓時間確實往前走，否則 last_seen 的比較可能落在同一毫秒。
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        "第二篇",
        &format!("另一篇分析同樣討論 {}。", fixture.cve),
        None,
        json!({}),
        None,
    )
    .await;
    service.extract_document(second.id).await.expect("second");

    let after_second = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Vulnerability, &normalized)
        .await
        .expect("query")
        .expect("entity");

    assert_eq!(
        after_first.id, after_second.id,
        "同一個 CVE 在兩份 Document 出現，必須對到同一個 Entity 而不是建第二個"
    );
    assert_eq!(
        after_first.first_seen, after_second.first_seen,
        "first_seen 必須保留第一次的值；每次重跑都往後推的話它就變成『最後處理時間』了"
    );
    assert!(
        after_second.last_seen > after_first.last_seen,
        "last_seen 必須更新。實際 {} → {}",
        after_first.last_seen,
        after_second.last_seen
    );

    // 兩篇各自留下一筆 extraction。
    let extractions = stack
        .pg
        .list_entity_extractions_by_entity(after_second.id, PAGE)
        .await
        .expect("query");
    let objects: HashSet<Uuid> = extractions.iter().map(|e| e.object_id).collect();
    assert!(
        objects.contains(&first.id) && objects.contains(&second.id),
        "兩份 Document 都要有自己的 EntityExtraction，實際 object_id={objects:?}"
    );
}

// ---------------------------------------------------------------------------
// 冪等
// ---------------------------------------------------------------------------

#[tokio::test]
async fn processing_the_same_document_twice_changes_nothing() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("公告 {}", fixture.cve),
        &fixture.article(),
        Some("Alice Chen"),
        json!({ "publisher": format!("Example Lab {}", fixture.run) }),
        None,
    )
    .await;

    let service = worker(&stack);
    let first = service.extract_document(document.id).await.expect("first");
    let (entities_1, extractions_1, relationships_1) = extracted(&first);

    let before_extractions = stack
        .pg
        .list_entity_extractions_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();
    let before_relationships = stack
        .pg
        .list_relationships_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();
    let before_provenance = stack
        .pg
        .list_provenance_by_subject(document.id)
        .await
        .expect("query")
        .iter()
        .filter(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .count();

    // 第二次：claim 已存在，應直接回 AlreadyDone。
    let second = service.extract_document(document.id).await.expect("second");
    assert!(
        matches!(second, ExtractOutcome::AlreadyDone { .. }),
        "第二次處理應回 AlreadyDone，實際 {second:?}"
    );

    let after_extractions = stack
        .pg
        .list_entity_extractions_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();
    let after_relationships = stack
        .pg
        .list_relationships_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();
    let after_provenance = stack
        .pg
        .list_provenance_by_subject(document.id)
        .await
        .expect("query")
        .iter()
        .filter(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .count();

    assert_eq!(before_extractions, after_extractions, "extraction 數不可變");
    assert_eq!(
        before_relationships, after_relationships,
        "relationship 數不可變"
    );
    assert_eq!(before_provenance, 1, "claim 只能有一列");
    assert_eq!(after_provenance, 1, "claim 仍然只能有一列");
    assert_eq!(before_extractions, extractions_1);
    assert!(entities_1 >= 5);
    // ⚠️ `relationship_count` 與「掛在 Document 上的關聯數」**不相等**，這是正確的：
    // 前者還含 Entity→Entity 的衍生邊（URL ──belongs_to──> Domain、
    // Email ──associated_with──> Domain），那些邊的兩端都不是 Document。
    assert!(
        before_relationships < relationships_1,
        "本次共建 {relationships_1} 條關聯，其中 {before_relationships} 條掛在 Document 上；\
         差額應該是 URL/Email → Domain 的衍生邊。相等代表衍生邊沒有被建立"
    );
}

/// claim 被刪掉之後重跑（模擬 write-then-claim 在 claim 前 crash 的情境）。
///
/// 這才是冪等真正的考驗：`AlreadyDone` 那條路只是省工，
/// **決定性的 v5 id 才是「重跑不會產生重複列」的保證**。
#[tokio::test]
async fn rerunning_without_the_claim_still_produces_no_duplicates() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("公告 {}", fixture.cve),
        &fixture.article(),
        None,
        json!({}),
        None,
    )
    .await;

    let service = worker(&stack);
    service.extract_document(document.id).await.expect("first");

    let before_extractions = stack
        .pg
        .list_entity_extractions_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();
    let before_relationships = stack
        .pg
        .list_relationships_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();

    // 刪掉 claim，模擬「寫完資料但還沒 claim 就 crash」。
    let claim = stack
        .pg
        .list_provenance_by_subject(document.id)
        .await
        .expect("query")
        .into_iter()
        .find(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .expect("claim");
    sqlx::query("DELETE FROM provenance WHERE id = $1")
        .bind(claim.id)
        .execute(stack.pg.pool())
        .await
        .expect("delete claim");

    let second = service.extract_document(document.id).await.expect("second");
    assert!(
        matches!(second, ExtractOutcome::Extracted { .. }),
        "claim 不在了，應該真的重跑一次，實際 {second:?}"
    );

    let after_extractions = stack
        .pg
        .list_entity_extractions_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();
    let after_relationships = stack
        .pg
        .list_relationships_by_object(document.id, PAGE)
        .await
        .expect("query")
        .len();

    assert_eq!(
        before_extractions, after_extractions,
        "重跑不可產生重複的 EntityExtraction——靠的是 v5 id，不是 claim"
    );
    assert_eq!(
        before_relationships, after_relationships,
        "重跑不可產生重複的 Relationship"
    );
}

// ---------------------------------------------------------------------------
// 重複 Document 不抽取
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duplicate_documents_are_skipped() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let canonical = seed_document(
        &stack.pg,
        &source,
        &connector,
        "canonical",
        &fixture.article(),
        None,
        json!({}),
        None,
    )
    .await;
    let duplicate = seed_document(
        &stack.pg,
        &source,
        &connector,
        "duplicate",
        &fixture.article(),
        None,
        json!({}),
        Some(canonical.id),
    )
    .await;

    let service = worker(&stack);

    // 路徑 1：事件說它是重複。
    let outcome = service
        .handle_payload(&json!({
            "document_id": duplicate.id,
            "is_duplicate": true,
            "canonical_object_id": canonical.id,
        }))
        .await
        .expect("payload");
    assert!(
        matches!(outcome, ExtractOutcome::SkippedDuplicate { .. }),
        "is_duplicate=true 必須跳過，實際 {outcome:?}"
    );

    // 路徑 2：事件說它不是重複（舊事件），但 DB 上的 duplicate_of 有值。
    // DB 才是事實，仍要跳過。
    let outcome = service
        .handle_payload(&json!({
            "document_id": duplicate.id,
            "is_duplicate": false,
        }))
        .await
        .expect("payload");
    assert!(
        matches!(outcome, ExtractOutcome::SkippedDuplicate { .. }),
        "DB 上已標記重複時，即使事件說不是也要跳過，實際 {outcome:?}"
    );

    assert!(
        stack
            .pg
            .list_entity_extractions_by_object(duplicate.id, PAGE)
            .await
            .expect("query")
            .is_empty(),
        "重複的 Document 不該留下任何 EntityExtraction"
    );

    // canonical 那份照樣要抽。
    let outcome = service
        .handle_payload(&json!({
            "document_id": canonical.id,
            "is_duplicate": false,
        }))
        .await
        .expect("payload");
    let (entity_count, _, _) = extracted(&outcome);
    assert!(entity_count >= 5, "canonical 必須正常抽取");
}

#[tokio::test]
async fn payload_missing_is_duplicate_is_an_error_not_a_default() {
    let stack = connect_stack().await;
    let service = worker(&stack);
    // 預設成 false 會讓所有重複文件都被抽取，而且不會有任何錯誤跡象。
    let err = service
        .handle_payload(&json!({ "document_id": Uuid::now_v7() }))
        .await
        .expect_err("缺 is_duplicate 必須報錯而不是預設成 false");
    assert!(
        err.to_string().contains("is_duplicate"),
        "錯誤訊息要指出缺哪個欄位，實際：{err}"
    );
}

// ---------------------------------------------------------------------------
// 抽取上限
// ---------------------------------------------------------------------------

#[tokio::test]
async fn extraction_limit_truncates_and_reports() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    // 60 個各不相同的 CVE。
    let base = fixture.run.as_u128() % 1_000_000;
    let body = (0..60)
        .map(|i| format!("CVE-2026-{:07}", base + i))
        .collect::<Vec<_>>()
        .join(" ");
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        "大量 CVE",
        &body,
        None,
        json!({}),
        None,
    )
    .await;

    let service = worker_with_bounds(
        &stack,
        ExtractionBounds {
            max_extractions: 10,
            max_scan_bytes: 256 * 1024,
        },
    );
    let outcome = service
        .extract_document(document.id)
        .await
        .expect("extract");
    match outcome {
        ExtractOutcome::Extracted {
            extraction_count,
            truncated,
            ..
        } => {
            assert!(truncated, "超過上限必須回報截斷，不可以靜默少抽");
            assert_eq!(extraction_count, 10, "必須剛好截到上限");
        }
        other => panic!("預期 Extracted，實際 {other:?}"),
    }

    let stored = stack
        .pg
        .list_entity_extractions_by_object(document.id, PAGE)
        .await
        .expect("query");
    assert_eq!(stored.len(), 10, "落地的 extraction 也必須是 10 筆");

    // 截斷這件事要能從 provenance 看出來，否則之後沒人知道這份資料是不完整的。
    let claim = stack
        .pg
        .list_provenance_by_subject(document.id)
        .await
        .expect("query")
        .into_iter()
        .find(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .expect("claim");
    assert_eq!(claim.metadata["truncated"], json!(true));
    assert_eq!(claim.metadata["total_candidates"], json!(60));
}

// ---------------------------------------------------------------------------
// Person / Organization（只從結構化欄位）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn person_and_organization_come_from_structured_fields_only() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let author = format!("Reporter {}", fixture.run.simple());
    let publisher = format!("Example Security Lab {}", fixture.run.simple());
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        "署名文章",
        // 正文裡也出現人名與組織名，但**不該**被抽出來（V0.1 無 NER）。
        "根據記者 Mallory Unnamed 的報導，Acme Unnamed Corporation 表示已修復。",
        Some(&author),
        json!({ "publisher": publisher }),
        None,
    )
    .await;

    worker(&stack)
        .extract_document(document.id)
        .await
        .expect("extract");

    let person = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Person, &author.to_lowercase())
        .await
        .expect("query")
        .expect("author 欄位必須產生 Person");
    let organization = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Organization, &publisher.to_lowercase())
        .await
        .expect("query")
        .expect("attributes.publisher 必須產生 Organization");

    // 自由文本裡的名字不該出現。
    assert!(
        stack
            .pg
            .find_entity_by_normalized_name(EntityType::Person, "mallory unnamed")
            .await
            .expect("query")
            .is_none(),
        "V0.1 不做自由文本 NER，正文裡的人名不該被抽出來"
    );
    assert!(
        stack
            .pg
            .find_entity_by_normalized_name(EntityType::Organization, "acme unnamed corporation")
            .await
            .expect("query")
            .is_none(),
        "V0.1 不做自由文本 NER，正文裡的組織名不該被抽出來"
    );

    // relationship type 要對：作者是 authored_by，發布者是 published_by。
    let person_rels = stack
        .pg
        .list_relationships_by_object(person.id, PAGE)
        .await
        .expect("query");
    assert!(
        person_rels.iter().any(|r| r.source_object_id == document.id
            && r.relationship_type == RelationshipType::AuthoredBy),
        "Document → Person 必須是 authored_by"
    );

    let org_rels = stack
        .pg
        .list_relationships_by_object(organization.id, PAGE)
        .await
        .expect("query");
    assert!(
        org_rels.iter().any(|r| r.source_object_id == document.id
            && r.relationship_type == RelationshipType::PublishedBy),
        "Document → Organization 必須是 published_by"
    );
}

// ---------------------------------------------------------------------------
// Domain ↔ URL / Domain ↔ Email 的衍生關聯
// ---------------------------------------------------------------------------

#[tokio::test]
async fn domain_is_linked_to_the_url_and_email_it_came_from() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        "連結與信箱",
        &fixture.article(),
        None,
        json!({}),
        None,
    )
    .await;

    worker(&stack)
        .extract_document(document.id)
        .await
        .expect("extract");

    let domain = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Domain, &fixture.domain)
        .await
        .expect("query")
        .expect("domain entity");
    let email = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Email, &fixture.email)
        .await
        .expect("query")
        .expect("email entity");

    let domain_rels = stack
        .pg
        .list_relationships_by_object(domain.id, PAGE)
        .await
        .expect("query");

    // URL → Domain 是 belongs_to。
    assert!(
        domain_rels.iter().any(|r| r.target_object_id == domain.id
            && r.relationship_type == RelationshipType::BelongsTo),
        "URL 與它的 Domain 之間必須有 belongs_to，實際關聯：{:?}",
        domain_rels
            .iter()
            .map(|r| r.relationship_type)
            .collect::<Vec<_>>()
    );

    // Email → Domain 是 associated_with。
    assert!(
        domain_rels.iter().any(|r| r.source_object_id == email.id
            && r.target_object_id == domain.id
            && r.relationship_type == RelationshipType::AssociatedWith),
        "Email 與它的 Domain 之間必須有 associated_with"
    );

    // 每一條衍生關聯也要有 evidence（SPEC §12 對**所有** relationship 都成立）。
    for relationship in &domain_rels {
        let evidence = stack
            .pg
            .list_relationship_evidence(relationship.id, PAGE)
            .await
            .expect("query");
        assert!(
            !evidence.is_empty(),
            "SPEC §12：relationship {} ({:?}) 查不到 evidence",
            relationship.id,
            relationship.relationship_type
        );
    }
}

// ---------------------------------------------------------------------------
// 找不到 Document
// ---------------------------------------------------------------------------

#[tokio::test]
async fn missing_document_is_skipped_not_fatal() {
    let stack = connect_stack().await;
    let outcome = worker(&stack)
        .extract_document(Uuid::now_v7())
        .await
        .expect("不存在的 Document 不該讓 consumer 掛掉");
    assert!(matches!(outcome, ExtractOutcome::DocumentMissing { .. }));
}

// ---------------------------------------------------------------------------
// V0.2 Phase 1c-0-data：entity_identifiers
// ---------------------------------------------------------------------------

/// Domain／Ip／Url／Email／CVE 會寫 identifier；Hash／Person／Organization 不會。
#[tokio::test]
async fn upsert_writes_identifiers_for_unique_key_types_and_skips_the_rest() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let author = format!("Reporter {}", fixture.run.simple());
    let publisher = format!("Example Security Lab {}", fixture.run.simple());
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("公告 {}", fixture.cve),
        &fixture.article(),
        Some(&author),
        json!({ "publisher": publisher }),
        None,
    )
    .await;

    worker(&stack)
        .extract_document(document.id)
        .await
        .expect("extract");

    let expected: [(EntityType, &str, String); 5] = [
        (EntityType::Vulnerability, "cve", fixture.cve.to_uppercase()),
        (EntityType::Domain, "domain", fixture.domain.clone()),
        (EntityType::Ip, "ip", fixture.ip.clone()),
        (EntityType::Email, "email", fixture.email.clone()),
        (
            EntityType::Url,
            "url",
            core_model::url_norm::canonicalize(&format!(
                "https://{}/advisory/{}?utm_source=news",
                fixture.domain, fixture.run
            ))
            .expect("fixture URL 必須能正規化"),
        ),
    ];
    for (kind, namespace, normalized) in &expected {
        let entity = stack
            .pg
            .find_entity_by_normalized_name(*kind, normalized)
            .await
            .expect("query")
            .unwrap_or_else(|| panic!("{kind:?} `{normalized}` 必須建立 Entity"));
        let rows = stack
            .pg
            .list_entity_identifiers_by_entity(entity.id, PAGE)
            .await
            .expect("identifiers");
        assert_eq!(
            rows.len(),
            1,
            "{kind:?} 應剛好一筆 identifier，實際 {rows:?}"
        );
        assert_eq!(rows[0].namespace, *namespace);
        assert_eq!(rows[0].normalized_value, *normalized);
        assert_eq!(rows[0].entity_id, entity.id);
        assert_eq!(
            rows[0].id,
            identifier_id(namespace, entity.id, normalized),
            "identifier id 必須是 UUID v5，重跑才不會累積重複列"
        );
        assert_eq!(
            rows[0].source_id,
            Some(source.id),
            "identifier.source_id 應從 RawEvidence 反查到本次 Source"
        );
    }

    let skipped: [(EntityType, String); 3] = [
        (EntityType::Hash, fixture.sha256.clone()),
        (EntityType::Person, author.to_lowercase()),
        (EntityType::Organization, publisher.to_lowercase()),
    ];
    for (kind, normalized) in &skipped {
        let entity = stack
            .pg
            .find_entity_by_normalized_name(*kind, normalized)
            .await
            .expect("query")
            .unwrap_or_else(|| panic!("{kind:?} `{normalized}` 必須建立 Entity"));
        let rows = stack
            .pg
            .list_entity_identifiers_by_entity(entity.id, PAGE)
            .await
            .expect("identifiers");
        assert!(
            rows.is_empty(),
            "{kind:?} 這次不該寫 identifier（Hash=T10；Person/Organization=名字不是唯一鍵），實際 {rows:?}"
        );
    }
}

/// 同一個 Entity 刪掉 claim 再抽一次，identifier 仍是同一列。
#[tokio::test]
async fn rerunning_upsert_does_not_duplicate_identifiers() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("公告 {}", fixture.cve),
        &format!("受影響的網站為 {}。", fixture.domain),
        None,
        json!({}),
        None,
    )
    .await;

    let service = worker(&stack);
    service.extract_document(document.id).await.expect("first");
    let entity = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Domain, &fixture.domain)
        .await
        .expect("query")
        .expect("domain");
    let before = stack
        .pg
        .list_entity_identifiers_by_entity(entity.id, PAGE)
        .await
        .expect("identifiers");
    assert_eq!(before.len(), 1);

    let claim = stack
        .pg
        .list_provenance_by_subject(document.id)
        .await
        .expect("query")
        .into_iter()
        .find(|p| p.action == ACTION_ENTITY_EXTRACTED)
        .expect("claim");
    sqlx::query("DELETE FROM provenance WHERE id = $1")
        .bind(claim.id)
        .execute(stack.pg.pool())
        .await
        .expect("delete claim");

    service.extract_document(document.id).await.expect("second");
    let after = stack
        .pg
        .list_entity_identifiers_by_entity(entity.id, PAGE)
        .await
        .expect("identifiers");
    assert_eq!(
        after.len(),
        1,
        "同一個 Entity 重跑不可累積 identifier；靠的是 UUID v5 主鍵"
    );
    assert_eq!(after[0].id, before[0].id);
}

/// 兩個不同 Entity 宣稱同一個 `(namespace, normalized_value)` 時寫
/// `method=exact_identifier` 的 resolution candidate。
///
/// 同一型別、同一 normalized_name 的 Entity 會被自然鍵合併，不會走到這條路。
/// 衝突要靠「另一個型別的 Entity 先佔了這個識別碼」才能觸發——這裡用一個
/// 預先寫入的 Person 去佔 `("domain", <本次 domain>)`。
#[tokio::test]
async fn identifier_conflict_writes_exact_identifier_candidate() {
    let stack = connect_stack().await;
    let fixture = Fixture::new();
    let source = seed_source(&stack.pg, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let now = Utc::now();
    let occupant_id = entity_id(EntityType::Person, &format!("occupant-{}", fixture.run));
    let occupant = Entity {
        id: occupant_id,
        entity_type: EntityType::Person,
        name: format!("Occupant {}", fixture.run),
        normalized_name: format!("occupant-{}", fixture.run),
        description: None,
        confidence: 0.5,
        first_seen: now,
        last_seen: now,
        merged_into: None,
        attributes: json!({}),
    };
    stack.pg.put_entity(&occupant).await.expect("occupant");
    let occupant_identifier = EntityIdentifier {
        id: identifier_id("domain", occupant_id, &fixture.domain),
        entity_id: occupant_id,
        namespace: "domain".into(),
        value: fixture.domain.clone(),
        normalized_value: fixture.domain.clone(),
        confidence: 0.5,
        source_id: Some(source.id),
        first_seen: now,
        last_seen: now,
    };
    stack
        .pg
        .put_entity_identifier(&occupant_identifier)
        .await
        .expect("pre-claim identifier");

    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        "衝突文件",
        &format!("受影響的網站為 {}。", fixture.domain),
        None,
        json!({}),
        None,
    )
    .await;
    worker(&stack)
        .extract_document(document.id)
        .await
        .expect("extract");

    let domain_entity = stack
        .pg
        .find_entity_by_normalized_name(EntityType::Domain, &fixture.domain)
        .await
        .expect("query")
        .expect("抽取仍必須建立 Domain Entity；identifier 衝突不該讓抽取失敗");

    let domain_ids = stack
        .pg
        .list_entity_identifiers_by_entity(domain_entity.id, PAGE)
        .await
        .expect("identifiers");
    assert!(
        domain_ids.is_empty(),
        "衝突時後來者寫不進去，識別碼仍屬於先佔的 Entity，實際 {domain_ids:?}"
    );
    let owner = stack
        .pg
        .find_entity_identifier_owner("domain", &fixture.domain)
        .await
        .expect("owner")
        .expect("識別碼仍應屬於預先寫入的 Person");
    assert_eq!(owner.entity_id, occupant_id);

    let candidates = stack
        .pg
        .list_resolution_candidates(Some(ResolutionStatus::Pending), None, PAGE)
        .await
        .expect("candidates");
    let hit: Vec<_> = candidates
        .iter()
        .filter(|c| c.method == "exact_identifier")
        .filter(|c| {
            (c.entity_a_id == occupant_id && c.entity_b_id == domain_entity.id)
                || (c.entity_a_id == domain_entity.id && c.entity_b_id == occupant_id)
        })
        .collect();
    assert_eq!(
        hit.len(),
        1,
        "必須剛好一筆 exact_identifier 候選，實際 {hit:?}"
    );
    assert_eq!(hit[0].score, 0.95);
    assert_eq!(hit[0].status, ResolutionStatus::Pending);
    assert!(hit[0].entity_a_id < hit[0].entity_b_id);
    assert_eq!(hit[0].evidence["namespace"], "domain");
    assert_eq!(hit[0].evidence["normalized_value"], fixture.domain);
    assert_eq!(hit[0].evidence["trigger"], "write_conflict");
}
