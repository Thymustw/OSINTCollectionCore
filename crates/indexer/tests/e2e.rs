//! indexer e2e：對本機 Docker（Postgres + OpenSearch 19200）真跑，不連外網。
//!
//! 涵蓋 SPEC §18 的八種搜尋、冪等、duplicate 不出現在結果、bulk 部分失敗、
//! `--rebuild`、查詢注入與中文查詢。
//!
//! # 共用後端的鐵則
//!
//! * **每個測試用自己的 index**（`osint-documents-e2e-<uuid>`）。共用一個 index
//!   會讓「總共幾筆」的斷言被別的測試污染，而且會踩到正式的 `osint-documents`。
//! * Entity 的自然鍵 `(entity_type, normalized_name)` 是**全域**唯一的，
//!   所以 fixture 的 CVE／domain 都含 run-specific 值。
//! * PostgreSQL 是共用的：關於「資料庫裡有幾筆」的斷言一律限縮在本次建立的範圍，
//!   或用「前後兩次計數相同才斷言」的方式避開並行寫入。

use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use core_model::{Connector, Document, DocumentType, NetworkRule, RawEvidence, Source, SourceType};
use core_observability::MetricsRegistry;
use entity_worker::{EntityWorker, ExtractionBounds};
use indexer::search::{EntityFilter, SearchRequest};
use indexer::service::RebuildOptions;
use indexer::{DateField, IndexBounds, Indexer, PrepareOutcome, schema};
use serde_json::{Value, json};
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::{
    ProjectionStore, RebuildState, RelationalStore, SearchDocument, SearchHits, SearchStore,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

// ---------------------------------------------------------------------------
// 基礎設施
// ---------------------------------------------------------------------------

struct Stack {
    pg: PostgresCanonicalStore,
    os: OpenSearchStore,
}

async fn connect_stack() -> Stack {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "e2e 只連本機 Postgres"
    );
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    verify_not_opencti_search(&url).expect("OPENSEARCH_URL 埠隔離");

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");

    // refresh_on_write：每次寫入後強制 refresh，讓斷言不必 sleep 等 1 秒的
    // refresh_interval。正式環境不會開（見 OpenSearchStore 的欄位說明）。
    let os = OpenSearchStore::connect(&url)
        .expect("opensearch client")
        .with_refresh_on_write(true);
    let info = os.cluster_info().await.expect("GET /");
    // 這一步不是形式：本機 9200 是 OpenCTI 的 Elasticsearch。
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");

    Stack { pg, os }
}

/// 這個測試專屬的 index 名稱。
fn test_index() -> String {
    format!("osint-documents-e2e-{}", Uuid::now_v7().simple())
}

fn indexer_for(stack: &Stack, index: &str) -> Indexer {
    Indexer::new(
        stack.pg.clone(),
        stack.os.clone(),
        // 不接 broker：e2e 不需要驗證事件送達，少一個非確定性來源就少一類 flaky。
        None,
        MetricsRegistry::new(),
        index,
        IndexBounds::default(),
    )
}

fn worker(stack: &Stack) -> EntityWorker {
    EntityWorker::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        ExtractionBounds::default(),
    )
}

/// 測試結束時把一次性 index 刪掉。留著會讓叢集慢慢長出幾百個測試 index。
///
/// **投影狀態也要清。** V0.2 Phase 0f 起每次 flush／rebuild 都會在
/// `osint-projection-state` 裡留一列（`_id` = 投影名 = 這個一次性 index 名）。
/// 刪掉 index 不會連帶刪掉那一列——那是刻意的（`--drop` 時狀態要留著），
/// 所以測試得自己 `reset_projection`，否則每跑一次 e2e 就多幾列孤兒狀態。
async fn cleanup(stack: &Stack, index: &str) {
    let _ = stack.os.reset_projection(index).await;
    let _ = stack.os.delete_index(index).await;
}

// ---------------------------------------------------------------------------
// fixture
// ---------------------------------------------------------------------------

/// 這一次測試專屬的一組值。
struct Fixture {
    /// 出現在正文裡的獨特詞，用來把搜尋結果限縮在本次的文件。
    tag: String,
    cve: String,
    domain: String,
}

impl Fixture {
    fn new() -> Self {
        let run = Uuid::now_v7();
        let serial = run.as_u128() % 10_000_000;
        Self {
            tag: format!("osintidx{}", run.simple()),
            cve: format!("CVE-2026-{serial:07}"),
            domain: format!("t{serial:07}.example.com"),
        }
    }
}

async fn seed_source(pg: &PostgresCanonicalStore) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("indexer-e2e-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: Some("indexer-e2e".into()),
        base_url: None,
        description: Some("indexer e2e fixture".into()),
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

async fn seed_connector(pg: &PostgresCanonicalStore, source: &Source) -> Connector {
    let now = Utc::now();
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "indexer e2e".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("indexer-e2e-connector-{}", Uuid::now_v7()),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({}),
        credential_reference: None,
        schedule: None,
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

/// 一份要寫進 PostgreSQL 的文件規格。
struct DocSpec {
    title: String,
    body: String,
    language: Option<String>,
    object_type: DocumentType,
    published_at: Option<DateTime<Utc>>,
    duplicate_of: Option<Uuid>,
}

impl DocSpec {
    fn new(title: &str, body: &str) -> Self {
        Self {
            title: title.into(),
            body: body.into(),
            language: Some("en".into()),
            object_type: DocumentType::Article,
            published_at: None,
            duplicate_of: None,
        }
    }

    fn language(mut self, value: &str) -> Self {
        self.language = Some(value.into());
        self
    }

    fn object_type(mut self, value: DocumentType) -> Self {
        self.object_type = value;
        self
    }

    fn published_at(mut self, value: DateTime<Utc>) -> Self {
        self.published_at = Some(value);
        self
    }

    fn duplicate_of(mut self, value: Uuid) -> Self {
        self.duplicate_of = Some(value);
        self
    }
}

/// 寫一份 RawEvidence + Document 到 PostgreSQL。
async fn seed_document(
    pg: &PostgresCanonicalStore,
    source: &Source,
    connector: &Connector,
    spec: DocSpec,
) -> Document {
    let now = Utc::now();
    let raw_id = Uuid::now_v7();
    let evidence = RawEvidence {
        id: raw_id,
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: Some(raw_id.to_string()),
        source_url: format!("http://127.0.0.1/indexer-e2e/{raw_id}"),
        retrieved_at: now,
        content_type: Some("application/rss+xml".into()),
        mime_type: Some("application/rss+xml".into()),
        content_length: Some(spec.body.len() as i64),
        sha256: format!("{}{}", raw_id.simple(), raw_id.simple()),
        storage_path: format!("raw/{}/{raw_id}", source.id),
        http_status: Some(200),
        http_headers: json!({}),
        metadata: json!({}),
        collector_version: "0.1.0".into(),
    };
    pg.insert_raw_evidence(&evidence).await.expect("raw");

    let document = Document {
        id: Uuid::now_v7(),
        object_type: spec.object_type,
        schema_version: "1".into(),
        title: Some(spec.title.clone()),
        body: Some(spec.body.clone()),
        summary: Some(spec.title.clone()),
        language: spec.language.clone(),
        author: Some("Indexer E2E".into()),
        published_at: spec.published_at,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: Some(format!("http://127.0.0.1/indexer-e2e/{raw_id}")),
        canonical_url: Some(format!("http://127.0.0.1/indexer-e2e/{raw_id}")),
        normalized_content_hash: Some(core_model::content_hash(
            Some(&spec.title),
            None,
            Some(&spec.body),
        )),
        confidence: 0.8,
        labels: Vec::new(),
        attributes: json!({ "raw_evidence_id": raw_id }),
        external_key: None,
        simhash: None,
        duplicate_of: spec.duplicate_of,
    };
    pg.put_document(&document).await.expect("document");
    document
}

/// 抽 entity → prepare → flush。回傳寫進 index 的 Document。
async fn index_document(stack: &Stack, service: &Indexer, document: &Document) {
    if document.duplicate_of.is_none() {
        worker(stack)
            .extract_document(document.id)
            .await
            .expect("extract");
    }
    match service.prepare(document.id).await.expect("prepare") {
        PrepareOutcome::Ready(doc) => {
            let report = service.flush(vec![*doc]).await.expect("flush");
            assert_eq!(
                report.permanent_failures.len(),
                0,
                "索引失敗：{:?}",
                report.permanent_failures
            );
            assert_eq!(report.indexed, 1);
        }
        other => panic!("預期 Ready，實際 {other:?}"),
    }
}

/// 跑一次搜尋。
async fn run_search(stack: &Stack, index: &str, request: SearchRequest) -> SearchHits {
    let query = indexer::search::build(index, &request).expect("build query");
    stack.os.search(query).await.expect("search")
}

fn ids(hits: &SearchHits) -> Vec<String> {
    hits.hits.iter().map(|h| h.id.clone()).collect()
}

/// 把搜尋限縮在本次測試的 source，避免共用叢集上的殘留資料干擾。
fn request_for(source: &Source, query: &str) -> SearchRequest {
    SearchRequest {
        query: query.into(),
        source_id: Some(source.id),
        limit: Some(50),
        ..SearchRequest::default()
    }
}

// ---------------------------------------------------------------------------
// SPEC §18 的八種搜尋
// ---------------------------------------------------------------------------

#[tokio::test]
async fn spec_18_all_eight_search_kinds() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;

    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let recent = Utc::now() - ChronoDuration::days(1);

    // A：英文文章，含 CVE 與 domain，發布於 2020。
    let doc_a = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} ransomware advisory", fx.tag),
            &format!(
                "{tag} A lockbit ransomware gang attacked {domain}. See {cve} for details.",
                tag = fx.tag,
                domain = fx.domain,
                cve = fx.cve
            ),
        )
        .published_at(old),
    )
    .await;
    // B：英文報告，講 phishing，不含 ransomware，發布於昨天。
    let doc_b = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} phishing report", fx.tag),
            &format!("{tag} B phishing campaign targeting banks.", tag = fx.tag),
        )
        .object_type(DocumentType::Report)
        .published_at(recent),
    )
    .await;
    // C：中文網頁，講勒索軟體。
    let doc_c = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} 勒索軟體攻擊公告", fx.tag),
            &format!("{tag} C 本次勒索軟體攻擊影響多個系統。", tag = fx.tag),
        )
        .language("zh")
        .object_type(DocumentType::WebPage)
        .published_at(recent),
    )
    .await;

    for document in [&doc_a, &doc_b, &doc_c] {
        index_document(&stack, &service, document).await;
    }

    // 1) keyword
    let hits = run_search(&stack, &index, request_for(&source, "ransomware")).await;
    assert_eq!(
        ids(&hits),
        vec![doc_a.id.to_string()],
        "keyword 搜尋應只命中 A"
    );

    // 2) phrase：`"ransomware gang"` 要求詞序，B 沒有這個片語，
    //    而「gang ransomware」這種相反順序也不該命中。
    let hits = run_search(&stack, &index, request_for(&source, "\"ransomware gang\"")).await;
    assert_eq!(ids(&hits), vec![doc_a.id.to_string()]);
    let hits = run_search(&stack, &index, request_for(&source, "\"gang ransomware\"")).await;
    assert!(
        hits.hits.is_empty(),
        "片語要求詞序；反過來不該命中，否則 phrase 與 keyword 沒有差別"
    );

    // 3) boolean AND / OR / NOT
    let hits = run_search(
        &stack,
        &index,
        request_for(&source, "ransomware AND lockbit"),
    )
    .await;
    assert_eq!(ids(&hits), vec![doc_a.id.to_string()]);
    let hits = run_search(&stack, &index, request_for(&source, "ransomware AND banks")).await;
    assert!(hits.hits.is_empty(), "AND 兩邊分屬不同文件時不該命中");

    let hits = run_search(&stack, &index, request_for(&source, "lockbit OR phishing")).await;
    let mut got = ids(&hits);
    got.sort();
    let mut want = vec![doc_a.id.to_string(), doc_b.id.to_string()];
    want.sort();
    assert_eq!(got, want, "OR 應同時命中 A 與 B");

    let hits = run_search(
        &stack,
        &index,
        request_for(&source, &format!("{} NOT phishing", fx.tag)),
    )
    .await;
    let got = ids(&hits);
    assert!(!got.contains(&doc_b.id.to_string()), "NOT 應排除 B");
    assert!(got.contains(&doc_a.id.to_string()));

    // 4) source filter：換一個 source 就查不到。
    let other_source = seed_source(&stack.pg).await;
    let hits = run_search(&stack, &index, request_for(&other_source, &fx.tag)).await;
    assert!(hits.hits.is_empty(), "source 過濾必須真的過濾");

    // 5) entity filter
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            entity: Some(EntityFilter {
                entity_type: Some("vulnerability".into()),
                // 刻意用小寫：normalized_name 是 lowercase normalizer，
                // 大小寫不該影響結果。
                name: fx.cve.to_lowercase(),
            }),
            ..request_for(&source, "")
        },
    )
    .await;
    assert_eq!(
        ids(&hits),
        vec![doc_a.id.to_string()],
        "entity 過濾應只命中含該 CVE 的 A"
    );
    // 型別對不上時不該命中——證明 nested 真的把兩個條件綁在同一個 entity 上。
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            entity: Some(EntityFilter {
                entity_type: Some("ip".into()),
                name: fx.cve.to_lowercase(),
            }),
            ..request_for(&source, "")
        },
    )
    .await;
    assert!(
        hits.hits.is_empty(),
        "型別與名稱必須落在同一個 entity 上；扁平陣列會在這裡誤命中"
    );

    // 6) date range：只要 2020 那一篇。
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            date_field: DateField::Published,
            date_to: Some(Utc.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap()),
            ..request_for(&source, "")
        },
    )
    .await;
    assert_eq!(ids(&hits), vec![doc_a.id.to_string()], "date range 上界");
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            date_field: DateField::Published,
            date_from: Some(Utc::now() - ChronoDuration::days(7)),
            ..request_for(&source, "")
        },
    )
    .await;
    let got = ids(&hits);
    assert!(!got.contains(&doc_a.id.to_string()), "date range 下界");
    assert_eq!(got.len(), 2, "最近七天應有 B 與 C");

    // 7) language
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            language: Some("zh".into()),
            ..request_for(&source, "")
        },
    )
    .await;
    assert_eq!(ids(&hits), vec![doc_c.id.to_string()]);

    // 8) object type
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            object_type: Some("report".into()),
            ..request_for(&source, "")
        },
    )
    .await;
    assert_eq!(ids(&hits), vec![doc_b.id.to_string()]);

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// 中文（cjk analyzer）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn chinese_query_matches_chinese_content() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;

    let zh = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} 勒索軟體攻擊公告", fx.tag),
            &format!("{} 本次事件涉及勒索軟體與資料外洩。", fx.tag),
        )
        .language("zh"),
    )
    .await;
    let en = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} english only", fx.tag),
            &format!("{} nothing related here.", fx.tag),
        ),
    )
    .await;
    for document in [&zh, &en] {
        index_document(&stack, &service, document).await;
    }

    let hits = run_search(&stack, &index, request_for(&source, "勒索軟體")).await;
    assert_eq!(
        ids(&hits),
        vec![zh.id.to_string()],
        "中文查詢查不到中文內容，代表 cjk sub-field 沒有生效"
    );

    // 只用 standard analyzer 的話「勒索軟體」會被拆成四個單字，
    // 任何含「體」或「軟」的中文文件都會命中。這裡確認沒有退化成那樣：
    // 一個完全不相關的中文詞不該命中。
    let hits = run_search(&stack, &index, request_for(&source, "颱風警報")).await;
    assert!(
        hits.hits.is_empty(),
        "不相關的中文詞命中了，代表中文斷詞退化成單字比對"
    );

    // highlight 要能折回母欄位（require_field_match=false）。
    let hits = run_search(&stack, &index, request_for(&source, "勒索軟體")).await;
    let hit = &hits.hits[0];
    assert!(
        !hit.highlights.is_empty(),
        "中文命中沒有 snippet，代表 highlight 的 require_field_match 設錯了"
    );

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// 冪等
// ---------------------------------------------------------------------------

#[tokio::test]
async fn indexing_the_same_document_twice_yields_one_hit() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} idempotency", fx.tag),
            &format!("{} body", fx.tag),
        ),
    )
    .await;

    // 索引三次，模擬事件被重送。
    for _ in 0..3 {
        index_document(&stack, &service, &document).await;
    }

    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    assert_eq!(
        hits.total, 1,
        "同一份 Document 索引三次應只有一筆 hit；\
         不是的話代表 _id 不是 Document.id"
    );
    assert_eq!(hits.hits[0].id, document.id.to_string());
    assert_eq!(service.indexed_count().await.expect("count"), 1);

    cleanup(&stack, &index).await;
}

/// indexer 重寫同一份文件時，不可清掉別的服務疊加的欄位。
///
/// 模擬：indexer 寫入 → embedding-worker 用 `update_fields` 疊加
/// `embedding_en_model_version`（mapping 已宣告的 keyword；`dynamic: strict`
/// 下不能用完全陌生的欄位）→ indexer 再 flush 一次（`--rebuild` 同一條路徑）
/// → overlay 還在。V0.1 的 `bulk_index` 會整份取代 `_source`，這一支就是在
/// 證明那個行為已經改掉。
#[tokio::test]
async fn rebuild_flush_preserves_fields_written_by_other_services() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} overlay keep", fx.tag),
            &format!("{} overlay body", fx.tag),
        ),
    )
    .await;
    index_document(&stack, &service, &document).await;

    stack
        .os
        .update_fields(
            &index,
            &document.id.to_string(),
            json!({ schema::F_EMBEDDING_EN_MODEL_VERSION: "overlay-v1" }),
        )
        .await
        .expect("模擬 embedding-worker 疊加向量版本欄位");

    // 第二次 flush = `--rebuild` 對同一份文件再寫一次。
    index_document(&stack, &service, &document).await;

    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    assert_eq!(hits.total, 1, "重建後仍應只有一筆");
    let source_doc = &hits.hits[0].source;
    assert_eq!(
        source_doc
            .get(schema::F_EMBEDDING_EN_MODEL_VERSION)
            .and_then(Value::as_str),
        Some("overlay-v1"),
        "indexer 第二次 flush 把 embedding-worker 疊加的欄位清掉了：{source_doc}"
    );
    assert_eq!(
        source_doc.get(schema::F_TITLE).and_then(Value::as_str),
        Some(document.title.as_deref().unwrap_or("")),
        "indexer 自己的欄位仍應在"
    );

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// duplicate 不進搜尋結果
// ---------------------------------------------------------------------------

#[tokio::test]
async fn duplicates_are_not_indexed_and_are_removed_if_they_slip_in() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;

    let canonical = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} canonical article", fx.tag),
            &format!("{} same content", fx.tag),
        ),
    )
    .await;
    index_document(&stack, &service, &canonical).await;

    // 一份指向 canonical 的 duplicate。
    let duplicate = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} canonical article", fx.tag),
            &format!("{} same content", fx.tag),
        )
        .duplicate_of(canonical.id),
    )
    .await;

    match service.prepare(duplicate.id).await.expect("prepare") {
        PrepareOutcome::RemovedDuplicate { was_indexed, .. } => {
            assert!(!was_indexed, "它本來就不在 index 裡");
        }
        other => panic!("duplicate 不該被準備成可索引的文件，實際 {other:?}"),
    }

    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    assert_eq!(
        ids(&hits),
        vec![canonical.id.to_string()],
        "搜尋結果只該有 canonical"
    );

    // 先索引、後來才被判成重複的情況：prepare 要把它從 index 移除。
    let late = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} late duplicate", fx.tag),
            &format!("{} late", fx.tag),
        ),
    )
    .await;
    index_document(&stack, &service, &late).await;
    assert_eq!(
        run_search(&stack, &index, request_for(&source, &fx.tag))
            .await
            .total,
        2
    );

    let mut late = late;
    late.duplicate_of = Some(canonical.id);
    stack.pg.put_document(&late).await.expect("mark duplicate");
    match service.prepare(late.id).await.expect("prepare") {
        PrepareOutcome::RemovedDuplicate { was_indexed, .. } => {
            assert!(
                was_indexed,
                "它原本在 index 裡，必須被移除；只是不再更新的話它會永遠留在搜尋結果中"
            );
        }
        other => panic!("預期 RemovedDuplicate，實際 {other:?}"),
    }
    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    assert_eq!(ids(&hits), vec![canonical.id.to_string()]);

    // include_duplicates 是偵錯開關：即使打開，被移除的文件也不會回來。
    let hits = run_search(
        &stack,
        &index,
        SearchRequest {
            include_duplicates: true,
            ..request_for(&source, &fx.tag)
        },
    )
    .await;
    assert_eq!(hits.total, 1);

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// bulk 部分失敗
// ---------------------------------------------------------------------------

#[tokio::test]
async fn bulk_partial_failure_is_reported_not_swallowed() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let good_doc = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} good", fx.tag),
            &format!("{} good body", fx.tag),
        ),
    )
    .await;
    worker(&stack)
        .extract_document(good_doc.id)
        .await
        .expect("extract");
    let PrepareOutcome::Ready(good) = service.prepare(good_doc.id).await.expect("prepare") else {
        panic!("預期 Ready");
    };

    // 壞的那筆帶一個沒有宣告在 mapping 裡的欄位。mapping 是 dynamic:strict，
    // OpenSearch 會以 400 strict_dynamic_mapping_exception 拒絕**這一筆**，
    // 但整個 bulk 請求仍然回 HTTP 200——這正是最容易被當成成功的情況。
    let bad_id = Uuid::now_v7();
    let bad = SearchDocument {
        index: index.clone(),
        id: bad_id.to_string(),
        body: json!({
            "document_id": bad_id.to_string(),
            "this_field_is_not_in_the_mapping": "boom",
        }),
    };

    let report = service
        .flush(vec![*good, bad])
        .await
        .expect("flush 不該整個失敗");

    assert_eq!(report.submitted, 2);
    assert_eq!(report.indexed, 1, "好的那筆必須進去");
    assert_eq!(
        report.permanent_failures.len(),
        1,
        "壞的那筆必須被回報，不能靜默丟掉"
    );
    let failure = &report.permanent_failures[0];
    assert_eq!(failure.id, bad_id.to_string());
    assert_eq!(failure.status, 400);
    assert!(
        !failure.is_retryable(),
        "400 是永久性失敗，重試只會把 partition 卡住"
    );
    assert!(
        failure.reason.contains("strict") || failure.reason.contains("mapping"),
        "原因訊息要指向 mapping，實際：{}",
        failure.reason
    );
    assert_eq!(report.retries, 0, "永久性失敗不該觸發重試");

    // 好的那筆真的查得到。
    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    assert_eq!(ids(&hits), vec![good_doc.id.to_string()]);

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// 查詢注入
// ---------------------------------------------------------------------------

#[tokio::test]
async fn opensearch_query_syntax_from_users_is_not_executed() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} injection probe", fx.tag),
            &format!("{} ordinary body text", fx.tag),
        ),
    )
    .await;
    index_document(&stack, &service, &document).await;

    // 先確認正常查詢查得到——否則下面的「查不到」可能只是因為索引根本沒生效。
    assert_eq!(
        run_search(&stack, &index, request_for(&source, &fx.tag))
            .await
            .total,
        1,
        "對照組：正常關鍵字必須查得到"
    );

    // 這些若被當成查詢語法會命中（或炸掉）；被當成文字則一筆都不會中，
    // 因為文件裡沒有 `title`／`_id`／`_exists_` 這些詞。
    for hostile in [
        "*",
        "title:*",
        "_id:*",
        "body:/.*(a|b)*.*/",
        "title:injection",
        "probe~3",
        "_exists_:title",
    ] {
        let hits = run_search(&stack, &index, request_for(&source, hostile)).await;
        assert_eq!(
            hits.total, 0,
            "`{hostile}` 命中了 {} 筆。它應該只是一個要比對的**文字**，\
             不是欄位存取或萬用查詢",
            hits.total
        );
    }

    // `*probe*` 要分開驗，而且不能只斷言「查不到」——它**會**命中一筆，
    // 但原因是 standard analyzer 把 `*` 當標點去掉，剩下字面上的 `probe`。
    // 要證明的是「它等同於查 probe」而**不是**萬用比對：
    assert_eq!(
        run_search(&stack, &index, request_for(&source, "*probe*"))
            .await
            .total,
        run_search(&stack, &index, request_for(&source, "probe"))
            .await
            .total,
        "`*probe*` 應等同於查 `probe`（`*` 被當成標點去掉）"
    );
    assert_eq!(
        run_search(&stack, &index, request_for(&source, "*prob*"))
            .await
            .total,
        0,
        "`*prob*` 命中了，代表 `*` 真的被當成萬用字元——那就是可以做 DoS 的注入面"
    );

    // 超長查詢在打到 OpenSearch 之前就該被擋下來。
    let err = indexer::search::build(
        &index,
        &SearchRequest {
            query: "x".repeat(5000),
            ..SearchRequest::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("太長"), "{err}");

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// rebuild
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rebuild_from_postgres_matches_the_canonical_document_count() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let canonical = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} rebuild canonical", fx.tag),
            &format!("{} rebuild body", fx.tag),
        ),
    )
    .await;
    let duplicate = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} rebuild duplicate", fx.tag),
            &format!("{} rebuild body", fx.tag),
        )
        .duplicate_of(canonical.id),
    )
    .await;
    worker(&stack)
        .extract_document(canonical.id)
        .await
        .expect("extract");

    // 這個資料庫是 workspace 所有 e2e 共用的，CI 上 nextest 同時跑 50+ 個 test
    // binary，「rebuild 前後全表 canonical 數相同」在那種環境下不是可靠的前提——
    // 舊版用三次重試等它相同，2026-09-12 在 GitHub runner 上三次都撞到並行寫入
    // 而失敗。真正要驗的不變量不需要全表數字穩定：
    //   (a) rebuild 內部一致：scanned == indexed（沒有掃到卻沒寫進去的）
    //   (b) index 內的文件數 == 本次寫入數（drop_index=true 起手是空的）
    //   (c) 掃到的數 >= rebuild 開始前的 canonical 數（其他測試只會加、不會刪）
    //   (d) 本次建的 canonical 在 index、duplicate 不在（下面另外斷言）
    // 這四條在並行寫入下仍然嚴格成立；「等於全表數」那條只在單獨跑時成立。
    let before = count_canonical_documents(&stack.pg).await;
    let report = service
        .rebuild(RebuildOptions {
            drop_index: true,
            page_size: 100,
        })
        .await
        .expect("rebuild");
    let indexed = service.indexed_count().await.expect("count");

    assert_eq!(
        report.failed.len(),
        0,
        "rebuild 不該有失敗：{:?}",
        report.failed
    );
    assert_eq!(
        report.scanned, report.indexed,
        "掃過的 canonical 數必須等於寫進 index 的數（沒有掃到卻沒寫的）"
    );
    assert_eq!(
        report.indexed, indexed,
        "從空 index 重建後，index 內的文件數必須等於本次寫入數"
    );
    assert!(
        report.scanned >= before,
        "rebuild 掃到的 canonical 數（{}）不該少於開始前的 PostgreSQL canonical 數（{before}）——\
         其他測試只會新增，不會刪除",
        report.scanned
    );
    assert!(
        report.skipped_duplicates >= 1,
        "至少要跳過本次建立的那一份 duplicate"
    );

    // 本次的 canonical 在 index 裡，duplicate 不在。
    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    assert_eq!(ids(&hits), vec![canonical.id.to_string()]);
    assert!(!ids(&hits).contains(&duplicate.id.to_string()));

    // 重建是冪等的：再跑一次（不 drop）不會產生重複文件。
    let again = service
        .rebuild(RebuildOptions {
            drop_index: false,
            page_size: 100,
        })
        .await
        .expect("rebuild again");
    assert_eq!(again.failed.len(), 0);
    assert_eq!(
        run_search(&stack, &index, request_for(&source, &fx.tag))
            .await
            .total,
        1,
        "重建兩次不該讓同一份文件出現兩筆"
    );

    cleanup(&stack, &index).await;
}

/// V0.2 Phase 0f：rebuild 之後 `ProjectionStore` 的狀態要對得上 report。
///
/// # 為什麼斷言長這樣
///
/// e2e 是並行跑的，但這裡的投影名就是**本測試專屬**的 index 名，狀態列的 `_id`
/// 就是投影名，所以這一列沒有別的測試會碰——下面四條在並行下嚴格成立：
///
/// 1. `state == Completed`（不是 Running：外層無論成功失敗都要寫最終狀態）；
/// 2. `written >= report.indexed`：本次 rebuild 寫了幾筆至少要記到
///    （`>=` 而不是 `==` 是因為每頁都會更新一次，而狀態是**累積**的 report 值——
///    若之後有人在 rebuild 裡加上重試，`written` 只會更大，不會更小）；
/// 3. checkpoint 存在且 `last_object_id` 有值：投影寫了東西就要有進度標記；
/// 4. lag 算得出來（`Some`）。`None` 代表 checkpoint 沒有來源時間戳，
///    那正是 `collected_at` 欄位被改名時會發生的靜默失效。
#[tokio::test]
async fn rebuild_records_projection_checkpoint_and_status() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} checkpoint canonical", fx.tag),
            &format!("{} checkpoint body", fx.tag),
        ),
    )
    .await;
    worker(&stack)
        .extract_document(document.id)
        .await
        .expect("extract");

    // drop_index=true 會先 reset_projection，所以起手狀態是乾淨的。
    let report = service
        .rebuild(RebuildOptions {
            drop_index: true,
            page_size: 100,
        })
        .await
        .expect("rebuild");
    assert!(report.indexed >= 1, "至少要寫進本次建立的那一份");

    let status = stack
        .os
        .rebuild_status(&index)
        .await
        .expect("rebuild_status");
    assert_eq!(
        status.state,
        RebuildState::Completed,
        "rebuild 結束後狀態必須是 Completed，實際 {status:?}"
    );
    assert!(
        status.started_at.is_some() && status.finished_at.is_some(),
        "Completed 必須同時有開始與結束時間：{status:?}"
    );
    assert!(
        status.written >= report.indexed,
        "rebuild 狀態的 written（{}）不該少於 report.indexed（{}）",
        status.written,
        report.indexed
    );
    assert_eq!(status.failed, report.failed.len() as u64);
    assert_eq!(status.last_error, None, "沒失敗就不該留錯誤訊息");

    let checkpoint = stack
        .os
        .checkpoint(&index)
        .await
        .expect("checkpoint")
        .expect("rebuild 寫了文件就必須有 checkpoint");
    assert!(
        checkpoint.last_object_id.is_some(),
        "checkpoint 必須記得最後一筆成功寫入的物件 id：{checkpoint:?}"
    );
    assert!(
        checkpoint.objects_written >= report.indexed,
        "checkpoint 累積計數（{}）不該少於本次寫入數（{}）",
        checkpoint.objects_written,
        report.indexed
    );
    let lag = stack
        .os
        .projection_lag(&index, Utc::now())
        .await
        .expect("projection_lag");
    assert!(
        lag.lag_seconds.is_some(),
        "checkpoint 有來源時間戳時 lag 必須算得出來；None 代表投影的 collected_at 讀不到"
    );

    // 狀態列是這個測試建立的，要自己清掉（CLAUDE.md §15）。
    stack
        .os
        .reset_projection(&index)
        .await
        .expect("reset_projection");
    assert!(
        stack
            .os
            .checkpoint(&index)
            .await
            .expect("checkpoint")
            .is_none(),
        "reset 之後 checkpoint 應該不見了"
    );
    cleanup(&stack, &index).await;
}

/// 數 PostgreSQL 裡的 canonical（非 duplicate）Document。
///
/// 用 cursor 翻頁而不是傳一個大 limit：`RelationalStore` 把 limit 夾在 1..=100，
/// 傳 100000 進去不會報錯，只會**靜默**回 100 筆。
async fn count_canonical_documents(pg: &PostgresCanonicalStore) -> u64 {
    let mut cursor: Option<Uuid> = None;
    let mut total = 0_u64;
    loop {
        let page = pg.list_documents(cursor, 100).await.expect("list");
        let got = page.len();
        total += page.iter().filter(|d| d.duplicate_of.is_none()).count() as u64;
        cursor = page.last().map(|d| d.id);
        if got < 100 {
            return total;
        }
    }
}

// ---------------------------------------------------------------------------
// mapping 與啟動
// ---------------------------------------------------------------------------

#[tokio::test]
async fn ensure_index_is_idempotent_and_applies_the_declared_mapping() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);

    assert!(service.ensure_index().await.expect("first"), "第一次應建立");
    assert!(
        !service.ensure_index().await.expect("second"),
        "第二次應走既有 index 的路徑，而不是報錯或重建"
    );
    // 重啟第三次也不能炸。
    service.ensure_index().await.expect("third");

    // 確認 mapping 真的套用了，不是靠 dynamic mapping。
    let request = SearchRequest {
        query: "anything".into(),
        ..SearchRequest::default()
    };
    let query = indexer::search::build(&index, &request).expect("build");
    let hits = stack.os.search(query).await.expect("search");
    assert_eq!(hits.total, 0, "空 index 應回 0 筆而不是報錯");

    cleanup(&stack, &index).await;
}

#[tokio::test]
async fn searching_a_missing_index_returns_empty_not_an_error() {
    // indexer 還沒跑過時，API 不該回 500——「還沒有東西被索引」不是伺服器錯誤。
    let stack = connect_stack().await;
    let index = format!(
        "osint-documents-e2e-never-created-{}",
        Uuid::now_v7().simple()
    );
    let query = indexer::search::build(&index, &SearchRequest::default()).expect("build");
    let hits = stack.os.search(query).await.expect("不該回錯誤");
    assert_eq!(hits.total, 0);
    assert!(hits.hits.is_empty());
}

// ---------------------------------------------------------------------------
// 分頁
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cursor_pagination_walks_every_document_exactly_once() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;

    // 刻意讓五份文件的內容**完全相同**（除了標題序號）：分數一樣時
    // 沒有唯一的收尾排序鍵就會漏掉或重複。
    let mut created = Vec::new();
    for i in 0..5 {
        let document = seed_document(
            &stack.pg,
            &source,
            &connector,
            DocSpec::new(
                &format!("{} paging {i}", fx.tag),
                &format!("{} identical body for paging", fx.tag),
            ),
        )
        .await;
        index_document(&stack, &service, &document).await;
        created.push(document.id.to_string());
    }
    created.sort();

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..10 {
        let request = SearchRequest {
            cursor: cursor.clone(),
            limit: Some(2),
            ..request_for(&source, &fx.tag)
        };
        let query = indexer::search::build(&index, &request).expect("build");
        let hits = stack.os.search(query).await.expect("search");
        assert_eq!(hits.total, 5);
        seen.extend(ids(&hits));
        cursor = indexer::next_cursor(&hits.hits, 2);
        if cursor.is_none() {
            break;
        }
    }
    seen.sort();
    assert_eq!(
        seen, created,
        "cursor 翻頁必須剛好走過每一筆一次；\
         漏掉或重複代表排序鍵沒有以唯一欄位收尾"
    );

    cleanup(&stack, &index).await;
}

// ---------------------------------------------------------------------------
// 溯源欄位
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_hit_carries_the_provenance_fields() {
    let stack = connect_stack().await;
    let index = test_index();
    let service = indexer_for(&stack, &index);
    service.ensure_index().await.expect("ensure index");

    let fx = Fixture::new();
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let document = seed_document(
        &stack.pg,
        &source,
        &connector,
        DocSpec::new(
            &format!("{} provenance", fx.tag),
            &format!("{} mentions {}", fx.tag, fx.cve),
        ),
    )
    .await;
    index_document(&stack, &service, &document).await;

    let hits = run_search(&stack, &index, request_for(&source, &fx.tag)).await;
    let hit = hits.hits.first().expect("一筆");
    let raw_evidence_id = hit
        .source
        .get(schema::F_RAW_EVIDENCE_ID)
        .and_then(Value::as_str)
        .expect("hit 必須帶 raw_evidence_id，Acceptance E 從這裡開始");
    assert_eq!(
        hit.source
            .get(schema::F_SOURCE_ID)
            .and_then(Value::as_str)
            .map(str::to_string),
        Some(source.id.to_string())
    );
    assert_eq!(
        hit.source
            .get(schema::F_CONNECTOR_ID)
            .and_then(Value::as_str)
            .map(str::to_string),
        Some(connector.id.to_string())
    );

    // 真的能回查。
    let evidence = stack
        .pg
        .get_raw_evidence(Uuid::parse_str(raw_evidence_id).expect("uuid"))
        .await
        .expect("query")
        .expect("RawEvidence 必須查得到");
    assert_eq!(evidence.source_id, source.id);
    assert_eq!(evidence.connector_id, connector.id);

    // entity 摘要也要在。
    let entities = hit
        .source
        .get(schema::F_ENTITIES)
        .and_then(Value::as_array)
        .expect("entities");
    assert!(
        entities.iter().any(|e| e
            .get("normalized_name")
            .and_then(Value::as_str)
            .is_some_and(|n| n.eq_ignore_ascii_case(&fx.cve))),
        "抽到的 CVE 應該出現在 index 的 entities 裡：{entities:?}"
    );

    cleanup(&stack, &index).await;
}
