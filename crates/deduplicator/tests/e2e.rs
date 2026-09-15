//! deduplicator e2e：對本機 Docker（Postgres／MinIO／Redpanda）真跑，不連外網。
//!
//! 涵蓋 SPEC §26 Acceptance B（同文章抓 10 次）與 C（兩來源轉載），
//! 五個 stage 各自的命中，以及冪等。
//!
//! **共用 DB 的鐵則**：每個測試的 dedup 鍵（external_key／canonical URL／內容）
//! 都要含一個 run-specific UUID。寫死字串會比對到前幾次測試留下的資料，
//! 得到「canonical 不是這次建立的那一份」這種難以理解的失敗。

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use chrono::Utc;
use collector::{CollectOutcome, CollectorRunner, RunBounds};
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_jobs::JobService;
use core_model::{Connector, Document, DocumentType, NetworkRule, RawEvidence, Source, SourceType};
use core_observability::MetricsRegistry;
use deduplicator::{DedupBounds, DedupOutcome, DedupStage, Deduplicator, duplicate_group_id};
use normalizer::{NormalizeOutcome, Normalizer};
use serde_json::json;
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use uuid::Uuid;

/// 一篇夠長的文章，讓 SimHash 有足夠 token（`simhash::MIN_TOKENS` = 16）。
const PARAGRAPHS: [&str; 6] = [
    "The vendor advisory describes a remote code execution flaw affecting the reporting service \
     bundled with the management console. Attackers can reach the vulnerable endpoint without \
     authentication whenever the management interface is exposed to untrusted networks.",
    "According to the published timeline the defect was reported by an external researcher during \
     a coordinated disclosure window, and the maintainers confirmed reproduction on every \
     supported branch within two working days of the initial report.",
    "Exploitation requires a single crafted request to the template rendering handler, which fails \
     to validate the supplied expression before evaluation. Successful exploitation yields command \
     execution under the account that owns the service process.",
    "The vendor published a patched release and recommends upgrading immediately. Administrators \
     who cannot upgrade should restrict network access to the management interface and monitor \
     authentication logs for unexpected requests originating outside the trusted range.",
    "Detection guidance in the advisory lists several indicators, including unusual child processes \
     spawned by the rendering worker, outbound connections from hosts that normally only accept \
     inbound traffic, and template cache entries written outside the deployment procedure.",
    "Downstream distributions have begun rebuilding their packages and several managed platform \
     operators announced maintenance windows. Organisations running the affected release behind a \
     reverse proxy should not assume the proxy alone prevents exploitation of this issue.",
];

/// run 標記在文章尾端重複幾次。
///
/// **這個數字是實測出來的，不是猜的。** 測試共用同一個 Postgres，前幾次跑留下的
/// Document 也在 Stage 4 的掃描範圍內；如果兩次 run 的文章內容相同，新的 canonical
/// 會被判成「舊 run 那份的重複」，測試就會以難以理解的方式失敗。
///
/// 但只是「附加幾個亂數詞」沒有用：SimHash 是加權投票，200 個共用詞會壓過
/// 20 個獨有詞，兩次 run 的指紋仍然幾乎相同（實測跨 run 距離只有 2～3，比同 run
/// 的轉載距離還小）。要讓 run 標記真的有份量，必須讓它帶足夠權重——也就是重複出現。
///
/// 用 60 組隨機 run id 實測（詞彙與本檔案的 PARAGRAPHS 相同）：
///
/// | 重複次數 | 同 run（改 3 個詞） | 跨 run |
/// |---|---|---|
/// | 30 | 0..=3 | ≥17 |
/// | 42 | **0..=2** | **≥22** |
/// | 46 以上 | 0..=0（標記完全蓋過改動） | ≥24 |
///
/// 選 42：同 run 穩定落在門檻 3 以內，跨 run 遠在門檻外，而且改動還看得出來
/// （不像 46 以上會被標記完全淹沒）。
const RUN_MARKER_REPEATS: usize = 42;

/// 這次 run 專屬的長文。同一個 `run` 算出來的內容完全相同。
fn article(run: Uuid) -> String {
    let marker = format!("runmarker{}", run.simple());
    let tail = std::iter::repeat_n(marker.as_str(), RUN_MARKER_REPEATS)
        .collect::<Vec<_>>()
        .join(" ");
    format!("{} {tail}", PARAGRAPHS.join(" "))
}

/// 短到不會產生 SimHash 指紋（token < `simhash::MIN_TOKENS`）的內容。
///
/// Stage 1／2／3／5 的單點測試用這個：沒有指紋 → Stage 4 一定不適用 →
/// 不可能被別的 run 留下的資料干擾，命中的一定是測試想測的那個 stage。
fn short_body(run: Uuid, tag: &str) -> String {
    format!("{tag} {}", run.simple())
}

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

fn dedup(stack: &Stack, producer: Option<Arc<EventProducer>>) -> Deduplicator {
    Deduplicator::new(
        stack.pg.clone(),
        producer,
        MetricsRegistry::new(),
        DedupBounds::default(),
    )
}

// ---------------------------------------------------------------------------
// 假 feed server + collector／normalizer 管線
// ---------------------------------------------------------------------------

fn rss_body(guid: &str, link: &str, title: &str, description: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Dedup Fixture</title>
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

async fn seed_source(
    pg: &PostgresCanonicalStore,
    platform: Option<&str>,
    base_url: Option<&str>,
) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("dedup-e2e-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: platform.map(str::to_string),
        base_url: base_url.map(str::to_string),
        description: Some("dedup e2e fixture".into()),
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
        reason: "dedup e2e 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("dedup-e2e-connector-{}", Uuid::now_v7()),
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

/// 抓一次 → 正規化 → 回傳 (raw_evidence_id, document_id)。
async fn collect_and_normalize(
    stack: &Stack,
    collector: &CollectorRunner,
    normalizer: &Normalizer,
    connector: &Connector,
) -> (Uuid, Uuid) {
    let outcome = collector
        .run_connector(connector.clone())
        .await
        .expect("collect");
    let CollectOutcome::Collected { raw_evidence_id } = outcome else {
        panic!("預期 Collected，得到 {outcome:?}。假 server 每次都回 200，不該是 Unchanged");
    };
    let created = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize");
    let NormalizeOutcome::Created { document_ids } = created else {
        panic!("預期 Created，得到 {created:?}");
    };
    assert_eq!(document_ids.len(), 1, "fixture 只有一則 item");
    let _ = stack;
    (raw_evidence_id, document_ids[0])
}

// ---------------------------------------------------------------------------
// 直接落地 Document（stage 單點測試用）
// ---------------------------------------------------------------------------

/// 建一份 Document + 它的 RawEvidence，不經 collector／normalizer。
///
/// stage 單點測試需要精確控制「哪些鍵相同、哪些不同」，走完整管線做不到這件事。
async fn seed_document(
    pg: &PostgresCanonicalStore,
    source: &Source,
    connector: &Connector,
    external_id: Option<&str>,
    source_url: &str,
    title: &str,
    body: &str,
) -> Document {
    let now = Utc::now();
    let raw_id = Uuid::now_v7();
    let evidence = RawEvidence {
        id: raw_id,
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: external_id.map(str::to_string),
        source_url: source_url.into(),
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

    let document = Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::Article,
        schema_version: "1".into(),
        title: Some(title.into()),
        body: Some(body.into()),
        summary: None,
        language: Some("en".into()),
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: Some(source_url.into()),
        canonical_url: Some(source_url.into()),
        normalized_content_hash: Some(core_model::content_hash(Some(title), None, Some(body))),
        confidence: 0.8,
        labels: Vec::new(),
        attributes: json!({
            "raw_evidence_id": raw_id,
            "external_id": external_id,
        }),
        external_key: None,
        simhash: None,
        duplicate_of: None,
    };
    pg.put_document(&document).await.expect("document");
    document
}

fn assert_duplicate(outcome: &DedupOutcome, canonical: Uuid, stage: DedupStage) {
    match outcome {
        DedupOutcome::Duplicate {
            canonical_object_id,
            stage: got,
            ..
        } => {
            assert_eq!(
                *got, stage,
                "命中的 stage 不對。實際 {got}，預期 {stage}——\
                 代表前一個 stage 的鍵不小心也對上了，這個測試就測不到目標 stage"
            );
            assert_eq!(*canonical_object_id, canonical, "canonical 指向錯誤");
        }
        other => panic!("預期判定為重複（stage={stage}），實際 {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// SPEC §26 Acceptance B：同一文章抓 10 次
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acceptance_b_same_article_ten_times_keeps_one_canonical_and_all_evidence() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let guid = format!("CVE-DEDUP-{run}");
    let link = format!("http://127.0.0.1/advisory/{run}");
    let feed = rss_body(&guid, &link, &format!("Advisory {run}"), &article(run));
    let feed_url = serve_body(feed).await;

    // platform 有值 + guid 有值 → Stage 1 成立，這是「完全相同」最典型的路徑。
    let source = seed_source(&stack.pg, Some("dedup-e2e-platform"), Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "dedup-e2e-b").expect("producer"));
    let collector = runner(&stack, producer.clone());
    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );

    let mut raw_ids = Vec::new();
    let mut doc_ids = Vec::new();
    for _ in 0..10 {
        let (raw_id, doc_id) =
            collect_and_normalize(&stack, &collector, &normalizer, &connector).await;
        raw_ids.push(raw_id);
        doc_ids.push(doc_id);
    }
    assert_eq!(raw_ids.len(), 10);
    // 10 次抓取必須是 10 筆各自獨立的 RawEvidence（SPEC §8：同 URL 新版本 → 新 RawEvidence）。
    let unique_raw: std::collections::HashSet<_> = raw_ids.iter().collect();
    assert_eq!(
        unique_raw.len(),
        10,
        "10 次抓取應產生 10 筆不同的 RawEvidence"
    );
    let unique_docs: std::collections::HashSet<_> = doc_ids.iter().collect();
    assert_eq!(unique_docs.len(), 10, "10 份 Document 應各自獨立");

    let service = dedup(&stack, None);
    let mut outcomes = Vec::new();
    for id in &doc_ids {
        outcomes.push(service.dedup_document(*id).await.expect("dedup"));
    }

    let canonicals: Vec<Uuid> = outcomes
        .iter()
        .filter_map(|o| match o {
            DedupOutcome::Canonical { document_id } => Some(*document_id),
            _ => None,
        })
        .collect();
    assert_eq!(
        canonicals.len(),
        1,
        "10 份完全相同的 Document 只能有 1 份是 canonical，實際 {canonicals:?}"
    );
    let canonical = canonicals[0];
    assert_eq!(
        canonical, doc_ids[0],
        "canonical 應該是最早的那一份（UUID v7 最小）"
    );

    for outcome in outcomes.iter().skip(1) {
        assert_duplicate(outcome, canonical, DedupStage::PlatformExternalId);
    }

    // duplicate group：9 筆，全部指向同一個 canonical。
    let groups = stack
        .pg
        .list_duplicate_groups_by_canonical(canonical, 100)
        .await
        .expect("list groups");
    assert_eq!(
        groups.len(),
        9,
        "9 份重複應各有一列 DuplicateGroup 指向同一個 canonical，實際 {}",
        groups.len()
    );
    for group in &groups {
        assert_eq!(group.canonical_object_id, canonical);
        assert_eq!(group.method, DedupStage::PlatformExternalId.as_str());
        assert!(
            (group.similarity - 1.0).abs() < f64::EPSILON,
            "完全相同的相似度應為 1.0"
        );
        assert!(
            group.member_raw_evidence_id.is_some(),
            "group 必須記得 member 的 RawEvidence，才能回查證據"
        );
        assert_eq!(
            group.model, None,
            "Stage 1-4 不靠模型，DuplicateGroup.model 必須是 NULL"
        );
    }

    // SPEC §16 的硬性規則：不得刪掉 duplicate evidence。逐筆數，不看有沒有報錯。
    for raw_id in &raw_ids {
        assert!(
            stack
                .pg
                .get_raw_evidence(*raw_id)
                .await
                .expect("get raw")
                .is_some(),
            "RawEvidence {raw_id} 不見了。SPEC §16 明令不得刪除 duplicate evidence"
        );
    }
    for doc_id in &doc_ids {
        let doc = stack
            .pg
            .get_document(*doc_id)
            .await
            .expect("get doc")
            .unwrap_or_else(|| panic!("Document {doc_id} 不見了，去重不可刪除 Document"));
        if *doc_id == canonical {
            assert_eq!(doc.duplicate_of, None, "canonical 不該被標成 duplicate");
        } else {
            assert_eq!(
                doc.duplicate_of,
                Some(canonical),
                "duplicate 必須指向 canonical"
            );
        }
        assert!(
            doc.external_key.is_some(),
            "每一份都要寫回 external_key，包含 canonical——\
             canonical 沒有鍵的話後面九份根本比不中"
        );
    }
}

// ---------------------------------------------------------------------------
// SPEC §26 Acceptance C：兩來源轉載
// ---------------------------------------------------------------------------

#[tokio::test]
async fn acceptance_c_reprint_from_two_sources_shares_one_duplicate_group() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();

    // 來源 A。
    let guid_a = format!("A-{run}");
    let link_a = format!("http://127.0.0.1/site-a/{run}");
    let feed_a = rss_body(&guid_a, &link_a, &format!("Advisory {run}"), &article(run));
    let url_a = serve_body(feed_a).await;
    let source_a = seed_source(&stack.pg, Some("site-a"), Some(&url_a)).await;
    let connector_a = seed_connector(&stack.pg, &source_a, &url_a).await;

    // 來源 B：轉載，改掉三個詞、換自己的標題前綴、不同 guid／link／platform。
    // → Stage 1（platform+guid 不同）、Stage 2（URL 不同）、Stage 3（內容不同）全部不命中，
    //   只剩 Stage 4 的 SimHash 能抓到。
    let reprint = article(run)
        .replace("Attackers", "Adversaries")
        .replace("immediately", "promptly")
        .replace("unexpected", "suspicious");
    let guid_b = format!("B-{run}");
    let link_b = format!("http://127.0.0.1/site-b/{run}");
    let feed_b = rss_body(&guid_b, &link_b, &format!("Advisory {run}"), &reprint);
    let url_b = serve_body(feed_b).await;
    let source_b = seed_source(&stack.pg, Some("site-b"), Some(&url_b)).await;
    let connector_b = seed_connector(&stack.pg, &source_b, &url_b).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "dedup-e2e-c").expect("producer"));
    let collector = runner(&stack, producer);
    let normalizer = Normalizer::new(
        stack.pg.clone(),
        stack.s3.clone(),
        None,
        MetricsRegistry::new(),
    );

    let (raw_a, doc_a) = collect_and_normalize(&stack, &collector, &normalizer, &connector_a).await;
    let (raw_b, doc_b) = collect_and_normalize(&stack, &collector, &normalizer, &connector_b).await;
    assert_ne!(raw_a, raw_b, "兩個來源必須是兩筆獨立的 RawEvidence");

    let service = dedup(&stack, None);
    let first = service.dedup_document(doc_a).await.expect("dedup a");
    assert_eq!(
        first,
        DedupOutcome::Canonical { document_id: doc_a },
        "先到的那份應是 canonical"
    );
    let second = service.dedup_document(doc_b).await.expect("dedup b");
    assert_duplicate(&second, doc_a, DedupStage::Simhash);

    let DedupOutcome::Duplicate { similarity, .. } = &second else {
        unreachable!("上面已斷言是 Duplicate");
    };
    // 相似度只斷言「很高」，不斷言「小於 1.0」：run 標記的權重可能讓那三個改動一個 bit
    // 都沒翻動（實測同 run 距離是 0..=2），distance 0 → similarity 1.0 仍是合法的
    // Stage 4 命中。「改幾個字會讓距離變小但不為零」由 simhash.rs 的單元測試
    // `changing_a_few_words_stays_within_default_threshold` 用固定輸入驗證，
    // 那裡沒有共用 DB 的干擾，斷言得起來。
    assert!(
        *similarity >= 0.95,
        "SimHash 命中的相似度應 ≥ 0.95（距離 ≤ 3/64），實際 {similarity}"
    );

    // 同一個 duplicate group，兩份 RawEvidence 各自保留、兩個 Source 各自保留。
    let group = stack
        .pg
        .get_duplicate_group_by_member(doc_b)
        .await
        .expect("get group")
        .expect("轉載那份應有 duplicate group");
    assert_eq!(group.canonical_object_id, doc_a);
    assert_eq!(group.method, DedupStage::Simhash.as_str());
    assert_eq!(group.member_raw_evidence_id, Some(raw_b));
    assert_eq!(group.model, None, "Stage 4 不靠模型");

    for raw_id in [raw_a, raw_b] {
        assert!(
            stack
                .pg
                .get_raw_evidence(raw_id)
                .await
                .expect("get raw")
                .is_some(),
            "轉載偵測不得刪除任一來源的 RawEvidence"
        );
    }
    for source_id in [source_a.id, source_b.id] {
        assert!(
            stack
                .pg
                .get_source(source_id)
                .await
                .expect("get source")
                .is_some(),
            "兩個 Source 都必須保留"
        );
    }
}

// ---------------------------------------------------------------------------
// 五個 stage 各自命中
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stage_1_matches_on_platform_and_external_id() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let external_id = format!("guid-{run}");
    let source = seed_source(&stack.pg, Some(&format!("platform-{run}")), None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    // 只有 platform+external_id 相同；URL 與內容都刻意不同，確保命中的真的是 Stage 1。
    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&external_id),
        &format!("http://127.0.0.1/a/{run}"),
        &format!("Title A {run}"),
        &short_body(run, "stage one first"),
    )
    .await;
    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&external_id),
        &format!("http://127.0.0.1/b/{run}"),
        &format!("Completely different heading {run}"),
        &short_body(run, "stage one second"),
    )
    .await;

    let service = dedup(&stack, None);
    service.dedup_document(first.id).await.expect("first");
    let outcome = service.dedup_document(second.id).await.expect("second");
    assert_duplicate(&outcome, first.id, DedupStage::PlatformExternalId);
}

#[tokio::test]
async fn stage_2_matches_on_canonical_url_after_stripping_tracking_params() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    // platform = None → Stage 1 不適用，才測得到 Stage 2。
    let source = seed_source(&stack.pg, None, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let base = format!("http://127.0.0.1/article/{run}?id=7");
    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &base,
        &format!("Title A {run}"),
        &short_body(run, "stage two first"),
    )
    .await;
    // 同一個 URL，只是多了追蹤參數、host 大小寫不同、帶 fragment。
    let noisy = format!("http://127.0.0.1/article/{run}?utm_source=newsletter&id=7&fbclid=zz#top");
    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &noisy,
        &format!("Different heading {run}"),
        &short_body(run, "stage two second"),
    )
    .await;

    let service = dedup(&stack, None);
    service.dedup_document(first.id).await.expect("first");
    let outcome = service.dedup_document(second.id).await.expect("second");
    assert_duplicate(&outcome, first.id, DedupStage::CanonicalUrl);

    let stored = stack
        .pg
        .get_document(second.id)
        .await
        .expect("get")
        .expect("document");
    assert_eq!(
        stored.canonical_url.as_deref(),
        Some(format!("http://127.0.0.1/article/{run}?id=7").as_str()),
        "正規化後的 URL 必須寫回 canonical_url——normalizer 原本只是把 source_url 複製過去"
    );
    assert_eq!(
        stored.source_url.as_deref(),
        Some(noisy.as_str()),
        "source_url 是觀測到的原始值，不可被正規化結果覆蓋"
    );
}

#[tokio::test]
async fn stage_3_matches_on_normalized_content_hash() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, None, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let title = format!("Shared headline {run}");
    let body = short_body(run, "stage three shared");
    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &format!("http://127.0.0.1/x/{run}"),
        &title,
        &body,
    )
    .await;
    // 不同 URL、相同內容，但空白排版不同——Stage 3 的空白正規化必須吸收掉這個差異。
    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &format!("http://127.0.0.1/y/{run}"),
        &format!("  {title}  "),
        &body.replace(' ', "\n  "),
    )
    .await;

    let service = dedup(&stack, None);
    service.dedup_document(first.id).await.expect("first");
    let outcome = service.dedup_document(second.id).await.expect("second");
    assert_duplicate(&outcome, first.id, DedupStage::ContentHash);
}

#[tokio::test]
async fn stage_4_matches_near_duplicate_via_simhash() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, None, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let title = format!("Advisory {run}");
    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &format!("http://127.0.0.1/p/{run}"),
        &title,
        &article(run),
    )
    .await;
    let reprint = article(run)
        .replace("Attackers", "Adversaries")
        .replace("immediately", "promptly");
    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &format!("http://127.0.0.1/q/{run}"),
        &title,
        &reprint,
    )
    .await;

    let service = dedup(&stack, None);
    service.dedup_document(first.id).await.expect("first");
    let outcome = service.dedup_document(second.id).await.expect("second");
    assert_duplicate(&outcome, first.id, DedupStage::Simhash);

    let stored = stack
        .pg
        .get_document(second.id)
        .await
        .expect("get")
        .expect("document");
    assert!(
        stored.simhash.is_some(),
        "fingerprint 必須寫回 documents.simhash，否則下一份比不到它"
    );
}

/// Stage 5：V0.1 的實作永遠回 `Unsupported`，所以真正要驗的是
/// 「介面接得上、命中時 method 寫成 semantic」。用一個測試用 detector 驗這件事。
#[tokio::test]
async fn stage_5_semantic_interface_is_wired_but_unsupported_by_default() {
    use async_trait::async_trait;
    use deduplicator::{SemanticDuplicateDetector, SemanticOutcome};

    struct FakeSemantic {
        canonical: Uuid,
    }

    #[async_trait]
    impl SemanticDuplicateDetector for FakeSemantic {
        fn detector_id(&self) -> &'static str {
            "fake-test-detector"
        }
        async fn detect(
            &self,
            _document: &Document,
        ) -> Result<SemanticOutcome, deduplicator::DeduplicatorError> {
            Ok(SemanticOutcome::Hit {
                canonical_object_id: self.canonical,
                similarity: 0.87,
                model: "intfloat/multilingual-e5-small-int8".into(),
            })
        }
    }

    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, None, None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;

    let canonical = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &format!("http://127.0.0.1/s1/{run}"),
        &format!("Heading one {run}"),
        &short_body(run, "stage five canonical"),
    )
    .await;
    // 與 canonical 在 Stage 1～4 全部不相似：只有假的 Stage 5 會命中。
    let other = seed_document(
        &stack.pg,
        &source,
        &connector,
        None,
        &format!("http://127.0.0.1/s2/{run}"),
        &format!("Heading two {run}"),
        &short_body(run, "stage five member"),
    )
    .await;

    // 預設 detector：不命中。
    let default_service = dedup(&stack, None);
    assert_eq!(
        default_service
            .dedup_document(canonical.id)
            .await
            .expect("canonical"),
        DedupOutcome::Canonical {
            document_id: canonical.id
        }
    );

    let semantic_service = dedup(&stack, None).with_semantic_detector(Arc::new(FakeSemantic {
        canonical: canonical.id,
    }));
    let outcome = semantic_service
        .dedup_document(other.id)
        .await
        .expect("semantic");
    assert_duplicate(&outcome, canonical.id, DedupStage::Semantic);
    let group = stack
        .pg
        .get_duplicate_group_by_member(other.id)
        .await
        .expect("get group")
        .expect("group");
    assert_eq!(group.method, "semantic");
    assert!((group.similarity - 0.87).abs() < 1e-9);
    assert_eq!(
        group.model.as_deref(),
        Some("intfloat/multilingual-e5-small-int8"),
        "Stage 5 命中必須把實際模型名寫進 DuplicateGroup.model"
    );
}

// ---------------------------------------------------------------------------
// 未命中、冪等、事件
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unique_document_is_marked_canonical() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, Some(&format!("solo-{run}")), None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let doc = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&format!("solo-guid-{run}")),
        &format!("http://127.0.0.1/solo/{run}"),
        &format!("Solo heading {run}"),
        &short_body(run, "solo body"),
    )
    .await;

    let service = dedup(&stack, None);
    let outcome = service.dedup_document(doc.id).await.expect("dedup");
    assert_eq!(
        outcome,
        DedupOutcome::Canonical {
            document_id: doc.id
        }
    );

    let stored = stack
        .pg
        .get_document(doc.id)
        .await
        .expect("get")
        .expect("document");
    assert_eq!(stored.duplicate_of, None);
    assert!(stored.external_key.is_some(), "canonical 也要有 Stage 1 鍵");
    assert!(
        stored.simhash.is_none(),
        "這份 fixture 的 token 少於 simhash::MIN_TOKENS，不該產生 fingerprint。\
         有值代表 MIN_TOKENS 的門檻被改小了，短文會開始互相誤判成近似重複"
    );
    assert!(
        stack
            .pg
            .get_duplicate_group_by_member(doc.id)
            .await
            .expect("group")
            .is_none(),
        "沒有重複就不該有 duplicate group"
    );
}

#[tokio::test]
async fn consuming_the_same_event_twice_creates_only_one_duplicate_group() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, Some(&format!("idem-{run}")), None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let external_id = format!("idem-guid-{run}");

    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&external_id),
        &format!("http://127.0.0.1/i1/{run}"),
        &format!("Heading {run}"),
        &short_body(run, "idempotency one"),
    )
    .await;
    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&external_id),
        &format!("http://127.0.0.1/i2/{run}"),
        &format!("Heading {run}"),
        &short_body(run, "idempotency two"),
    )
    .await;

    let service = dedup(&stack, None);
    // 模擬 normalizer 送出的 object.normalized payload，整則消費兩次。
    let payload = json!({
        "raw_evidence_id": Uuid::now_v7(),
        "document_ids": [first.id, second.id],
        "count": 2,
    });
    let round_one = service.handle_payload(&payload).await.expect("first pass");
    assert_eq!(round_one.len(), 2);
    assert_duplicate(&round_one[1], first.id, DedupStage::PlatformExternalId);

    let round_two = service.handle_payload(&payload).await.expect("second pass");
    assert_eq!(round_two.len(), 2);
    for outcome in &round_two {
        assert!(
            matches!(outcome, DedupOutcome::AlreadyDone { .. }),
            "第二次消費必須全部回 AlreadyDone，實際 {outcome:?}"
        );
    }
    assert_eq!(
        round_two[1],
        DedupOutcome::AlreadyDone {
            document_id: second.id,
            canonical_object_id: Some(first.id),
        },
        "AlreadyDone 要能回報當初判到的 canonical，否則重播事件的下游拿不到結果"
    );

    // 真正的驗收點：group 只有一列，而且 id 是 v5 算出來的那個。
    let groups = stack
        .pg
        .list_duplicate_groups_by_canonical(first.id, 100)
        .await
        .expect("list");
    assert_eq!(
        groups.len(),
        1,
        "重複消費同一事件不可建出第二個 DuplicateGroup，實際 {} 列",
        groups.len()
    );
    assert_eq!(groups[0].id, duplicate_group_id(second.id));

    let claims = stack
        .pg
        .list_provenance_by_subject(second.id)
        .await
        .expect("provenance");
    assert_eq!(
        claims
            .iter()
            .filter(|p| p.action == deduplicator::ACTION_DEDUPLICATED)
            .count(),
        1,
        "deduplicated claim 必須嚴格只有一列"
    );
}

#[tokio::test]
async fn concurrent_dedup_of_same_document_keeps_one_claim() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, Some(&format!("race-{run}")), None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let doc = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&format!("race-guid-{run}")),
        &format!("http://127.0.0.1/race/{run}"),
        &format!("Heading {run}"),
        &short_body(run, "race body"),
    )
    .await;

    let a = dedup(&stack, None);
    let b = a.clone();
    let id = doc.id;
    let left = tokio::spawn(async move { a.dedup_document(id).await });
    let right = tokio::spawn(async move { b.dedup_document(id).await });
    left.await.expect("join left").expect("dedup left");
    right.await.expect("join right").expect("dedup right");

    let claims = stack
        .pg
        .list_provenance_by_subject(doc.id)
        .await
        .expect("provenance");
    assert_eq!(
        claims
            .iter()
            .filter(|p| p.action == deduplicator::ACTION_DEDUPLICATED)
            .count(),
        1,
        "unique index 必須讓並發的兩次只留一列 claim"
    );
}

#[tokio::test]
async fn dedup_completed_event_is_published_with_stage_and_canonical() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let source = seed_source(&stack.pg, Some(&format!("evt-{run}")), None).await;
    let connector = seed_connector(&stack.pg, &source, "http://127.0.0.1/none").await;
    let external_id = format!("evt-guid-{run}");

    let group_id = format!("osint-e2e-dedup-{run}");
    let consumer = EventConsumer::connect(
        &stack.brokers,
        &group_id,
        &[EventTopic::DedupCompleted.as_str()],
    )
    .expect("dedup.completed consumer");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "dedup-e2e-event").expect("producer"));
    let service = dedup(&stack, Some(producer));

    let first = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&external_id),
        &format!("http://127.0.0.1/e1/{run}"),
        &format!("Heading {run}"),
        &short_body(run, "event one"),
    )
    .await;
    let second = seed_document(
        &stack.pg,
        &source,
        &connector,
        Some(&external_id),
        &format!("http://127.0.0.1/e2/{run}"),
        &format!("Heading {run}"),
        &short_body(run, "event two"),
    )
    .await;
    service.dedup_document(first.id).await.expect("first");
    service.dedup_document(second.id).await.expect("second");

    let envelope = wait_for_document(&consumer, second.id).await;
    assert_eq!(envelope.event_type, "dedup.completed");
    assert_eq!(envelope.payload["is_duplicate"], json!(true));
    assert_eq!(
        envelope.payload["stage"],
        json!(DedupStage::PlatformExternalId.as_str())
    );
    assert_eq!(envelope.payload["canonical_object_id"], json!(first.id));
    assert_eq!(
        envelope.payload["duplicate_group_id"],
        json!(duplicate_group_id(second.id))
    );
}

async fn wait_for_document(
    consumer: &EventConsumer,
    document_id: Uuid,
) -> core_events::EventEnvelope {
    let want = document_id.to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "等 dedup.completed（document_id={want}）逾時。請確認 Redpanda 在跑、topic 可自動建立"
        );
        let envelope = consumer
            .next_envelope(remaining)
            .await
            .expect("consume 失敗。請確認 osint-core-redpanda-1 在跑（埠 9092）");
        if envelope
            .payload
            .get("document_id")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == want)
        {
            let _ = consumer.commit_last();
            return envelope;
        }
    }
}
