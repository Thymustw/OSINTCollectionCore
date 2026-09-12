//! 跨服務驗收測試共用的組裝。只連本機 Docker，不打外網。
//!
//! 共用 DB 的鐵則（同 `deduplicator/tests/e2e.rs`）：每個測試的 dedup 鍵
//! （platform／external_id／URL／內容）都要含一個 run-specific UUID。
//! 寫死字串會比對到前幾次測試留下的資料，得到「canonical 不是這次建立的那一份」
//! 這種難以理解的失敗。

#![allow(dead_code)] // 每支測試只用得到其中幾個 helper。

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use chrono::Utc;
use collector::{CollectOutcome, CollectorRunner, RunBounds};
use core_events::{EventConsumer, EventEnvelope, EventProducer, EventTopic};
use core_jobs::JobService;
use core_model::{Connector, NetworkRule, Source, SourceType};
use core_observability::MetricsRegistry;
use deduplicator::{DedupBounds, Deduplicator};
use entity_worker::{EntityWorker, ExtractionBounds};
use normalizer::Normalizer;
use serde_json::json;
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use uuid::Uuid;

pub struct Stack {
    pub pg: PostgresCanonicalStore,
    pub s3: S3ObjectStore,
    pub bucket: String,
    pub brokers: String,
}

pub async fn connect_stack() -> Stack {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "驗收測試只連本機 Postgres"
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
        "驗收測試只連本機 Redpanda，實際 brokers={brokers}"
    );

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");
    let s3 = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("s3");
    s3.ensure_bucket().await.expect("bucket");
    Stack {
        pg,
        s3,
        bucket,
        brokers,
    }
}

// --------------------------------------------------------------------- 假 feed

/// run 標記在文章尾端重複幾次。
///
/// **這個數字不是裝飾。** 這個測試套件共用同一個 Postgres，前幾次跑留下的
/// Document 也在 dedup Stage 4（SimHash）的掃描範圍內。只是在文章裡插入幾個
/// run-specific 的詞沒有用：SimHash 是加權投票，幾十個共用詞會壓過少數獨有詞。
///
/// 這裡踩過一次：`article()` 原本只帶四處 run id，第二次跑時 Acceptance F 的
/// 第一份 Document 就被判成**前一次 run** 那份的重複（stage=simhash,
/// similarity=0.953125，也就是 Hamming 距離 3，剛好卡在預設門檻上）。
/// 測試因此失敗，而失敗的原因與被測邏輯無關。
///
/// 42 這個值沿用 `deduplicator/tests/e2e.rs` 的 `RUN_MARKER_REPEATS`——
/// 那裡用 60 組隨機 run id 實測過：42 次重複時同 run 距離 0..=2、跨 run ≥22。
/// 本檔的文章比那邊短，標記佔的權重只會更高，跨 run 距離只會更遠。
const RUN_MARKER_REPEATS: usize = 42;

/// 一篇夠長的文章，讓 SimHash 有足夠 token，也讓 entity 抽取有東西可抽
/// （CVE／domain／IP／email／hash 各至少一個）。
///
/// 同一個 `run` 算出來的內容完全相同（Acceptance F 需要「同一筆證據重跑」
/// 產生內容一致的兩份 Document）；不同 `run` 之間則靠 [`RUN_MARKER_REPEATS`]
/// 拉開 SimHash 距離。
pub fn article(run: Uuid) -> String {
    let marker = format!("runmarker{}", run.simple());
    let tail = std::iter::repeat_n(marker.as_str(), RUN_MARKER_REPEATS)
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "The advisory CVE-2026-9{:04} affects the reporting service on host \
         node{}.example.invalid (203.0.113.{}). Contact soc-{}@example.invalid for details. \
         The published SHA256 of the patched artefact is {}{}. Administrators should restrict \
         network access to the management interface and monitor authentication logs for \
         unexpected requests originating outside the trusted range. {tail}",
        run.as_u128() % 10_000,
        run.simple(),
        run.as_u128() % 200,
        run.simple(),
        run.simple(),
        run.simple()
    )
}

pub fn rss_body(guid: &str, link: &str, title: &str, description: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Acceptance Fixture</title>
    <link>http://127.0.0.1/feed</link>
    <description>acceptance</description>
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

/// 起一個只回固定內容的本機 HTTP server，回傳它的 URL。
pub async fn serve_body(body: String) -> String {
    let app = Router::new().route("/rss.xml", get(move || std::future::ready(body.clone())));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/rss.xml", addr.port())
}

// ------------------------------------------------------------------ 種子資料

pub async fn seed_source(
    pg: &PostgresCanonicalStore,
    platform: Option<&str>,
    base_url: Option<&str>,
) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("acceptance-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: platform.map(str::to_string),
        base_url: base_url.map(str::to_string),
        description: Some("acceptance fixture".into()),
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

pub async fn seed_connector(
    pg: &PostgresCanonicalStore,
    source: &Source,
    feed_url: &str,
) -> Connector {
    let now = Utc::now();
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "acceptance 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("acceptance-connector-{}", Uuid::now_v7()),
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

// --------------------------------------------------------------------- 服務

pub fn collector_runner(stack: &Stack, producer: Arc<EventProducer>) -> CollectorRunner {
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

pub fn normalizer(stack: &Stack, metrics: MetricsRegistry) -> Normalizer {
    Normalizer::new(stack.pg.clone(), stack.s3.clone(), None, metrics)
}

/// 會真的發 `object.normalized` 的 normalizer。
///
/// 「重送事件」的測試必須拿到**真正發出去的那一則 payload**，不能自己組一個
/// 形狀相近的 JSON——那樣 payload 欄位改名時測試不會失敗，而線上會。
pub fn normalizer_with_producer(
    stack: &Stack,
    metrics: MetricsRegistry,
    producer: Arc<EventProducer>,
) -> Normalizer {
    Normalizer::new(stack.pg.clone(), stack.s3.clone(), Some(producer), metrics)
}

pub fn deduplicator(stack: &Stack, metrics: MetricsRegistry) -> Deduplicator {
    Deduplicator::new(stack.pg.clone(), None, metrics, DedupBounds::default())
}

/// 會真的發 `dedup.completed` 的 deduplicator。理由同 [`normalizer_with_producer`]。
pub fn deduplicator_with_producer(
    stack: &Stack,
    metrics: MetricsRegistry,
    producer: Arc<EventProducer>,
) -> Deduplicator {
    Deduplicator::new(
        stack.pg.clone(),
        Some(producer),
        metrics,
        DedupBounds::default(),
    )
}

pub fn entity_worker(stack: &Stack, metrics: MetricsRegistry) -> EntityWorker {
    EntityWorker::new(
        stack.pg.clone(),
        None,
        metrics,
        ExtractionBounds {
            max_extractions: 200,
            max_scan_bytes: 262_144,
        },
    )
}

/// 跑一次採集，回 `raw_evidence_id`。
pub async fn collect_once(collector: &CollectorRunner, connector: &Connector) -> Uuid {
    let outcome = collector
        .run_connector(connector.clone())
        .await
        .expect("collect");
    match outcome {
        CollectOutcome::Collected { raw_evidence_id } => raw_evidence_id,
        other => panic!("預期 Collected，得到 {other:?}。假 server 每次都回 200，不該是 Unchanged"),
    }
}

// ------------------------------------------------------------------ 事件往返

/// 訂閱一個 topic 的一次性 consumer（run-specific group，不會干擾真正的服務）。
pub fn probe_consumer(brokers: &str, topic: EventTopic) -> EventConsumer {
    let group = format!("osint-acceptance-{}", Uuid::now_v7());
    EventConsumer::connect(brokers, &group, &[topic.as_str()]).expect("consumer")
}

/// 等到 payload 的 `field == value` 的那一則 envelope。
pub async fn wait_for_event(consumer: &EventConsumer, field: &str, value: &str) -> EventEnvelope {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "等 {field}={value} 逾時。請確認 Redpanda 在跑、topic 可自動建立"
        );
        let envelope = consumer
            .next_envelope(remaining)
            .await
            .expect("consume 失敗。請確認 osint-core-redpanda-1 在跑（埠 9092）");
        let got = envelope
            .payload
            .get(field)
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if got == value {
            let _ = consumer.commit_last();
            return envelope;
        }
    }
}

// -------------------------------------------------------------- crash 模擬

/// 刪掉某個 subject 的 claim，模擬「寫完資料但還沒 claim 就 crash」。
///
/// 回傳刪掉幾列。**呼叫端要斷言它是 1**——刪到 0 列的話後面整個測試
/// 驗的就不是 crash window 了，而是「重跑一個已經 claim 過的東西」，
/// 那條路徑早就被別的測試蓋掉了。
pub async fn delete_claim(pg: &PostgresCanonicalStore, subject_id: Uuid, action: &str) -> u64 {
    sqlx::query("DELETE FROM provenance WHERE subject_id = $1 AND action = $2")
        .bind(subject_id)
        .bind(action)
        .execute(pg.pool())
        .await
        .expect("delete claim")
        .rows_affected()
}

/// 這個 source 底下有幾筆 RawEvidence。
pub async fn count_raw_evidence(pg: &PostgresCanonicalStore, source_id: Uuid) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT count(*) FROM raw_evidence WHERE source_id = $1")
        .bind(source_id)
        .fetch_one(pg.pool())
        .await
        .expect("count raw_evidence")
}

/// 這筆 RawEvidence 衍生出幾份 Document（看 `attributes->>'raw_evidence_id'`）。
pub async fn documents_of_raw_evidence(
    pg: &PostgresCanonicalStore,
    raw_evidence_id: Uuid,
) -> Vec<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM documents WHERE attributes->>'raw_evidence_id' = $1::text ORDER BY id",
    )
    .bind(raw_evidence_id.to_string())
    .fetch_all(pg.pool())
    .await
    .expect("query documents")
}
