//! chunk-worker e2e：一次性 PostgreSQL DB（TestDatabase）＋一次性 topic namespace（TestTopics），
//! 不碰開發 DB、不跟執行中的服務共用 topic。需要 compose 與 e5 tokenizer
//! （`scripts/fetch-e5-tokenizer.sh`）。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use chunk_worker::{ChunkOutcome, ChunkWorker};
use chunker::{Chunker, ChunkerConfig, E5_TOKENIZER_SHA256, E5Tokenizer};
use core_events::{EventTopic, TestTopics};
use core_model::{Document, DocumentType};
use core_observability::MetricsRegistry;
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use storage_core::{ChunkStore, RelationalStore};
use storage_postgres::{PostgresCanonicalStore, TestDatabase};
use uuid::Uuid;

fn chunker() -> Arc<Chunker> {
    let path = std::env::var("E5_TOKENIZER_PATH").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../models/e5-small/tokenizer.json"),
        PathBuf::from,
    );
    let tok = E5Tokenizer::load(&path, E5_TOKENIZER_SHA256)
        .unwrap_or_else(|err| panic!("{err}；請先跑 scripts/fetch-e5-tokenizer.sh"));
    Arc::new(Chunker::new(Arc::new(tok), ChunkerConfig::default()).unwrap())
}

fn brokers() -> String {
    let b = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        b.contains("127.0.0.1") || b.contains("localhost"),
        "e2e 只連本機 Redpanda，實際 brokers={b}"
    );
    b
}

fn document(body: &str, duplicate_of: Option<Uuid>) -> Document {
    let now = Utc::now();
    Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::WebPage,
        schema_version: "1".into(),
        title: Some("chunk-worker e2e".into()),
        body: Some(body.into()),
        summary: None,
        language: Some("zh".into()),
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: None,
        canonical_url: None,
        normalized_content_hash: None,
        confidence: 1.0,
        labels: vec![],
        attributes: json!({}),
        external_key: None,
        simhash: None,
        duplicate_of,
    }
}

fn long_body() -> String {
    "研究人員指出，攻擊者利用 CVE-2026-1234 入侵邊界設備，並在內網橫向移動。\
     The actor deployed a loader that fetched the second stage from 203.0.113.45. "
        .repeat(40)
}

#[tokio::test]
async fn dedup_completed_to_document_chunked() {
    load_workspace_dotenv();
    let db = TestDatabase::create().await;
    let pg: PostgresCanonicalStore = db.store().clone();
    let topics = TestTopics::create(&brokers());
    let producer = Arc::new(topics.producer("chunk-worker-e2e").expect("producer"));
    let consumer = topics
        .consumer("chunked", &[EventTopic::DocumentChunked.as_str()])
        .expect("consumer");
    let worker = ChunkWorker::new(
        pg.clone(),
        chunker(),
        Some(producer),
        MetricsRegistry::new(),
    );

    let doc = document(&long_body(), None);
    pg.put_document(&doc).await.unwrap();

    // 1. 第一次：寫段落、發事件。
    let outcome = worker
        .handle_payload(&json!({"document_id": doc.id, "is_duplicate": false}))
        .await
        .unwrap();
    let ChunkOutcome::Chunked {
        chunk_count,
        strategy,
        ..
    } = outcome
    else {
        panic!("應寫入段落，實際 {outcome:?}");
    };
    assert_eq!(strategy, "fixed");
    assert!(chunk_count >= 3, "段數 {chunk_count}");
    let stored = pg.list_document_chunks(doc.id).await.unwrap();
    assert_eq!(stored.len(), chunk_count);

    let deadline = Instant::now() + Duration::from_secs(20);
    let envelope = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "20 秒內沒收到 document.chunked");
        if let Ok(env) = consumer.next_envelope(remaining).await
            && env.payload["document_id"] == json!(doc.id)
        {
            break env;
        }
    };
    assert_eq!(envelope.event_type, "document.chunked");
    assert_eq!(envelope.payload["chunk_count"], json!(chunk_count));
    assert_eq!(envelope.payload["strategy"], json!("fixed"));
    assert_eq!(envelope.payload["strategy_version"], json!(1));
    let ids: Vec<Uuid> = serde_json::from_value(envelope.payload["chunk_ids"].clone()).unwrap();
    assert_eq!(ids, stored.iter().map(|c| c.id).collect::<Vec<_>>());

    // 2. 重送同一則：段落相同，不寫、不發事件。
    let again = worker
        .handle_payload(&json!({"document_id": doc.id, "is_duplicate": false}))
        .await
        .unwrap();
    assert_eq!(
        again,
        ChunkOutcome::Unchanged {
            document_id: doc.id
        }
    );
    assert!(
        consumer
            .next_envelope(Duration::from_secs(3))
            .await
            .is_err(),
        "未變更不該再發 document.chunked"
    );
}

#[tokio::test]
async fn duplicates_missing_and_bad_payloads_are_skipped() {
    load_workspace_dotenv();
    let db = TestDatabase::create().await;
    let pg: PostgresCanonicalStore = db.store().clone();
    let worker = ChunkWorker::new(pg.clone(), chunker(), None, MetricsRegistry::new());

    let canonical = document("正本。", None);
    pg.put_document(&canonical).await.unwrap();
    // 事件說重複。
    let dup_event = document("轉載。", Some(canonical.id));
    pg.put_document(&dup_event).await.unwrap();
    assert_eq!(
        worker
            .handle_payload(&json!({"document_id": dup_event.id, "is_duplicate": true}))
            .await
            .unwrap(),
        ChunkOutcome::SkippedDuplicate {
            document_id: dup_event.id
        }
    );
    // 事件說不重複，但 DB 上 duplicate_of 有值（事件較舊）。
    assert_eq!(
        worker
            .handle_payload(&json!({"document_id": dup_event.id, "is_duplicate": false}))
            .await
            .unwrap(),
        ChunkOutcome::SkippedDuplicate {
            document_id: dup_event.id
        }
    );
    assert!(
        pg.list_document_chunks(dup_event.id)
            .await
            .unwrap()
            .is_empty()
    );

    let missing = Uuid::now_v7();
    assert_eq!(
        worker
            .handle_payload(&json!({"document_id": missing, "is_duplicate": false}))
            .await
            .unwrap(),
        ChunkOutcome::DocumentMissing {
            document_id: missing
        }
    );
    // 缺 is_duplicate 必須報錯，不能預設成 false。
    assert!(
        worker
            .handle_payload(&json!({"document_id": canonical.id}))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn rebuild_chunks_all_canonical_documents_and_is_idempotent() {
    load_workspace_dotenv();
    let db = TestDatabase::create().await;
    let pg: PostgresCanonicalStore = db.store().clone();
    let worker = ChunkWorker::new(pg.clone(), chunker(), None, MetricsRegistry::new());

    let mut canonical = Vec::new();
    for i in 0..5 {
        let d = document(&format!("第 {i} 份。{}", long_body()), None);
        pg.put_document(&d).await.unwrap();
        canonical.push(d);
    }
    let dup = document("轉載。", Some(canonical[0].id));
    pg.put_document(&dup).await.unwrap();

    let (_tx, mut stop) = tokio::sync::watch::channel(false);
    // page_size 2：驗分頁游標沒有漏頁。
    let first = worker.rebuild(2, &mut stop).await.unwrap();
    assert_eq!(first.chunked, 5, "{first:?}");
    assert_eq!(first.failed, 0);
    assert!(!first.interrupted);
    for d in &canonical {
        assert!(!pg.list_document_chunks(d.id).await.unwrap().is_empty());
    }
    assert!(pg.list_document_chunks(dup.id).await.unwrap().is_empty());

    let second = worker.rebuild(2, &mut stop).await.unwrap();
    assert_eq!(second.chunked, 0, "{second:?}");
    assert_eq!(second.unchanged, 5);

    // 停止訊號在開始前就到：一份都不處理。
    let (tx, mut stopped) = tokio::sync::watch::channel(false);
    tx.send(true).unwrap();
    let report = worker.rebuild(2, &mut stopped).await.unwrap();
    assert!(report.interrupted);
    assert_eq!(report.scanned, 0);
}
