//! chunk-embedder e2e：一次性 PostgreSQL DB（TestDatabase）＋一次性 topic namespace（TestTopics）＋
//! 一次性 OpenSearch index（`osint-chunks-e2e-<uuid>`，Drop 時刪除，含 panic）＋真的 osint-inference
//! （compose 的 127.0.0.1:18091，或 `INFERENCE_URL`）。需要 `make compose-up` 加 `osint-inference` 容器。

use std::time::{Duration, Instant};

use chrono::Utc;
use chunk_embedder::{ChunkEmbedder, EmbedOutcome, EmbedderConfig, InferenceClient, schema};
use core_events::{EventTopic, TestTopics};
use core_model::{Collection, Document, DocumentType};
use core_observability::MetricsRegistry;
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_search};
use storage_core::{
    ChunkStore, DocumentChunk, RelationalStore, SearchFilter, SearchStore, SortField,
    StructuredSearch, VectorSearch, chunk_id,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::{PostgresCanonicalStore, TestDatabase};
use uuid::Uuid;

/// Drop 時刪掉測試 index（含 panic）。Drop 不能 async，另開執行緒用自己的 runtime 刪。
struct DropIndex {
    url: String,
    index: String,
}

impl Drop for DropIndex {
    fn drop(&mut self) {
        let (url, index) = (self.url.clone(), self.index.clone());
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .ok()?;
            rt.block_on(async {
                let os = OpenSearchStore::connect(&url).ok()?;
                os.delete_index(&index).await.ok()
            })
        })
        .join();
    }
}

struct Stack {
    pg: PostgresCanonicalStore,
    os: OpenSearchStore,
    index: String,
    _drop_index: DropIndex,
    _db: TestDatabase,
}

async fn stack() -> Stack {
    load_workspace_dotenv();
    indexer::script::init().expect("簡繁轉換器");
    let db = TestDatabase::create().await;
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    verify_not_opencti_search(&url).expect("OpenSearch 埠隔離");
    let os = OpenSearchStore::connect(&url)
        .expect("os")
        .with_refresh_on_write(true);
    let index = format!("osint-chunks-e2e-{}", Uuid::now_v7().simple());
    os.ensure_index_with(&index, &schema::index_settings(), &schema::index_mappings())
        .await
        .expect("建測試 index");
    Stack {
        pg: db.store().clone(),
        os,
        _drop_index: DropIndex {
            url,
            index: index.clone(),
        },
        index,
        _db: db,
    }
}

fn inference() -> InferenceClient {
    let url = std::env::var("INFERENCE_URL").unwrap_or_else(|_| "http://127.0.0.1:18091".into());
    InferenceClient::new(&url, Duration::from_secs(30)).unwrap()
}

fn embedder(s: &Stack) -> ChunkEmbedder<PostgresCanonicalStore, OpenSearchStore> {
    ChunkEmbedder::new(
        s.pg.clone(),
        s.os.clone(),
        inference(),
        MetricsRegistry::new(),
        EmbedderConfig {
            index: s.index.clone(),
            batch_size: 2,
            ..EmbedderConfig::default()
        },
    )
}

fn document() -> Document {
    let now = Utc::now();
    Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::Article,
        schema_version: "1".into(),
        title: Some("e2e 標題".into()),
        body: Some("x".into()),
        summary: None,
        language: Some("zh".into()),
        author: None,
        published_at: Some(now),
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
        duplicate_of: None,
    }
}

fn chunks(document_id: Uuid, texts: &[&str]) -> Vec<DocumentChunk> {
    let mut start = 0u32;
    texts
        .iter()
        .enumerate()
        .map(|(i, t)| {
            let seq = i as u32;
            let len = t.chars().count() as u32;
            let c = DocumentChunk {
                id: chunk_id(document_id, "fixed", 1, seq),
                document_id,
                seq,
                start_char: start,
                end_char: start + len,
                text: (*t).into(),
                header: (seq > 0).then(|| "e2e 標題".into()),
                token_count: len,
                strategy: "fixed".into(),
                strategy_version: 1,
                simhash: None,
                group_id: None,
                created_at: Utc::now(),
            };
            start += len;
            c
        })
        .collect()
}

async fn ids_in_index(s: &Stack, document_id: Uuid) -> Vec<String> {
    let hits =
        s.os.search(StructuredSearch {
            index: s.index.clone(),
            expression: None,
            fields: vec![],
            filters: vec![SearchFilter::Term {
                field: schema::F_DOCUMENT_ID.into(),
                value: document_id.to_string(),
            }],
            size: 100,
            search_after: None,
            sort: vec![SortField {
                field: schema::F_CHUNK_ID.into(),
                ascending: true,
            }],
            highlight_fields: vec![],
            highlight_expression: None,
            normalized: None,
        })
        .await
        .unwrap();
    hits.hits.into_iter().map(|h| h.id).collect()
}

#[tokio::test]
async fn embeds_chunks_shrinks_on_rechunk_and_clears_on_duplicate() {
    let s = stack().await;
    let (_tx, stop) = tokio::sync::watch::channel(false);
    let worker = embedder(&s);

    let doc = document();
    s.pg.put_document(&doc).await.unwrap();
    let col = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: format!("chunk-embedder-e2e-{}", doc.id),
        description: None,
        status: "active".into(),
        priority: 1,
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    s.pg.put_collection(&col).await.unwrap();
    s.pg.link_collection_object(col.id, doc.id).await.unwrap();

    let three = chunks(
        doc.id,
        &[
            "勒索軟體攻擊醫院。",
            "攻擊者索取贖金。",
            "The FBI issued an advisory.",
        ],
    );
    s.pg.replace_document_chunks(doc.id, &three).await.unwrap();

    // 1. 寫入三段，欄位與 collection_ids 正確。
    let out = worker
        .handle_payload(
            &json!({"document_id": doc.id, "chunk_ids": ["偽造的 id 不應被採用"]}),
            &stop,
        )
        .await
        .unwrap();
    assert_eq!(
        out,
        EmbedOutcome::Embedded {
            document_id: doc.id,
            chunks: 3,
            removed: 0
        }
    );
    let mut want: Vec<String> = three.iter().map(|c| c.id.to_string()).collect();
    want.sort();
    assert_eq!(ids_in_index(&s, doc.id).await, want);

    let hits =
        s.os.vector_search(VectorSearch {
            index: s.index.clone(),
            field: schema::F_EMBEDDING.into(),
            vector: inference()
                .embed(&["query: 醫院遭勒索".to_string()], 384)
                .await
                .unwrap()
                .1
                .remove(0),
            k: 3,
            filters: vec![SearchFilter::Term {
                field: schema::F_COLLECTION_IDS.into(),
                value: col.id.to_string(),
            }],
        })
        .await
        .unwrap();
    assert_eq!(hits.hits.len(), 3, "collection filter 應命中三段");
    let top = &hits.hits[0].source;
    assert_eq!(
        top[schema::F_TEXT],
        "勒索軟體攻擊醫院。",
        "最相近的應是醫院那段：{top}"
    );
    assert_eq!(top[schema::F_MODEL], "multilingual-e5-small-int8");
    assert_eq!(top[schema::F_COLLECTION_IDS], json!([col.id]));
    // 字級簡繁轉換（不做兩岸用詞轉換）：軟體 → 软体，不是 软件。
    assert_eq!(top[schema::F_TEXT_S], "勒索软体攻击医院。", "簡體副本");

    // 2. 重送：冪等，向量數不變。
    worker.embed_document(doc.id, &stop).await.unwrap();
    assert_eq!(ids_in_index(&s, doc.id).await.len(), 3);

    // 3. 重切成兩段（策略版本升級）：舊三段要被刪掉。
    let two: Vec<DocumentChunk> = chunks(
        doc.id,
        &[
            "勒索軟體攻擊醫院，攻擊者索取贖金。",
            "The FBI issued an advisory.",
        ],
    )
    .into_iter()
    .map(|mut c| {
        c.strategy_version = 2;
        c.id = chunk_id(doc.id, "fixed", 2, c.seq);
        c
    })
    .collect();
    s.pg.replace_document_chunks(doc.id, &two).await.unwrap();
    let out = worker.embed_document(doc.id, &stop).await.unwrap();
    assert_eq!(
        out,
        EmbedOutcome::Embedded {
            document_id: doc.id,
            chunks: 2,
            removed: 3
        }
    );
    let mut want: Vec<String> = two.iter().map(|c| c.id.to_string()).collect();
    want.sort();
    assert_eq!(ids_in_index(&s, doc.id).await, want);

    // 4. 文件被判成重複：清空它的向量。
    let canonical = document();
    s.pg.put_document(&canonical).await.unwrap();
    let mut dup = doc.clone();
    dup.duplicate_of = Some(canonical.id);
    s.pg.put_document(&dup).await.unwrap();
    let out = worker.embed_document(doc.id, &stop).await.unwrap();
    assert_eq!(
        out,
        EmbedOutcome::Cleared {
            document_id: doc.id,
            removed: 2
        }
    );
    assert!(ids_in_index(&s, doc.id).await.is_empty());

    // 5. 不存在的文件：不報錯、0 筆可刪。
    let missing = Uuid::now_v7();
    assert_eq!(
        worker.embed_document(missing, &stop).await.unwrap(),
        EmbedOutcome::Cleared {
            document_id: missing,
            removed: 0
        }
    );
    // 缺 document_id 報錯。
    assert!(worker.handle_payload(&json!({}), &stop).await.is_err());
}

#[tokio::test]
async fn consumes_document_chunked_from_broker_and_rebuilds() {
    let s = stack().await;
    let brokers = required_env("REDPANDA_BROKERS").unwrap_or_else(|_| "127.0.0.1:9092".into());
    let topics = TestTopics::create(&brokers);
    let producer = topics.producer("chunk-embedder-e2e").unwrap();
    let consumer = topics
        .consumer("embed", &[EventTopic::DocumentChunked.as_str()])
        .unwrap();
    let (_tx, stop) = tokio::sync::watch::channel(false);
    let worker = embedder(&s);

    let mut docs = Vec::new();
    for i in 0..3 {
        let d = document();
        s.pg.put_document(&d).await.unwrap();
        s.pg.replace_document_chunks(
            d.id,
            &chunks(d.id, &[&format!("第 {i} 份文件的第一段。"), "第二段。"]),
        )
        .await
        .unwrap();
        docs.push(d);
    }
    producer
        .publish(
            EventTopic::DocumentChunked,
            Some(&docs[0].id.to_string()),
            None,
            json!({"document_id": docs[0].id}),
        )
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let env = loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "20 秒內沒收到 document.chunked");
        if let Ok(env) = consumer.next_envelope(remaining).await
            && env.payload["document_id"] == json!(docs[0].id)
        {
            break env;
        }
    };
    worker.handle_payload(&env.payload, &stop).await.unwrap();
    assert_eq!(ids_in_index(&s, docs[0].id).await.len(), 2);

    // rebuild（page_size 2）補齊其他兩份。
    let report = worker.rebuild(2, &stop).await.unwrap();
    assert_eq!(report.documents, 3, "{report:?}");
    assert_eq!(report.chunks, 6);
    assert_eq!(report.failed, 0);
    for d in &docs {
        assert_eq!(ids_in_index(&s, d.id).await.len(), 2);
    }
    // 停止訊號先到：一份都不處理。
    let (tx, stopped) = tokio::sync::watch::channel(false);
    tx.send(true).unwrap();
    assert!(worker.rebuild(2, &stopped).await.unwrap().interrupted);
}

#[tokio::test]
async fn unreachable_inference_is_retried_until_stopped() {
    let s = stack().await;
    let worker = ChunkEmbedder::new(
        s.pg.clone(),
        s.os.clone(),
        // 沒有人聽的埠：連線失敗是暫時性錯誤，應退避重試而不是放棄。
        InferenceClient::new("http://127.0.0.1:9", Duration::from_secs(1)).unwrap(),
        MetricsRegistry::new(),
        EmbedderConfig {
            index: s.index.clone(),
            backoff_initial: Duration::from_millis(50),
            backoff_max: Duration::from_millis(100),
            ..EmbedderConfig::default()
        },
    );
    let d = document();
    s.pg.put_document(&d).await.unwrap();
    s.pg.replace_document_chunks(d.id, &chunks(d.id, &["一段。"]))
        .await
        .unwrap();
    let (tx, stop) = tokio::sync::watch::channel(false);
    let handle = {
        let stop = stop.clone();
        tokio::spawn(async move { worker.embed_document(d.id, &stop).await })
    };
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!handle.is_finished(), "連不上推論服務應持續退避，不該放棄");
    tx.send(true).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(chunk_embedder::ChunkEmbedderError::Stopped)),
        "{result:?}"
    );
}
