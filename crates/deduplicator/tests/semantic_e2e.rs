//! Stage 5 對本機 ml-commons + OpenSearch + Redis 的 e2e。
//!
//! **預設略過。** 要跑先：
//! `bash scripts/opensearch-ml-setup.sh` 與 `bash scripts/opensearch-ml-setup-e5.sh`。
//!
//! 不依賴 `embedding-worker` crate：canonical 的向量由本檔直接 `embed` 後
//! `index()` 進 per-run index。理由見本 crate `Cargo.toml`——indexer 的
//! dev-dep 已經指向 deduplicator，再把 embedding-worker（它 normal-dep
//! indexer）加進來會在 cargo 測試圖上形成環。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use core_model::{Connector, Document, DocumentType, NetworkRule, RawEvidence, Source, SourceType};
use core_observability::MetricsRegistry;
use deduplicator::{
    DedupBounds, DedupOutcome, DedupStage, Deduplicator, SemanticDuplicateDetector,
    VectorSemanticDetector,
};
use indexer::schema as doc_schema;
use serde_json::json;
use storage_core::HealthProvider;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, KeyValueStore, RelationalStore,
    SearchDocument, SearchStore, embedding_cache_key, embedding_content_hash,
};
use storage_opensearch::{MINILM_MODEL_NAME, MlCommonsEmbeddingProvider, OpenSearchStore};
use storage_postgres::PostgresCanonicalStore;
use storage_redis::RedisKeyValueStore;
use uuid::Uuid;

struct Stack {
    pg: PostgresCanonicalStore,
    os: OpenSearchStore,
    embeddings: MlCommonsEmbeddingProvider,
    redis: RedisKeyValueStore,
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
    let redis_url = required_env("REDIS_URL").expect("REDIS_URL");

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");

    let os = OpenSearchStore::connect(&url)
        .expect("opensearch client")
        .with_refresh_on_write(true);
    let info = os.cluster_info().await.expect("GET /");
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");

    let embeddings = MlCommonsEmbeddingProvider::connect(&url)
        .await
        .expect("兩個模型都必須 DEPLOYED");
    let redis = RedisKeyValueStore::connect(&redis_url).expect("redis client");
    redis.health().await.expect("Redis PING");

    Stack {
        pg,
        os,
        embeddings,
        redis,
    }
}

fn documents_index() -> String {
    format!("osint-documents-dedup-e2e-{}", Uuid::now_v7().simple())
}

async fn cleanup(stack: &Stack, index: &str) {
    let _ = stack.os.delete_index(index).await;
}

async fn seed_source(pg: &PostgresCanonicalStore) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("dedup-semantic-e2e-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: None,
        base_url: None,
        description: Some("stage 5 e2e".into()),
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
        reason: "stage 5 e2e".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("dedup-semantic-e2e-connector-{}", Uuid::now_v7()),
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

/// 短到不會產生 SimHash 指紋，避免 Stage 4 搶先命中。
async fn seed_document(
    pg: &PostgresCanonicalStore,
    source: &Source,
    connector: &Connector,
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
        external_id: None,
        source_url: source_url.into(),
        retrieved_at: now,
        content_type: Some("text/plain".into()),
        mime_type: Some("text/plain".into()),
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
        attributes: json!({ "raw_evidence_id": raw_id }),
        external_key: None,
        simhash: None,
        duplicate_of: None,
    };
    pg.put_document(&document).await.expect("document");
    document
}

fn detector(
    stack: &Stack,
    index: &str,
) -> VectorSemanticDetector<MlCommonsEmbeddingProvider, OpenSearchStore> {
    VectorSemanticDetector::new(
        stack.embeddings.clone(),
        stack.os.clone(),
        Some(Arc::new(stack.redis.clone()) as Arc<dyn KeyValueStore>),
        index,
        0.90,
        Duration::from_secs(900),
    )
}

/// 兩份文件：不同 title／URL（Stage 1–4 全 miss）、**同一段 body**（cosine ≈ 1）。
/// canonical 的向量由本測試直接 overlay，不經 embedding-worker。
#[tokio::test]
#[ignore = "需要本機已部署 MiniLM + e5，以及 Redis"]
async fn stage_5_marks_same_body_as_semantic_duplicate_and_writes_model() {
    let stack = connect_stack().await;
    let index = documents_index();
    stack
        .os
        .ensure_index_with(
            &index,
            &doc_schema::index_settings(),
            &doc_schema::index_mappings(),
        )
        .await
        .expect("documents mapping");

    let run = Uuid::now_v7();
    let body = format!("stage five identical body {run}");
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;

    let canonical = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("http://127.0.0.1/a/{run}"),
        &format!("Heading one {run}"),
        &body,
    )
    .await;
    let vector = stack
        .embeddings
        .embed(&EmbeddingRequest {
            text: body.clone(),
            kind: EmbeddingKind::Passage,
            language: Some("en".into()),
        })
        .await
        .expect("embed canonical");
    assert_eq!(vector.model, MINILM_MODEL_NAME);
    stack
        .os
        .index(SearchDocument {
            index: index.clone(),
            id: canonical.id.to_string(),
            body: json!({
                doc_schema::F_DOCUMENT_ID: canonical.id.to_string(),
                doc_schema::F_OBJECT_TYPE: "article",
                doc_schema::F_TITLE: canonical.title,
                doc_schema::F_BODY: canonical.body,
                doc_schema::F_LANGUAGE: "en",
                doc_schema::F_EMBEDDING_EN: vector.vector,
                doc_schema::F_EMBEDDING_EN_MODEL_VERSION: vector.model_version,
            }),
        })
        .await
        .expect("overlay canonical vector");

    let member = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("http://127.0.0.1/b/{run}"),
        &format!("Heading two {run}"),
        &body,
    )
    .await;

    let service = Deduplicator::new(
        stack.pg.clone(),
        None,
        MetricsRegistry::new(),
        DedupBounds::default(),
    )
    .with_semantic_detector(Arc::new(detector(&stack, &index)));

    let first = service
        .dedup_document(canonical.id)
        .await
        .expect("canonical");
    assert_eq!(
        first,
        DedupOutcome::Canonical {
            document_id: canonical.id
        }
    );

    let outcome = service.dedup_document(member.id).await.expect("member");
    match outcome {
        DedupOutcome::Duplicate {
            canonical_object_id,
            stage,
            ..
        } => {
            assert_eq!(canonical_object_id, canonical.id);
            assert_eq!(stage, DedupStage::Semantic);
        }
        other => panic!("預期 Stage 5 Duplicate，得到 {other:?}"),
    }

    let group = stack
        .pg
        .get_duplicate_group_by_member(member.id)
        .await
        .expect("get group")
        .expect("group");
    assert_eq!(group.method, "semantic");
    assert_eq!(
        group.model.as_deref(),
        Some(MINILM_MODEL_NAME),
        "Stage 5 必須把實際模型名寫進 DuplicateGroup.model"
    );
    assert!(
        group.similarity >= 0.90,
        "同一段 body 的 cosine 應 ≥ 0.90，實際 {}",
        group.similarity
    );

    cleanup(&stack, &index).await;
}

/// Stage 5 寫進 Redis 的向量，同一把 `embedding-cache:v1:` key 必須讀得回來。
#[tokio::test]
#[ignore = "需要本機已部署 MiniLM + e5，以及 Redis"]
async fn stage_5_writes_embedding_cache_key() {
    let stack = connect_stack().await;
    let index = documents_index();
    stack
        .os
        .ensure_index_with(
            &index,
            &doc_schema::index_settings(),
            &doc_schema::index_mappings(),
        )
        .await
        .expect("documents mapping");

    let run = Uuid::now_v7();
    let body = format!("cache probe body {run}");
    let source = seed_source(&stack.pg).await;
    let connector = seed_connector(&stack.pg, &source).await;
    let doc = seed_document(
        &stack.pg,
        &source,
        &connector,
        &format!("http://127.0.0.1/c/{run}"),
        &format!("Cache heading {run}"),
        &body,
    )
    .await;

    let outcome = detector(&stack, &index).detect(&doc).await.expect("detect");
    assert!(
        matches!(
            outcome,
            deduplicator::SemanticOutcome::NoMatch | deduplicator::SemanticOutcome::Hit { .. }
        ),
        "基礎設施必須可用；得到 {outcome:?}"
    );

    let hash = embedding_content_hash(&body);
    let key = embedding_cache_key(&hash);
    let cached = stack
        .redis
        .get(&key)
        .await
        .expect("redis get")
        .expect("Stage 5 必須把向量寫進 embedding-cache:v1:{{hash}}");
    let parsed: storage_core::EmbeddingVector =
        serde_json::from_slice(&cached).expect("JSON EmbeddingVector");
    assert_eq!(parsed.content_hash, hash);
    assert_eq!(parsed.model, MINILM_MODEL_NAME);

    let _ = stack.redis.del(&key).await;
    cleanup(&stack, &index).await;
}
