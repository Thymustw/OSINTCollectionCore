//! `POST /api/v1/search/hybrid` 與 `GET /objects/{id}/similar` 對真實
//! OpenSearch + ml-commons 的端到端。
//!
//! ⚠️ **標了 `#[ignore]`，CI 預設不跑。** 理由與
//! `semantic_search_api_e2e.rs` 相同：CI 沒部署兩個模型。本機驗證：
//!
//! ```bash
//! bash scripts/opensearch-ml-setup.sh
//! bash scripts/opensearch-ml-setup-e5.sh
//! cargo nextest run -p core-api --test hybrid_search_api_e2e -- --ignored --nocapture
//! ```
//!
//! 每個測試用自己的 index，結尾刪掉（CLAUDE.md §15）。

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use core_api::{
    AppState, AuthState, RateLimiter, SearchState, SemanticSearchState, ready_always, router,
};
use core_model::{Document, DocumentType};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use indexer::schema as doc_schema;
use serde_json::{Value, json};
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, RelationalStore, SearchDocument,
    SearchStore,
};
use storage_opensearch::{MlCommonsEmbeddingProvider, OpenSearchStore};
use storage_postgres::PostgresCanonicalStore;
use tower::ServiceExt;
use uuid::Uuid;

struct Stack {
    os: OpenSearchStore,
    embeddings: MlCommonsEmbeddingProvider,
    url: String,
}

async fn connect_stack() -> Stack {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    verify_not_opencti_search(&url).expect("OpenSearch 埠隔離");

    let os = OpenSearchStore::connect(&url)
        .expect("opensearch")
        .with_refresh_on_write(true);
    assert_opensearch_identity(&os.cluster_info().await.expect("GET /"))
        .expect("必須是 OpenSearch");
    let embeddings = MlCommonsEmbeddingProvider::connect(&url)
        .await
        .expect("兩個模型都必須 DEPLOYED；請先跑 scripts/opensearch-ml-setup.sh 與 -e5.sh");
    Stack {
        os,
        embeddings,
        url,
    }
}

struct TestApi {
    app: Router,
    token: String,
}

fn build_api(stack: &Stack, index: &str) -> TestApi {
    build_api_with_store(stack, index, None)
}

fn build_api_with_store(
    stack: &Stack,
    index: &str,
    store: Option<core_api::SharedStore>,
) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let token = jwt.issue("hybrid-e2e", Role::Viewer).expect("issue");
    let search_store = OpenSearchStore::connect(&stack.url)
        .expect("search 自己的連線")
        .with_refresh_on_write(true);
    let semantic_store = OpenSearchStore::connect(&stack.url)
        .expect("semantic search 自己的連線")
        .with_refresh_on_write(true);
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        store,
        objects: None,
        jobs: None,
        merge: None,
        resolver: None,
        auto_approval: None,
        graph_resolver: None,
        graph: None,
        graph_projection: None,
        import: None,
        search: Some(Arc::new(SearchState {
            store: search_store,
            index: index.to_string(),
        })),
        semantic_search: Some(Arc::new(SemanticSearchState {
            store: semantic_store,
            embeddings: stack.embeddings.clone(),
            index: index.to_string(),
        })),
        hybrid_weights: core_config::HybridSearchSection::default(),
        ready: ready_always(),
        backends: core_api::ReadyProbe::new(Vec::new()),
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

async fn post_hybrid(api: &TestApi, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/search/hybrid")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", api.token))
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = api.app.clone().oneshot(request).await.expect("call api");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn get_similar(api: &TestApi, id: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!("/api/v1/objects/{id}/similar"))
        .header("Authorization", format!("Bearer {}", api.token))
        .body(Body::empty())
        .unwrap();
    let response = api.app.clone().oneshot(request).await.expect("call api");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn seed_document(
    stack: &Stack,
    index: &str,
    id: &str,
    title: &str,
    body: Option<&str>,
    language: Option<&str>,
    text_for_vector: Option<&str>,
) {
    let mut source = json!({
        doc_schema::F_DOCUMENT_ID: id,
        doc_schema::F_OBJECT_TYPE: "article",
        doc_schema::F_TITLE: title,
        doc_schema::F_SOURCE_ID: "22222222-2222-7222-8222-222222222222",
        doc_schema::F_PUBLISHED_AT: "2026-09-10T12:00:00Z",
    });
    if let Some(body) = body {
        source[doc_schema::F_BODY] = json!(body);
    }
    if let Some(language) = language {
        source[doc_schema::F_LANGUAGE] = json!(language);
    }
    if let Some(text_for_vector) = text_for_vector {
        let model = stack.embeddings.model_for(language);
        let field = if model.model == storage_opensearch::MINILM_MODEL_NAME {
            doc_schema::F_EMBEDDING_EN
        } else {
            doc_schema::F_EMBEDDING_MULTI
        };
        let version_field = if field == doc_schema::F_EMBEDDING_EN {
            doc_schema::F_EMBEDDING_EN_MODEL_VERSION
        } else {
            doc_schema::F_EMBEDDING_MULTI_MODEL_VERSION
        };
        let vector = stack
            .embeddings
            .embed(&EmbeddingRequest {
                text: text_for_vector.to_string(),
                kind: EmbeddingKind::Passage,
                language: language.map(str::to_string),
            })
            .await
            .expect("embed passage");
        source[field] = json!(vector.vector);
        source[version_field] = json!(vector.model_version);
    }
    stack
        .os
        .index(SearchDocument {
            index: index.into(),
            id: id.into(),
            body: source,
        })
        .await
        .expect("index document");
}

#[tokio::test]
#[ignore = "需要本機 ml-commons 已部署兩個模型；CI 沒有這個環境，見檔頭說明"]
async fn hybrid_ranks_the_document_that_hits_both_signals_first() {
    let stack = connect_stack().await;
    let index = format!("osint-documents-hyb-e2e-{}", Uuid::now_v7().simple());
    let created = stack
        .os
        .ensure_index_with(
            &index,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("建立測試 index");
    assert!(created, "per-run index 不該已經存在");

    let result = run_hybrid_case(&stack, &index).await;
    stack.os.delete_index(&index).await.expect("刪除測試 index");
    result.expect("hybrid 融合");
}

async fn run_hybrid_case(stack: &Stack, index: &str) -> Result<(), String> {
    // 兩條訊號都命中：標題含 ransomware，向量也是醫院勒索活動。
    seed_document(
        stack,
        index,
        "both-signals",
        "LockBit ransomware advisory",
        Some("The LockBit gang published a new ransomware campaign targeting hospitals."),
        Some("en"),
        Some("The LockBit gang published a new ransomware campaign targeting hospitals."),
    )
    .await;
    // 只有 BM25：標題含 ransomware，向量是完全不相干的天氣。
    seed_document(
        stack,
        index,
        "bm25-only",
        "ransomware weather note",
        Some("Sunny skies and mild temperatures expected this weekend."),
        Some("en"),
        Some("Sunny skies and mild temperatures expected this weekend."),
    )
    .await;
    // 只有向量：標題沒有 ransomware 這個詞，內容語意接近醫院勒索。
    seed_document(
        stack,
        index,
        "vector-only",
        "Hospital security briefing",
        Some("A criminal group is extorting clinics with encryption malware."),
        Some("en"),
        Some("A criminal group is extorting clinics with encryption malware."),
    )
    .await;

    let api = build_api(stack, index);
    let (status, body) = post_hybrid(
        &api,
        json!({
            "query": "ransomware campaign against hospitals",
            "language": "en",
            "limit": 10
        }),
    )
    .await;
    if status != StatusCode::OK {
        return Err(format!("應 200，實際 {status} {body}"));
    }
    let hits = body["hits"]
        .as_array()
        .ok_or_else(|| "hits 不是陣列".to_string())?;
    if hits.is_empty() {
        return Err("hybrid 0 筆。請確認 BM25 與向量都有寫進 index".into());
    }
    let top_id = hits[0]["document_id"].as_str().unwrap_or("");
    if top_id != "both-signals" {
        return Err(format!(
            "兩條訊號都命中的文件應排第一，實際第一名 {top_id}，整份 {body}"
        ));
    }
    let top = &hits[0];
    if top["bm25_rank"].as_u64().is_none() {
        return Err(format!("both-signals 應有 bm25_rank：{top}"));
    }
    if top["vector_rank"].as_u64().is_none() {
        return Err(format!("both-signals 應有 vector_rank：{top}"));
    }
    if top["fused_score"].as_f64().is_none() {
        return Err("fused_score 應是數字".into());
    }

    let ids: Vec<&str> = hits
        .iter()
        .filter_map(|h| h["document_id"].as_str())
        .collect();
    if !ids.contains(&"bm25-only") {
        return Err(format!("應找得到只有 BM25 命中的文件，實際 {ids:?}"));
    }
    if !ids.contains(&"vector-only") {
        return Err(format!("應找得到只有向量命中的文件，實際 {ids:?}"));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "需要本機 ml-commons 已部署兩個模型與 Postgres；CI 沒有這個環境，見檔頭說明"]
async fn similar_excludes_self_and_returns_nearest_neighbour() {
    let stack = connect_stack().await;
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "e2e 只連本機 Postgres"
    );
    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");

    let index = format!("osint-documents-sim-e2e-{}", Uuid::now_v7().simple());
    stack
        .os
        .ensure_index_with(
            &index,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("建立測試 index");

    let result = run_similar_case(&stack, &pg, &index).await;
    stack.os.delete_index(&index).await.expect("刪除測試 index");
    result.expect("similar 排除自己");
}

fn pg_document(id: Uuid, title: &str, body: &str) -> Document {
    let now = Utc::now();
    Document {
        id,
        object_type: DocumentType::Article,
        schema_version: "0.1".into(),
        title: Some(title.into()),
        body: Some(body.into()),
        summary: None,
        language: Some("en".into()),
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: None,
        canonical_url: None,
        normalized_content_hash: None,
        confidence: 0.9,
        labels: vec![],
        attributes: json!({}),
        external_key: None,
        simhash: None,
        duplicate_of: None,
    }
}

async fn run_similar_case(
    stack: &Stack,
    pg: &PostgresCanonicalStore,
    index: &str,
) -> Result<(), String> {
    let lockbit_a = Uuid::now_v7();
    let lockbit_b = Uuid::now_v7();
    let weather = Uuid::now_v7();
    let no_vector = Uuid::now_v7();

    let seed = "The LockBit gang published a new ransomware campaign targeting hospitals.";
    pg.put_document(&pg_document(lockbit_a, "LockBit ransomware advisory", seed))
        .await
        .map_err(|e| e.to_string())?;
    pg.put_document(&pg_document(
        lockbit_b,
        "LockBit hospital campaign update",
        "LockBit continues targeting hospitals with ransomware.",
    ))
    .await
    .map_err(|e| e.to_string())?;
    pg.put_document(&pg_document(
        weather,
        "Local weather update",
        "Sunny skies and mild temperatures expected this weekend.",
    ))
    .await
    .map_err(|e| e.to_string())?;
    pg.put_document(&pg_document(
        no_vector,
        "Not yet embedded",
        "This document has no vector yet.",
    ))
    .await
    .map_err(|e| e.to_string())?;

    seed_document(
        stack,
        index,
        &lockbit_a.to_string(),
        "LockBit ransomware advisory",
        Some(seed),
        Some("en"),
        Some(seed),
    )
    .await;
    seed_document(
        stack,
        index,
        &lockbit_b.to_string(),
        "LockBit hospital campaign update",
        Some("LockBit continues targeting hospitals with ransomware."),
        Some("en"),
        Some("LockBit continues targeting hospitals with ransomware."),
    )
    .await;
    seed_document(
        stack,
        index,
        &weather.to_string(),
        "Local weather update",
        Some("Sunny skies and mild temperatures expected this weekend."),
        Some("en"),
        Some("Sunny skies and mild temperatures expected this weekend."),
    )
    .await;
    // 投影裡有這份、但沒寫向量：API 應回空清單，不是錯誤。
    seed_document(
        stack,
        index,
        &no_vector.to_string(),
        "Not yet embedded",
        Some("This document has no vector yet."),
        Some("en"),
        None,
    )
    .await;

    let api = build_api_with_store(stack, index, Some(Arc::new(pg.clone())));
    let (status, body) = get_similar(&api, &lockbit_a.to_string()).await;
    if status != StatusCode::OK {
        return Err(format!("similar 應 200，實際 {status} {body}"));
    }
    let hits = body["hits"]
        .as_array()
        .ok_or_else(|| "hits 不是陣列".to_string())?;
    let ids: Vec<&str> = hits
        .iter()
        .filter_map(|h| h["document_id"].as_str())
        .collect();
    if ids.iter().any(|id| *id == lockbit_a.to_string()) {
        return Err(format!("自己不該出現在相似清單，實際 {ids:?}"));
    }
    if hits.is_empty() {
        return Err("應找得到至少一個鄰居".into());
    }
    if ids[0] != lockbit_b.to_string() {
        return Err(format!(
            "最近鄰應是另一份 LockBit 文件 {lockbit_b}，實際 {}",
            ids[0]
        ));
    }

    let (status, body) = get_similar(&api, &no_vector.to_string()).await;
    if status != StatusCode::OK {
        return Err(format!("沒有向量應 200 空清單，實際 {status} {body}"));
    }
    let empty = body["hits"]
        .as_array()
        .ok_or_else(|| "hits 不是陣列".to_string())?;
    if !empty.is_empty() {
        return Err(format!("沒有向量應回空 hits，實際 {body}"));
    }
    Ok(())
}
