//! `POST /api/v1/search/semantic` 對真實 OpenSearch + ml-commons 的端到端。
//!
//! ⚠️ **標了 `#[ignore]`，CI 預設不跑。** 理由與
//! `crates/storage-opensearch/tests/embedding_conformance.rs` 相同：CI 的
//! OpenSearch 沒有跑過 `opensearch-ml-setup.sh`／`-e5.sh`，硬跑只會穩定失敗
//! （模型查不到 DEPLOYED），不是這批程式碼壞了。本機驗證：
//!
//! ```bash
//! bash scripts/opensearch-ml-setup.sh
//! bash scripts/opensearch-ml-setup-e5.sh
//! cargo nextest run -p core-api --test semantic_search_api_e2e -- --ignored --nocapture
//! ```
//!
//! 每個測試用自己的 index（`osint-documents-sem-e2e-<uuid>`），結尾刪掉——
//! 即使斷言失敗也先清（CLAUDE.md §15）。

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use core_api::{AppState, AuthState, RateLimiter, SemanticSearchState, ready_always, router};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use indexer::schema as doc_schema;
use serde_json::{Value, json};
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, SearchDocument, SearchStore,
};
use storage_opensearch::{MlCommonsEmbeddingProvider, OpenSearchStore};
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
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let token = jwt.issue("semantic-e2e", Role::Viewer).expect("issue");
    // 查詢路徑自己再連一次：比照 main.rs `connect_search`／`connect_semantic_search`
    // 各自持有連線。測試裡 embeddings 已經連過，clone 即可。
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        store: None,
        objects: None,
        jobs: None,
        merge: None,
        resolver: None,
        graph_resolver: None,
        graph: None,
        graph_projection: None,
        import: None,
        search: None,
        hybrid_weights: core_config::HybridSearchSection::default(),
        semantic_search: Some(Arc::new(SemanticSearchState {
            store: OpenSearchStore::connect(&stack.url)
                .expect("semantic search 自己的連線")
                .with_refresh_on_write(true),
            embeddings: stack.embeddings.clone(),
            index: index.to_string(),
        })),
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

async fn post_semantic(api: &TestApi, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/search/semantic")
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

async fn seed_document(
    stack: &Stack,
    index: &str,
    id: &str,
    title: &str,
    body: Option<&str>,
    language: Option<&str>,
    text_for_vector: &str,
) {
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

    let mut source = json!({
        doc_schema::F_DOCUMENT_ID: id,
        doc_schema::F_OBJECT_TYPE: "article",
        doc_schema::F_TITLE: title,
        doc_schema::F_SOURCE_ID: "22222222-2222-7222-8222-222222222222",
        doc_schema::F_PUBLISHED_AT: "2026-09-10T12:00:00Z",
    });
    source[field] = json!(vector.vector);
    source[version_field] = json!(vector.model_version);
    if let Some(body) = body {
        source[doc_schema::F_BODY] = json!(body);
    }
    if let Some(language) = language {
        source[doc_schema::F_LANGUAGE] = json!(language);
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
async fn semantic_search_ranks_english_documents_and_returns_spec_fields() {
    let stack = connect_stack().await;
    let index = format!("osint-documents-sem-e2e-{}", Uuid::now_v7().simple());
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

    let result = run_english_case(&stack, &index).await;
    stack.os.delete_index(&index).await.expect("刪除測試 index");
    result.expect("英文語意搜尋");
}

async fn run_english_case(stack: &Stack, index: &str) -> Result<(), String> {
    seed_document(
        stack,
        index,
        "lockbit-report",
        "LockBit ransomware advisory",
        Some("The LockBit gang published a new ransomware campaign targeting hospitals."),
        Some("en"),
        "The LockBit gang published a new ransomware campaign targeting hospitals.",
    )
    .await;
    seed_document(
        stack,
        index,
        "weather-note",
        "Local weather update",
        Some("Sunny skies and mild temperatures expected this weekend."),
        Some("en"),
        "Sunny skies and mild temperatures expected this weekend.",
    )
    .await;

    let api = build_api(stack, index);
    let (status, body) = post_semantic(
        &api,
        json!({
            "query": "ransomware campaign against hospitals",
            "language": "en",
            "limit": 5
        }),
    )
    .await;
    if status != StatusCode::OK {
        return Err(format!("應 200，實際 {status} {body}"));
    }
    if body["model"].as_str() != Some(storage_opensearch::MINILM_MODEL_NAME) {
        return Err(format!(
            "language=en 必須走 MiniLM，實際 model={}",
            body["model"]
        ));
    }
    if body["model_version"].as_str().unwrap_or("").is_empty() {
        return Err("model_version 不該是空的：呼叫端要靠它判斷向量是否過期".into());
    }

    let hits = body["hits"]
        .as_array()
        .ok_or_else(|| "hits 不是陣列".to_string())?;
    if hits.is_empty() {
        return Err("語意搜尋 0 筆。請確認文件向量有寫進 embedding_en".into());
    }
    let top = &hits[0];
    if top["document_id"].as_str() != Some("lockbit-report") {
        return Err(format!(
            "最近鄰應是 lockbit-report，實際 {}",
            top["document_id"]
        ));
    }
    if top["matched_section"].as_str() != Some("body") {
        return Err(format!(
            "有 body 的文件 matched_section 應是 body（overlay 近似值），實際 {}",
            top["matched_section"]
        ));
    }
    if top["title"].as_str() != Some("LockBit ransomware advisory") {
        return Err(format!("title 沒帶出來：{}", top["title"]));
    }
    if top["source_id"].as_str() != Some("22222222-2222-7222-8222-222222222222") {
        return Err(format!("source_id 沒帶出來：{}", top["source_id"]));
    }
    if top["object_type"].as_str() != Some("article") {
        return Err(format!("object_type 沒帶出來：{}", top["object_type"]));
    }
    if top["published_at"].as_str().unwrap_or("").is_empty() {
        return Err("published_at 沒帶出來".into());
    }
    if top["score"].as_f64().is_none() {
        return Err("k-NN score 應是數字".into());
    }

    // 沒有 body 的文件：matched_section 應退回 title。
    seed_document(
        stack,
        index,
        "title-only",
        "Hospital ransomware briefing",
        None,
        Some("en"),
        "Hospital ransomware briefing",
    )
    .await;
    let (status, body) = post_semantic(
        &api,
        json!({
            "query": "hospital ransomware briefing",
            "language": "en",
            "limit": 5
        }),
    )
    .await;
    if status != StatusCode::OK {
        return Err(format!("title-only 查詢應 200，實際 {status} {body}"));
    }
    let title_hit = body["hits"]
        .as_array()
        .and_then(|hits| hits.iter().find(|h| h["document_id"] == "title-only"))
        .ok_or_else(|| format!("應找得到 title-only，實際 {body}"))?;
    if title_hit["matched_section"].as_str() != Some("title") {
        return Err(format!(
            "沒有 body 時 matched_section 應是 title，實際 {}",
            title_hit["matched_section"]
        ));
    }
    Ok(())
}

#[tokio::test]
#[ignore = "需要本機 ml-commons 已部署兩個模型；CI 沒有這個環境，見檔頭說明"]
async fn language_none_uses_e5_and_does_not_see_english_only_vectors() {
    let stack = connect_stack().await;
    let index = format!("osint-documents-sem-e2e-{}", Uuid::now_v7().simple());
    stack
        .os
        .ensure_index_with(
            &index,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("建立測試 index");

    let result = run_unknown_language_case(&stack, &index).await;
    stack.os.delete_index(&index).await.expect("刪除測試 index");
    result.expect("未知語言走 e5");
}

async fn run_unknown_language_case(stack: &Stack, index: &str) -> Result<(), String> {
    // 只寫 embedding_en 的英文文件：language 省略時走 e5／embedding_multi，
    // 這份文件不該被搜到。這就是「語言未指定時英文文件搜不到」的已知限制。
    seed_document(
        stack,
        index,
        "en-only",
        "LockBit ransomware advisory",
        Some("The LockBit gang published a new ransomware campaign."),
        Some("en"),
        "The LockBit gang published a new ransomware campaign.",
    )
    .await;
    seed_document(
        stack,
        index,
        "zh-body",
        "勒索軟體報告",
        Some("LockBit 集團發布針對醫院的新一波勒索活動。"),
        Some("zh"),
        "LockBit 集團發布針對醫院的新一波勒索活動。",
    )
    .await;

    let api = build_api(stack, index);
    let (status, body) = post_semantic(
        &api,
        json!({
            "query": "針對醫院的勒索活動",
            "limit": 5
        }),
    )
    .await;
    if status != StatusCode::OK {
        return Err(format!("應 200，實際 {status} {body}"));
    }
    if body["model"].as_str() != Some(storage_opensearch::E5_MODEL_NAME) {
        return Err(format!(
            "language 省略必須走 e5，實際 model={}",
            body["model"]
        ));
    }
    let empty = Vec::new();
    let ids: Vec<&str> = body["hits"]
        .as_array()
        .unwrap_or(&empty)
        .iter()
        .filter_map(|h| h["document_id"].as_str())
        .collect();
    if ids.contains(&"en-only") {
        return Err(format!(
            "語言未指定時不該搜到只寫了 embedding_en 的文件，實際 {ids:?}"
        ));
    }
    if !ids.contains(&"zh-body") {
        return Err(format!("應找得到 zh-body，實際 {ids:?}"));
    }
    Ok(())
}
