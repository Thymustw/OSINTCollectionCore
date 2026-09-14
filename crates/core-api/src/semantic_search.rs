//! `POST /api/v1/search/semantic`（SPEC_V0.2 §13）。
//!
//! # 範圍
//!
//! 只查 `osint-documents`（Document 語意）。`osint-entities` 不在這條路由。
//!
//! # 三個已知限制（API 文件與這裡的註解都要講清楚）
//!
//! 1. **語言由呼叫端提供，本服務不做偵測。** 這個 codebase 沒有語言偵測工具。
//!    `language` 省略（`None`）比照既有規則「未知 → 多語 e5」，因此只會搜到
//!    `embedding_multi` 有值的文件；只寫了 `embedding_en` 的英文文件搜不到。
//! 2. **`matched_section` 是近似值，不是精確 per-query 溯源。**
//!    `osint-documents` 每個語言空間只有一個向量欄位，title 先寫、body 後寫、
//!    body 蓋掉 title（見 `docs/developer/embedding-worker.md`
//!    「Document overlay last-write-wins」）。這條路由沒辦法回答「這次命中
//!    是靠 title 還是 body」，只能誠實回報「這份文件目前這個向量欄位裡放的
//!    是哪一種內容」：有 body 就回 `"body"`，沒有 body 才回 `"title"`。
//! 3. **k-NN 分數不是 BM25 分數。** 這裡的 `score` 是 cosine／l2 空間的
//!    最近鄰分數（約 0~1 附近，看 `space_type`），不能跟 `POST /search`
//!    的 Lucene TF-IDF 分數直接比大小。hybrid search（Step 5）才處理融合。
//!
//! # 查詢向量與哪個欄位比對
//!
//! 由 [`storage_core::EmbeddingProvider::model_for`] 決定，再用回傳的
//! `EmbeddingModelRef.model` 與 [`storage_opensearch::MINILM_MODEL_NAME`]／
//! [`storage_opensearch::E5_MODEL_NAME`] **精確字串相等**比對。MiniLM 查
//! [`indexer::schema::F_EMBEDDING_EN`]，否則查
//! [`indexer::schema::F_EMBEDDING_MULTI`]。不要用 `contains("MiniLM")`。

use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use core_security::{Permission, Principal};
use indexer::schema;
use indexer::search::{DEFAULT_LIMIT, MAX_LIMIT};
use storage_core::{
    EmbeddingKind, EmbeddingModelRef, EmbeddingProvider, EmbeddingRequest, SearchHit, SearchStore,
    VectorSearch,
};
use storage_opensearch::MINILM_MODEL_NAME;

use crate::error::ApiError;
use crate::state::AppState;

/// 一次語意搜尋請求。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SemanticSearchRequest {
    pub query: String,
    /// BCP-47 主語言（例如 `"en"`／`"zh-Hant"`）。省略代表未知 → 多語 e5。
    ///
    /// 本服務**不做語言偵測**。未指定時只會搜到 `embedding_multi` 有值的文件。
    #[serde(default)]
    pub language: Option<String>,
    /// 回幾筆。預設 20、上限 100（超過夾回去，不報錯），與全文搜尋同一套。
    #[serde(default)]
    pub limit: Option<u32>,
}

impl SemanticSearchRequest {
    #[must_use]
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }
}

/// 一筆語意搜尋結果。欄位命名對齊 [`crate::search::SearchHitBody`]
/// （`document_id` 不是 `id`，`object_type` 不是 `type`）。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SemanticSearchHit {
    pub document_id: String,
    /// k-NN 分數（cosine／l2，約 0~1），**不是** BM25。不要跟 `POST /search` 的 score 比。
    pub score: Option<f64>,
    pub object_type: Option<String>,
    /// 近似值：`"body"` 或 `"title"`。不是精確 per-query 溯源，見模組說明第 2 點。
    pub matched_section: Option<&'static str>,
    pub title: Option<String>,
    pub source_id: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
}

/// 語意搜尋回應。`model`／`model_version` 在頂層：一次查詢只用一個模型。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SemanticSearchResponse {
    pub hits: Vec<SemanticSearchHit>,
    pub model: String,
    pub model_version: String,
}

fn semantic_or_unavailable(
    state: &AppState,
) -> Result<&crate::state::SharedSemanticSearchState, ApiError> {
    state.semantic_search.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "語意搜尋未接上 OpenSearch ml-commons。請確認 [storage.search].url \
             （本機 dev 是 http://127.0.0.1:19200）可連、兩個模型已 DEPLOYED \
             （`bash scripts/opensearch-ml-setup.sh` 與 `scripts/opensearch-ml-setup-e5.sh`），\
             然後重啟 osint-api。全文搜尋 POST /api/v1/search 不受影響",
        )
    })
}

fn reject_empty_query(query: &str) -> Result<(), ApiError> {
    if query.trim().is_empty() {
        return Err(ApiError::bad_request(
            "query 不可為空。請提供要找的文字，例如 \
             {\"query\": \"ransomware campaign\", \"language\": \"en\"}",
        ));
    }
    Ok(())
}

/// 依 `model_for` 回傳的模型名稱決定要查哪個向量欄位。
///
/// MiniLM 名稱必須與 [`MINILM_MODEL_NAME`] **精確相等**才走 `embedding_en`。
/// 不相等（含未知模型）走 `embedding_multi`——那是多語／未知語言的既定後備，
/// 不要自己猜。
fn vector_field_for(model: &EmbeddingModelRef) -> &'static str {
    if model.model == MINILM_MODEL_NAME {
        schema::F_EMBEDDING_EN
    } else {
        schema::F_EMBEDDING_MULTI
    }
}

/// 文件有非空白 body 就回 `"body"`，否則有非空白 title 才回 `"title"`。
///
/// 這是 overlay last-write-wins 的近似值，不是「這次 k-NN 命中的是哪一段」。
fn matched_section(source: &Value) -> Option<&'static str> {
    if text(source, schema::F_BODY).is_some_and(|s| !s.trim().is_empty()) {
        Some("body")
    } else if text(source, schema::F_TITLE).is_some_and(|s| !s.trim().is_empty()) {
        Some("title")
    } else {
        None
    }
}

fn to_hit(hit: &SearchHit) -> SemanticSearchHit {
    let source = &hit.source;
    SemanticSearchHit {
        document_id: hit.id.clone(),
        score: hit.score,
        object_type: text(source, schema::F_OBJECT_TYPE),
        matched_section: matched_section(source),
        title: text(source, schema::F_TITLE),
        source_id: text(source, schema::F_SOURCE_ID),
        published_at: date(source, schema::F_PUBLISHED_AT),
    }
}

fn text(source: &Value, field: &str) -> Option<String> {
    source
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn date(source: &Value, field: &str) -> Option<DateTime<Utc>> {
    source
        .get(field)
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

/// `POST /api/v1/search/semantic`。viewer 以上可用（唯讀）。
pub async fn semantic_search(
    State(state): State<AppState>,
    principal: Principal,
    Json(request): Json<SemanticSearchRequest>,
) -> Result<Json<SemanticSearchResponse>, ApiError> {
    principal.role.require(Permission::Read)?;
    // 空 query 是呼叫端的錯，不該等 ml-commons 接上才回 400——否則本機沒部署
    // 模型時，打錯的請求一律看起來像後端沒接上。
    reject_empty_query(&request.query)?;
    let search_state = semantic_or_unavailable(&state)?;

    let language = request.language.as_deref();
    let model_ref = search_state.embeddings.model_for(language);
    let field = vector_field_for(&model_ref);
    let limit = request.effective_limit();

    let started = Instant::now();
    let embedded = search_state
        .embeddings
        .embed(&EmbeddingRequest {
            text: request.query.clone(),
            kind: EmbeddingKind::Query,
            language: request.language.clone(),
        })
        .await?;
    let hits = search_state
        .store
        .vector_search(VectorSearch {
            index: search_state.index.clone(),
            field: field.to_string(),
            vector: embedded.vector,
            k: limit,
            filters: vec![],
        })
        .await?;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    // 延遲獨立計，不要灌進 `observe_search_latency_ms`：那是 BM25 全文搜尋的
    // histogram。語意搜尋含一次 ml-commons 推論，尺度完全不同，混在一起會讓
    // `osint_search_latency` 的平均值看起來像突然變慢。
    state.metrics.inc("osint_semantic_search_requests_total", 1);
    state
        .metrics
        .inc("osint_semantic_search_latency_ms_sum", elapsed_ms);
    state.metrics.inc("osint_semantic_search_latency_count", 1);

    Ok(Json(SemanticSearchResponse {
        hits: hits.hits.iter().map(to_hit).collect(),
        model: embedded.model,
        model_version: embedded.model_version,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use storage_core::mock::{MOCK_E5_MODEL, MOCK_MINILM_MODEL, MockEmbeddingProvider};

    fn hit(id: &str, source: Value) -> SearchHit {
        SearchHit {
            id: id.into(),
            score: Some(0.91),
            source,
            sort: vec![],
            highlights: Default::default(),
        }
    }

    #[test]
    fn empty_query_is_rejected() {
        assert!(reject_empty_query("").is_err());
        assert!(reject_empty_query("   \n\t  ").is_err());
        let err = reject_empty_query("").expect_err("empty");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(
            err.message.contains("query 不可為空"),
            "訊息要講下一步：{}",
            err.message
        );
    }

    #[test]
    fn non_empty_query_is_accepted() {
        assert!(reject_empty_query("ransomware").is_ok());
        assert!(reject_empty_query("  勒索軟體  ").is_ok());
    }

    #[test]
    fn language_none_selects_e5_field() {
        let provider = MockEmbeddingProvider::new();
        let model = provider.model_for(None);
        assert_eq!(model.model, MOCK_E5_MODEL);
        assert_eq!(
            vector_field_for(&model),
            schema::F_EMBEDDING_MULTI,
            "語言未知必須走 embedding_multi，否則只寫了 e5 的文件搜不到、\
             只寫了 MiniLM 的英文文件會被拿去跟不相通的空間比"
        );
    }

    #[test]
    fn language_en_selects_minilm_field() {
        let provider = MockEmbeddingProvider::new();
        let model = provider.model_for(Some("en"));
        assert_eq!(model.model, MOCK_MINILM_MODEL);
        assert_eq!(
            model.model, MINILM_MODEL_NAME,
            "mock 與生產常數必須是同一個名字"
        );
        assert_eq!(vector_field_for(&model), schema::F_EMBEDDING_EN);
    }

    #[test]
    fn language_en_us_also_selects_minilm_field() {
        let provider = MockEmbeddingProvider::new();
        let model = provider.model_for(Some("en-US"));
        assert_eq!(vector_field_for(&model), schema::F_EMBEDDING_EN);
    }

    #[test]
    fn language_zh_selects_e5_field() {
        let provider = MockEmbeddingProvider::new();
        let model = provider.model_for(Some("zh"));
        assert_eq!(model.model, MOCK_E5_MODEL);
        assert_eq!(vector_field_for(&model), schema::F_EMBEDDING_MULTI);
    }

    #[test]
    fn unknown_model_name_falls_back_to_multi() {
        let model = EmbeddingModelRef {
            model: "some-future-model".into(),
            model_version: "abc".into(),
            dimensions: 384,
        };
        assert_eq!(vector_field_for(&model), schema::F_EMBEDDING_MULTI);
    }

    #[test]
    fn matched_section_prefers_body_when_present() {
        let body = to_hit(&hit(
            "doc-1",
            json!({
                "title": "標題",
                "body": "內文",
            }),
        ));
        assert_eq!(
            body.matched_section,
            Some("body"),
            "有 body 時 overlay 後寫的是 body，近似值必須回 body 而不是 title"
        );
    }

    #[test]
    fn matched_section_is_title_when_body_missing() {
        let body = to_hit(&hit("doc-1", json!({ "title": "只有標題" })));
        assert_eq!(body.matched_section, Some("title"));
    }

    #[test]
    fn matched_section_is_title_when_body_is_blank() {
        let body = to_hit(&hit("doc-1", json!({ "title": "標題", "body": "   " })));
        assert_eq!(body.matched_section, Some("title"));
    }

    #[test]
    fn matched_section_is_none_when_both_missing() {
        let body = to_hit(&hit("doc-1", json!({})));
        assert!(body.matched_section.is_none());
        assert!(body.title.is_none());
    }

    #[test]
    fn hit_body_pulls_fields_from_source() {
        let body = to_hit(&hit(
            "doc-from-id",
            json!({
                "title": "LockBit 報告",
                "object_type": "advisory",
                "source_id": "22222222-2222-7222-8222-222222222222",
                "published_at": "2026-09-10T12:00:00Z",
                "body": "內文",
            }),
        ));
        assert_eq!(body.document_id, "doc-from-id");
        assert_eq!(body.title.as_deref(), Some("LockBit 報告"));
        assert_eq!(body.object_type.as_deref(), Some("advisory"));
        assert_eq!(
            body.source_id.as_deref(),
            Some("22222222-2222-7222-8222-222222222222")
        );
        assert_eq!(
            body.published_at.map(|d| d.to_rfc3339()),
            Some("2026-09-10T12:00:00+00:00".to_string())
        );
        assert_eq!(body.score, Some(0.91));
        assert_eq!(body.matched_section, Some("body"));
    }

    #[test]
    fn limit_clamps_like_full_text_search() {
        let huge = SemanticSearchRequest {
            query: "x".into(),
            language: None,
            limit: Some(9999),
        };
        assert_eq!(huge.effective_limit(), MAX_LIMIT);
        let tiny = SemanticSearchRequest {
            query: "x".into(),
            language: None,
            limit: Some(0),
        };
        assert_eq!(tiny.effective_limit(), 1);
        let default = SemanticSearchRequest {
            query: "x".into(),
            language: None,
            limit: None,
        };
        assert_eq!(default.effective_limit(), DEFAULT_LIMIT);
    }
}
