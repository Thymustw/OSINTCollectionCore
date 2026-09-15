//! SPEC §17 Stage 5：對 OpenSearch k-NN 做語意重複判定。
//!
//! # 為什麼 Stage 5 要當場算向量
//!
//! deduplicator 在 `object.normalized` 上**同步**跑完 Stage 1–5。這時候
//! embedding-worker 還沒動過（它訂 `entity.extracted`，那則事件是
//! `dedup.completed` 之後才發的），`osint-documents` 裡也還沒有這份文件
//! 的向量。等 embedding-worker 寫完再回來比，等於把去重延遲到下一條
//! 管線——那已經不是「五階段同步判定」。
//!
//! 所以這裡自己呼叫 [`EmbeddingProvider::embed`] **一次**（body 有值用
//! body，沒有才用 title，跟 embedding-worker overlay 的 last-write-wins
//! 同一套「body 優先」），結果**不**寫進 `embeddings` 表、也**不** overlay
//! `osint-documents`。那兩件事仍是 embedding-worker 的職責。
//!
//! 算完的向量另外以 `embedding-cache:v1:{content_hash}` 寫進 Redis
//! （best-effort，TTL 見 `[embedding].dedup_cache_ttl_secs`），讓稍後
//! 同一段文字走到 embedding-worker 時不必再打 ml-commons。
//!
//! # 失敗不可阻斷 ingestion
//!
//! ml-commons／OpenSearch／Redis 任一失敗都回
//! [`SemanticOutcome::Unsupported`]，不是 `Err`。CLAUDE.md §5：
//! AI failure must not block base ingestion。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use core_model::Document;
use indexer::schema as doc_schema;
use serde_json::Value;
use storage_core::{
    EmbeddingKind, EmbeddingModelRef, EmbeddingProvider, EmbeddingRequest, EmbeddingVector,
    KeyValueStore, SearchHit, SearchStore, VectorSearch, embedding_cache_key,
    embedding_content_hash,
};
use storage_opensearch::MINILM_MODEL_NAME;
use uuid::Uuid;

use crate::error::DeduplicatorError;
use crate::semantic::{SemanticDuplicateDetector, SemanticOutcome};

/// 最近鄰取幾個。
///
/// k=1 在「結果裡第一筆是自己」時會變成空集合——這份文件理論上還沒被
/// indexer 寫進 `osint-documents`，但時序不是契約，防禦性濾掉自己之後
/// 必須還有候選。取 8 個足夠跳過自己與幾筆解析不出 id 的殘缺 hit，
/// 又不至於把 k-NN 掃成大範圍主題搜尋。
const NEIGHBOR_K: u32 = 8;

/// 生產用 Stage 5。`E`／`S` 泛型讓單元測試能注入 mock。
pub struct VectorSemanticDetector<E, S> {
    embeddings: E,
    search: S,
    cache: Option<Arc<dyn KeyValueStore>>,
    documents_index: String,
    similarity_threshold: f64,
    cache_ttl: Duration,
}

impl<E, S> VectorSemanticDetector<E, S> {
    #[must_use]
    pub fn new(
        embeddings: E,
        search: S,
        cache: Option<Arc<dyn KeyValueStore>>,
        documents_index: impl Into<String>,
        similarity_threshold: f64,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            embeddings,
            search,
            cache,
            documents_index: documents_index.into(),
            similarity_threshold,
            cache_ttl,
        }
    }
}

#[async_trait]
impl<E, S> SemanticDuplicateDetector for VectorSemanticDetector<E, S>
where
    E: EmbeddingProvider + 'static,
    S: SearchStore + 'static,
{
    fn detector_id(&self) -> &'static str {
        "vector-knn"
    }

    async fn detect(&self, document: &Document) -> Result<SemanticOutcome, DeduplicatorError> {
        Ok(self.detect_inner(document).await)
    }
}

impl<E, S> VectorSemanticDetector<E, S>
where
    E: EmbeddingProvider,
    S: SearchStore,
{
    async fn detect_inner(&self, document: &Document) -> SemanticOutcome {
        let Some(text) = matched_text(document) else {
            tracing::debug!(
                document_id = %document.id,
                "Document 沒有 title／body，Stage 5 無文字可嵌入，當作沒有語意重複"
            );
            return SemanticOutcome::NoMatch;
        };

        let language = document.language.as_deref();
        let model_ref = self.embeddings.model_for(language);
        let field = vector_field_for(&model_ref);

        let vector = match self.embed_for_dedup(text, document.language.clone()).await {
            Ok(v) => v,
            Err(err) => {
                tracing::warn!(
                    document_id = %document.id,
                    error = %err,
                    "Stage 5 推論失敗（ml-commons 連不上或回錯），這次停用語意判定，不中斷 ingestion"
                );
                return SemanticOutcome::Unsupported;
            }
        };

        self.store_cache(&vector).await;

        let hits = match self
            .search
            .vector_search(VectorSearch {
                index: self.documents_index.clone(),
                field: field.to_string(),
                vector: vector.vector.clone(),
                k: NEIGHBOR_K,
                filters: vec![],
            })
            .await
        {
            Ok(hits) => hits,
            Err(err) => {
                tracing::warn!(
                    document_id = %document.id,
                    error = %err,
                    "Stage 5 向量搜尋失敗，這次停用語意判定，不中斷 ingestion"
                );
                return SemanticOutcome::Unsupported;
            }
        };

        let self_id = document.id.to_string();
        let mut best: Option<(Uuid, f64)> = None;
        for hit in &hits.hits {
            if hit.id == self_id {
                continue;
            }
            let Some(canonical) = canonical_id_of(hit) else {
                tracing::debug!(
                    hit_id = %hit.id,
                    "Stage 5 最近鄰沒有可解析的 document_id，跳過這一筆"
                );
                continue;
            };
            if canonical == document.id {
                continue;
            }
            let similarity =
                cosine_from_hit(hit, field, &vector.vector).unwrap_or(hit.score.unwrap_or(0.0));
            if similarity >= self.similarity_threshold
                && best.as_ref().is_none_or(|(_, s)| similarity > *s)
            {
                best = Some((canonical, similarity));
            }
        }

        match best {
            Some((canonical_object_id, similarity)) => SemanticOutcome::Hit {
                canonical_object_id,
                similarity,
                model: model_ref.model,
            },
            None => SemanticOutcome::NoMatch,
        }
    }

    async fn embed_for_dedup(
        &self,
        text: &str,
        language: Option<String>,
    ) -> Result<EmbeddingVector, storage_core::StorageError> {
        if let Some(cached) = self.lookup_cache(text).await {
            return Ok(cached);
        }
        self.embeddings
            .embed(&EmbeddingRequest {
                text: text.to_string(),
                kind: EmbeddingKind::Passage,
                language,
            })
            .await
    }

    async fn lookup_cache(&self, text: &str) -> Option<EmbeddingVector> {
        let cache = self.cache.as_ref()?;
        let hash = embedding_content_hash(text);
        let key = embedding_cache_key(&hash);
        let bytes = match cache.get(&key).await {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(%key, error = %err, "讀 embedding Redis 快取失敗，當 miss");
                return None;
            }
        };
        match serde_json::from_slice::<EmbeddingVector>(&bytes) {
            Ok(vector) if vector.content_hash == hash => Some(vector),
            Ok(_) => {
                tracing::warn!(%key, "embedding Redis 快取的 content_hash 對不上，當 miss");
                None
            }
            Err(err) => {
                tracing::warn!(%key, error = %err, "embedding Redis 快取 JSON 解不開，當 miss");
                None
            }
        }
    }

    async fn store_cache(&self, vector: &EmbeddingVector) {
        let Some(cache) = &self.cache else {
            return;
        };
        let key = embedding_cache_key(&vector.content_hash);
        let Ok(bytes) = serde_json::to_vec(vector) else {
            tracing::warn!(%key, "序列化 EmbeddingVector 失敗，這次不寫 Redis 快取");
            return;
        };
        if let Err(err) = cache.set_ex(&key, &bytes, self.cache_ttl).await {
            tracing::warn!(
                %key,
                error = %err,
                "寫 embedding Redis 快取失敗（best-effort，不影響判定）"
            );
        }
    }
}

/// body 優先、沒有 body 才用 title。空字串視同沒有。
fn matched_text(document: &Document) -> Option<&str> {
    document
        .body
        .as_deref()
        .filter(|s| !s.is_empty())
        .or_else(|| document.title.as_deref().filter(|s| !s.is_empty()))
}

/// MiniLM 名稱必須與 [`MINILM_MODEL_NAME`] **精確相等**才走 `embedding_en`。
/// 不要用 `contains("MiniLM")`——mock 與生產的名稱剛好相同，但那是巧合不是契約。
fn vector_field_for(model: &EmbeddingModelRef) -> &'static str {
    if model.model == MINILM_MODEL_NAME {
        doc_schema::F_EMBEDDING_EN
    } else {
        doc_schema::F_EMBEDDING_MULTI
    }
}

fn canonical_id_of(hit: &SearchHit) -> Option<Uuid> {
    hit.source
        .get(doc_schema::F_DOCUMENT_ID)
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
        .or_else(|| Uuid::parse_str(&hit.id).ok())
}

/// 用 hit `_source` 裡的向量重算 cosine。
///
/// `embedding_en` 的 knn `space_type` 是 `l2`，OpenSearch `_score` 是
/// `1/(1+l2)`，跟 `[embedding].similarity_threshold`（cosine 0.90）
/// **不是同一個尺度**。`embedding_multi` 是 `cosinesimil`，分數接近 cosine。
/// 兩個欄位都改用 `_source` 向量重算，門檻才對兩種模型有同一意義。
/// 向量缺席或維度不合時回 `None`，呼叫端再退回 `_score`。
fn cosine_from_hit(hit: &SearchHit, field: &str, query: &[f32]) -> Option<f64> {
    let stored = hit.source.get(field).and_then(Value::as_array)?;
    if stored.len() != query.len() || query.is_empty() {
        return None;
    }
    let mut dot = 0.0f64;
    let mut nq = 0.0f64;
    let mut ns = 0.0f64;
    for (q, s) in query.iter().zip(stored.iter()) {
        let sv = s.as_f64()?;
        let qv = f64::from(*q);
        dot += qv * sv;
        nq += qv * qv;
        ns += sv * sv;
    }
    let denom = nq.sqrt() * ns.sqrt();
    if denom == 0.0 {
        None
    } else {
        Some(dot / denom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::DocumentType;
    use serde_json::json;
    use storage_core::SearchDocument;
    use storage_core::mock::{
        MOCK_E5_MODEL, MOCK_MINILM_MODEL, MockEmbeddingProvider, MockKeyValueStore, MockSearchStore,
    };

    fn document(title: Option<&str>, body: Option<&str>, language: Option<&str>) -> Document {
        Document {
            id: Uuid::now_v7(),
            object_type: DocumentType::Article,
            schema_version: "1".into(),
            title: title.map(str::to_string),
            body: body.map(str::to_string),
            summary: None,
            language: language.map(str::to_string),
            author: None,
            published_at: None,
            modified_at: None,
            observed_at: Utc::now(),
            collected_at: Utc::now(),
            source_url: None,
            canonical_url: None,
            normalized_content_hash: None,
            confidence: 0.8,
            labels: Vec::new(),
            attributes: json!({}),
            external_key: None,
            simhash: None,
            duplicate_of: None,
        }
    }

    fn detector(
        embeddings: MockEmbeddingProvider,
        search: MockSearchStore,
        cache: Option<MockKeyValueStore>,
        threshold: f64,
    ) -> VectorSemanticDetector<MockEmbeddingProvider, MockSearchStore> {
        VectorSemanticDetector::new(
            embeddings,
            search,
            cache.map(|c| Arc::new(c) as Arc<dyn KeyValueStore>),
            "osint-documents",
            threshold,
            Duration::from_secs(900),
        )
    }

    async fn seed_neighbor(
        search: &MockSearchStore,
        id: Uuid,
        text: &str,
        language: Option<&str>,
        embeddings: &MockEmbeddingProvider,
    ) {
        let vector = embeddings
            .embed(&EmbeddingRequest {
                text: text.to_string(),
                kind: EmbeddingKind::Passage,
                language: language.map(str::to_string),
            })
            .await
            .expect("seed embed");
        let field =
            if language.is_some_and(|l| l.eq_ignore_ascii_case("en") || l.starts_with("en-")) {
                doc_schema::F_EMBEDDING_EN
            } else {
                doc_schema::F_EMBEDDING_MULTI
            };
        search
            .index(SearchDocument {
                index: "osint-documents".into(),
                id: id.to_string(),
                body: json!({
                    doc_schema::F_DOCUMENT_ID: id.to_string(),
                    field: vector.vector,
                }),
            })
            .await
            .expect("seed neighbor");
    }

    #[test]
    fn matched_text_prefers_body() {
        let mut doc = document(Some("title"), Some("body"), Some("en"));
        assert_eq!(matched_text(&doc), Some("body"));
        doc.body = Some(String::new());
        assert_eq!(matched_text(&doc), Some("title"));
        doc.title = None;
        doc.body = None;
        assert_eq!(matched_text(&doc), None);
    }

    #[test]
    fn vector_field_matches_minilm_exactly() {
        let mini = EmbeddingModelRef {
            model: MOCK_MINILM_MODEL.into(),
            model_version: "x".into(),
            dimensions: 384,
        };
        let e5 = EmbeddingModelRef {
            model: MOCK_E5_MODEL.into(),
            model_version: "x".into(),
            dimensions: 384,
        };
        let almost = EmbeddingModelRef {
            model: "something-MiniLM-else".into(),
            model_version: "x".into(),
            dimensions: 384,
        };
        assert_eq!(vector_field_for(&mini), doc_schema::F_EMBEDDING_EN);
        assert_eq!(vector_field_for(&e5), doc_schema::F_EMBEDDING_MULTI);
        assert_eq!(
            vector_field_for(&almost),
            doc_schema::F_EMBEDDING_MULTI,
            "不可用 contains('MiniLM')"
        );
    }

    #[tokio::test]
    async fn hit_above_threshold_returns_model() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let canonical = Uuid::now_v7();
        seed_neighbor(&search, canonical, "hello body", Some("en"), &embeddings).await;
        let before = embeddings.embed_calls();

        let doc = document(None, Some("hello body"), Some("en"));
        let outcome = detector(embeddings.clone(), search, None, 0.90)
            .detect(&doc)
            .await
            .expect("detect");
        match outcome {
            SemanticOutcome::Hit {
                canonical_object_id,
                similarity,
                model,
            } => {
                assert_eq!(canonical_object_id, canonical);
                assert!(similarity >= 0.90, "same text cosine={similarity}");
                assert_eq!(model, MOCK_MINILM_MODEL);
            }
            other => panic!("預期 Hit，得到 {other:?}"),
        }
        assert!(embeddings.embed_calls() > before, "沒有快取時必須真的推論");
    }

    #[tokio::test]
    async fn below_threshold_is_nomatch() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let canonical = Uuid::now_v7();
        seed_neighbor(
            &search,
            canonical,
            "totally different topic about weather",
            Some("en"),
            &embeddings,
        )
        .await;
        let doc = document(None, Some("unrelated cryptography advisory"), Some("en"));
        let outcome = detector(embeddings, search, None, 0.99)
            .detect(&doc)
            .await
            .expect("detect");
        assert_eq!(outcome, SemanticOutcome::NoMatch);
    }

    #[tokio::test]
    async fn embed_error_is_unsupported_not_err() {
        let embeddings = MockEmbeddingProvider::unsupported();
        let search = MockSearchStore::new();
        let doc = document(None, Some("text"), Some("en"));
        let outcome = detector(embeddings, search, None, 0.90)
            .detect(&doc)
            .await
            .expect("基礎設施失敗不可回 Err");
        assert_eq!(outcome, SemanticOutcome::Unsupported);
    }

    #[tokio::test]
    async fn search_error_is_unsupported_not_err() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        search
            .fail_next_vector_search("模擬 OpenSearch 掛了")
            .unwrap();
        let doc = document(None, Some("text"), Some("en"));
        let outcome = detector(embeddings, search, None, 0.90)
            .detect(&doc)
            .await
            .expect("搜尋失敗不可回 Err");
        assert_eq!(outcome, SemanticOutcome::Unsupported);
    }

    #[tokio::test]
    async fn self_id_is_filtered() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let doc = document(None, Some("hello body"), Some("en"));
        seed_neighbor(&search, doc.id, "hello body", Some("en"), &embeddings).await;
        let outcome = detector(embeddings, search, None, 0.50)
            .detect(&doc)
            .await
            .expect("detect");
        assert_eq!(
            outcome,
            SemanticOutcome::NoMatch,
            "最近鄰只有自己時必須當沒有重複"
        );
    }

    #[tokio::test]
    async fn empty_text_is_nomatch() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let doc = document(None, None, Some("en"));
        let before = embeddings.embed_calls();
        let outcome = detector(embeddings.clone(), search, None, 0.90)
            .detect(&doc)
            .await
            .expect("detect");
        assert_eq!(outcome, SemanticOutcome::NoMatch);
        assert_eq!(embeddings.embed_calls(), before, "沒文字就不該打推論");
    }

    #[tokio::test]
    async fn chinese_uses_e5_model_name() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let canonical = Uuid::now_v7();
        seed_neighbor(
            &search,
            canonical,
            "同一段中文正文",
            Some("zh"),
            &embeddings,
        )
        .await;
        let doc = document(None, Some("同一段中文正文"), Some("zh"));
        let outcome = detector(embeddings, search, None, 0.90)
            .detect(&doc)
            .await
            .expect("detect");
        match outcome {
            SemanticOutcome::Hit { model, .. } => assert_eq!(model, MOCK_E5_MODEL),
            other => panic!("預期 Hit，得到 {other:?}"),
        }
    }

    #[tokio::test]
    async fn redis_write_failure_still_returns_hit() {
        // cache = None 等價於 Redis 沒接上；寫不進去不能讓判定失敗。
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let canonical = Uuid::now_v7();
        seed_neighbor(&search, canonical, "cached body", Some("en"), &embeddings).await;
        let doc = document(None, Some("cached body"), Some("en"));
        let outcome = detector(embeddings, search, None, 0.90)
            .detect(&doc)
            .await
            .expect("detect");
        assert!(matches!(outcome, SemanticOutcome::Hit { .. }));
    }

    #[tokio::test]
    async fn cache_hit_skips_embed() {
        let embeddings = MockEmbeddingProvider::new();
        let search = MockSearchStore::new();
        let cache = MockKeyValueStore::new();
        let canonical = Uuid::now_v7();
        seed_neighbor(&search, canonical, "cached body", Some("en"), &embeddings).await;

        let first = detector(
            embeddings.clone(),
            search.clone(),
            Some(cache.clone()),
            0.90,
        );
        let doc = document(None, Some("cached body"), Some("en"));
        first.detect(&doc).await.expect("first");
        let after_first = embeddings.embed_calls();

        let second = detector(embeddings.clone(), search, Some(cache), 0.90);
        let outcome = second.detect(&doc).await.expect("second");
        assert!(matches!(outcome, SemanticOutcome::Hit { .. }));
        assert_eq!(
            embeddings.embed_calls(),
            after_first,
            "第二次必須走 Redis 快取，不再打推論"
        );
    }
}
