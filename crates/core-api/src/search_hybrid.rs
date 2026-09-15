//! `POST /api/v1/search/hybrid`（SPEC_V0.2 §15）。
//!
//! 同時跑 BM25 全文（[`crate::search`]）與語意 k-NN（[`crate::semantic_search`]），
//! 再用 **RRF（Reciprocal Rank Fusion）** 融合排名。不是加權分數和——BM25 與
//! cosine 的尺度不同，硬加會讓權重失去意義。
//!
//! # V0.2 真正有算的訊號
//!
//! 只有 `bm25_weight` 與 `vector_weight`。設定檔另外四個
//! （`entity_match_weight`／`recency_weight`／`source_score_weight`／
//! `confidence_weight`）**完全沒有對應的排名清單**——不是「權重設 0 但其實有算」，
//! 是沒實作。非預設值會在啟動時打 warning，見 [`warn_unimplemented_hybrid_weights`]。
//!
//! # 候選集大小
//!
//! 兩條訊號各自抓 `limit * 3`（夾在 OpenSearch adapter 的 size 上限 200 以內）。
//! 只抓 `limit` 筆時，一份在兩條訊號都排第 `limit+1` 的文件會被截掉，融合後
//! 前幾名可能全是「只出現在一條訊號」的文件。3 倍是常見經驗值；超過 200
//! 會被 adapter 靜默夾回去，這裡先夾，避免 RRF 以為抓到 300 其實只有 200。

use std::collections::HashMap;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use core_config::HybridSearchSection;
use core_security::{Permission, Principal};
use indexer::schema;
use indexer::search::{self as indexer_search, DEFAULT_LIMIT, MAX_LIMIT, SearchRequest};
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, SearchHit, SearchStore, VectorSearch,
};

use crate::error::ApiError;
use crate::semantic_search::{reject_empty_query, source_date, source_text, vector_field_for};
use crate::state::AppState;

/// RRF 公式的 k。
///
/// Cormack, Clarke, Buettcher 2009，
/// *Reciprocal Rank Fusion outperforms Condorcet and individual Rank Learning Methods*
/// （SIGIR），以及 Elasticsearch／OpenSearch 內建 RRF 的預設值，都是 60。
/// 不是隨手套的魔法數字。
pub const RRF_K: f64 = 60.0;

/// OpenSearch adapter 把 `size`／`k` 夾在 200（`storage-opensearch` 的 `MAX_SIZE`）。
/// 這裡先夾，RRF 才不會以為抓到 `limit*3` 但其實被下游靜默截斷。
const OPENSEARCH_MAX_SIZE: u32 = 200;

/// 一次 hybrid 搜尋請求。與 [`crate::semantic_search::SemanticSearchRequest`] 同構：
/// `language` 只影響向量那條訊號要嵌入哪個模型，BM25 那條跟語言無關。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HybridSearchRequest {
    pub query: String,
    /// BCP-47 主語言。省略代表未知 → 多語 e5。本服務不做語言偵測。
    #[serde(default)]
    pub language: Option<String>,
    /// 回幾筆。預設 20、上限 100（超過夾回去，不報錯）。
    #[serde(default)]
    pub limit: Option<u32>,
}

impl HybridSearchRequest {
    #[must_use]
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }
}

/// 一筆融合後的結果。`bm25_rank`／`vector_rank` 從 1 開始；沒出現在該訊號就是 `null`。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HybridSearchHit {
    pub document_id: String,
    pub title: Option<String>,
    pub object_type: Option<String>,
    pub source_id: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub fused_score: f64,
    pub bm25_rank: Option<u32>,
    pub vector_rank: Option<u32>,
}

/// hybrid 搜尋回應。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct HybridSearchResponse {
    pub hits: Vec<HybridSearchHit>,
}

/// 兩條訊號各自要抓幾筆再融合。
#[must_use]
pub fn candidate_pool_size(limit: u32) -> u32 {
    limit.saturating_mul(3).clamp(1, OPENSEARCH_MAX_SIZE)
}

/// V0.2 沒有排名清單的權重，只要不是預設 `0.0` 就列出來。
#[must_use]
pub fn unimplemented_hybrid_signals(weights: &HybridSearchSection) -> Vec<&'static str> {
    let mut names = Vec::new();
    if weights.entity_match_weight != 0.0 {
        names.push("entity_match_weight");
    }
    if weights.recency_weight != 0.0 {
        names.push("recency_weight");
    }
    if weights.source_score_weight != 0.0 {
        names.push("source_score_weight");
    }
    if weights.confidence_weight != 0.0 {
        names.push("confidence_weight");
    }
    names
}

/// 啟動時呼叫一次：設定檔把未實作權重設成非 0 時，不要靜默忽略。
pub fn warn_unimplemented_hybrid_weights(weights: &HybridSearchSection) {
    let names = unimplemented_hybrid_signals(weights);
    if names.is_empty() {
        return;
    }
    tracing::warn!(
        unimplemented = %names.join(","),
        "V0.2 hybrid search 只融合 BM25 與向量兩個訊號。設定檔裡這些權重不是預設的 0.0，\
         但沒有對應的排名清單，這個設定不會影響排序。請改回 0.0，或等到後續版本實作這些訊號"
    );
}

/// Reciprocal Rank Fusion。輸入是各訊號的 document id 列表（排名由位置決定，從 1 開始）。
///
/// `fused_score(doc) = Σ_i weight_i / (k_rrf + rank_i(doc))`。
/// 文件沒出現在某條訊號就不計入那一項（不是給一個很差的 rank）。
/// 同一條訊號出現重複 id 時保留**第一次**出現的排名。
///
/// 回傳已按 `fused_score` 降序、同分再按 `document_id` 升序排好。
#[must_use]
pub fn fuse_ranks(
    bm25: &[String],
    vector: &[String],
    bm25_weight: f64,
    vector_weight: f64,
) -> Vec<(String, f64, Option<u32>, Option<u32>)> {
    let mut ranks: HashMap<String, (Option<u32>, Option<u32>)> = HashMap::new();
    for (i, id) in bm25.iter().enumerate() {
        let entry = ranks.entry(id.clone()).or_insert((None, None));
        if entry.0.is_none() {
            entry.0 = Some(u32::try_from(i + 1).unwrap_or(u32::MAX));
        }
    }
    for (i, id) in vector.iter().enumerate() {
        let entry = ranks.entry(id.clone()).or_insert((None, None));
        if entry.1.is_none() {
            entry.1 = Some(u32::try_from(i + 1).unwrap_or(u32::MAX));
        }
    }

    let mut fused: Vec<(String, f64, Option<u32>, Option<u32>)> = ranks
        .into_iter()
        .map(|(id, (bm25_rank, vector_rank))| {
            let mut score = 0.0;
            if let Some(rank) = bm25_rank {
                score += bm25_weight / (RRF_K + f64::from(rank));
            }
            if let Some(rank) = vector_rank {
                score += vector_weight / (RRF_K + f64::from(rank));
            }
            (id, score, bm25_rank, vector_rank)
        })
        .collect();

    fused.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    fused
}

/// 缺哪一邊時的 503 訊息。抽出來是為了不靠真實 OpenSearch 就能測三種組合。
#[must_use]
pub(crate) fn hybrid_unavailable_message(
    has_search: bool,
    has_semantic: bool,
) -> Option<&'static str> {
    match (has_search, has_semantic) {
        (true, true) => None,
        (false, false) => Some(
            "hybrid search 需要全文搜尋與語意搜尋都接上，目前兩個都沒接。\
             請設定 [storage.search].url（本機 dev 是 http://127.0.0.1:19200）、\
             確認兩個 ml-commons 模型已 DEPLOYED（`bash scripts/opensearch-ml-setup.sh` 與 \
             `scripts/opensearch-ml-setup-e5.sh`），然後重啟 osint-api",
        ),
        (false, true) => Some(
            "hybrid search 的 BM25 訊號未接上 OpenSearch（`search` 是 None）。\
             請設定 [storage.search].url（本機 dev 是 http://127.0.0.1:19200）並重啟 osint-api；\
             確認 `make compose-ps` 裡 osint-core-opensearch-1 是 healthy。\
             語意搜尋本身不受影響（POST /api/v1/search/semantic 仍可用）",
        ),
        (true, false) => Some(
            "hybrid search 的向量訊號未接上 OpenSearch ml-commons（`semantic_search` 是 None）。\
             請確認兩個模型已 DEPLOYED（`bash scripts/opensearch-ml-setup.sh` 與 \
             `scripts/opensearch-ml-setup-e5.sh`），然後重啟 osint-api。\
             全文搜尋本身不受影響（POST /api/v1/search 仍可用）",
        ),
    }
}

fn hybrid_or_unavailable(
    state: &AppState,
) -> Result<
    (
        &crate::state::SharedSearchState,
        &crate::state::SharedSemanticSearchState,
    ),
    ApiError,
> {
    match (&state.search, &state.semantic_search) {
        (Some(search), Some(semantic)) => Ok((search, semantic)),
        (search, semantic) => {
            let message = hybrid_unavailable_message(search.is_some(), semantic.is_some())
                .unwrap_or("hybrid search 未接上");
            Err(ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                message,
            ))
        }
    }
}

fn to_hit(
    document_id: &str,
    fused_score: f64,
    bm25_rank: Option<u32>,
    vector_rank: Option<u32>,
    source: Option<&Value>,
) -> HybridSearchHit {
    HybridSearchHit {
        document_id: document_id.to_string(),
        title: source.and_then(|s| source_text(s, schema::F_TITLE)),
        object_type: source.and_then(|s| source_text(s, schema::F_OBJECT_TYPE)),
        source_id: source.and_then(|s| source_text(s, schema::F_SOURCE_ID)),
        published_at: source.and_then(|s| source_date(s, schema::F_PUBLISHED_AT)),
        fused_score,
        bm25_rank,
        vector_rank,
    }
}

/// `POST /api/v1/search/hybrid`。viewer 以上可用（唯讀）。
pub async fn hybrid_search(
    State(state): State<AppState>,
    principal: Principal,
    Json(request): Json<HybridSearchRequest>,
) -> Result<Json<HybridSearchResponse>, ApiError> {
    principal.role.require(Permission::Read)?;
    reject_empty_query(&request.query)?;
    let (search_state, semantic_state) = hybrid_or_unavailable(&state)?;

    let limit = request.effective_limit();
    let pool = candidate_pool_size(limit);
    let language = request.language.as_deref();
    let model_ref = semantic_state.embeddings.model_for(language);
    let field = vector_field_for(&model_ref);

    // BM25 不帶 language 過濾：hybrid 的 language 只決定向量模型，不是「只搜這個語言的文件」。
    let bm25_request = SearchRequest {
        query: request.query.clone(),
        limit: Some(pool),
        ..SearchRequest::default()
    };
    let bm25_query = indexer_search::build(&search_state.index, &bm25_request)?;

    let started = Instant::now();
    let embedded = semantic_state
        .embeddings
        .embed(&EmbeddingRequest {
            text: request.query.clone(),
            kind: EmbeddingKind::Query,
            language: request.language.clone(),
        })
        .await?;

    let (bm25_hits, vector_hits) = tokio::join!(
        search_state.store.search(bm25_query),
        semantic_state.store.vector_search(VectorSearch {
            index: semantic_state.index.clone(),
            field: field.to_string(),
            vector: embedded.vector,
            k: pool,
            filters: vec![],
        }),
    );
    let bm25_hits = bm25_hits?;
    let vector_hits = vector_hits?;

    let elapsed_ms = started.elapsed().as_millis() as u64;
    state.metrics.inc("osint_hybrid_search_requests_total", 1);
    state
        .metrics
        .inc("osint_hybrid_search_latency_ms_sum", elapsed_ms);
    state.metrics.inc("osint_hybrid_search_latency_count", 1);

    let bm25_ids: Vec<String> = bm25_hits.hits.iter().map(|h| h.id.clone()).collect();
    let vector_ids: Vec<String> = vector_hits.hits.iter().map(|h| h.id.clone()).collect();
    let fused = fuse_ranks(
        &bm25_ids,
        &vector_ids,
        state.hybrid_weights.bm25_weight,
        state.hybrid_weights.vector_weight,
    );

    let mut sources: HashMap<String, &SearchHit> = HashMap::new();
    for hit in bm25_hits.hits.iter().chain(vector_hits.hits.iter()) {
        sources.entry(hit.id.clone()).or_insert(hit);
    }

    let hits = fused
        .into_iter()
        .take(limit as usize)
        .map(|(id, score, bm25_rank, vector_rank)| {
            let source = sources.get(&id).map(|h| &h.source);
            to_hit(&id, score, bm25_rank, vector_rank, source)
        })
        .collect();

    Ok(Json(HybridSearchResponse { hits }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rrf_hand_calculated_example() {
        // bm25: a=1, b=2, c=3；vector: c=1, a=2；weight 皆 1；k=60。
        // score(a) = 1/61 + 1/62
        // score(b) = 1/62
        // score(c) = 1/63 + 1/61
        let fused = fuse_ranks(
            &["a".into(), "b".into(), "c".into()],
            &["c".into(), "a".into()],
            1.0,
            1.0,
        );
        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].0, "a");
        assert_eq!(fused[0].1, 1.0 / 61.0 + 1.0 / 62.0);
        assert_eq!(fused[0].2, Some(1));
        assert_eq!(fused[0].3, Some(2));
        assert_eq!(fused[1].0, "c");
        assert_eq!(fused[1].1, 1.0 / 63.0 + 1.0 / 61.0);
        assert_eq!(fused[1].2, Some(3));
        assert_eq!(fused[1].3, Some(1));
        assert_eq!(fused[2].0, "b");
        assert_eq!(fused[2].1, 1.0 / 62.0);
        assert_eq!(fused[2].2, Some(2));
        assert_eq!(fused[2].3, None);
        assert!(
            fused[0].1 > fused[1].1,
            "a 應排在 c 前面：{} vs {}",
            fused[0].1,
            fused[1].1
        );
    }

    #[test]
    fn only_one_signal_does_not_invent_a_bad_rank() {
        let fused = fuse_ranks(&["a".into(), "b".into()], &[], 1.0, 1.0);
        assert_eq!(fused.len(), 2);
        assert_eq!(fused[0], ("a".into(), 1.0 / 61.0, Some(1), None));
        assert_eq!(fused[1], ("b".into(), 1.0 / 62.0, Some(2), None));

        let fused = fuse_ranks(&[], &["z".into()], 1.0, 1.0);
        assert_eq!(fused[0], ("z".into(), 1.0 / 61.0, None, Some(1)));
    }

    #[test]
    fn both_weights_zero_does_not_divide_by_zero() {
        let fused = fuse_ranks(&["b".into(), "a".into()], &["a".into()], 0.0, 0.0);
        assert_eq!(fused.len(), 2);
        assert!(fused.iter().all(|h| h.1 == 0.0));
        assert_eq!(
            fused[0].0, "a",
            "分數全 0 時改依 document_id 升序，結果必須穩定"
        );
        assert_eq!(fused[1].0, "b");
    }

    #[test]
    fn duplicate_id_in_one_list_keeps_first_rank() {
        let fused = fuse_ranks(&["a".into(), "a".into()], &[], 1.0, 0.0);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].2, Some(1), "重複 id 必須保留第一次出現的排名");
        assert_eq!(fused[0].1, 1.0 / 61.0);
    }

    #[test]
    fn unimplemented_signals_are_those_not_at_default_zero() {
        assert!(unimplemented_hybrid_signals(&HybridSearchSection::default()).is_empty());
        let weights = HybridSearchSection {
            recency_weight: 0.5,
            confidence_weight: 1.0,
            ..HybridSearchSection::default()
        };
        assert_eq!(
            unimplemented_hybrid_signals(&weights),
            ["recency_weight", "confidence_weight"]
        );
    }

    #[test]
    fn candidate_pool_is_triple_limit_clamped_to_opensearch() {
        assert_eq!(candidate_pool_size(20), 60);
        assert_eq!(candidate_pool_size(100), OPENSEARCH_MAX_SIZE);
        assert_eq!(candidate_pool_size(1), 3);
    }

    #[test]
    fn unavailable_message_names_the_missing_backend() {
        assert!(hybrid_unavailable_message(true, true).is_none());
        let both = hybrid_unavailable_message(false, false).expect("both");
        assert!(both.contains("兩個都沒接"), "{both}");
        let bm25 = hybrid_unavailable_message(false, true).expect("bm25");
        assert!(bm25.contains("`search` 是 None"), "{bm25}");
        let vector = hybrid_unavailable_message(true, false).expect("vector");
        assert!(vector.contains("`semantic_search` 是 None"), "{vector}");
    }

    #[test]
    fn limit_clamps_like_other_search_routes() {
        let huge = HybridSearchRequest {
            query: "x".into(),
            language: None,
            limit: Some(9999),
        };
        assert_eq!(huge.effective_limit(), MAX_LIMIT);
        let default = HybridSearchRequest {
            query: "x".into(),
            language: None,
            limit: None,
        };
        assert_eq!(default.effective_limit(), DEFAULT_LIMIT);
    }
}
