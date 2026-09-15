//! `GET /api/v1/objects/{id}/similar`（SPEC_V0.2 §16）。
//!
//! 用這份文件自己的向量，在同一個欄位做 k-NN。不需要把查詢文字再 embed 一次，
//! 但仍共用 [`crate::state::SemanticSearchState`]：資料來源就是 embedding-worker
//! 寫進 `osint-documents` 的向量欄位，跟語意搜尋是同一組依賴。`semantic_search`
//! 是 `None` 時整條回 503。
//!
//! # 為什麼 duplicate 回 409 而不是 404
//!
//! `GET /objects/{id}` 對 duplicate 是 200（明細看得到它指向誰）。相似清單若對
//! 同一份回 404，呼叫端會以為 id 打錯。409 說的是「這份存在，但政策上不該當
//! 搜尋種子」——列表與搜尋預設排除 duplicate，從一份不該被搜到的文件長出
//! 「相似文件」會讓兩邊語意對不上。請改打 canonical id。
//!
//! # 沒有向量不是錯誤
//!
//! embedding-worker 還沒處理、或這份沒有 title／body 可 embed 時，回 200、
//! `hits: []`。沒有資料是合法狀態，比照 `ProjectionLag` 的 `None` 語意。

use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use core_security::{Permission, Principal};
use indexer::schema;
use indexer::search::{DEFAULT_LIMIT, MAX_LIMIT};
use storage_core::{SearchFilter, SearchStore, SortField, StructuredSearch, VectorSearch};

use crate::error::ApiError;
use crate::resources::{storage_error, store};
use crate::semantic_search::{
    embedding_from_source, semantic_or_unavailable, source_date, source_text,
};
use crate::state::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct SimilarQuery {
    pub limit: Option<u32>,
}

impl SimilarQuery {
    #[must_use]
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }
}

/// 一筆相似文件。沒有 `matched_section`：向量就是這份文件自己的，沒有「近似值」問題。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SimilarHit {
    pub document_id: String,
    pub score: Option<f64>,
    pub object_type: Option<String>,
    pub title: Option<String>,
    pub source_id: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SimilarResponse {
    pub hits: Vec<SimilarHit>,
}

/// `GET /api/v1/objects/{id}/similar`。viewer 以上可用（唯讀）。
pub async fn similar_objects(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<SimilarQuery>,
) -> Result<Json<SimilarResponse>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let document = store
        .get_document(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 object `{id}`。請用 GET /api/v1/objects 確認 id；\
                 RawEvidence 要先經 normalizer 正規化才會產生 Document"
            ))
        })?;

    if let Some(canonical) = document.duplicate_of {
        return Err(ApiError::conflict(format!(
            "object `{id}` 已被判為重複（duplicate_of={canonical}），不提供相似文件清單。\
             請改對 canonical 文件 GET /api/v1/objects/{canonical}/similar。\
             列表與搜尋預設不含重複，從一份不該被搜到的文件長出相似結果會讓兩邊語意對不上"
        )));
    }

    let search_state = semantic_or_unavailable(&state)?;
    let limit = query.effective_limit();

    let own = search_state
        .store
        .search(StructuredSearch {
            index: search_state.index.clone(),
            expression: None,
            fields: vec![],
            filters: vec![SearchFilter::Term {
                field: schema::F_DOCUMENT_ID.to_string(),
                value: id.to_string(),
            }],
            size: 1,
            search_after: None,
            sort: vec![SortField {
                field: schema::F_DOCUMENT_ID.to_string(),
                ascending: true,
            }],
            highlight_fields: vec![],
        })
        .await?;

    let Some(own_hit) = own.hits.first() else {
        // canonical store 有這份，搜尋投影還沒寫入（indexer／embedding-worker 落後）。
        // 沒有向量可查，回空清單而不是 404——GET /objects/{id} 會成功。
        return Ok(Json(SimilarResponse { hits: vec![] }));
    };

    let Some((field, vector)) = embedding_from_source(&own_hit.source) else {
        return Ok(Json(SimilarResponse { hits: vec![] }));
    };

    // 自己一定是自己的最近鄰。多抓 1 筆再濾掉，呼叫端拿到的才是「別人」。
    let k = limit.saturating_add(1);
    let neighbors = search_state
        .store
        .vector_search(VectorSearch {
            index: search_state.index.clone(),
            field: field.to_string(),
            vector,
            k,
            filters: vec![],
        })
        .await?;

    let self_id = id.to_string();
    let hits = neighbors
        .hits
        .iter()
        .filter(|hit| hit.id != self_id)
        .take(limit as usize)
        .map(|hit| SimilarHit {
            document_id: hit.id.clone(),
            score: hit.score,
            object_type: source_text(&hit.source, schema::F_OBJECT_TYPE),
            title: source_text(&hit.source, schema::F_TITLE),
            source_id: source_text(&hit.source, schema::F_SOURCE_ID),
            published_at: source_date(&hit.source, schema::F_PUBLISHED_AT),
        })
        .collect();

    Ok(Json(SimilarResponse { hits }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limit_clamps_like_search() {
        assert_eq!(
            SimilarQuery { limit: Some(9999) }.effective_limit(),
            MAX_LIMIT
        );
        assert_eq!(SimilarQuery { limit: Some(0) }.effective_limit(), 1);
        assert_eq!(
            SimilarQuery { limit: None }.effective_limit(),
            DEFAULT_LIMIT
        );
    }
}
