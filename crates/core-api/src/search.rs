//! `POST /api/v1/search`（SPEC §19／§18）。
//!
//! # 這裡不組查詢
//!
//! 請求的翻譯全部在 `indexer::search::build`，`osint-cli search` 走同一支函式。
//! 兩邊各寫一份的結果是「CLI 查得到、API 查不到」這種問題，
//! 而且不會有任何錯誤訊息——只是結果不一樣。
//!
//! # 注入防護
//!
//! 使用者的 `query` 字串不會被交給 OpenSearch 的查詢語言，
//! 而是先被 `indexer::query::parse` 解析成語法樹（見該模組的對照表）。
//! `title:*`、`*`、`body:/.*/ ` 都只是「要比對的文字」。
//!
//! # 為什麼 hit 一定帶 `raw_evidence_id`
//!
//! SPEC §26 Acceptance E：search result 必須能一路反查到 RawEvidence／Source／Connector。
//! 少了這一欄，那條鏈在第一步就斷了，而使用者看到的是一個完全正常的搜尋結果。

use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use core_security::{Permission, Principal};
use indexer::search::{SearchRequest, SearchRequestError};
use indexer::{schema, search};
use storage_core::{SearchHit, SearchStore};

use crate::error::ApiError;
use crate::state::AppState;

/// 一筆搜尋結果。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchHitBody {
    pub document_id: String,
    pub score: Option<f64>,
    pub title: Option<String>,
    /// highlight 片段（含 `<em>` 標記）。沒有全文條件時退回摘要開頭。
    pub snippet: Option<String>,
    pub object_type: Option<String>,
    pub language: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<String>,
    /// Acceptance E 的起點。
    pub raw_evidence_id: Option<String>,
    pub canonical_url: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub observed_at: Option<DateTime<Utc>>,
    pub entities: Vec<EntitySummary>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EntitySummary {
    pub entity_id: Option<String>,
    pub entity_type: Option<String>,
    pub name: Option<String>,
    pub normalized_name: Option<String>,
}

/// 搜尋回應。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchResponse {
    /// 符合條件的總筆數（精確值，不是「至少」）。
    pub total: u64,
    pub hits: Vec<SearchHitBody>,
    /// 下一頁的游標。`null` 代表已經是最後一頁。
    pub next_cursor: Option<String>,
}

/// entities 摘要最多回幾筆。
///
/// 一份 IOC 清單型的文件可能有上百個 entity，全部塞進每一筆 hit 會讓
/// 20 筆結果的回應變成好幾 MB。要完整清單請用 `GET /api/v1/objects/{id}`（V0.2）
/// 或 `osint-cli documents show`。
const MAX_ENTITIES_PER_HIT: usize = 20;
/// 沒有 highlight 時，退回用摘要的前幾個字元。
const FALLBACK_SNIPPET_CHARS: usize = 200;

fn search_or_unavailable(state: &AppState) -> Result<&crate::state::SharedSearchState, ApiError> {
    state.search.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "搜尋未接上 OpenSearch。請設定 [storage.search].url（本機 dev 是 \
             http://127.0.0.1:19200）並重啟 osint-api；確認 `make compose-ps` 裡 \
             osint-core-opensearch-1 是 healthy",
        )
    })
}

/// `POST /api/v1/search`。viewer 以上可用（唯讀）。
pub async fn search(
    State(state): State<AppState>,
    principal: Principal,
    Json(request): Json<SearchRequest>,
) -> Result<Json<SearchResponse>, ApiError> {
    principal.role.require(Permission::Read)?;
    let search_state = search_or_unavailable(&state)?;

    let limit = request.effective_limit();
    let query = search::build(&search_state.index, &request)?;

    let started = Instant::now();
    let hits = search_state.store.search(query).await?;
    state
        .metrics
        .observe_search_latency_ms(started.elapsed().as_millis() as u64);
    state.metrics.inc("osint_search_requests_total", 1);

    let next_cursor = search::next_cursor(&hits.hits, limit);
    Ok(Json(SearchResponse {
        total: hits.total,
        hits: hits.hits.iter().map(to_body).collect(),
        next_cursor,
    }))
}

fn to_body(hit: &SearchHit) -> SearchHitBody {
    let source = &hit.source;
    SearchHitBody {
        // `_id` 就是 Document.id，但 `_source.document_id` 也有；取 `_id` 是因為
        // 它一定存在（`_source` 可能被 source filtering 關掉）。
        document_id: hit.id.clone(),
        score: hit.score,
        title: text(source, schema::F_TITLE),
        snippet: snippet(hit),
        object_type: text(source, schema::F_OBJECT_TYPE),
        language: text(source, schema::F_LANGUAGE),
        source_id: text(source, schema::F_SOURCE_ID),
        connector_id: text(source, schema::F_CONNECTOR_ID),
        raw_evidence_id: text(source, schema::F_RAW_EVIDENCE_ID),
        canonical_url: text(source, "canonical_url"),
        published_at: date(source, schema::F_PUBLISHED_AT),
        observed_at: date(source, schema::F_OBSERVED_AT),
        entities: entities(source),
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

/// highlight 片段優先，沒有就退回摘要開頭。
///
/// 退回機制是必要的：只用過濾條件（沒有全文查詢）時 OpenSearch 不會產生任何
/// highlight，若不退回，那些結果的 snippet 會全部是 null，看起來像資料有問題。
fn snippet(hit: &SearchHit) -> Option<String> {
    for field in [schema::F_TITLE, schema::F_SUMMARY, schema::F_BODY] {
        if let Some(fragment) = hit
            .highlights
            .get(field)
            .and_then(|frags| frags.first())
            .filter(|f| !f.trim().is_empty())
        {
            return Some(fragment.clone());
        }
    }
    for field in [schema::F_SUMMARY, schema::F_BODY] {
        if let Some(raw) = text(&hit.source, field).filter(|t| !t.trim().is_empty()) {
            return Some(truncate_chars(&raw, FALLBACK_SNIPPET_CHARS));
        }
    }
    None
}

fn truncate_chars(text: &str, max: usize) -> String {
    let flat = text.replace(['\n', '\r'], " ");
    if flat.chars().count() <= max {
        return flat;
    }
    let kept: String = flat.chars().take(max.saturating_sub(1)).collect();
    format!("{kept}…")
}

fn entities(source: &Value) -> Vec<EntitySummary> {
    source
        .get(schema::F_ENTITIES)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .take(MAX_ENTITIES_PER_HIT)
                .map(|item| EntitySummary {
                    entity_id: text(item, "entity_id"),
                    entity_type: text(item, "entity_type"),
                    name: text(item, "name"),
                    normalized_name: text(item, "normalized_name"),
                })
                .collect()
        })
        .unwrap_or_default()
}

impl From<SearchRequestError> for ApiError {
    fn from(err: SearchRequestError) -> Self {
        // 全部都是使用者送錯東西，不是伺服器問題。訊息本身已經寫了怎麼修。
        Self::bad_request(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hit(source: Value, highlights: &[(&str, &str)]) -> SearchHit {
        SearchHit {
            id: "doc-1".into(),
            score: Some(2.5),
            source,
            sort: vec![json!(2.5), json!("doc-1")],
            highlights: highlights
                .iter()
                .map(|(field, frag)| ((*field).to_string(), vec![(*frag).to_string()]))
                .collect(),
        }
    }

    #[test]
    fn hit_carries_the_provenance_chain_start() {
        let body = to_body(&hit(
            json!({
                "raw_evidence_id": "11111111-1111-7111-8111-111111111111",
                "source_id": "22222222-2222-7222-8222-222222222222",
                "connector_id": "33333333-3333-7333-8333-333333333333",
            }),
            &[],
        ));
        assert_eq!(
            body.raw_evidence_id.as_deref(),
            Some("11111111-1111-7111-8111-111111111111"),
            "Acceptance E 從這一欄開始；少了它鏈在第一步就斷"
        );
        assert!(body.source_id.is_some());
        assert!(body.connector_id.is_some());
    }

    #[test]
    fn snippet_prefers_highlight() {
        let body = to_body(&hit(
            json!({"summary": "一般摘要"}),
            &[("body", "命中 <em>勒索</em> 片段")],
        ));
        assert_eq!(body.snippet.as_deref(), Some("命中 <em>勒索</em> 片段"));
    }

    #[test]
    fn snippet_falls_back_to_summary_when_there_is_no_highlight() {
        // 只用過濾條件時不會有 highlight。全部回 null 看起來像資料壞掉。
        let body = to_body(&hit(json!({"summary": "一般摘要"}), &[]));
        assert_eq!(body.snippet.as_deref(), Some("一般摘要"));
    }

    #[test]
    fn snippet_is_truncated_without_splitting_characters() {
        let long = "勒".repeat(500);
        let body = to_body(&hit(json!({ "summary": long }), &[]));
        let snippet = body.snippet.expect("snippet");
        assert_eq!(snippet.chars().count(), FALLBACK_SNIPPET_CHARS);
        assert!(snippet.ends_with('…'));
    }

    #[test]
    fn entities_are_capped() {
        let many: Vec<Value> = (0..100)
            .map(|i| json!({"entity_id": format!("e{i}"), "entity_type": "ip"}))
            .collect();
        let body = to_body(&hit(json!({ "entities": many }), &[]));
        assert_eq!(
            body.entities.len(),
            MAX_ENTITIES_PER_HIT,
            "一份 IOC 清單文件有上百個 entity，不設限會讓一頁結果變成好幾 MB"
        );
    }

    #[test]
    fn missing_fields_become_null_not_empty_string() {
        // 空字串會讓前端分不出「沒有標題」與「標題是空的」。
        let body = to_body(&hit(json!({}), &[]));
        assert!(body.title.is_none());
        assert!(body.raw_evidence_id.is_none());
        assert!(body.entities.is_empty());
    }

    #[test]
    fn dates_are_parsed_from_rfc3339() {
        let body = to_body(&hit(json!({"published_at": "2026-09-10T12:00:00Z"}), &[]));
        assert_eq!(
            body.published_at.map(|d| d.to_rfc3339()),
            Some("2026-09-10T12:00:00+00:00".to_string())
        );
    }

    #[test]
    fn parse_errors_become_400_not_500() {
        // 查詢語法錯是使用者的問題。回 500 會讓人去查伺服器 log 找不存在的故障。
        let err: ApiError = SearchRequestError::EmptyEntityName.into();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }
}
