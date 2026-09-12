//! 使用者層級的搜尋請求 → [`StructuredSearch`]（SPEC §18 的八種搜尋）。
//!
//! core-api 的 `POST /api/v1/search` 與 `osint-cli search` 都走這裡，
//! 兩條路徑的語意因此必定相同。分成兩份實作只會讓「CLI 查得到、API 查不到」
//! 這種問題出現，而且不會有任何錯誤訊息。
//!
//! | SPEC §18 | 這裡的欄位 |
//! |---|---|
//! | keyword | `query`（[`crate::query::parse`] 的 Term） |
//! | phrase | `query` 裡的 `"..."` |
//! | boolean | `query` 裡的 `AND`／`OR`／`NOT`／括號 |
//! | source | `source_id` |
//! | entity | `entity`（nested：型別與名稱必須是同一個 entity） |
//! | date range | `date_from`／`date_to` ＋ `date_field` |
//! | language | `language` |
//! | object type | `object_type` |

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use storage_core::{SearchFilter, SearchHit, SortField, StructuredSearch};

use crate::query::{self, QueryParseError};
use crate::schema::{self, DateField};

/// 單次搜尋最多回幾筆。防止一次請求拖垮 OpenSearch 與 API。
pub const MAX_LIMIT: u32 = 100;
pub const DEFAULT_LIMIT: u32 = 20;

/// entity 過濾條件。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityFilter {
    /// `vulnerability`／`ip`／`domain`／`email`／`hash`／`url`／`person`／`organization`…
    /// 省略時代表「任何型別，只比對名稱」。
    #[serde(default, rename = "type")]
    pub entity_type: Option<String>,
    /// entity 的名稱。比對的是正規化後的名稱，大小寫不敏感。
    pub name: String,
}

/// 一次搜尋請求。
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SearchRequest {
    /// 全文查詢字串。空字串代表只用過濾條件。
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub source_id: Option<Uuid>,
    #[serde(default)]
    pub connector_id: Option<Uuid>,
    #[serde(default)]
    pub entity: Option<EntityFilter>,
    #[serde(default)]
    pub date_from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub date_to: Option<DateTime<Utc>>,
    /// date range 比對哪一個時間欄位。預設 `effective`（見 [`DateField`]）。
    #[serde(default)]
    pub date_field: DateField,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub object_type: Option<String>,
    /// 是否把 duplicate 一起列出來。預設 `false`。
    ///
    /// 正常情況下 index 裡根本沒有 duplicate（indexer 不索引它們），
    /// 這個開關存在是為了**偵錯**：打開後若真的查到東西，代表有 duplicate 漏進 index。
    #[serde(default)]
    pub include_duplicates: bool,
    #[serde(default)]
    pub limit: Option<u32>,
    /// 上一頁回傳的 `next_cursor`。
    #[serde(default)]
    pub cursor: Option<String>,
}

/// 請求本身不合法。這些都要在打到 OpenSearch **之前**擋下來。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SearchRequestError {
    #[error("查詢語法錯誤：{0}")]
    Query(#[from] QueryParseError),
    #[error(
        "date_from（{from}）比 date_to（{to}）晚。\
         這個區間不會有任何結果；請對調兩個值"
    )]
    InvalidDateRange { from: String, to: String },
    #[error(
        "entity.name 不可為空。請提供要找的實體名稱，例如 \
         `{{\"type\": \"vulnerability\", \"name\": \"CVE-2026-0001\"}}`"
    )]
    EmptyEntityName,
    #[error(
        "cursor 不是有效的分頁游標：{reason}。\
         請直接使用上一頁回應裡的 next_cursor，不要自己組"
    )]
    InvalidCursor { reason: String },
}

impl SearchRequest {
    /// 夾過上限的 limit。
    #[must_use]
    pub fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }
}

/// 把請求翻成後端中立的結構化查詢。
pub fn build(index: &str, request: &SearchRequest) -> Result<StructuredSearch, SearchRequestError> {
    if let (Some(from), Some(to)) = (request.date_from, request.date_to) {
        if from > to {
            return Err(SearchRequestError::InvalidDateRange {
                from: from.to_rfc3339(),
                to: to.to_rfc3339(),
            });
        }
    }

    let expression = query::parse(&request.query)?;
    let mut filters = Vec::new();

    if let Some(source_id) = request.source_id {
        filters.push(SearchFilter::Term {
            field: schema::F_SOURCE_ID.into(),
            value: source_id.to_string(),
        });
    }
    if let Some(connector_id) = request.connector_id {
        filters.push(SearchFilter::Term {
            field: schema::F_CONNECTOR_ID.into(),
            value: connector_id.to_string(),
        });
    }
    if let Some(language) = non_empty(request.language.as_deref()) {
        filters.push(SearchFilter::Term {
            field: schema::F_LANGUAGE.into(),
            value: language.to_string(),
        });
    }
    if let Some(object_type) = non_empty(request.object_type.as_deref()) {
        filters.push(SearchFilter::Term {
            field: schema::F_OBJECT_TYPE.into(),
            value: object_type.to_string(),
        });
    }
    if let Some(entity) = &request.entity {
        let name = entity.name.trim();
        if name.is_empty() {
            return Err(SearchRequestError::EmptyEntityName);
        }
        let mut terms = vec![(
            schema::F_ENTITY_NORMALIZED_NAME.to_string(),
            name.to_string(),
        )];
        if let Some(entity_type) = non_empty(entity.entity_type.as_deref()) {
            terms.push((schema::F_ENTITY_TYPE.to_string(), entity_type.to_string()));
        }
        filters.push(SearchFilter::Nested {
            path: schema::F_ENTITIES.into(),
            terms,
        });
    }
    if request.date_from.is_some() || request.date_to.is_some() {
        filters.push(SearchFilter::DateRange {
            field: request.date_field.field_name().into(),
            from: request.date_from,
            to: request.date_to,
        });
    }
    if !request.include_duplicates {
        // 防禦縱深。indexer 本來就不索引 duplicate，這條過濾是為了「萬一漏進來」——
        // 搜尋結果出現同一篇文章的十個轉載，比少一筆結果嚴重得多。
        filters.push(SearchFilter::Missing {
            field: schema::F_DUPLICATE_OF.into(),
        });
    }

    // 排序鍵一律以 document_id 收尾：search_after 要求排序值能唯一決定一筆文件，
    // 否則同分（或同日期）的文件在翻頁時會漏掉或重複出現。
    let sort = if expression.is_some() {
        vec![
            SortField {
                field: "_score".into(),
                ascending: false,
            },
            SortField {
                field: schema::F_SORT_TIEBREAK.into(),
                ascending: true,
            },
        ]
    } else {
        // 沒有全文條件時每筆分數都一樣，用 _score 排等於隨機順序。
        // 改用時間由新到舊——那是「列出這個來源的文件」最合理的預設。
        vec![
            SortField {
                field: schema::F_EFFECTIVE_DATE.into(),
                ascending: false,
            },
            SortField {
                field: schema::F_SORT_TIEBREAK.into(),
                ascending: true,
            },
        ]
    };

    Ok(StructuredSearch {
        index: index.to_string(),
        expression,
        fields: schema::full_text_fields(),
        filters,
        size: request.effective_limit(),
        search_after: decode_cursor(request.cursor.as_deref())?,
        sort,
        highlight_fields: schema::highlight_fields(),
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

/// cursor = base64url(JSON 陣列的排序值)。
///
/// 不用「頁碼」或「offset」：那需要 `from/size` 深分頁，成本與頁碼成正比。
/// 也不直接把 JSON 攤在 query string 裡——那會讓人以為可以自己編，
/// 編出來的值會被原樣送進 `search_after`。
#[must_use]
pub fn encode_cursor(sort: &[Value]) -> Option<String> {
    if sort.is_empty() {
        return None;
    }
    let json = serde_json::to_vec(sort).ok()?;
    Some(base64_encode(&json))
}

fn decode_cursor(cursor: Option<&str>) -> Result<Option<Vec<Value>>, SearchRequestError> {
    let Some(raw) = cursor.map(str::trim).filter(|c| !c.is_empty()) else {
        return Ok(None);
    };
    let bytes = base64_decode(raw).ok_or_else(|| SearchRequestError::InvalidCursor {
        reason: "不是合法的 base64url".into(),
    })?;
    let values: Vec<Value> =
        serde_json::from_slice(&bytes).map_err(|err| SearchRequestError::InvalidCursor {
            reason: format!("解碼後不是 JSON 陣列（{err}）"),
        })?;
    if values.is_empty() {
        return Err(SearchRequestError::InvalidCursor {
            reason: "排序值是空陣列".into(),
        });
    }
    Ok(Some(values))
}

/// 從最後一筆 hit 產生下一頁的 cursor。不滿一頁時回 `None`（已經是最後一頁）。
#[must_use]
pub fn next_cursor(hits: &[SearchHit], limit: u32) -> Option<String> {
    if (hits.len() as u32) < limit {
        return None;
    }
    hits.last().and_then(|hit| encode_cursor(&hit.sort))
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn base64_decode(text: &str) -> Option<Vec<u8>> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(text))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use storage_core::QueryExpr;

    fn request() -> SearchRequest {
        SearchRequest::default()
    }

    fn has_term(built: &StructuredSearch, field: &str, value: &str) -> bool {
        built.filters.iter().any(|f| {
            matches!(f, SearchFilter::Term { field: f2, value: v2 } if f2 == field && v2 == value)
        })
    }

    #[test]
    fn duplicates_are_excluded_by_default() {
        let built = build("idx", &request()).unwrap();
        assert!(
            built.filters.iter().any(|f| matches!(
                f,
                SearchFilter::Missing { field } if field == schema::F_DUPLICATE_OF
            )),
            "預設就必須排除 duplicate，否則同一篇文章的十個轉載會塞滿結果"
        );
    }

    #[test]
    fn include_duplicates_removes_the_filter() {
        let mut req = request();
        req.include_duplicates = true;
        let built = build("idx", &req).unwrap();
        assert!(!built.filters.iter().any(|f| matches!(
            f,
            SearchFilter::Missing { field } if field == schema::F_DUPLICATE_OF
        )));
    }

    #[test]
    fn source_language_and_object_type_become_term_filters() {
        let source = Uuid::now_v7();
        let mut req = request();
        req.source_id = Some(source);
        req.language = Some("en".into());
        req.object_type = Some("article".into());
        let built = build("idx", &req).unwrap();
        assert!(has_term(&built, schema::F_SOURCE_ID, &source.to_string()));
        assert!(has_term(&built, schema::F_LANGUAGE, "en"));
        assert!(has_term(&built, schema::F_OBJECT_TYPE, "article"));
    }

    #[test]
    fn blank_filters_are_dropped_not_turned_into_empty_terms() {
        // `language: ""` 若變成 term 過濾，結果一定是空的，使用者會以為沒有資料。
        let mut req = request();
        req.language = Some("   ".into());
        req.object_type = Some(String::new());
        let built = build("idx", &req).unwrap();
        assert!(!built.filters.iter().any(|f| matches!(
            f,
            SearchFilter::Term { field, .. }
                if field == schema::F_LANGUAGE || field == schema::F_OBJECT_TYPE
        )));
    }

    #[test]
    fn entity_filter_is_nested_with_both_terms() {
        let mut req = request();
        req.entity = Some(EntityFilter {
            entity_type: Some("vulnerability".into()),
            name: "CVE-2026-0001".into(),
        });
        let built = build("idx", &req).unwrap();
        let nested = built
            .filters
            .iter()
            .find_map(|f| match f {
                SearchFilter::Nested { terms, .. } => Some(terms),
                _ => None,
            })
            .expect("entity 應該產生 nested 過濾");
        assert_eq!(nested.len(), 2);
        assert!(
            nested
                .iter()
                .any(|(f, v)| f == schema::F_ENTITY_NORMALIZED_NAME && v == "CVE-2026-0001")
        );
        assert!(
            nested
                .iter()
                .any(|(f, v)| f == schema::F_ENTITY_TYPE && v == "vulnerability")
        );
    }

    #[test]
    fn entity_type_is_optional() {
        let mut req = request();
        req.entity = Some(EntityFilter {
            entity_type: None,
            name: "example.com".into(),
        });
        let built = build("idx", &req).unwrap();
        let nested = built
            .filters
            .iter()
            .find_map(|f| match f {
                SearchFilter::Nested { terms, .. } => Some(terms),
                _ => None,
            })
            .expect("nested");
        assert_eq!(nested.len(), 1);
    }

    #[test]
    fn empty_entity_name_is_rejected_before_querying() {
        let mut req = request();
        req.entity = Some(EntityFilter {
            entity_type: Some("ip".into()),
            name: "  ".into(),
        });
        assert_eq!(
            build("idx", &req).unwrap_err(),
            SearchRequestError::EmptyEntityName
        );
    }

    #[test]
    fn reversed_date_range_is_rejected_with_an_actionable_message() {
        let mut req = request();
        req.date_from = Some(Utc::now());
        req.date_to = Some(Utc::now() - chrono::Duration::days(1));
        let err = build("idx", &req).unwrap_err();
        assert!(err.to_string().contains("對調"), "{err}");
    }

    #[test]
    fn date_range_defaults_to_effective_date() {
        let mut req = request();
        req.date_from = Some(Utc::now() - chrono::Duration::days(7));
        let built = build("idx", &req).unwrap();
        assert!(built.filters.iter().any(|f| matches!(
            f,
            SearchFilter::DateRange { field, .. } if field == schema::F_EFFECTIVE_DATE
        )));
    }

    #[test]
    fn date_field_can_be_overridden() {
        let mut req = request();
        req.date_field = DateField::Published;
        req.date_to = Some(Utc::now());
        let built = build("idx", &req).unwrap();
        assert!(built.filters.iter().any(|f| matches!(
            f,
            SearchFilter::DateRange { field, .. } if field == schema::F_PUBLISHED_AT
        )));
    }

    #[test]
    fn sort_always_ends_with_a_unique_tiebreak() {
        for query in ["", "ransomware"] {
            let mut req = request();
            req.query = query.into();
            let built = build("idx", &req).unwrap();
            assert_eq!(
                built.sort.last().map(|s| s.field.as_str()),
                Some(schema::F_SORT_TIEBREAK),
                "沒有唯一的收尾排序鍵，search_after 會漏掉或重複文件"
            );
        }
    }

    #[test]
    fn relevance_sort_only_when_there_is_a_full_text_condition() {
        let mut req = request();
        req.query = "ransomware".into();
        assert_eq!(build("idx", &req).unwrap().sort[0].field, "_score");
        // 沒有全文條件時 _score 是常數，用它排序等於隨機順序。
        req.query = String::new();
        assert_eq!(
            build("idx", &req).unwrap().sort[0].field,
            schema::F_EFFECTIVE_DATE
        );
    }

    #[test]
    fn boolean_query_is_parsed_into_the_expression_tree() {
        let mut req = request();
        req.query = "\"lockbit ransomware\" NOT decryptor".into();
        let built = build("idx", &req).unwrap();
        match built.expression.expect("expression") {
            QueryExpr::And(parts) => {
                assert!(matches!(parts[0], QueryExpr::Phrase(_)));
                assert!(matches!(parts[1], QueryExpr::Not(_)));
            }
            other => panic!("預期 And，得到 {other:?}"),
        }
    }

    #[test]
    fn limit_is_clamped() {
        let mut req = request();
        req.limit = Some(100_000);
        assert_eq!(req.effective_limit(), MAX_LIMIT);
        req.limit = Some(0);
        assert_eq!(req.effective_limit(), 1);
        req.limit = None;
        assert_eq!(req.effective_limit(), DEFAULT_LIMIT);
    }

    #[test]
    fn cursor_round_trip() {
        let sort = vec![json!(1.5), json!("0199-doc")];
        let cursor = encode_cursor(&sort).expect("cursor");
        let mut req = request();
        req.cursor = Some(cursor);
        assert_eq!(build("idx", &req).unwrap().search_after, Some(sort));
    }

    #[test]
    fn garbage_cursor_is_rejected_with_an_actionable_message() {
        let mut req = request();
        req.cursor = Some("!!!not-base64!!!".into());
        let err = build("idx", &req).unwrap_err();
        assert!(err.to_string().contains("next_cursor"), "{err}");
    }

    #[test]
    fn next_cursor_is_none_on_the_last_page() {
        let hit = SearchHit {
            id: "d".into(),
            score: Some(1.0),
            source: json!({}),
            sort: vec![json!(1.0), json!("d")],
            highlights: Default::default(),
        };
        assert!(next_cursor(std::slice::from_ref(&hit), 20).is_none());
        assert!(next_cursor(std::slice::from_ref(&hit), 1).is_some());
    }

    #[test]
    fn unknown_request_fields_are_rejected() {
        // 打錯欄位名（`langauge`）若被靜默忽略，使用者會拿到「沒有過濾」的結果
        // 卻以為自己過濾了。
        let err = serde_json::from_value::<SearchRequest>(json!({"langauge": "en"}));
        assert!(err.is_err(), "未知欄位必須報錯");
    }
}
