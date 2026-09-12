//! OpenSearch SearchStore adapter。連線 URL 由呼叫端／設定注入，不寫死 9200。

use async_trait::async_trait;
use opensearch::http::Url;
use opensearch::http::request::JsonBody;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::indices::{IndicesCreateParts, IndicesPutMappingParts, IndicesRefreshParts};
use opensearch::{BulkParts, DeleteParts, IndexParts, OpenSearch, SearchParts};
use serde_json::{Value, json};
use storage_core::{
    BulkFailure, BulkIndexResult, CapabilityDescriptor, HealthProvider, QueryExpr, SearchDocument,
    SearchField, SearchFilter, SearchHit, SearchHits, SearchQuery, SearchStore, StorageAdapter,
    StorageError, StorageHealth, StructuredSearch,
};

/// OpenSearch 搜尋投影。
#[derive(Debug, Clone)]
pub struct OpenSearchStore {
    client: OpenSearch,
    /// 測試用：每次寫入後 refresh。正式環境應為 false。
    refresh_on_write: bool,
}

impl OpenSearchStore {
    pub fn connect(url: &str) -> Result<Self, StorageError> {
        let parsed = Url::parse(url).map_err(|err| StorageError::Configuration {
            message: format!(
                "OpenSearch URL `{url}` 無效：{err}。請用完整 URL，本機 dev 應為 http://127.0.0.1:19200"
            ),
        })?;
        let pool = SingleNodeConnectionPool::new(parsed);
        let transport = TransportBuilder::new(pool)
            .disable_proxy()
            .build()
            .map_err(|err| StorageError::Unavailable {
                backend: "opensearch",
                message: StorageError::sanitize(&err.to_string()),
            })?;
        Ok(Self {
            client: OpenSearch::new(transport),
            refresh_on_write: false,
        })
    }

    #[must_use]
    pub fn with_refresh_on_write(mut self, yes: bool) -> Self {
        self.refresh_on_write = yes;
        self
    }

    /// GET `/` 的叢集身分 JSON。測試用來確認不是 Elasticsearch。
    pub async fn cluster_info(&self) -> Result<Value, StorageError> {
        let response = self
            .client
            .send::<(), ()>(
                opensearch::http::Method::Get,
                "/",
                opensearch::http::headers::HeaderMap::new(),
                None,
                None,
                None,
            )
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!("GET / 回 {}", response.status_code()),
            });
        }
        response.json().await.map_err(map_os)
    }

    pub async fn ensure_index(&self, index: &str) -> Result<(), StorageError> {
        let exists = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[index]))
            .send()
            .await
            .map_err(map_os)?;
        if exists.status_code().as_u16() == 200 {
            return Ok(());
        }
        let response = self
            .client
            .indices()
            .create(IndicesCreateParts::Index(index))
            .body(json!({
                "settings": { "number_of_shards": 1, "number_of_replicas": 0 }
            }))
            .send()
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() && response.status_code().as_u16() != 400 {
            return Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!("建立 index `{index}` 失敗：{}", response.status_code()),
            });
        }
        Ok(())
    }

    /// 建立帶明確 settings／mappings 的 index；已存在時改成「補上缺少的欄位對映」。
    ///
    /// # 為什麼要冪等
    ///
    /// indexer 每次啟動都會呼叫它。已存在就整個略過的話，加了新欄位的版本上線後
    /// 會**靜默**吃 dynamic mapping（型別由第一筆資料猜），之後那個欄位的過濾行為
    /// 與新叢集上完全不同，而且不會有任何錯誤。所以已存在時要走 `_mapping` 補欄位。
    ///
    /// ⚠️ `_mapping` 只能**新增**欄位，不能改既有欄位的型別——那是 OpenSearch 的限制，
    /// 不是這裡的。要改型別只能重建 index（`osint-indexer --rebuild`）。
    /// 因此回傳的 `Err` 若訊息含 `mapper_parsing_exception`，代表 mapping 有破壞性變更，
    /// 必須重建而不是重試。
    pub async fn ensure_index_with(
        &self,
        index: &str,
        settings: &Value,
        mappings: &Value,
    ) -> Result<bool, StorageError> {
        let exists = self
            .client
            .indices()
            .exists(opensearch::indices::IndicesExistsParts::Index(&[index]))
            .send()
            .await
            .map_err(map_os)?;
        if exists.status_code().as_u16() == 200 {
            self.put_mapping(index, mappings).await?;
            return Ok(false);
        }
        let response = self
            .client
            .indices()
            .create(IndicesCreateParts::Index(index))
            .body(json!({ "settings": settings, "mappings": mappings }))
            .send()
            .await
            .map_err(map_os)?;
        let code = response.status_code().as_u16();
        if code == 400 {
            // 400 有兩種：另一個實例同時建好了（resource_already_exists_exception），
            // 以及 mapping 本身寫錯。不能一律當成「已存在」吞掉——那會讓寫錯的 mapping
            // 靜默留在原地，之後所有查詢都走 dynamic mapping。
            let body: Value = response.json().await.map_err(map_os)?;
            let kind = body
                .pointer("/error/type")
                .and_then(Value::as_str)
                .unwrap_or("");
            if kind == "resource_already_exists_exception" {
                self.put_mapping(index, mappings).await?;
                return Ok(false);
            }
            return Err(StorageError::Configuration {
                message: format!(
                    "建立 index `{index}` 被拒（400 {kind}）：{}。請檢查 mapping 定義",
                    StorageError::sanitize(&body.to_string())
                ),
            });
        }
        if !response.status_code().is_success() {
            return Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!("建立 index `{index}` 失敗：{code}"),
            });
        }
        Ok(true)
    }

    /// 對既有 index 套用 mapping（只能新增欄位）。
    pub async fn put_mapping(&self, index: &str, mappings: &Value) -> Result<(), StorageError> {
        let response = self
            .client
            .indices()
            .put_mapping(IndicesPutMappingParts::Index(&[index]))
            .body(mappings.clone())
            .send()
            .await
            .map_err(map_os)?;
        if response.status_code().is_success() {
            return Ok(());
        }
        let code = response.status_code().as_u16();
        let body: Value = response.json().await.map_err(map_os)?;
        Err(StorageError::Configuration {
            message: format!(
                "更新 index `{index}` 的 mapping 失敗（{code}）：{}。\
                 OpenSearch 的 _mapping 只能新增欄位，不能改既有欄位的型別；\
                 若是型別變更，請跑 `osint-indexer --rebuild` 重建 index",
                StorageError::sanitize(&body.to_string())
            ),
        })
    }

    /// 強制 refresh，讓剛寫入的文件可被搜尋。測試與 rebuild 結束時用。
    pub async fn refresh(&self, index: &str) -> Result<(), StorageError> {
        let response = self
            .client
            .indices()
            .refresh(IndicesRefreshParts::Index(&[index]))
            .send()
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!("refresh `{index}` 失敗：{}", response.status_code()),
            });
        }
        Ok(())
    }

    /// 刪掉整個 index。`--rebuild` 用；index 不存在時回 `false` 而不是報錯。
    pub async fn delete_index(&self, index: &str) -> Result<bool, StorageError> {
        let response = self
            .client
            .indices()
            .delete(opensearch::indices::IndicesDeleteParts::Index(&[index]))
            .send()
            .await
            .map_err(map_os)?;
        match response.status_code().as_u16() {
            200 => Ok(true),
            404 => Ok(false),
            code => Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!("刪除 index `{index}` 失敗：{code}"),
            }),
        }
    }

    /// index 內的文件總數。`--rebuild` 驗證用。index 不存在時回 0。
    pub async fn count(&self, index: &str) -> Result<u64, StorageError> {
        let response = self
            .client
            .count(opensearch::CountParts::Index(&[index]))
            .send()
            .await
            .map_err(map_os)?;
        if response.status_code().as_u16() == 404 {
            return Ok(0);
        }
        if !response.status_code().is_success() {
            return Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!("count `{index}` 失敗：{}", response.status_code()),
            });
        }
        let body: Value = response.json().await.map_err(map_os)?;
        Ok(body.get("count").and_then(Value::as_u64).unwrap_or(0))
    }

    async fn maybe_refresh(&self, index: &str) -> Result<(), StorageError> {
        if !self.refresh_on_write {
            return Ok(());
        }
        self.refresh(index).await
    }
}

fn map_os(err: opensearch::Error) -> StorageError {
    let message = StorageError::sanitize(&err.to_string());
    if message.contains("timed out") || message.contains("timeout") {
        StorageError::Timeout {
            backend: "opensearch",
            message,
        }
    } else {
        StorageError::Unavailable {
            backend: "opensearch",
            message,
        }
    }
}

#[async_trait]
impl HealthProvider for OpenSearchStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let response = self
            .client
            .cluster()
            .health(opensearch::cluster::ClusterHealthParts::None)
            .send()
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Ok(StorageHealth::down(
                "opensearch",
                format!("_cluster/health 回 {}", response.status_code()),
            ));
        }
        let body: Value = response.json().await.map_err(map_os)?;
        let status = body
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let healthy = status == "green" || status == "yellow";
        let mut health = if healthy {
            StorageHealth::ok("opensearch", format!("cluster status={status}"))
        } else {
            StorageHealth::down("opensearch", format!("cluster status={status}"))
        };
        health.details = body;
        Ok(health)
    }
}

impl StorageAdapter for OpenSearchStore {
    fn descriptor(&self) -> CapabilityDescriptor {
        CapabilityDescriptor::new("opensearch", env!("CARGO_PKG_VERSION"), &["search"])
            .with_feature("vector", Value::Bool(false))
            .with_feature("bulk_write", Value::Bool(true))
    }
}

#[async_trait]
impl SearchStore for OpenSearchStore {
    async fn index(&self, document: SearchDocument) -> Result<(), StorageError> {
        self.ensure_index(&document.index).await?;
        let response = self
            .client
            .index(IndexParts::IndexId(&document.index, &document.id))
            .body(document.body)
            .send()
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!("index 文件失敗：{}", response.status_code()),
            });
        }
        self.maybe_refresh(&document.index).await
    }

    async fn bulk_index(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError> {
        if documents.is_empty() {
            return Ok(BulkIndexResult::empty());
        }
        let mut seen = Vec::new();
        for doc in &documents {
            if !seen.iter().any(|i| i == &doc.index) {
                self.ensure_index(&doc.index).await?;
                seen.push(doc.index.clone());
            }
        }
        let body: Vec<JsonBody<Value>> = documents
            .iter()
            .flat_map(|doc| {
                [
                    JsonBody::from(json!({
                        "index": { "_index": doc.index, "_id": doc.id }
                    })),
                    JsonBody::from(doc.body.clone()),
                ]
            })
            .collect();
        // Bulk API 吃 NDJSON 行：action 列 + source 列。JsonBody 實作 Body，可組成 NdBody。
        let response = self
            .client
            .bulk(BulkParts::None)
            .body(body)
            .send()
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!("bulk 失敗：{}", response.status_code()),
            });
        }
        let payload: Value = response.json().await.map_err(map_os)?;
        // ⚠️ bulk 的 HTTP 狀態碼是 200，**即使每一筆都失敗**。錯誤只在 items 裡逐筆出現。
        // 只看狀態碼就會把整批丟失當成成功——這是 OpenSearch bulk 最典型的靜默失效。
        let items = payload
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let total = items.len() as u32;
        let failures = collect_bulk_failures(&items);
        let errors = failures.len() as u32;
        for index in seen {
            self.maybe_refresh(&index).await?;
        }
        if total != documents.len() as u32 {
            // 送進去 N 筆卻只回 M 筆 item，代表有請求根本沒被處理。回報成全部失敗，
            // 讓呼叫端重送，而不是讓差額默默消失。
            return Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!(
                    "bulk 送出 {} 筆但只回 {total} 筆結果。差額的文件狀態未知，請整批重送",
                    documents.len()
                ),
            });
        }
        Ok(BulkIndexResult {
            indexed: total.saturating_sub(errors),
            errors,
            failures,
        })
    }

    async fn query(&self, query: SearchQuery) -> Result<SearchHits, StorageError> {
        let response = self
            .client
            .search(SearchParts::Index(&[&query.index]))
            .from(i64::from(query.from))
            .size(i64::from(query.size))
            .body(json!({
                "query": {
                    "query_string": { "query": query.query_string }
                }
            }))
            .send()
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!("search 失敗：{}", response.status_code()),
            });
        }
        let payload: Value = response.json().await.map_err(map_os)?;
        let total = payload
            .pointer("/hits/total/value")
            .and_then(Value::as_u64)
            .or_else(|| payload.pointer("/hits/total").and_then(Value::as_u64))
            .unwrap_or(0);
        Ok(parse_hits(&payload, total))
    }

    async fn search(&self, query: StructuredSearch) -> Result<SearchHits, StorageError> {
        let body = build_search_body(&query)?;
        let response = self
            .client
            .search(SearchParts::Index(&[&query.index]))
            .body(body)
            .send()
            .await
            .map_err(map_os)?;
        if response.status_code().as_u16() == 404 {
            // index 還沒建立（indexer 從沒跑過）。回空結果而不是 500——
            // 「還沒有任何文件被索引」不是伺服器錯誤。
            return Ok(SearchHits {
                total: 0,
                hits: Vec::new(),
            });
        }
        if !response.status_code().is_success() {
            let code = response.status_code().as_u16();
            let detail: Value = response.json().await.unwrap_or(Value::Null);
            return Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!(
                    "search 失敗（{code}）：{}",
                    StorageError::sanitize(&detail.to_string())
                ),
            });
        }
        let payload: Value = response.json().await.map_err(map_os)?;
        let total = payload
            .pointer("/hits/total/value")
            .and_then(Value::as_u64)
            .or_else(|| payload.pointer("/hits/total").and_then(Value::as_u64))
            .unwrap_or(0);
        Ok(parse_hits(&payload, total))
    }

    async fn delete(&self, index: &str, id: &str) -> Result<bool, StorageError> {
        let response = self
            .client
            .delete(DeleteParts::IndexId(index, id))
            .send()
            .await
            .map_err(map_os)?;
        match response.status_code().as_u16() {
            200 | 201 => {
                self.maybe_refresh(index).await?;
                Ok(true)
            }
            404 => Ok(false),
            code => Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!("delete 失敗：{code}"),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// 回應解析
// ---------------------------------------------------------------------------

/// bulk 回應的 items 陣列 → 逐筆失敗細節。
///
/// bulk 的每個 item 外層是動作名（`index`／`create`／`update`／`delete`），
/// 這裡不寫死成 `index`：之後若改用 `create` 做 create-only 語意，
/// 寫死的版本會**找不到任何錯誤**（靜默把失敗當成功），而不是報錯。
fn collect_bulk_failures(items: &[Value]) -> Vec<BulkFailure> {
    let mut failures = Vec::new();
    for row in items {
        let Some(action) = row.as_object().and_then(|map| map.values().next()) else {
            continue;
        };
        let Some(error) = action.get("error") else {
            continue;
        };
        let status = action
            .get("status")
            .and_then(Value::as_u64)
            .and_then(|code| u16::try_from(code).ok())
            .unwrap_or(0);
        let reason = error
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| error.to_string());
        failures.push(BulkFailure {
            id: action
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or("(未知 id)")
                .to_string(),
            status,
            reason: StorageError::sanitize(&reason),
        });
    }
    failures
}

fn parse_hits(payload: &Value, total: u64) -> SearchHits {
    let hits = payload
        .pointer("/hits/hits")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|hit| SearchHit {
            id: hit
                .get("_id")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            score: hit.get("_score").and_then(Value::as_f64),
            source: hit.get("_source").cloned().unwrap_or(Value::Null),
            sort: hit
                .get("sort")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            highlights: hit
                .get("highlight")
                .and_then(Value::as_object)
                .map(|map| {
                    map.iter()
                        .map(|(field, frags)| {
                            let list = frags
                                .as_array()
                                .map(|items| {
                                    items
                                        .iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_string)
                                        .collect()
                                })
                                .unwrap_or_default();
                            (field.clone(), list)
                        })
                        .collect()
                })
                .unwrap_or_default(),
        })
        .collect();
    SearchHits { total, hits }
}

// ---------------------------------------------------------------------------
// StructuredSearch → OpenSearch Query DSL
// ---------------------------------------------------------------------------

/// adapter 端的硬上限。呼叫端該自己夾，但這裡再夾一次——
/// 少一個地方忘了夾就是一個 DoS 入口。
const MAX_SIZE: u32 = 200;
/// 全文條件樹的深度上限。巢狀過深的查詢會讓 OpenSearch 端遞迴爆掉。
const MAX_EXPR_DEPTH: u32 = 8;

fn build_search_body(query: &StructuredSearch) -> Result<Value, StorageError> {
    let mut bool_query = serde_json::Map::new();

    if let Some(expr) = &query.expression {
        if query.fields.is_empty() {
            return Err(StorageError::Configuration {
                message: "StructuredSearch 有 expression 但沒有 fields。\
                          沒有欄位清單就只能讓 OpenSearch 自己挑欄位，行為不可預期"
                    .into(),
            });
        }
        bool_query.insert(
            "must".into(),
            Value::Array(vec![translate_expr(expr, &query.fields, 0)?]),
        );
    }

    let filters: Vec<Value> = query.filters.iter().map(translate_filter).collect();
    if !filters.is_empty() {
        bool_query.insert("filter".into(), Value::Array(filters));
    }
    if bool_query.is_empty() {
        bool_query.insert("must".into(), Value::Array(vec![json!({"match_all": {}})]));
    }

    let mut body = json!({
        "query": { "bool": Value::Object(bool_query) },
        "size": query.size.clamp(1, MAX_SIZE),
        // 預設只算到 10000 就停。search API 要回真實 total（rebuild 驗證與
        // 「共 N 筆」都靠它），算到一半停下來會回一個看起來正常但錯的數字。
        "track_total_hits": true,
    });
    let map = body.as_object_mut().expect("json! 建的是 object");

    if !query.sort.is_empty() {
        map.insert(
            "sort".into(),
            Value::Array(
                query
                    .sort
                    .iter()
                    .map(|s| {
                        json!({ &s.field: { "order": if s.ascending { "asc" } else { "desc" } } })
                    })
                    .collect(),
            ),
        );
    }
    if let Some(after) = &query.search_after {
        if !after.is_empty() {
            map.insert("search_after".into(), Value::Array(after.clone()));
        }
    }
    if !query.highlight_fields.is_empty() {
        let fields: serde_json::Map<String, Value> = query
            .highlight_fields
            .iter()
            .map(|field| {
                (
                    field.clone(),
                    json!({ "fragment_size": 160, "number_of_fragments": 1 }),
                )
            })
            .collect();
        map.insert(
            "highlight".into(),
            json!({
                // require_field_match=false 是必要的：命中可能發生在 `title.cjk`
                // 這個 sub-field 上，但我們要 highlight 的是 `title`。
                // 設成 true 的話中文查詢會回一個沒有 snippet 的結果。
                "require_field_match": false,
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "fields": Value::Object(fields),
            }),
        );
    }
    Ok(body)
}

fn field_list(fields: &[SearchField]) -> Vec<String> {
    fields
        .iter()
        .map(|f| {
            if (f.boost - 1.0).abs() < f32::EPSILON {
                f.name.clone()
            } else {
                format!("{}^{}", f.name, f.boost)
            }
        })
        .collect()
}

fn translate_expr(
    expr: &QueryExpr,
    fields: &[SearchField],
    depth: u32,
) -> Result<Value, StorageError> {
    if depth > MAX_EXPR_DEPTH {
        return Err(StorageError::Configuration {
            message: format!(
                "查詢條件巢狀超過 {MAX_EXPR_DEPTH} 層。請把括號拆成多次查詢，或簡化條件"
            ),
        });
    }
    Ok(match expr {
        // `operator: and` 讓「一個 term 被 analyzer 拆成多個 token」時全部都要命中。
        // 中文靠的就是這個：`勒索軟體` 經 cjk analyzer 是 3 個 bigram，
        // 預設的 or 會讓只含「軟體」的文件也命中。
        QueryExpr::Term(text) => json!({
            "multi_match": {
                "query": text,
                "fields": field_list(fields),
                "type": "best_fields",
                "operator": "and",
            }
        }),
        QueryExpr::Phrase(text) => json!({
            "multi_match": {
                "query": text,
                "fields": field_list(fields),
                "type": "phrase",
            }
        }),
        QueryExpr::And(parts) => {
            let clauses = translate_all(parts, fields, depth)?;
            json!({ "bool": { "must": clauses } })
        }
        QueryExpr::Or(parts) => {
            let clauses = translate_all(parts, fields, depth)?;
            json!({ "bool": { "should": clauses, "minimum_should_match": 1 } })
        }
        QueryExpr::Not(inner) => {
            let clause = translate_expr(inner, fields, depth + 1)?;
            // 補一個 match_all 的 must：只有 must_not 的 bool 在某些巢狀位置
            // 會被算成「不計分且不篩選」。明確寫出「全部文件裡排除這些」。
            json!({ "bool": { "must": [{ "match_all": {} }], "must_not": [clause] } })
        }
    })
}

fn translate_all(
    parts: &[QueryExpr],
    fields: &[SearchField],
    depth: u32,
) -> Result<Vec<Value>, StorageError> {
    parts
        .iter()
        .map(|part| translate_expr(part, fields, depth + 1))
        .collect()
}

fn translate_filter(filter: &SearchFilter) -> Value {
    match filter {
        SearchFilter::Term { field, value } => json!({ "term": { field: value } }),
        SearchFilter::DateRange { field, from, to } => {
            let mut range = serde_json::Map::new();
            if let Some(from) = from {
                range.insert("gte".into(), Value::String(from.to_rfc3339()));
            }
            if let Some(to) = to {
                range.insert("lte".into(), Value::String(to.to_rfc3339()));
            }
            json!({ "range": { field: Value::Object(range) } })
        }
        SearchFilter::Nested { path, terms } => {
            let must: Vec<Value> = terms
                .iter()
                .map(|(field, value)| json!({ "term": { field: value } }))
                .collect();
            json!({ "nested": { "path": path, "query": { "bool": { "must": must } } } })
        }
        SearchFilter::Missing { field } => {
            json!({ "bool": { "must_not": [{ "exists": { "field": field } }] } })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn fields() -> Vec<SearchField> {
        vec![
            SearchField::new("title", 3.0),
            SearchField::new("body", 1.0),
        ]
    }

    fn base(expr: Option<QueryExpr>) -> StructuredSearch {
        StructuredSearch {
            index: "documents".into(),
            expression: expr,
            fields: fields(),
            filters: Vec::new(),
            size: 20,
            search_after: None,
            sort: vec![storage_core::SortField {
                field: "id".into(),
                ascending: false,
            }],
            highlight_fields: vec!["title".into()],
        }
    }

    #[test]
    fn boost_one_is_not_written_into_field_name() {
        // `body^1` 與 `body` 語意相同，但多寫一個 ^1 會讓 body 進到
        // 「有 boost」的路徑，之後比對查詢字串的測試會無謂地壞掉。
        assert_eq!(field_list(&fields()), vec!["title^3", "body"]);
    }

    #[test]
    fn term_uses_and_operator_so_cjk_bigrams_all_match() {
        let body = build_search_body(&base(Some(QueryExpr::Term("勒索軟體".into())))).unwrap();
        let op = body
            .pointer("/query/bool/must/0/multi_match/operator")
            .and_then(Value::as_str);
        assert_eq!(
            op,
            Some("and"),
            "改成 or 會讓只含部分 bigram 的文件命中，中文搜尋精確度直接崩掉"
        );
    }

    #[test]
    fn phrase_uses_phrase_type() {
        let body =
            build_search_body(&base(Some(QueryExpr::Phrase("ransomware gang".into())))).unwrap();
        assert_eq!(
            body.pointer("/query/bool/must/0/multi_match/type")
                .and_then(Value::as_str),
            Some("phrase")
        );
    }

    #[test]
    fn wildcard_input_stays_literal_text() {
        // 使用者打 `title:*`。它是一個 Term，會變成要比對的**文字**，
        // 不會變成欄位存取或萬用查詢——這是 injection 防護的結構性保證。
        let body = build_search_body(&base(Some(QueryExpr::Term("title:*".into())))).unwrap();
        let dsl = body.to_string();
        assert!(
            !dsl.contains("query_string") && !dsl.contains("wildcard"),
            "查詢 DSL 不該出現 query_string／wildcard：{dsl}"
        );
        assert_eq!(
            body.pointer("/query/bool/must/0/multi_match/query")
                .and_then(Value::as_str),
            Some("title:*")
        );
    }

    #[test]
    fn or_requires_at_least_one_match() {
        let expr = QueryExpr::Or(vec![
            QueryExpr::Term("a".into()),
            QueryExpr::Term("b".into()),
        ]);
        let body = build_search_body(&base(Some(expr))).unwrap();
        assert_eq!(
            body.pointer("/query/bool/must/0/bool/minimum_should_match")
                .and_then(Value::as_u64),
            Some(1),
            "沒有 minimum_should_match 的 should 在有 must 時完全不篩選，OR 會失效"
        );
    }

    #[test]
    fn not_carries_a_positive_clause() {
        let expr = QueryExpr::Not(Box::new(QueryExpr::Term("a".into())));
        let body = build_search_body(&base(Some(expr))).unwrap();
        assert!(
            body.pointer("/query/bool/must/0/bool/must").is_some(),
            "只有 must_not 的 bool 不保證會篩選"
        );
        assert!(body.pointer("/query/bool/must/0/bool/must_not").is_some());
    }

    #[test]
    fn depth_limit_rejects_deep_nesting() {
        let mut expr = QueryExpr::Term("x".into());
        for _ in 0..20 {
            expr = QueryExpr::Not(Box::new(expr));
        }
        let err = build_search_body(&base(Some(expr))).unwrap_err();
        assert!(err.to_string().contains("巢狀"), "{err}");
    }

    #[test]
    fn expression_without_fields_is_rejected() {
        let mut query = base(Some(QueryExpr::Term("x".into())));
        query.fields.clear();
        assert!(build_search_body(&query).is_err());
    }

    #[test]
    fn size_is_clamped() {
        let mut query = base(None);
        query.size = 100_000;
        let body = build_search_body(&query).unwrap();
        assert_eq!(body.get("size").and_then(Value::as_u64), Some(200));
    }

    #[test]
    fn total_is_tracked_exactly() {
        let body = build_search_body(&base(None)).unwrap();
        assert_eq!(
            body.get("track_total_hits").and_then(Value::as_bool),
            Some(true),
            "預設只數到 10000，total 會回一個看起來正常但錯的數字"
        );
    }

    #[test]
    fn filter_only_query_still_matches_documents() {
        let mut query = base(None);
        query.filters = vec![SearchFilter::Term {
            field: "source_id".into(),
            value: "s1".into(),
        }];
        let body = build_search_body(&query).unwrap();
        assert!(
            body.pointer("/query/bool/filter/0/term/source_id")
                .is_some()
        );
    }

    #[test]
    fn empty_query_becomes_match_all() {
        let body = build_search_body(&base(None)).unwrap();
        assert!(body.pointer("/query/bool/must/0/match_all").is_some());
    }

    #[test]
    fn date_range_uses_rfc3339() {
        let from = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let filter = translate_filter(&SearchFilter::DateRange {
            field: "effective_date".into(),
            from: Some(from),
            to: None,
        });
        assert_eq!(
            filter
                .pointer("/range/effective_date/gte")
                .and_then(Value::as_str),
            Some("2026-01-02T03:04:05+00:00")
        );
        assert!(filter.pointer("/range/effective_date/lte").is_none());
    }

    #[test]
    fn nested_filter_binds_terms_to_the_same_element() {
        let filter = translate_filter(&SearchFilter::Nested {
            path: "entities".into(),
            terms: vec![
                ("entities.entity_type".into(), "vulnerability".into()),
                ("entities.normalized_name".into(), "cve-2026-0001".into()),
            ],
        });
        let must = filter
            .pointer("/nested/query/bool/must")
            .and_then(Value::as_array)
            .expect("nested must");
        assert_eq!(
            must.len(),
            2,
            "兩個條件必須在同一個 nested query 裡，拆成兩個 nested 會變成\
             「有這個型別的 entity」且「有這個名字的 entity」，不保證是同一個"
        );
    }

    #[test]
    fn missing_filter_excludes_documents_having_the_field() {
        let filter = translate_filter(&SearchFilter::Missing {
            field: "duplicate_of".into(),
        });
        assert_eq!(
            filter
                .pointer("/bool/must_not/0/exists/field")
                .and_then(Value::as_str),
            Some("duplicate_of")
        );
    }

    #[test]
    fn search_after_is_passed_through() {
        let mut query = base(None);
        query.search_after = Some(vec![json!(1.5), json!("abc")]);
        let body = build_search_body(&query).unwrap();
        assert_eq!(
            body.get("search_after")
                .and_then(Value::as_array)
                .map(Vec::len),
            Some(2)
        );
        assert!(
            body.get("from").is_none(),
            "不可以同時用 from：深分頁的成本與頁碼成正比"
        );
    }

    #[test]
    fn highlight_does_not_require_field_match() {
        let body = build_search_body(&base(None)).unwrap();
        assert_eq!(
            body.pointer("/highlight/require_field_match")
                .and_then(Value::as_bool),
            Some(false),
            "命中在 title.cjk 時要能 highlight title，設 true 中文查詢會沒有 snippet"
        );
    }

    #[test]
    fn bulk_failures_are_read_regardless_of_action_name() {
        let items = vec![
            json!({"index": {"_id": "ok", "status": 201}}),
            json!({"index": {"_id": "bad", "status": 400,
                   "error": {"type": "mapper_parsing_exception", "reason": "欄位型別不符"}}}),
            json!({"create": {"_id": "busy", "status": 429,
                   "error": {"type": "es_rejected_execution_exception", "reason": "queue full"}}}),
        ];
        let failures = collect_bulk_failures(&items);
        assert_eq!(failures.len(), 2);
        assert_eq!(failures[0].id, "bad");
        assert!(!failures[0].is_retryable(), "400 重試幾次都一樣，應進 DLQ");
        assert_eq!(failures[1].id, "busy");
        assert!(failures[1].is_retryable(), "429 是暫時性的，退避後應重試");
    }

    #[test]
    fn hits_parse_sort_and_highlight() {
        let payload = json!({
            "hits": {
                "total": {"value": 2},
                "hits": [{
                    "_id": "d1",
                    "_score": 1.25,
                    "_source": {"title": "t"},
                    "sort": [1.25, "d1"],
                    "highlight": {"title": ["<em>t</em>"]},
                }]
            }
        });
        let hits = parse_hits(&payload, 2);
        assert_eq!(hits.total, 2);
        assert_eq!(hits.hits[0].sort.len(), 2, "沒有 sort 就翻不了下一頁");
        assert_eq!(hits.hits[0].highlights.get("title").map(Vec::len), Some(1));
    }
}
