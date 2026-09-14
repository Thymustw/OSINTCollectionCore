//! OpenSearch SearchStore adapter，以及 ml-commons [`MlCommonsEmbeddingProvider`]。
//! 連線 URL 由呼叫端／設定注入，不寫死 9200。

mod embedding;

pub use embedding::{
    E5_CONTENT_HASH, E5_MODEL_NAME, MINILM_CONTENT_HASH, MINILM_MODEL_NAME,
    MlCommonsEmbeddingProvider, classify_ml_http,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use opensearch::http::Url;
use opensearch::http::request::JsonBody;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::indices::{IndicesCreateParts, IndicesPutMappingParts, IndicesRefreshParts};
use opensearch::params::Refresh;
use opensearch::{
    BulkParts, DeleteParts, GetParts, IndexParts, OpenSearch, SearchParts, UpdateParts,
};
use serde_json::{Value, json};
use storage_core::{
    BulkFailure, BulkIndexResult, CapabilityDescriptor, HealthProvider, ProjectionCheckpoint,
    ProjectionLag, ProjectionStore, QueryExpr, RebuildState, RebuildStatus, SearchDocument,
    SearchField, SearchFilter, SearchHit, SearchHits, SearchQuery, SearchStore, StorageAdapter,
    StorageError, StorageHealth, StructuredSearch, VectorSearch,
};
use uuid::Uuid;

/// 投影狀態的預設 index 名。
///
/// **刻意與被投影的 index 分開。** 放在 `osint-documents` 裡的話，
/// `osint-indexer --rebuild --drop` 刪掉那個 index 的同一瞬間，
/// 「上次 rebuild 何時、寫了幾筆、失敗了嗎」也一起消失——而那正是重建當下
/// 最需要回報的東西（SPEC_V0.2 §27）。要清掉狀態只能明確呼叫
/// [`ProjectionStore::reset_projection`]。
pub const PROJECTION_STATE_INDEX: &str = "osint-projection-state";

/// OpenSearch 搜尋投影。
#[derive(Debug, Clone)]
pub struct OpenSearchStore {
    client: OpenSearch,
    /// 測試用：每次寫入後 refresh。正式環境應為 false。
    refresh_on_write: bool,
    /// 投影狀態存在哪個 index。測試會換成 per-run 名稱以免互相覆寫。
    projection_state_index: String,
}

impl OpenSearchStore {
    pub fn connect(url: &str) -> Result<Self, StorageError> {
        Ok(Self {
            client: build_opensearch_client(url)?,
            refresh_on_write: false,
            projection_state_index: PROJECTION_STATE_INDEX.to_string(),
        })
    }

    #[must_use]
    pub fn with_refresh_on_write(mut self, yes: bool) -> Self {
        self.refresh_on_write = yes;
        self
    }

    /// 換掉投影狀態 index 的名稱。
    ///
    /// **只給測試用。** conformance 與 e2e 共用同一個叢集，用預設名稱的話測試會
    /// 往正式的狀態 index 寫東西；用 per-run 名稱才能在測試結尾整個刪掉
    /// （`CLAUDE.md` §15：測試不可以留下 index）。
    #[must_use]
    pub fn with_projection_state_index(mut self, index: impl Into<String>) -> Self {
        self.projection_state_index = index.into();
        self
    }

    /// 目前使用的投影狀態 index 名。
    #[must_use]
    pub fn projection_state_index(&self) -> &str {
        &self.projection_state_index
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

pub(crate) fn build_opensearch_client(url: &str) -> Result<OpenSearch, StorageError> {
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
    Ok(OpenSearch::new(transport))
}

pub(crate) fn map_os(err: opensearch::Error) -> StorageError {
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
        CapabilityDescriptor::new(
            "opensearch",
            env!("CARGO_PKG_VERSION"),
            // projection：V0.2 Phase 0f 起也實作 ProjectionStore（checkpoint／lag／rebuild 狀態）。
            &["search", "projection"],
        )
        .with_feature("vector", Value::Bool(true))
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

    async fn bulk_upsert_fields(
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
        // `_update` + `doc_as_upsert`：文件不存在就用整份 doc 建立；已存在則只合併
        // 這份 body 裡的欄位。`bulk_index` 的 `"index"` action 會整份取代 `_source`，
        // 把別的服務（embedding-worker）事後疊加的向量欄位靜默清掉。
        let body: Vec<JsonBody<Value>> = bulk_upsert_action_lines(&documents)
            .into_iter()
            .map(JsonBody::from)
            .collect();
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
                message: format!("bulk upsert 失敗：{}", response.status_code()),
            });
        }
        let payload: Value = response.json().await.map_err(map_os)?;
        // ⚠️ bulk 的 HTTP 狀態碼是 200，**即使每一筆都失敗**。錯誤只在 items 裡逐筆出現。
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
            return Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!(
                    "bulk upsert 送出 {} 筆但只回 {total} 筆結果。差額的文件狀態未知，請整批重送",
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

    async fn update_fields(
        &self,
        index: &str,
        id: &str,
        fields: Value,
    ) -> Result<(), StorageError> {
        // 刻意不 doc_as_upsert：文件應該已由 indexer 寫入。不存在就回 NotFound，
        // 不要憑空補一份只有向量欄位的殘缺文件。
        let response = self
            .client
            .update(UpdateParts::IndexId(index, id))
            .body(json!({ "doc": fields }))
            .send()
            .await
            .map_err(map_os)?;
        match response.status_code().as_u16() {
            200 | 201 => self.maybe_refresh(index).await,
            404 => Err(StorageError::NotFound {
                message: format!(
                    "部分更新 `{index}/{id}` 失敗：文件不存在。\
                     向量欄位是疊加在 indexer 已寫入的文件上，不該在這裡憑空建立一份"
                ),
            }),
            code => {
                let detail: Value = response.json().await.unwrap_or(Value::Null);
                Err(StorageError::Unknown {
                    backend: "opensearch",
                    message: format!(
                        "部分更新 `{index}/{id}` 失敗（{code}）：{}",
                        StorageError::sanitize(&detail.to_string())
                    ),
                })
            }
        }
    }

    async fn vector_search(&self, query: VectorSearch) -> Result<SearchHits, StorageError> {
        let body = build_knn_body(&query)?;
        let response = self
            .client
            .search(SearchParts::Index(&[&query.index]))
            .body(body)
            .send()
            .await
            .map_err(map_os)?;
        if response.status_code().as_u16() == 404 {
            // index 還沒建立。回空結果而不是 500——還沒有任何文件被索引不是伺服器錯誤。
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
                    "vector_search 失敗（{code}）：{}",
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
}

// ---------------------------------------------------------------------------
// ProjectionStore（V0.2 Phase 0f）
// ---------------------------------------------------------------------------

/// 狀態 index 的 mapping。`dynamic: strict` 的理由同 `osint-documents`：
/// 打錯一個欄位名會被 400 擋下來，而不是靜默長出一個型別由第一筆資料猜出來的新欄位。
fn projection_state_mappings() -> Value {
    json!({
        "dynamic": "strict",
        "properties": {
            "projection": { "type": "keyword" },
            // checkpoint。`checkpoint_updated_at` 的「有沒有值」就是
            // 「這個投影寫過東西沒有」——checkpoint() 靠它回 None 而不是零值。
            "last_source_at": { "type": "date" },
            "last_object_id": { "type": "keyword" },
            "objects_written": { "type": "long" },
            "checkpoint_updated_at": { "type": "date" },
            // rebuild 狀態。`rebuild_state` 缺值代表沒有重建紀錄（Idle）。
            "rebuild_state": { "type": "keyword" },
            "rebuild_started_at": { "type": "date" },
            "rebuild_finished_at": { "type": "date" },
            "rebuild_scanned": { "type": "long" },
            "rebuild_written": { "type": "long" },
            "rebuild_failed": { "type": "long" },
            // 錯誤原文可能很長（OpenSearch 的 keyword 上限 32766 bytes）。
            // 不需要查詢它，所以 index: false。
            "rebuild_last_error": { "type": "text", "index": false }
        }
    })
}

impl OpenSearchStore {
    /// 建立（或補齊）投影狀態 index。冪等，可以每次寫入前呼叫。
    pub async fn ensure_projection_state_index(&self) -> Result<(), StorageError> {
        self.ensure_index_with(
            &self.projection_state_index,
            &json!({ "number_of_shards": 1, "number_of_replicas": 0 }),
            &projection_state_mappings(),
        )
        .await
        .map(|_| ())
    }

    /// 讀狀態列的 `_source`。index 或文件不存在時回 `None`。
    async fn projection_state_doc(&self, projection: &str) -> Result<Option<Value>, StorageError> {
        let response = self
            .client
            .get(GetParts::IndexId(&self.projection_state_index, projection))
            .send()
            .await
            .map_err(map_os)?;
        let code = response.status_code().as_u16();
        if code == 404 {
            // 兩種 404 都是「還沒有狀態」：index 沒建（indexer 從沒跑過）
            // 或這個投影沒有列。都不是錯誤。
            return Ok(None);
        }
        if !response.status_code().is_success() {
            return Err(StorageError::Unavailable {
                backend: "opensearch",
                message: format!(
                    "讀投影狀態 `{projection}`（index `{}`）失敗：{code}",
                    self.projection_state_index
                ),
            });
        }
        let body: Value = response.json().await.map_err(map_os)?;
        Ok(body.get("_source").cloned())
    }

    /// 部分更新狀態列（`doc_as_upsert`）。
    ///
    /// **必須是部分更新，不能整列覆寫。** checkpoint 與 rebuild 狀態同在一列
    /// （`_id` 就是投影名），整列覆寫的話每次 flush 寫 checkpoint 都會把
    /// 「上次 rebuild 何時、寫了幾筆」清成 null——而那是 `--drop` 之後唯一還留著的資訊。
    ///
    /// 「寫完就讀得到」不是為了測試方便：投影 worker 下一批會先讀 checkpoint 再累加，
    /// 讀到 refresh 前的舊值等於累積計數永遠停在原地。
    ///
    /// # 為什麼是 `refresh=true` 而不是 `wait_for`
    ///
    /// 兩者都保證讀得到，但 `wait_for` 是**等到下一次排程 refresh**，也就是等滿
    /// `index.refresh_interval`（預設 1 秒）。2026-09-12 在本機 19200 實測同一個
    /// 單 shard index：5 次 `_update` 用 `wait_for` 共 4985 ms（≈1 秒／次），
    /// 用 `true` 共 44 ms（≈9 ms／次）。
    ///
    /// rebuild 每頁會寫兩次狀態（checkpoint + 進度），用 `wait_for` 等於每 100 份
    /// 文件多花 2 秒純等待——重建一萬筆就是多三分鐘，而且那段時間完全沒在做事。
    /// 這個 index 只有一個 shard、幾列文件、每批才寫一次，強制 refresh 的成本
    /// （刷一個極小的 segment）遠低於等一個 refresh 週期。
    ///
    /// ⚠️ 同樣的推論**不適用於** `osint-documents`：那裡是高頻 bulk，
    /// 每批強制 refresh 會把 segment 數量炸開。不要把這個選擇複製過去。
    async fn update_projection_state(
        &self,
        projection: &str,
        doc: Value,
    ) -> Result<(), StorageError> {
        self.ensure_projection_state_index().await?;
        let response = self
            .client
            .update(UpdateParts::IndexId(
                &self.projection_state_index,
                projection,
            ))
            .refresh(Refresh::True)
            .body(json!({ "doc": doc, "doc_as_upsert": true }))
            .send()
            .await
            .map_err(map_os)?;
        if response.status_code().is_success() {
            return Ok(());
        }
        let code = response.status_code().as_u16();
        let detail: Value = response.json().await.unwrap_or(Value::Null);
        Err(StorageError::Unknown {
            backend: "opensearch",
            message: format!(
                "寫投影狀態 `{projection}`（index `{}`）失敗（{code}）：{}。\
                 400 strict_dynamic_mapping_exception 代表狀態欄位有新增卻沒改 mapping，\
                 請看 storage-opensearch 的 projection_state_mappings()",
                self.projection_state_index,
                StorageError::sanitize(&detail.to_string())
            ),
        })
    }
}

fn state_ts(source: &Value, key: &str) -> Option<DateTime<Utc>> {
    source
        .get(key)
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|at| at.with_timezone(&Utc))
}

fn state_u64(source: &Value, key: &str) -> u64 {
    source.get(key).and_then(Value::as_u64).unwrap_or(0)
}

#[async_trait]
impl ProjectionStore for OpenSearchStore {
    async fn checkpoint(
        &self,
        projection: &str,
    ) -> Result<Option<ProjectionCheckpoint>, StorageError> {
        let Some(source) = self.projection_state_doc(projection).await? else {
            return Ok(None);
        };
        // 只有 rebuild 狀態、沒有 checkpoint 的列是正常的（跑了 rebuild 但一筆都沒寫）。
        // 那種情況要回 None，不是回一個 objects_written = 0 的假 checkpoint。
        let Some(updated_at) = state_ts(&source, "checkpoint_updated_at") else {
            return Ok(None);
        };
        Ok(Some(ProjectionCheckpoint {
            projection: projection.to_string(),
            last_source_at: state_ts(&source, "last_source_at"),
            last_object_id: source
                .get("last_object_id")
                .and_then(Value::as_str)
                .and_then(|raw| Uuid::parse_str(raw).ok()),
            objects_written: state_u64(&source, "objects_written"),
            updated_at,
        }))
    }

    async fn save_checkpoint(&self, checkpoint: &ProjectionCheckpoint) -> Result<(), StorageError> {
        self.update_projection_state(
            &checkpoint.projection,
            json!({
                "projection": checkpoint.projection,
                "last_source_at": checkpoint.last_source_at,
                "last_object_id": checkpoint.last_object_id.map(|id| id.to_string()),
                "objects_written": checkpoint.objects_written,
                "checkpoint_updated_at": checkpoint.updated_at,
            }),
        )
        .await
    }

    async fn projection_lag(
        &self,
        projection: &str,
        now: DateTime<Utc>,
    ) -> Result<ProjectionLag, StorageError> {
        Ok(ProjectionLag::from_checkpoint(
            self.checkpoint(projection).await?,
            now,
        ))
    }

    async fn rebuild_status(&self, projection: &str) -> Result<RebuildStatus, StorageError> {
        let Some(source) = self.projection_state_doc(projection).await? else {
            return Ok(RebuildStatus::idle(projection));
        };
        let Some(state) = source.get("rebuild_state") else {
            return Ok(RebuildStatus::idle(projection));
        };
        // 認不出來的狀態字串**不能**默默當成 Idle：那會讓「重建中」看起來像
        // 「沒有人在重建」，於是有人再開一個重建。寧可回錯誤讓人去看那一列。
        let state: RebuildState =
            serde_json::from_value(state.clone()).map_err(|err| StorageError::Unknown {
                backend: "opensearch",
                message: format!(
                    "投影 `{projection}` 的 rebuild_state 是無法識別的值 {state}：{err}。\
                     請檢查 index `{}` 的那一列",
                    self.projection_state_index
                ),
            })?;
        Ok(RebuildStatus {
            projection: projection.to_string(),
            state,
            started_at: state_ts(&source, "rebuild_started_at"),
            finished_at: state_ts(&source, "rebuild_finished_at"),
            scanned: state_u64(&source, "rebuild_scanned"),
            written: state_u64(&source, "rebuild_written"),
            failed: state_u64(&source, "rebuild_failed"),
            last_error: source
                .get("rebuild_last_error")
                .and_then(Value::as_str)
                .map(str::to_string),
        })
    }

    async fn set_rebuild_status(&self, status: &RebuildStatus) -> Result<(), StorageError> {
        self.update_projection_state(
            &status.projection,
            json!({
                "projection": status.projection,
                "rebuild_state": status.state,
                "rebuild_started_at": status.started_at,
                "rebuild_finished_at": status.finished_at,
                "rebuild_scanned": status.scanned,
                "rebuild_written": status.written,
                "rebuild_failed": status.failed,
                // 呼叫端應該已經 sanitize 過（trait 契約）。這裡再過一次：
                // 少一個地方忘了過就是一次 DSN 外洩，而成本只是一次字串掃描。
                "rebuild_last_error": status
                    .last_error
                    .as_deref()
                    .map(StorageError::sanitize),
            }),
        )
        .await
    }

    async fn reset_projection(&self, projection: &str) -> Result<(), StorageError> {
        let response = self
            .client
            .delete(DeleteParts::IndexId(
                &self.projection_state_index,
                projection,
            ))
            .refresh(Refresh::True)
            .send()
            .await
            .map_err(map_os)?;
        match response.status_code().as_u16() {
            // 404 有兩種（index 不存在／列不存在），都代表「已經沒有狀態了」。
            200 | 201 | 404 => Ok(()),
            code => Err(StorageError::Unknown {
                backend: "opensearch",
                message: format!(
                    "清除投影狀態 `{projection}`（index `{}`）失敗：{code}",
                    self.projection_state_index
                ),
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// 回應解析
// ---------------------------------------------------------------------------

/// `bulk_upsert_fields` 的 NDJSON 列（action + payload 成對）。
///
/// 抽出函式是為了單元測試能直接斷言組出來的是 `"update"` + `doc_as_upsert`，
/// 而不是 `"index"`——那兩種 bulk 對既有 `_source` 的語意完全不同，組錯了
/// 要等真實 OpenSearch 才會被發現。
fn bulk_upsert_action_lines(documents: &[SearchDocument]) -> Vec<Value> {
    documents
        .iter()
        .flat_map(|doc| {
            [
                json!({
                    "update": { "_index": doc.index, "_id": doc.id }
                }),
                json!({
                    "doc": doc.body.clone(),
                    "doc_as_upsert": true
                }),
            ]
        })
        .collect()
}

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

/// 組 k-NN 查詢 body。
///
/// filter 放在 knn 子句**裡面**，不是外層 bool 的 post-filter。
/// 2026-09-14 在 OpenSearch 2.19.6 + lucene engine 實測：
///
/// * native（knn 子句內 `filter`）：k=1、最近鄰不符合條件時，仍回傳下一個符合的；
/// * bool must knn + filter：同一組輸入回空——knn 先取 k 再過濾，最近鄰全被濾掉就沒東西。
///
/// 那正是「過濾條件被忽略／看似生效但其實漏結果」的靜默失效，所以這裡走 native。
fn build_knn_body(query: &VectorSearch) -> Result<Value, StorageError> {
    if query.field.is_empty() {
        return Err(StorageError::Configuration {
            message: "VectorSearch.field 是空的。請指定 embedding_en 或 embedding_multi，\
                      兩個欄位的向量空間不相通，查錯欄位不會報錯只會得到無意義鄰居"
                .into(),
        });
    }
    if query.vector.is_empty() {
        return Err(StorageError::Configuration {
            message: format!(
                "VectorSearch.vector 是空的（欄位 `{}`）。空向量無法做 k-NN",
                query.field
            ),
        });
    }
    if query.k == 0 {
        return Err(StorageError::Configuration {
            message: "VectorSearch.k 不可為 0。請指定要回幾個最近鄰居".into(),
        });
    }
    let k = query.k.clamp(1, MAX_SIZE);
    let mut knn_inner = json!({
        "vector": query.vector,
        "k": k,
    });
    if !query.filters.is_empty() {
        let translated: Vec<Value> = query.filters.iter().map(translate_filter).collect();
        let filter = if translated.len() == 1 {
            translated.into_iter().next().expect("長度已確認為 1")
        } else {
            json!({ "bool": { "filter": translated } })
        };
        knn_inner
            .as_object_mut()
            .expect("json! 建的是 object")
            .insert("filter".into(), filter);
    }
    Ok(json!({
        "query": { "knn": { query.field.clone(): knn_inner } },
        "size": k,
        "track_total_hits": true,
    }))
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
            json!({"update": {"_id": "conflict", "status": 409,
                   "error": {"type": "version_conflict_engine_exception", "reason": "version conflict"}}}),
        ];
        let failures = collect_bulk_failures(&items);
        assert_eq!(failures.len(), 3);
        assert_eq!(failures[0].id, "bad");
        assert!(!failures[0].is_retryable(), "400 重試幾次都一樣，應進 DLQ");
        assert_eq!(failures[1].id, "busy");
        assert!(failures[1].is_retryable(), "429 是暫時性的，退避後應重試");
        assert_eq!(
            failures[2].id, "conflict",
            "update action 的失敗必須被讀到，不能因為 key 不是 index 就漏掉"
        );
    }

    #[test]
    fn bulk_upsert_ndjson_uses_update_action_with_doc_as_upsert() {
        let lines = bulk_upsert_action_lines(&[SearchDocument {
            index: "osint-documents".into(),
            id: "d1".into(),
            body: json!({"title": "t", "body": "x"}),
        }]);
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].get("update").is_some(),
            "action 必須是 update，不能是 index：{}",
            lines[0]
        );
        assert!(
            lines[0].get("index").is_none(),
            "用 index action 會整份取代 _source，把未知欄位清掉"
        );
        assert_eq!(
            lines[0].pointer("/update/_index").and_then(Value::as_str),
            Some("osint-documents")
        );
        assert_eq!(
            lines[0].pointer("/update/_id").and_then(Value::as_str),
            Some("d1")
        );
        assert_eq!(
            lines[1].get("doc_as_upsert").and_then(Value::as_bool),
            Some(true),
            "文件不存在時必須用整份 doc 建立，否則 indexer 第一次寫入會失敗"
        );
        assert_eq!(
            lines[1].pointer("/doc/title").and_then(Value::as_str),
            Some("t")
        );
        assert!(
            lines[1].get("doc").is_some(),
            "payload 必須包在 doc 裡：裸 body 會被當成 script 或整份覆寫"
        );
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

    fn knn_query(filters: Vec<SearchFilter>) -> VectorSearch {
        VectorSearch {
            index: "osint-documents".into(),
            field: "embedding_en".into(),
            vector: vec![0.1, 0.2],
            k: 5,
            filters,
        }
    }

    #[test]
    fn knn_body_without_filters_is_plain_knn() {
        let body = build_knn_body(&knn_query(Vec::new())).unwrap();
        assert!(
            body.pointer("/query/knn/embedding_en/vector").is_some(),
            "空 filters 必須是最單純的 knn 子句，不要包一層空 bool"
        );
        assert!(
            body.pointer("/query/knn/embedding_en/filter").is_none(),
            "沒有 filter 時不該寫 filter key"
        );
        assert_eq!(
            body.pointer("/query/knn/embedding_en/k")
                .and_then(Value::as_u64),
            Some(5)
        );
        assert_eq!(body.get("size").and_then(Value::as_u64), Some(5));
    }

    #[test]
    fn knn_filter_lives_inside_the_knn_clause() {
        // 放外層 bool 是 post-filter：knn 先取 k 再過濾，最近鄰全不符合時結果變空。
        let body = build_knn_body(&knn_query(vec![SearchFilter::Term {
            field: "kind".into(),
            value: "report".into(),
        }]))
        .unwrap();
        assert_eq!(
            body.pointer("/query/knn/embedding_en/filter/term/kind")
                .and_then(Value::as_str),
            Some("report")
        );
        assert!(
            body.pointer("/query/bool").is_none(),
            "單一 filter 不該被包進外層 bool"
        );
    }

    #[test]
    fn knn_multiple_filters_are_anded_inside_knn() {
        let body = build_knn_body(&knn_query(vec![
            SearchFilter::Term {
                field: "kind".into(),
                value: "report".into(),
            },
            SearchFilter::Term {
                field: "language".into(),
                value: "en".into(),
            },
        ]))
        .unwrap();
        let filters = body
            .pointer("/query/knn/embedding_en/filter/bool/filter")
            .and_then(Value::as_array)
            .expect("多個 filter 應收成 knn 內的 bool.filter 陣列");
        assert_eq!(filters.len(), 2);
    }

    #[test]
    fn knn_rejects_empty_field_vector_or_zero_k() {
        let mut q = knn_query(Vec::new());
        q.field.clear();
        assert!(build_knn_body(&q).is_err());
        q = knn_query(Vec::new());
        q.vector.clear();
        assert!(build_knn_body(&q).is_err());
        q = knn_query(Vec::new());
        q.k = 0;
        assert!(build_knn_body(&q).is_err());
    }

    #[test]
    fn knn_k_is_clamped() {
        let mut q = knn_query(Vec::new());
        q.k = 100_000;
        let body = build_knn_body(&q).unwrap();
        assert_eq!(
            body.pointer("/query/knn/embedding_en/k")
                .and_then(Value::as_u64),
            Some(200)
        );
    }
}
