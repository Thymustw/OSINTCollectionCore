//! OpenSearch SearchStore adapter。連線 URL 由呼叫端／設定注入，不寫死 9200。

use async_trait::async_trait;
use opensearch::http::Url;
use opensearch::http::request::JsonBody;
use opensearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use opensearch::indices::{IndicesCreateParts, IndicesRefreshParts};
use opensearch::{BulkParts, DeleteParts, IndexParts, OpenSearch, SearchParts};
use serde_json::{Value, json};
use storage_core::{
    BulkIndexResult, CapabilityDescriptor, HealthProvider, SearchDocument, SearchHit, SearchHits,
    SearchQuery, SearchStore, StorageAdapter, StorageError, StorageHealth,
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

    async fn maybe_refresh(&self, index: &str) -> Result<(), StorageError> {
        if !self.refresh_on_write {
            return Ok(());
        }
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
            return Ok(BulkIndexResult {
                indexed: 0,
                errors: 0,
            });
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
        let had_errors = payload
            .get("errors")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let items = payload.get("items").and_then(Value::as_array);
        let total = items.map(Vec::len).unwrap_or(0) as u32;
        let errors = if had_errors {
            items
                .map(|rows| {
                    rows.iter()
                        .filter(|row| {
                            row.pointer("/index/error").is_some()
                                || row.pointer("/create/error").is_some()
                        })
                        .count() as u32
                })
                .unwrap_or(1)
        } else {
            0
        };
        for index in seen {
            self.maybe_refresh(&index).await?;
        }
        Ok(BulkIndexResult {
            indexed: total.saturating_sub(errors),
            errors,
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
            })
            .collect();
        Ok(SearchHits { total, hits })
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
