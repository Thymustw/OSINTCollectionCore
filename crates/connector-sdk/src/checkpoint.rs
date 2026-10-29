//! Connector checkpoint。更新必須能容忍重試（同一值再寫仍成功）。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{Connector, ConnectorId};
use serde::{Deserialize, Serialize};
use serde_json::json;
use storage_core::{CheckpointUpdate, ConnectorRunStateUpdate, RelationalStore};

use crate::ConnectorError;

/// RSS／HTTP 常見的增量欄位。
///
/// `content_sha256` 給沒有 ETag／Last-Modified 的來源（Static Web／REST）比對內容有沒有變。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ConnectorCheckpoint {
    pub last_retrieved_at: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    #[serde(default)]
    pub content_sha256: Option<String>,
}

impl ConnectorCheckpoint {
    #[must_use]
    pub fn from_value(value: &serde_json::Value) -> Self {
        serde_json::from_value(value.clone()).unwrap_or_default()
    }

    #[must_use]
    pub fn to_value(&self) -> serde_json::Value {
        json!({
            "last_retrieved_at": self.last_retrieved_at,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "content_sha256": self.content_sha256,
        })
    }
}

#[async_trait]
pub trait CheckpointStore: Send + Sync {
    async fn load(&self, connector_id: ConnectorId) -> Result<ConnectorCheckpoint, ConnectorError>;
    async fn save(
        &self,
        connector: &Connector,
        checkpoint: &ConnectorCheckpoint,
    ) -> Result<(), ConnectorError>;
}

/// 把 checkpoint 寫回 `connectors.checkpoint` JSON。
pub struct RelationalCheckpointStore<S> {
    store: S,
}

impl<S> RelationalCheckpointStore<S> {
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> CheckpointStore for RelationalCheckpointStore<S>
where
    S: RelationalStore + Send + Sync,
{
    async fn load(&self, connector_id: ConnectorId) -> Result<ConnectorCheckpoint, ConnectorError> {
        let connector = self
            .store
            .get_connector(connector_id)
            .await
            .map_err(|err| ConnectorError::Storage {
                message: err.to_string(),
            })?
            .ok_or_else(|| ConnectorError::Storage {
                message: format!("找不到 connector `{connector_id}`。請確認已寫入 connectors 表"),
            })?;
        Ok(ConnectorCheckpoint::from_value(&connector.checkpoint))
    }

    async fn save(
        &self,
        connector: &Connector,
        checkpoint: &ConnectorCheckpoint,
    ) -> Result<(), ConnectorError> {
        let marked = self
            .store
            .update_connector_run_state(
                connector.id,
                &ConnectorRunStateUpdate {
                    checkpoint: CheckpointUpdate::Merge(checkpoint.to_value()),
                    ..ConnectorRunStateUpdate::default()
                },
            )
            .await
            .map_err(|err| ConnectorError::Storage {
                message: err.to_string(),
            })?;
        if !marked {
            return Err(ConnectorError::Storage {
                message: format!(
                    "找不到 connector `{}`，無法寫入 checkpoint。請確認已寫入 connectors 表",
                    connector.id
                ),
            });
        }
        Ok(())
    }
}
