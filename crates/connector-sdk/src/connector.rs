//! `ConnectorTrait`（SPEC §6 的執行面，不是資料表欄位）。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{CollectionId, Connector, RawEvidence, Source};
use serde_json::Value;

use crate::ConnectorError;
use crate::checkpoint::ConnectorCheckpoint;
use crate::evidence::NewRawEvidence;

/// 一次 collect 的輸入。
#[derive(Debug, Clone)]
pub struct CollectContext {
    pub source: Source,
    pub connector: Connector,
    pub collection_id: Option<CollectionId>,
    pub checkpoint: ConnectorCheckpoint,
    pub now: DateTime<Utc>,
}

/// collect 結果：可能沒有新 body（304）。
#[derive(Debug, Clone)]
pub struct CollectResult {
    pub fetched: bool,
    pub evidence: Option<NewRawEvidence>,
    pub checkpoint: ConnectorCheckpoint,
}

/// discover 找到的項目（RSS 是 feed URL 本身）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoverItem {
    pub url: String,
    pub title: Option<String>,
}

/// parse 後的一筆（尚未正規化成 Document）。
#[derive(Debug, Clone, PartialEq)]
pub struct ParsedItem {
    pub external_id: Option<String>,
    pub url: Option<String>,
    pub title: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub summary: Option<String>,
    pub attributes: Value,
}

/// Connector 健康狀態。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorHealth {
    pub healthy: bool,
    pub message: String,
}

/// SPEC §6 執行契約。
///
/// 規格只列資料欄位，沒有方法簽名。這裡定的語意：
/// - `discover`：從 Source 找出要抓的 URL（RSS 通常就是 `base_url`）
/// - `collect`：抓取（含 SSRF／rate limit／checkpoint 條件請求）
/// - `parse`：把 body 解析成結構化項目（不寫 Document）
/// - `create_raw_evidence`：把 collect 的 body 交給 `EvidenceSink`
/// - `update_checkpoint`：回寫 checkpoint
/// - `health`：connector 自身是否可跑（不探活遠端）
#[async_trait]
pub trait ConnectorTrait: Send + Sync {
    fn connector_type(&self) -> &'static str;
    fn version(&self) -> &'static str;

    async fn discover(&self, source: &Source) -> Result<Vec<DiscoverItem>, ConnectorError>;

    async fn collect(&self, ctx: &CollectContext) -> Result<CollectResult, ConnectorError>;

    async fn parse(&self, body: &[u8]) -> Result<Vec<ParsedItem>, ConnectorError>;

    async fn create_raw_evidence(
        &self,
        evidence: NewRawEvidence,
    ) -> Result<RawEvidence, ConnectorError>;

    async fn update_checkpoint(
        &self,
        connector: &Connector,
        checkpoint: &ConnectorCheckpoint,
    ) -> Result<(), ConnectorError>;

    async fn health(&self) -> ConnectorHealth;
}
