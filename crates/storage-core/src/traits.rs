//! Capability traits。每個 trait 對應一種真實能力，不要合成巨型萬能介面。

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use core_model::{
    Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DuplicateGroup,
    DuplicateGroupId, Entity, EntityExtraction, EntityExtractionId, EntityId, Event, EventId, Job,
    JobId, NetworkRule, NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence,
    RawEvidenceId, Relationship, RelationshipEvidence, RelationshipEvidenceId, RelationshipId,
    Source, SourceId,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::StorageError;
use crate::health::HealthProvider;

/// 關聯式 CRUD（PostgreSQL canonical 與 SQLite embedded 共用語意，效能不要求相同）。
///
/// `put_*` 是依主鍵 upsert。`insert_raw_evidence` 只插入；同一 `id` 再寫必須失敗。
#[async_trait]
pub trait RelationalStore: HealthProvider {
    async fn put_source(&self, source: &Source) -> Result<(), StorageError>;
    async fn get_source(&self, id: SourceId) -> Result<Option<Source>, StorageError>;
    async fn delete_source(&self, id: SourceId) -> Result<bool, StorageError>;

    async fn put_network_rule(&self, rule: &NetworkRule) -> Result<(), StorageError>;
    async fn get_network_rule(
        &self,
        id: NetworkRuleId,
    ) -> Result<Option<NetworkRule>, StorageError>;
    async fn list_network_rules(
        &self,
        source_id: SourceId,
    ) -> Result<Vec<NetworkRule>, StorageError>;
    async fn delete_network_rule(&self, id: NetworkRuleId) -> Result<bool, StorageError>;

    async fn put_connector(&self, connector: &Connector) -> Result<(), StorageError>;
    async fn get_connector(&self, id: ConnectorId) -> Result<Option<Connector>, StorageError>;
    async fn delete_connector(&self, id: ConnectorId) -> Result<bool, StorageError>;
    /// `enabled = true` 的 connector，依 id 升序。collector 排程迴圈用。
    async fn list_enabled_connectors(&self) -> Result<Vec<Connector>, StorageError>;

    async fn put_collection(&self, collection: &Collection) -> Result<(), StorageError>;
    async fn get_collection(&self, id: CollectionId) -> Result<Option<Collection>, StorageError>;
    async fn delete_collection(&self, id: CollectionId) -> Result<bool, StorageError>;
    async fn link_collection_source(
        &self,
        collection_id: CollectionId,
        source_id: SourceId,
    ) -> Result<(), StorageError>;
    async fn link_collection_connector(
        &self,
        collection_id: CollectionId,
        connector_id: ConnectorId,
    ) -> Result<(), StorageError>;
    async fn link_collection_object(
        &self,
        collection_id: CollectionId,
        object_id: ObjectId,
    ) -> Result<(), StorageError>;

    /// Raw Evidence 寫入後不可變。重複主鍵回 `Conflict`，不可變成 update。
    async fn insert_raw_evidence(&self, evidence: &RawEvidence) -> Result<(), StorageError>;
    async fn get_raw_evidence(
        &self,
        id: RawEvidenceId,
    ) -> Result<Option<RawEvidence>, StorageError>;

    async fn put_document(&self, document: &Document) -> Result<(), StorageError>;
    async fn get_document(&self, id: DocumentId) -> Result<Option<Document>, StorageError>;
    async fn delete_document(&self, id: DocumentId) -> Result<bool, StorageError>;

    async fn put_entity(&self, entity: &Entity) -> Result<(), StorageError>;
    async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>, StorageError>;
    async fn delete_entity(&self, id: EntityId) -> Result<bool, StorageError>;

    async fn put_relationship(&self, relationship: &Relationship) -> Result<(), StorageError>;
    async fn get_relationship(
        &self,
        id: RelationshipId,
    ) -> Result<Option<Relationship>, StorageError>;
    async fn delete_relationship(&self, id: RelationshipId) -> Result<bool, StorageError>;

    async fn put_relationship_evidence(
        &self,
        evidence: &RelationshipEvidence,
    ) -> Result<(), StorageError>;
    async fn get_relationship_evidence(
        &self,
        id: RelationshipEvidenceId,
    ) -> Result<Option<RelationshipEvidence>, StorageError>;

    async fn put_event(&self, event: &Event) -> Result<(), StorageError>;
    async fn get_event(&self, id: EventId) -> Result<Option<Event>, StorageError>;
    async fn delete_event(&self, id: EventId) -> Result<bool, StorageError>;

    async fn put_provenance(&self, provenance: &Provenance) -> Result<(), StorageError>;
    async fn get_provenance(&self, id: ProvenanceId) -> Result<Option<Provenance>, StorageError>;
    /// 依 RawEvidence 查出溯源列。normalizer 用來判斷同一筆是否已正規化。
    async fn list_provenance_by_raw_evidence(
        &self,
        raw_evidence_id: RawEvidenceId,
    ) -> Result<Vec<Provenance>, StorageError>;

    async fn put_job(&self, job: &Job) -> Result<(), StorageError>;
    async fn get_job(&self, id: JobId) -> Result<Option<Job>, StorageError>;
    async fn delete_job(&self, id: JobId) -> Result<bool, StorageError>;
    /// 依 UUID v7 由新到舊列出。`after` 為上一頁最後一筆 id（嚴格小於）。
    /// `limit` 由呼叫端夾在 1..=100。
    async fn list_jobs(&self, after: Option<JobId>, limit: u32) -> Result<Vec<Job>, StorageError>;

    async fn put_duplicate_group(&self, group: &DuplicateGroup) -> Result<(), StorageError>;
    async fn get_duplicate_group(
        &self,
        id: DuplicateGroupId,
    ) -> Result<Option<DuplicateGroup>, StorageError>;

    async fn put_entity_extraction(
        &self,
        extraction: &EntityExtraction,
    ) -> Result<(), StorageError>;
    async fn get_entity_extraction(
        &self,
        id: EntityExtractionId,
    ) -> Result<Option<EntityExtraction>, StorageError>;
}

/// 生產環境 Core canonical store。V0.1 由 PostgreSQL adapter 實作。
///
/// 語意上比 `RelationalStore` 多了「這是 Core 真實來源」的契約；CRUD 沿用關聯式介面。
#[async_trait]
pub trait CanonicalStore: RelationalStore {
    /// 例如 `"postgres"`。
    fn canonical_backend_id(&self) -> &'static str;
}

/// 嵌入式／本機關聯式 store。V0.1 由 SQLite adapter 實作。
///
/// 不是高併發 Core canonical。
#[async_trait]
pub trait EmbeddedStore: RelationalStore {
    fn database_path(&self) -> &Path;
}

/// 要寫進 SearchStore 的一筆文件。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchDocument {
    pub index: String,
    pub id: String,
    pub body: Value,
}

/// 簡易全文查詢。V0.1 只支援 `query_string`；語法細節留給 adapter。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchQuery {
    pub index: String,
    pub query_string: String,
    pub from: u32,
    pub size: u32,
}

impl SearchQuery {
    #[must_use]
    pub fn new(index: impl Into<String>, query_string: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query_string: query_string.into(),
            from: 0,
            size: 10,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub id: String,
    pub score: Option<f64>,
    pub source: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHits {
    pub total: u64,
    pub hits: Vec<SearchHit>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkIndexResult {
    pub indexed: u32,
    pub errors: u32,
}

/// 搜尋投影。V0.1 不包含 rebuild/checkpoint（那是 V0.2 `ProjectionStore`）。
#[async_trait]
pub trait SearchStore: HealthProvider {
    async fn index(&self, document: SearchDocument) -> Result<(), StorageError>;
    async fn bulk_index(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError>;
    async fn query(&self, query: SearchQuery) -> Result<SearchHits, StorageError>;
    async fn delete(&self, index: &str, id: &str) -> Result<bool, StorageError>;
}

/// 快取／暫存鍵值。
#[async_trait]
pub trait KeyValueStore: HealthProvider {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    async fn set(&self, key: &str, value: &[u8]) -> Result<(), StorageError>;
    async fn set_ex(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError>;
    async fn del(&self, key: &str) -> Result<bool, StorageError>;
    async fn expire(&self, key: &str, ttl: Duration) -> Result<bool, StorageError>;
}

/// 物件儲存（Raw Evidence blob）。
#[async_trait]
pub trait ObjectStore: HealthProvider {
    async fn put(
        &self,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<(), StorageError>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError>;
    async fn delete(&self, key: &str) -> Result<bool, StorageError>;
    async fn exists(&self, key: &str) -> Result<bool, StorageError>;
}
