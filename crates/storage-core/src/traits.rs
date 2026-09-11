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
    /// 依 UUID v7 由新到舊列出。cursor 語意同 [`RelationalStore::list_jobs`]。
    async fn list_sources(
        &self,
        after: Option<SourceId>,
        limit: u32,
    ) -> Result<Vec<Source>, StorageError>;

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
    /// 全部 connector（含 `enabled = false`），依 `id` 遞減、cursor 分頁。
    /// 與 [`RelationalStore::list_enabled_connectors`] 的差別是不過濾 `enabled`，
    /// 排程迴圈不該用這個——它會把停用的 connector 也跑起來。
    ///
    /// ⚠️ 這裡**不能**說「最新的在前」。connector 的 id 不保證是 UUID v7：
    /// `POST /api/v1/import` 建立的 connector 用的是 UUID v5
    /// （`core-api::import` 以 source + format 推導，為了冪等），沒有時間序。
    /// 其他 list 方法（source／raw evidence／document／job）的 id 都是 v7，才有時間序。
    async fn list_connectors(
        &self,
        after: Option<ConnectorId>,
        limit: u32,
    ) -> Result<Vec<Connector>, StorageError>;

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
    /// 依 UUID v7 由新到舊列出。排序用 `id` 而不是 `retrieved_at`：cursor 是 id，
    /// 排序鍵與 cursor 必須是同一欄，否則同一 `retrieved_at` 的多筆會在翻頁時漏掉或重複。
    /// UUID v7 前 48 bit 是毫秒時間戳，實務上等同「最新的在前」。
    async fn list_raw_evidence(
        &self,
        after: Option<RawEvidenceId>,
        limit: u32,
    ) -> Result<Vec<RawEvidence>, StorageError>;
    /// 同 [`RelationalStore::list_raw_evidence`]，但只含指定 source。
    async fn list_raw_evidence_by_source(
        &self,
        source_id: SourceId,
        after: Option<RawEvidenceId>,
        limit: u32,
    ) -> Result<Vec<RawEvidence>, StorageError>;

    async fn put_document(&self, document: &Document) -> Result<(), StorageError>;
    async fn get_document(&self, id: DocumentId) -> Result<Option<Document>, StorageError>;
    async fn delete_document(&self, id: DocumentId) -> Result<bool, StorageError>;
    /// 依 UUID v7 由新到舊列出。排序鍵同樣是 `id` 而非 `observed_at`，理由見
    /// [`RelationalStore::list_raw_evidence`]。
    async fn list_documents(
        &self,
        after: Option<DocumentId>,
        limit: u32,
    ) -> Result<Vec<Document>, StorageError>;

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
    /// 依 subject（Document／Entity 等衍生物件）查出溯源列，依時間升序。
    /// `osint-cli documents show` 用來把「Document ← 哪個 processor ← 哪筆 RawEvidence」串起來。
    async fn list_provenance_by_subject(
        &self,
        subject_id: ObjectId,
    ) -> Result<Vec<Provenance>, StorageError>;

    async fn put_job(&self, job: &Job) -> Result<(), StorageError>;
    async fn get_job(&self, id: JobId) -> Result<Option<Job>, StorageError>;
    async fn delete_job(&self, id: JobId) -> Result<bool, StorageError>;
    /// 依 UUID v7 由新到舊列出。`after` 為上一頁最後一筆 id（嚴格小於）。
    /// `limit` 由呼叫端夾在 1..=100。
    async fn list_jobs(&self, after: Option<JobId>, limit: u32) -> Result<Vec<Job>, StorageError>;

    /// Dedup Stage 1 候選：`documents.external_key` 完全相同、且 `id` **嚴格小於 `before`**
    /// 的 Document，依 `id` 升序（最舊在前）。`limit` 由 adapter 夾在 1..=100，不可無界。
    ///
    /// 兩個約束都是刻意的：
    /// * **升序**與其他 `list_*`（降序）相反——dedup 要的是「誰先存在」，
    ///   UUID v7 最小的那筆就是 canonical 候選。
    /// * **只看比自己早的**不只是「排除自己」。若允許比對到更新的 Document，
    ///   事件亂序時可能出現 A 指向 B、B 指向 A 的 `duplicate_of` 環，
    ///   之後任何一次 canonical 解析都會無限繞下去。限制成單向（永遠指向更小的 id）
    ///   讓環在結構上不可能存在。代價是亂序抵達時可能漏判一組重複——
    ///   漏判可以事後重跑補回來，環不行。
    async fn find_document_ids_by_external_key(
        &self,
        external_key: &str,
        before: DocumentId,
        limit: u32,
    ) -> Result<Vec<DocumentId>, StorageError>;
    /// Dedup Stage 2 候選：`documents.canonical_url` 完全相同者。語意同
    /// [`RelationalStore::find_document_ids_by_external_key`]。
    ///
    /// ⚠️ 比對的是**已正規化**的 URL。呼叫端要自己先正規化再查，
    /// 直接丟原始 `source_url` 進來只會比到剛好沒有追蹤參數的那些。
    async fn find_document_ids_by_canonical_url(
        &self,
        canonical_url: &str,
        before: DocumentId,
        limit: u32,
    ) -> Result<Vec<DocumentId>, StorageError>;
    /// Dedup Stage 3 候選：`documents.normalized_content_hash` 完全相同者。語意同
    /// [`RelationalStore::find_document_ids_by_external_key`]。
    async fn find_document_ids_by_content_hash(
        &self,
        content_hash: &str,
        before: DocumentId,
        limit: u32,
    ) -> Result<Vec<DocumentId>, StorageError>;
    /// Dedup Stage 4 候選：SimHash fingerprint 與 `fingerprint` 的 Hamming 距離
    /// `<= max_distance` 的 Document。
    ///
    /// **契約（兩個 backend 必須一致）**：只掃描「`id` 嚴格小於 `before`、最近
    /// `scan_limit` 筆有 fingerprint 的 Document」，在這個範圍內回傳全部命中者，
    /// 依 `id` 升序（最舊在前）。`before` 的單向約束理由同
    /// [`RelationalStore::find_document_ids_by_external_key`]。
    /// 這是刻意的有界查詢——SimHash 沒有可走索引的等值條件，不設上限就等於每來一份
    /// Document 就全表掃一次。代價是**比 `scan_limit` 更舊的近似文件會漏掉**，
    /// 已知限制寫在 `docs/developer/deduplicator.md`。
    ///
    /// PostgreSQL 在 DB 端用 `bit_count((simhash # $1)::bit(64))` 過濾；
    /// SQLite 沒有 popcount 也沒有整數 XOR 運算子，改成取回掃描範圍後在程式端算距離。
    /// 兩者對外行為相同。
    async fn find_simhash_candidates(
        &self,
        fingerprint: i64,
        max_distance: u32,
        before: DocumentId,
        scan_limit: u32,
    ) -> Result<Vec<SimhashCandidate>, StorageError>;

    async fn put_duplicate_group(&self, group: &DuplicateGroup) -> Result<(), StorageError>;
    async fn get_duplicate_group(
        &self,
        id: DuplicateGroupId,
    ) -> Result<Option<DuplicateGroup>, StorageError>;
    /// 查「這份 Document 屬於哪個 duplicate group」。member 最多屬於一個 group
    /// （由 `idx_duplicate_groups_member_object` 保證）。
    async fn get_duplicate_group_by_member(
        &self,
        member_object_id: ObjectId,
    ) -> Result<Option<DuplicateGroup>, StorageError>;
    /// 查「這份 canonical 底下有哪些 duplicate」，依 `first_seen` 升序。`limit` 夾在 1..=100。
    async fn list_duplicate_groups_by_canonical(
        &self,
        canonical_object_id: ObjectId,
        limit: u32,
    ) -> Result<Vec<DuplicateGroup>, StorageError>;

    async fn put_entity_extraction(
        &self,
        extraction: &EntityExtraction,
    ) -> Result<(), StorageError>;
    async fn get_entity_extraction(
        &self,
        id: EntityExtractionId,
    ) -> Result<Option<EntityExtraction>, StorageError>;
}

/// Dedup Stage 4 的候選列。
///
/// 刻意**不回傳整份 `Document`**：候選查詢一次可能掃幾百筆，把 `body` 一起拉回來
/// 是白花的 I/O。呼叫端挑中 canonical 之後再 `get_document` 一次就好。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimhashCandidate {
    pub id: DocumentId,
    pub simhash: i64,
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
