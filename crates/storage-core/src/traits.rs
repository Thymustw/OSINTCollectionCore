//! Capability traits。每個 trait 對應一種真實能力，不要合成巨型萬能介面。

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{
    Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DocumentType,
    DuplicateGroup, DuplicateGroupId, Entity, EntityAlias, EntityAliasId, EntityExtraction,
    EntityExtractionId, EntityId, EntityIdentifier, EntityIdentifierId, EntityType, Event, EventId,
    FailedEvent, FailedEventId, Job, JobId, JobStatus, MergeHistory, MergeHistoryId, NetworkRule,
    NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence, RawEvidenceId, Relationship,
    RelationshipEvidence, RelationshipEvidenceId, RelationshipId, RelationshipType,
    ResolutionCandidate, ResolutionCandidateId, ResolutionStatus, Source, SourceId,
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
    /// 同 [`RelationalStore::list_connectors`]，但可依 `enabled` 過濾。
    /// `enabled = None` 等同不過濾。
    ///
    /// 過濾**必須在 SQL 裡做**，理由同 [`RelationalStore::list_jobs_by_status`]：
    /// 先取一頁再在程式端 filter，會讓「這個 source 沒有停用的 connector」與
    /// 「最新一頁裡沒有停用的 connector」變成同一個答案。
    async fn list_connectors_by_enabled(
        &self,
        enabled: Option<bool>,
        after: Option<ConnectorId>,
        limit: u32,
    ) -> Result<Vec<Connector>, StorageError>;

    async fn put_collection(&self, collection: &Collection) -> Result<(), StorageError>;
    async fn get_collection(&self, id: CollectionId) -> Result<Option<Collection>, StorageError>;
    async fn delete_collection(&self, id: CollectionId) -> Result<bool, StorageError>;
    /// 依 UUID v7 由新到舊列出。cursor 語意同 [`RelationalStore::list_jobs`]。
    async fn list_collections(
        &self,
        after: Option<CollectionId>,
        limit: u32,
    ) -> Result<Vec<Collection>, StorageError>;
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
    /// 這個 Collection 連到哪些 Source，依 `source_id` 升序，`limit` 夾在 1..=100。
    ///
    /// SPEC §7 的 Relations（sources／connectors／objects）在關聯表裡，
    /// 少了這三個反查就只能寫進去、讀不出來——`GET /api/v1/collections/{id}`
    /// 會永遠回空清單，而且不會有任何錯誤。
    async fn list_collection_sources(
        &self,
        collection_id: CollectionId,
        limit: u32,
    ) -> Result<Vec<SourceId>, StorageError>;
    /// 同 [`RelationalStore::list_collection_sources`]，回 Connector。
    async fn list_collection_connectors(
        &self,
        collection_id: CollectionId,
        limit: u32,
    ) -> Result<Vec<ConnectorId>, StorageError>;
    /// 同 [`RelationalStore::list_collection_sources`]，回 object（Document 等）。
    async fn list_collection_objects(
        &self,
        collection_id: CollectionId,
        limit: u32,
    ) -> Result<Vec<ObjectId>, StorageError>;

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
    /// 同 [`RelationalStore::list_documents`]，但可依 `object_type` 過濾，
    /// 並可選擇是否含重複文件（`duplicate_of IS NOT NULL` 的那些）。
    ///
    /// `GET /api/v1/objects` 的預設是**排除**重複：一份文章被十個站轉載時，
    /// 預設列表應該看到一筆而不是十一筆。要看全部就傳 `include_duplicates=true`。
    ///
    /// 兩個條件都在 SQL 裡做，理由同 [`RelationalStore::list_jobs_by_status`]：
    /// 取回一頁再在程式端濾掉重複，會讓「最近 20 筆剛好全是轉載」變成一個空頁，
    /// 使用者只會以為系統沒有資料。
    async fn list_documents_filtered(
        &self,
        object_type: Option<DocumentType>,
        include_duplicates: bool,
        after: Option<DocumentId>,
        limit: u32,
    ) -> Result<Vec<Document>, StorageError>;

    async fn put_entity(&self, entity: &Entity) -> Result<(), StorageError>;
    async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>, StorageError>;
    async fn delete_entity(&self, id: EntityId) -> Result<bool, StorageError>;
    /// 依 Entity 的**自然鍵** `(entity_type, normalized_name)` 查出既有 Entity。
    ///
    /// 這是 entity-worker 的去重依據：同一個 `CVE-2026-0001` 出現在一百篇文章裡，
    /// 應該對到**同一個** Entity（更新 `last_seen`），而不是建一百個。
    /// `0005_entity_natural_key.sql` 的 unique index 保證最多只會有一列。
    ///
    /// ⚠️ `normalized_name` 由呼叫端負責正規化（大小寫、IPv6 壓縮形式等），
    /// 這裡是**完全相等**比對，不做大小寫折疊——把折疊放進 SQL 會讓查詢走不到索引，
    /// 而且兩個 backend 的 collation 規則不同，等於在 PG 與 SQLite 上有兩種語意。
    async fn find_entity_by_normalized_name(
        &self,
        entity_type: EntityType,
        normalized_name: &str,
    ) -> Result<Option<Entity>, StorageError>;
    /// 依 `id` 遞減、cursor 分頁列出 Entity。`osint-cli entities list` 用。
    ///
    /// ⚠️ 這裡**不能**說「最新的在前」。Entity 的 id 是 UUID v5
    /// （entity-worker 由 `(entity_type, normalized_name)` 推導，為了冪等），沒有時間序。
    /// 要按時間看請自己比對 `last_seen`。理由同 [`RelationalStore::list_connectors`]。
    async fn list_entities(
        &self,
        after: Option<EntityId>,
        limit: u32,
    ) -> Result<Vec<Entity>, StorageError>;
    /// 同 [`RelationalStore::list_entities`]，但可依 `entity_type` 過濾
    /// （`None` 等同不過濾）。過濾在 SQL 裡做，理由同
    /// [`RelationalStore::list_documents_filtered`]。
    async fn list_entities_by_type(
        &self,
        entity_type: Option<EntityType>,
        after: Option<EntityId>,
        limit: u32,
    ) -> Result<Vec<Entity>, StorageError>;

    async fn put_relationship(&self, relationship: &Relationship) -> Result<(), StorageError>;
    async fn get_relationship(
        &self,
        id: RelationshipId,
    ) -> Result<Option<Relationship>, StorageError>;
    async fn delete_relationship(&self, id: RelationshipId) -> Result<bool, StorageError>;
    /// 列出「這個物件參與的」Relationship——`source_object_id` **或** `target_object_id`
    /// 命中都算，依 `id` 遞減，`limit` 夾在 1..=100。
    ///
    /// 刻意合成一個方法而不是分成 `by_source` / `by_target`：SPEC §26 Acceptance E 要從
    /// **Entity** 往回走（Entity 在 `mentions` 裡是 target），CLI 的 `documents show` 要從
    /// **Document** 往下走（Document 是 source）。拆成兩個方法只會讓每個呼叫端都得各查一次
    /// 再自己合併去重。`idx_relationships_source` 與 `idx_relationships_target` 兩個索引都在，
    /// PostgreSQL 會走 bitmap OR。
    async fn list_relationships_by_object(
        &self,
        object_id: ObjectId,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError>;
    /// 全部 Relationship，依 `id` 遞減、cursor 分頁。cursor 語意同
    /// [`RelationalStore::list_jobs`]。
    ///
    /// 與 [`RelationalStore::list_relationships_by_object`] 的差別是**不綁任何一端**：
    /// 那個方法回答「這個物件牽涉到什麼」，這個回答「系統裡有哪些邊」
    /// （Operations Center 的瀏覽、圖投影重建的來源）。
    ///
    /// ⚠️ 這裡**不能**說「最新的在前」。entity-worker 寫的 Relationship id 是 UUID v5
    /// （由 `(source, type, target)` 推導，為了冪等），沒有時間序。理由同
    /// [`RelationalStore::list_connectors`]；要按時間看請比對 `last_seen`。
    async fn list_relationships(
        &self,
        after: Option<RelationshipId>,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError>;
    /// 同 [`RelationalStore::list_relationships`]，但可依 `relationship_type` 過濾
    /// （`None` 等同不過濾）。過濾在 SQL 裡做，理由同
    /// [`RelationalStore::list_documents_filtered`]。
    async fn list_relationships_by_type(
        &self,
        relationship_type: Option<RelationshipType>,
        after: Option<RelationshipId>,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError>;

    async fn put_relationship_evidence(
        &self,
        evidence: &RelationshipEvidence,
    ) -> Result<(), StorageError>;
    async fn get_relationship_evidence(
        &self,
        id: RelationshipEvidenceId,
    ) -> Result<Option<RelationshipEvidence>, StorageError>;
    /// 一條 Relationship 的證據列，依 `created_at` 升序，`limit` 夾在 1..=100。
    ///
    /// **SPEC §12「任何 relationship 必須能回查 evidence」就是靠這個方法落地的。**
    /// 沒有它，`relationship_evidence` 只能用主鍵單筆取回——等於知道答案才查得到，
    /// 那條可追溯性要求形同虛設。
    async fn list_relationship_evidence(
        &self,
        relationship_id: RelationshipId,
        limit: u32,
    ) -> Result<Vec<RelationshipEvidence>, StorageError>;

    async fn put_event(&self, event: &Event) -> Result<(), StorageError>;
    async fn get_event(&self, id: EventId) -> Result<Option<Event>, StorageError>;
    async fn delete_event(&self, id: EventId) -> Result<bool, StorageError>;
    /// 依 UUID v7 由新到舊列出。cursor 語意同 [`RelationalStore::list_jobs`]。
    ///
    /// ⚠️ 這裡的 `Event` 是 SPEC §13 的**領域事件物件**（存在 `events` 表裡的
    /// 情報事件），不是 Redpanda 上的 `EventEnvelope`。兩者只是名字撞在一起。
    async fn list_events(
        &self,
        after: Option<EventId>,
        limit: u32,
    ) -> Result<Vec<Event>, StorageError>;

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
    /// 同 [`RelationalStore::list_jobs`]，但只含指定 `status`。
    ///
    /// 過濾**必須在 SQL 裡做**。先 `list_jobs` 再在程式端 filter 是錯的：
    /// 一頁只有 100 筆，佇列裡有一萬筆 queued 時，取回最新 100 筆再過濾出
    /// running 的，得到的可能是空頁——而「沒有 running 的 job」與
    /// 「最新 100 筆裡沒有 running 的 job」是完全不同的兩件事，
    /// 前者會讓運維以為佇列空了。
    async fn list_jobs_by_status(
        &self,
        status: JobStatus,
        after: Option<JobId>,
        limit: u32,
    ) -> Result<Vec<Job>, StorageError>;

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
    /// 某一份 Document（或其他 object）身上的 extraction 列，依 `id` 遞增，
    /// `limit` 夾在 1..=100。entity-worker 的冪等檢查與 `osint-cli documents show` 都用它。
    async fn list_entity_extractions_by_object(
        &self,
        object_id: ObjectId,
        limit: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError>;
    /// 某一個 Entity 被哪些 object 抽出過，依 `id` 遞增，`limit` 夾在 1..=100。
    /// `osint-cli entities show` 用它回答「這個 CVE 在哪幾篇文章出現」。
    async fn list_entity_extractions_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError>;

    // =========================================================================
    // ===== V0.2 =====
    //
    // Entity Resolution（SPEC_V0.2 §3／§4／§5／§7）與 ADR-008 的 failed_events。
    // Schema 在 `migrations/*/0007_v0_2_resolution_and_failed_events.sql`。
    //
    // ⚠️ V0.2 Phase 0 只有這層介面與 schema，**沒有任何服務會呼叫它們**
    //    （resolver / graph-worker / DLQ 重放都還沒做）。五張表是空的是預期結果。
    // =========================================================================

    /// 依主鍵 upsert 一筆 alias。
    ///
    /// 沒有唯一鍵擋重複的 `(entity_id, alias)`：同一個別名由兩個來源、
    /// 兩種信心度分別觀察到是正常的，理由見 migration 0007 的註解。
    async fn put_entity_alias(&self, alias: &EntityAlias) -> Result<(), StorageError>;
    async fn get_entity_alias(
        &self,
        id: EntityAliasId,
    ) -> Result<Option<EntityAlias>, StorageError>;
    /// 這個 Entity 的別名列，依 `id` 升序，`limit` 夾在 1..=100。
    /// 語意同 [`RelationalStore::list_entity_extractions_by_entity`]。
    async fn list_entity_aliases_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<EntityAlias>, StorageError>;

    /// 依主鍵 upsert 一筆識別碼。
    ///
    /// ⚠️ `(namespace, normalized_value)` 是 UNIQUE。**兩個不同 Entity 宣稱同一個
    /// 識別碼時，第二次寫入會回 [`StorageError::Conflict`]，不是靜默覆蓋。**
    /// 那個衝突正是 SPEC §6「exact identifier」要偵測的訊號——呼叫端應該據此
    /// 建立 resolution candidate，把它當雜訊吞掉的話識別碼會少記一筆而且毫無跡象。
    async fn put_entity_identifier(
        &self,
        identifier: &EntityIdentifier,
    ) -> Result<(), StorageError>;
    async fn get_entity_identifier(
        &self,
        id: EntityIdentifierId,
    ) -> Result<Option<EntityIdentifier>, StorageError>;
    /// 這個 Entity 的識別碼列，依 `id` 升序，`limit` 夾在 1..=100。
    async fn list_entity_identifiers_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<EntityIdentifier>, StorageError>;

    /// 依主鍵 upsert 一筆合併候選。
    ///
    /// ⚠️ 兩個前置條件由**資料庫**強制，兩者的錯誤型別不同：
    /// * `entity_a_id < entity_b_id`（CHECK constraint）→ 違反回
    ///   [`StorageError::ConstraintViolation`]。用
    ///   [`core_model::ResolutionCandidate::ordered_pair`] 排好再寫；候選對是無向的，
    ///   不排序就會讓同一對存成兩列。
    /// * `(entity_a_id, entity_b_id, method)` UNIQUE → 違反回
    ///   [`StorageError::Conflict`]。同一對用同一方法只有一筆；
    ///   **重跑 resolver 要沿用同一個 `id` 才是 upsert**，換 id 重寫會撞唯一鍵。
    async fn put_resolution_candidate(
        &self,
        candidate: &ResolutionCandidate,
    ) -> Result<(), StorageError>;
    async fn get_resolution_candidate(
        &self,
        id: ResolutionCandidateId,
    ) -> Result<Option<ResolutionCandidate>, StorageError>;
    /// 依 `id` 遞減、cursor 分頁列出候選，可依 `status` 過濾（`None` = 不過濾）。
    ///
    /// 過濾**在 SQL 裡做**，理由同 [`RelationalStore::list_jobs_by_status`]：
    /// Resolution Review 問的是「還有幾筆 pending」，取回最新一頁再在程式端 filter
    /// 會讓「沒有待審候選」與「最新 100 筆剛好都審完了」變成同一個答案。
    async fn list_resolution_candidates(
        &self,
        status: Option<ResolutionStatus>,
        after: Option<ResolutionCandidateId>,
        limit: u32,
    ) -> Result<Vec<ResolutionCandidate>, StorageError>;

    /// 依主鍵 upsert 一筆 merge 紀錄。
    ///
    /// `repointed_references` 空陣列代表「當時沒有任何列需要 repoint」。
    /// **不要用空陣列表示「沒記錄」**——那是資料遺失，會讓 undo 悄悄少還原一批參照。
    async fn put_merge_history(&self, history: &MergeHistory) -> Result<(), StorageError>;
    async fn get_merge_history(
        &self,
        id: MergeHistoryId,
    ) -> Result<Option<MergeHistory>, StorageError>;
    /// 這個 Entity 參與過的 merge，`survivor_id` **或** `merged_id` 命中都算，
    /// 依 `id` 遞減，`limit` 夾在 1..=100。
    ///
    /// 兩端合成一個方法的理由同 [`RelationalStore::list_relationships_by_object`]：
    /// Console 的 Entity Merge History 要問「這個 canonical 吃掉了誰」，
    /// API 收到舊 entity id 時要問「這個 id 被併去哪了」，拆成兩個方法只會讓
    /// 每個呼叫端各查一次再自己合併。
    ///
    /// 已撤銷的 merge（`undone_at` 非 NULL）**照樣回傳**。它是歷史的一部分，
    /// 過濾掉等於違反 Acceptance C；要不要顯示由呼叫端決定。
    async fn list_merge_history_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<MergeHistory>, StorageError>;

    /// 記錄一則永久失敗的事件（ADR-008）。回傳**資料庫裡實際存著的那一列**。
    ///
    /// 自然鍵是 `(topic, partition, offset)`，不是 `id`。同一則事件再次失敗時：
    ///
    /// | 欄位 | 行為 |
    /// |---|---|
    /// | `attempt_count` | **由資料庫 +1**，傳入值被忽略 |
    /// | `last_seen`／`failure_reason`／`consumer_group`／`envelope` | 以傳入值覆寫 |
    /// | `replayed_at` | 以傳入值覆寫。**傳 `None` 會清掉先前的重放時間**——
    ///   一則重放後又失敗的事件是「還沒修好」，不是「已重放」 |
    /// | `id`／`first_seen` | 保留**既有列**的值，傳入值被忽略 |
    ///
    /// 所以回傳的 `id` 可能不是你傳進去的那個。回傳整列而不是 `()` 就是為了這件事：
    /// 呼叫端要記 log 或之後重放時，拿著自己產生的 id 會查不到任何東西。
    async fn put_failed_event(&self, event: &FailedEvent) -> Result<FailedEvent, StorageError>;
    async fn get_failed_event(
        &self,
        id: FailedEventId,
    ) -> Result<Option<FailedEvent>, StorageError>;
    /// 依 UUID v7 由新到舊列出。cursor 語意同 [`RelationalStore::list_jobs`]。
    ///
    /// **含已重放的列。** `replayed_at` 非 NULL 的那些是「這則事件修好了」的紀錄，
    /// 刪掉或藏起來就沒有人能回答「上次那批到底補回去了沒」。
    async fn list_failed_events(
        &self,
        after: Option<FailedEventId>,
        limit: u32,
    ) -> Result<Vec<FailedEvent>, StorageError>;
    /// 標記一則 failed event 已重放成功。回傳是否真的更新到列。
    ///
    /// `replayed_at` 由呼叫端傳入而不是 adapter 自己取 `Utc::now()`：
    /// 重放時間應該是「重放動作發生的時間」，由發起端決定，
    /// adapter 內部取當下時間會讓測試無法斷言，也讓批次重放的時間戳散開。
    ///
    /// 已經標記過的列**照樣覆寫**成新時間並回 `true`——重放兩次是操作事實，
    /// 不是錯誤。要避免重複重放是呼叫端的冪等責任。
    async fn mark_replayed(
        &self,
        id: FailedEventId,
        replayed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;
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

/// 簡易全文查詢。語法細節留給 adapter，只給 conformance 與臨時查詢用。
///
/// ⚠️ **不要拿這個型別接使用者輸入**。`query_string` 會被 adapter 原樣交給後端的
/// 查詢語言（OpenSearch 是 `query_string`），使用者可以用 `欄位名:值`、`*`、`~`
/// 存取任意欄位或做 wildcard DoS。面向使用者的搜尋一律走 [`StructuredSearch`]——
/// 那條路徑沒有任何字串會進到查詢語言裡。
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
    /// 後端回傳的排序鍵。cursor pagination（OpenSearch 的 `search_after`）要原樣送回去。
    ///
    /// **不能用 hit 的順序或 `from` 偏移量代替。** `from/size` 深分頁在每個 shard 上都要
    /// 取回 `from + size` 筆再丟掉前面的，翻到第 1000 頁時等於每個 shard 排序 20000 筆；
    /// `search_after` 是「從這個排序鍵之後繼續」，成本與頁碼無關。
    #[serde(default)]
    pub sort: Vec<Value>,
    /// 命中片段（欄位名 → 片段列）。查詢沒要求 highlight 時是空的。
    #[serde(default)]
    pub highlights: std::collections::BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchHits {
    pub total: u64,
    pub hits: Vec<SearchHit>,
}

/// Bulk 寫入中單筆失敗的細節。
///
/// **存在的理由是「不要靜默丟資料」。** 只回一個 `errors: 3` 沒辦法重試，也沒辦法判斷
/// 是暫時性（429 佇列滿）還是永久性（mapping 衝突）——呼叫端只能整批重送或整批放棄。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkFailure {
    /// 失敗那筆的 document id。
    pub id: String,
    /// 後端回的 HTTP 狀態碼（OpenSearch bulk 每筆 item 都有）。
    pub status: u16,
    /// 後端回的原因字串（已經過 [`StorageError::sanitize`]）。
    pub reason: String,
}

impl BulkFailure {
    /// 這筆失敗值不值得重試。
    ///
    /// 429（佇列滿）／503（暫時不可用）／502／504 是暫時性的，退避後重送有意義。
    /// 400（mapping 衝突、欄位型別不符）重送一百次還是同樣的結果——那要進 DLQ 讓人看，
    /// 不是在迴圈裡一直重試。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self.status, 429 | 502 | 503 | 504)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkIndexResult {
    pub indexed: u32,
    pub errors: u32,
    /// 每一筆失敗的細節。長度應等於 `errors`。
    #[serde(default)]
    pub failures: Vec<BulkFailure>,
}

impl BulkIndexResult {
    #[must_use]
    pub fn empty() -> Self {
        Self {
            indexed: 0,
            errors: 0,
            failures: Vec::new(),
        }
    }
}

/// 全文條件的後端中立語法樹。
///
/// # 為什麼是 AST 而不是字串
///
/// 使用者輸入的字串**永遠不會**被交給後端的查詢語言。呼叫端先把它 parse 成這棵樹，
/// adapter 再把樹翻成後端查詢。這讓 injection 在結構上不可能發生：
/// `title:*` 只會變成一個 [`QueryExpr::Term`]，被當作要比對的文字，
/// 而不是「欄位 title 的萬用字元查詢」。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum QueryExpr {
    /// 單一詞彙。由 analyzer 斷詞後比對。
    Term(String),
    /// 片語。詞序必須相符。
    Phrase(String),
    /// 全部都要命中。
    And(Vec<QueryExpr>),
    /// 至少命中一個。
    Or(Vec<QueryExpr>),
    /// 不可命中。
    Not(Box<QueryExpr>),
}

/// 要查的全文欄位與權重。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchField {
    pub name: String,
    pub boost: f32,
}

impl SearchField {
    #[must_use]
    pub fn new(name: impl Into<String>, boost: f32) -> Self {
        Self {
            name: name.into(),
            boost,
        }
    }
}

/// 結構化過濾條件。不計分，只縮小候選集合。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchFilter {
    /// `field` 精確等於 `value`（keyword 欄位）。
    Term { field: String, value: String },
    /// `field` 落在 `[from, to]`（含端點）。兩端都可省略。
    DateRange {
        field: String,
        from: Option<chrono::DateTime<chrono::Utc>>,
        to: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// 巢狀物件過濾：`path` 底下**同一個元素**要同時滿足 `terms` 的全部條件。
    ///
    /// 「同一個元素」是重點。entities 若用扁平的 keyword 陣列存，
    /// 查「type=vulnerability 且 name=example.com」會命中「有漏洞、也有網域」的文件——
    /// 兩個條件落在不同元素上。nested 才能保證是同一個 entity。
    Nested {
        path: String,
        terms: Vec<(String, String)>,
    },
    /// `field` 必須不存在（或為 null）。搜尋結果排除 duplicate 用。
    Missing { field: String },
}

/// 排序欄位。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    pub ascending: bool,
}

/// 後端中立的結構化搜尋請求。面向使用者的搜尋一律走這條路徑。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructuredSearch {
    pub index: String,
    /// 全文條件。`None` 代表只靠 `filters`（例如「列出這個 source 的全部文件」）。
    pub expression: Option<QueryExpr>,
    /// 全文條件要查哪些欄位。`expression` 是 `None` 時忽略。
    pub fields: Vec<SearchField>,
    pub filters: Vec<SearchFilter>,
    /// 要回幾筆。呼叫端負責夾上限，adapter 會再夾一次。
    pub size: u32,
    /// 上一頁最後一筆的 [`SearchHit::sort`]。`None` 代表第一頁。
    pub search_after: Option<Vec<Value>>,
    /// 排序鍵。**必須以一個唯一欄位收尾**（例如 document id），
    /// 否則排序值相同的文件在翻頁時會漏掉或重複。
    pub sort: Vec<SortField>,
    /// 要產生命中片段的欄位。空的代表不做 highlight。
    pub highlight_fields: Vec<String>,
}

/// 搜尋投影。V0.1 不包含 rebuild/checkpoint（那是 V0.2 `ProjectionStore`）。
#[async_trait]
pub trait SearchStore: HealthProvider {
    async fn index(&self, document: SearchDocument) -> Result<(), StorageError>;
    async fn bulk_index(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError>;
    /// 原始查詢字串。**只給 conformance 與運維臨時查詢用**，不要接使用者輸入
    /// （理由見 [`SearchQuery`]）。
    async fn query(&self, query: SearchQuery) -> Result<SearchHits, StorageError>;
    /// 結構化搜尋。面向使用者的路徑走這個。
    async fn search(&self, query: StructuredSearch) -> Result<SearchHits, StorageError>;
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
