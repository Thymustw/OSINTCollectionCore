//! Capability traits。每個 trait 對應一種真實能力，不要合成巨型萬能介面。

use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{
    AiRun, AiRunId, Candidate, CandidateEvidence, CandidateEvidenceId, CandidateId,
    CandidateStatus, Collection, CollectionId, Connector, ConnectorId, Document, DocumentId,
    DocumentType, DuplicateGroup, DuplicateGroupId, Embedding, EmbeddingTarget, Entity,
    EntityAlias, EntityAliasId, EntityExtraction, EntityExtractionId, EntityId, EntityIdentifier,
    EntityIdentifierId, EntityType, Event, EventId, FailedEvent, FailedEventId, Job, JobId,
    JobStatus, MergeHistory, MergeHistoryId, NetworkRule, NetworkRuleId, ObjectId, Provenance,
    ProvenanceId, RawEvidence, RawEvidenceId, Relationship, RelationshipEvidence,
    RelationshipEvidenceId, RelationshipId, RelationshipType, ResolutionCandidate,
    ResolutionCandidateId, ResolutionStatus, Seed, SeedId, Source, SourceId,
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
    /// 反向查詢：哪些 Entity 擁有這個 alias 文字（**精確比對**，不做大小寫／正規化折疊——
    /// 理由同 [`RelationalStore::find_entity_by_normalized_name`]：折疊放進 SQL 會走不到索引，
    /// 兩個 backend 的 collation 規則也不同）。用 `idx_entity_aliases_alias` 索引。
    /// 是 SPEC §6「alias」這條 resolution method 的資料來源。`limit` 夾在 1..=100。
    async fn find_entity_aliases_by_text(
        &self,
        alias: &str,
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
    /// 反向查詢：`(namespace, normalized_value)` 這個識別碼目前屬於哪個 Entity。
    /// `entity_identifiers` 有這兩欄的 UNIQUE constraint（`idx_entity_identifiers_natural`），
    /// 所以最多一筆——回 `Option`，不是 `Vec`。是 SPEC §6「exact identifier」／
    /// 「domain」／「email」／「external ID」等方法反查既有 owner 的資料來源。
    async fn find_entity_identifier_owner(
        &self,
        namespace: &str,
        normalized_value: &str,
    ) -> Result<Option<EntityIdentifier>, StorageError>;
    /// 反向查詢：所有 namespace 底下 `normalized_value` 相同的識別碼。
    ///
    /// 與 [`RelationalStore::find_entity_identifier_owner`] 不同：那條是精確
    /// `(namespace, normalized_value)`，最多一筆。這條**不限 namespace**，
    /// 給 SPEC §6 `account_handle` 找「同一個 handle 出現在不同平台」
    /// （`github_handle` vs `twitter_handle`）。過濾同 namespace／同
    /// `entity_id` 是呼叫端的責任。`limit` 夾在 1..=100，依 `id` 升序。
    async fn find_entity_identifiers_by_normalized_value(
        &self,
        normalized_value: &str,
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
    /// 這個 Entity 參與過的 resolution candidate，`entity_a_id` **或** `entity_b_id`
    /// 命中都算，可依 `status` 過濾（`None` = 不過濾），依 `id` 遞減、cursor 分頁。
    ///
    /// 兩端合成一個方法的理由同 [`RelationalStore::list_relationships_by_object`]：
    /// Console 的 Entity 頁要問「這個 Entity 跟誰可能是同一個」，拆成兩個方法
    /// 只會讓每個呼叫端各查一次再自己合併。過濾放在 SQL 裡，理由同
    /// [`RelationalStore::list_resolution_candidates`]。
    async fn list_resolution_candidates_by_entity(
        &self,
        entity_id: EntityId,
        status: Option<ResolutionStatus>,
        after: Option<ResolutionCandidateId>,
        limit: u32,
    ) -> Result<Vec<ResolutionCandidate>, StorageError>;

    /// 更新一筆 resolution candidate 的審核狀態與 `reviewed_at`。
    ///
    /// 這個方法同時服務兩種呼叫端：ADR-012 的自動核准（`Pending` → `AutoConfirmed`）、
    /// 以及未來人工 Review API（`Pending` → `Confirmed`/`Rejected`，目前還沒有這支
    /// API，但方法先做成通用的）。
    ///
    /// 回傳 `true` 代表真的更新到一列；`false` 代表 `id` 不存在——比照
    /// [`RelationalStore::mark_replayed`] 的慣例回布林值而不是 `NotFound` 錯誤，
    /// 因為呼叫端（自動核准流程）在呼叫這個方法之前一定已經讀過這筆候選，
    /// `false` 只會發生在真正異常的競爭情況，讓呼叫端自己決定要不要當錯誤處理。
    async fn update_resolution_candidate_status(
        &self,
        id: ResolutionCandidateId,
        status: ResolutionStatus,
        reviewed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

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

    // =========================================================================
    // ===== V0.2 Phase 3 Step 1：Embedding metadata =====
    //
    // Schema 在 `migrations/*/0010_v0_2_embeddings.sql`。**不含向量本體**。
    // =========================================================================

    /// 寫入一筆 Embedding metadata。**不是 upsert**。
    ///
    /// 撞到 `idx_embeddings_target_model_hash`（同一目標、同一模型、同一內容雜湊）
    /// 或主鍵時回 [`StorageError::Conflict`]。同一個 key 出現第二次代表呼叫端
    /// 邏輯有問題——應該先 [`RelationalStore::find_embedding`] 確認不存在才 `put`。
    async fn put_embedding(&self, embedding: &Embedding) -> Result<(), StorageError>;
    /// 查「這個目標、這個模型、這個內容雜湊」算過了沒——re-generate 判斷的
    /// 唯一入口，Step 3 的 embedding-worker 靠這個決定要不要重算。
    async fn find_embedding(
        &self,
        target_id: ObjectId,
        target_type: EmbeddingTarget,
        model: &str,
        content_hash: &str,
    ) -> Result<Option<Embedding>, StorageError>;
    /// 查一個目標的所有 embedding（不同模型／語言可能各留一筆）。
    /// `limit` 夾在 1..=100，依 `id` 升序。
    async fn list_embeddings_by_target(
        &self,
        target_id: ObjectId,
        target_type: EmbeddingTarget,
        limit: u32,
    ) -> Result<Vec<Embedding>, StorageError>;

    // =========================================================================
    // ===== V0.3 Phase 0：Discovery Foundation
    // ===== （Seed／Candidate／CandidateEvidence／AiRun）=====
    //
    // Schema 在 `migrations/*/0014_v0_3_discovery.sql`。core_model 型別定義見
    // `core-model/src/{seed,candidate,candidate_evidence,ai_run}.rs`。
    //
    // 沒有機械式套用 ResolutionCandidate 的五方法模板：CandidateEvidence 沒有
    // status 欄位（不需要 update_status），也沒有跨 candidate 瀏覽的產品需求
    // （只留 by_candidate 一種查法，仿 list_merge_history_by_entity：不接
    // cursor，只夾 limit）。AiRun 是一次呼叫結束後寫一次的終態紀錄，SPEC §4
    // 沒有狀態轉換也沒有可掛查詢的 FK（只留 put/get/list）。
    // =========================================================================

    /// 依主鍵 upsert 一筆 Seed（SPEC_V0.3 §2）。
    async fn put_seed(&self, seed: &Seed) -> Result<(), StorageError>;
    async fn get_seed(&self, id: SeedId) -> Result<Option<Seed>, StorageError>;
    /// 依 `id` 遞減、cursor 分頁列出 Seed，可依 `status` 過濾（`None` = 不過濾）。
    /// `status` 是自由字串（`Seed::status` 沒有封閉列舉），過濾**在 SQL 裡做**，
    /// 理由同 [`RelationalStore::list_resolution_candidates`]。
    async fn list_seeds(
        &self,
        status: Option<&str>,
        after: Option<SeedId>,
        limit: u32,
    ) -> Result<Vec<Seed>, StorageError>;
    /// 這個 Collection 底下的 Seed，可依 `status` 過濾，依 `id` 遞減、cursor 分頁。
    async fn list_seeds_by_collection(
        &self,
        collection_id: CollectionId,
        status: Option<&str>,
        after: Option<SeedId>,
        limit: u32,
    ) -> Result<Vec<Seed>, StorageError>;
    /// 更新一筆 Seed 的 `status`（自由字串）。回傳 `true` 代表真的改到一列，
    /// `false` 代表 `id` 不存在——比照 [`RelationalStore::update_resolution_candidate_status`]
    /// 的慣例。
    async fn update_seed_status(&self, id: SeedId, status: &str) -> Result<bool, StorageError>;

    /// 依主鍵 upsert 一筆 Candidate（SPEC_V0.3 §6）。
    async fn put_candidate(&self, candidate: &Candidate) -> Result<(), StorageError>;
    async fn get_candidate(&self, id: CandidateId) -> Result<Option<Candidate>, StorageError>;
    /// 依 `id` 遞減、cursor 分頁列出 Candidate，可依 `status` 過濾。
    async fn list_candidates(
        &self,
        status: Option<CandidateStatus>,
        after: Option<CandidateId>,
        limit: u32,
    ) -> Result<Vec<Candidate>, StorageError>;
    /// 這個 Collection 底下的 Candidate（`GET /collections/{id}/discovery`
    /// 之後會用到），可依 `status` 過濾，依 `id` 遞減、cursor 分頁。
    async fn list_candidates_by_collection(
        &self,
        collection_id: CollectionId,
        status: Option<CandidateStatus>,
        after: Option<CandidateId>,
        limit: u32,
    ) -> Result<Vec<Candidate>, StorageError>;
    /// 更新一筆 Candidate 的審核狀態與 `reviewed_at`
    /// （`POST /candidates/{id}/approve|reject` 之後會用到）。
    async fn update_candidate_status(
        &self,
        id: CandidateId,
        status: CandidateStatus,
        reviewed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    /// 依主鍵 upsert 一筆 Candidate Evidence（SPEC_V0.3 §7）。
    async fn put_candidate_evidence(
        &self,
        evidence: &CandidateEvidence,
    ) -> Result<(), StorageError>;
    async fn get_candidate_evidence(
        &self,
        id: CandidateEvidenceId,
    ) -> Result<Option<CandidateEvidence>, StorageError>;
    /// 這個 Candidate 的全部證據，回答「Why was this discovered?」
    /// （Acceptance C）。不接 cursor，依 `id` 遞減，`limit` 夾在 1..=100——
    /// 仿 [`RelationalStore::list_merge_history_by_entity`]。
    async fn list_candidate_evidence_by_candidate(
        &self,
        candidate_id: CandidateId,
        limit: u32,
    ) -> Result<Vec<CandidateEvidence>, StorageError>;

    /// 依主鍵 upsert 一筆 AI Run（SPEC_V0.3 §4）。AI output 是 derived data
    /// （CLAUDE.md §5）——這張表只記錄，不覆寫 source/raw。
    async fn put_ai_run(&self, run: &AiRun) -> Result<(), StorageError>;
    async fn get_ai_run(&self, id: AiRunId) -> Result<Option<AiRun>, StorageError>;
    /// 依 `id` 遞減、cursor 分頁列出 AI Run，可依 `task_type` 過濾
    /// （對到 `idx_ai_runs_task_type`）。
    async fn list_ai_runs(
        &self,
        task_type: Option<&str>,
        after: Option<AiRunId>,
        limit: u32,
    ) -> Result<Vec<AiRun>, StorageError>;
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

/// 能開啟跨表交易的 relational store（`STORAGE_ARCHITECTURE.md` §11）。
///
/// 需要「多張表要嘛全成功要嘛全失敗」的操作（V0.2 Entity Merge：
/// `entities` + `entity_aliases` + `entity_identifiers` + `relationships` + `merge_history`）
/// 一律走這裡，不要用「先寫 A 再寫 B，中間 crash 就算了」那種寫法。
///
/// # 為什麼回 `Box<dyn Transaction>` 而不是 `type Tx`
///
/// 關聯型別會讓 trait 失去 object safety：服務端只能寫
/// `Arc<dyn TransactionalStore<Tx = PostgresTransaction>>`——一寫下去就綁死後端，
/// 等於把 §9「不要讓 domain 認得具體 adapter」那條規則從介面層繞過去了。
/// 動態分派的成本是每次 `begin()` 一次配置，跟一次 `BEGIN` 的 round-trip 相比可以忽略。
#[async_trait]
pub trait TransactionalStore: RelationalStore {
    /// 開一個交易。回傳值被 drop 而沒有 commit 時**必須 rollback**
    /// （PostgreSQL／SQLite adapter 都由 sqlx 的 `Transaction::drop` 保證，
    /// conformance 的 `assert_transactional_contract` 會驗）。
    async fn begin(&self) -> Result<Box<dyn Transaction>, StorageError>;
}

/// 進行中的交易。
///
/// # 為什麼是 `store()` 而不是 `Transaction: RelationalStore`
///
/// 讓 `Transaction` 繼承 `RelationalStore` 對呼叫端比較順手（`tx.put_entity(..)`），
/// 但那會強迫每個 adapter **再寫一份 80 個方法的實作**——兩份 SQL 遲早分岔，
/// 而且分岔的那一份只在交易路徑上跑，最難被測到。
///
/// 現在的做法是交易 handle 內部持有**同一個** store 型別（只是把執行對象從連線池
/// 換成這條交易的連線），所以交易內外用的是同一份 SQL，結構上不可能分岔。
/// 代價是呼叫端要多寫一個 `.store()`：
///
/// ```ignore
/// let tx = store.begin().await?;
/// let db = tx.store();
/// db.put_entity(&survivor).await?;
/// db.put_merge_history(&history).await?;
/// tx.commit().await?;
/// ```
#[async_trait]
pub trait Transaction: Send {
    /// 交易內的關聯式操作。這個 handle 上的每次寫入都在同一條交易連線上，
    /// **commit 之前交易外看不到**。
    fn store(&self) -> &dyn RelationalStore;

    /// 提交。`self: Box<Self>` 是為了保留 object safety
    /// （by-value `self` 的方法不能出現在 trait object 上）。
    async fn commit(self: Box<Self>) -> Result<(), StorageError>;

    /// 明確回滾。**不呼叫也會回滾**（drop 時），這個方法是為了讓錯誤路徑
    /// 看得出意圖，以及讓回滾本身的失敗可以被回報。
    async fn rollback(self: Box<Self>) -> Result<(), StorageError>;
}

/// 要寫進 SearchStore 的一筆文件。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchDocument {
    pub index: String,
    pub id: String,
    pub body: Value,
}

/// k-NN 向量查詢請求。
///
/// 呼叫端依語言／模型自己選 [`Self::field`]（`embedding_en`／`embedding_multi`）。
/// trait 不做語言判斷——那是 [`EmbeddingProvider`] 的責任。兩個模型維度都是 384，
/// 但向量空間不相通，查錯欄位會得到無意義鄰居而且**不會報錯**。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VectorSearch {
    pub index: String,
    /// 要查哪個向量欄位。
    pub field: String,
    pub vector: Vec<f32>,
    /// 要回幾個最近鄰居。
    pub k: u32,
    /// 與 knn 一起送的結構化過濾條件。adapter 必須真的排除不符合的文件，
    /// 不能先取 k 再事後過濾——最近鄰全不符合時結果會變空。
    pub filters: Vec<SearchFilter>,
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

/// 搜尋投影的**寫入與查詢**。
///
/// 進度／lag／重建狀態不在這裡，在 [`ProjectionStore`]（V0.2 Phase 0f）。
/// 兩個 trait 由同一個 adapter 實作，但刻意分開：只讀搜尋的呼叫端
/// （core-api 的 `POST /search`）不需要也不該碰得到 `reset_projection`。
#[async_trait]
pub trait SearchStore: HealthProvider {
    async fn index(&self, document: SearchDocument) -> Result<(), StorageError>;
    async fn bulk_index(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError>;
    /// 批次部分更新／建立（`_update` bulk action，`doc_as_upsert=true`）。
    ///
    /// 跟 [`Self::update_fields`] 的關鍵差異：**這個方法允許 upsert**——文件不存在
    /// 就直接用整份 `body` 當新文件建立。`update_fields` 刻意不 upsert，是因為
    /// embedding-worker 的向量欄位邏輯上必須疊加在「已存在」的文件上；這個方法
    /// 是給 indexer 這類「本來就要負責建立文件」的寫入者用的：只覆寫呼叫端知道的
    /// 欄位，**不會動呼叫端不知道的欄位**（例如別的服務事後疊加上去的向量欄位）。
    /// 跟 [`Self::bulk_index`] 的差異：`bulk_index` 整份取代 `_source`，這個方法
    /// 只合併 `body` 裡列出的欄位，其餘既有欄位維持原樣。
    async fn bulk_upsert_fields(
        &self,
        documents: Vec<SearchDocument>,
    ) -> Result<BulkIndexResult, StorageError>;
    /// 原始查詢字串。**只給 conformance 與運維臨時查詢用**，不要接使用者輸入
    /// （理由見 [`SearchQuery`]）。
    async fn query(&self, query: SearchQuery) -> Result<SearchHits, StorageError>;
    /// 結構化搜尋。面向使用者的路徑走這個。
    async fn search(&self, query: StructuredSearch) -> Result<SearchHits, StorageError>;
    async fn delete(&self, index: &str, id: &str) -> Result<bool, StorageError>;

    /// 部分更新既有文件的欄位（`_update` API，`doc_as_upsert=false`）。
    ///
    /// **不會整份覆寫 `_source`**——[`Self::index`]／[`Self::bulk_index`] 才會那樣做。
    /// indexer 的寫入路徑是 [`Self::bulk_upsert_fields`]（部分合併 + upsert），
    /// 不是這兩個整份取代的方法。
    /// 用於 embedding-worker 事後補寫向量欄位：文件本體已由 indexer 寫入，
    /// 只需要疊加 `embedding_en`／`embedding_en_model_version` 等欄位；用
    /// `index()` 整份覆寫會把 title／body／entities 全部清空，因為 OpenSearch
    /// 的 index API 是取代整個 `_source`，不是合併。
    ///
    /// 文件不存在時回 [`StorageError::NotFound`]（不會憑空 upsert 出一份文件——
    /// 那份文件應該已經被 indexer 建過，不存在代表上游有問題，不該悄悄補一份殘缺的）。
    async fn update_fields(&self, index: &str, id: &str, fields: Value)
    -> Result<(), StorageError>;

    /// k-NN 向量查詢。
    async fn vector_search(&self, query: VectorSearch) -> Result<SearchHits, StorageError>;
}

// ---------------------------------------------------------------------------
// ProjectionStore（V0.2 Phase 0f）
// ---------------------------------------------------------------------------

/// 一個投影的進度標記。
///
/// `projection` 就是目標 index／graph 名（例 `"osint-documents"`），不是後端名——
/// 同一個後端可以同時承載多個投影，用後端名當鍵會讓它們互相覆寫。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCheckpoint {
    pub projection: String,
    /// 最後一筆成功寫入的**來源物件**的時間戳。lag 由此算。
    ///
    /// 「來源物件的時間」不是「投影寫入的時間」。寫入時間永遠是「剛剛」，
    /// 用它算 lag 會永遠得到接近 0——一個看起來很健康但毫無資訊的數字。
    pub last_source_at: Option<DateTime<Utc>>,
    /// 與 `last_source_at` **成對**的那一筆物件 id（不是本批最後一筆）。
    pub last_object_id: Option<ObjectId>,
    /// 累積寫入計數。rebuild（`reset_projection`）會重置。
    pub objects_written: u64,
    /// 這一列最後被更新的時間。
    pub updated_at: DateTime<Utc>,
}

impl ProjectionCheckpoint {
    /// 一個還沒寫過任何東西的 checkpoint。
    #[must_use]
    pub fn empty(projection: impl Into<String>, now: DateTime<Utc>) -> Self {
        Self {
            projection: projection.into(),
            last_source_at: None,
            last_object_id: None,
            objects_written: 0,
            updated_at: now,
        }
    }

    /// 併入一批新進度：`objects_written` **累加**，`last_source_at` **只前進不後退**。
    ///
    /// # 為什麼時間戳只能前進
    ///
    /// 重建是依 `id DESC`（最新在前）掃過來的，所以第二頁的來源時間戳比第一頁**舊**。
    /// 若直接覆寫，一次成功的 rebuild 結束後 checkpoint 會停在**最舊**那一頁的時間，
    /// lag 看起來像是落後好幾個月——而實際上投影是完整的。這不會報錯，
    /// 只會讓運維在對著一個假的落後數字找不存在的問題。
    ///
    /// `last_object_id` 跟著 `last_source_at` 一起換，兩者必須是同一筆物件；
    /// 分開更新會產生「時間是 A 的、id 是 B 的」這種對不起來的紀錄。
    pub fn advance(
        &mut self,
        source_at: Option<DateTime<Utc>>,
        object_id: Option<ObjectId>,
        written: u64,
        now: DateTime<Utc>,
    ) {
        self.objects_written = self.objects_written.saturating_add(written);
        self.updated_at = now;
        // 刻意不用 let-chain：workspace 的 rust-version 是 1.85，let-chain 要 1.88。
        if let Some(source_at) = source_at {
            if self
                .last_source_at
                .is_none_or(|current| source_at > current)
            {
                self.last_source_at = Some(source_at);
                self.last_object_id = object_id;
            }
        }
    }
}

/// 投影落後多久。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionLag {
    pub checkpoint: Option<ProjectionCheckpoint>,
    /// `now - last_source_at` 的秒數。
    ///
    /// **沒有 checkpoint（或 checkpoint 沒有來源時間戳）時是 `None`，不是 `0`。**
    /// 0 會被讀成「完全沒落後」，而實際狀況是「這個投影從來沒寫過東西」——
    /// 那是需要有人去看的狀態，不是健康狀態。
    pub lag_seconds: Option<i64>,
}

impl ProjectionLag {
    /// 由 checkpoint 算 lag。**adapter 一律呼叫這個**，不要各自算一遍——
    /// 「沒有 checkpoint 時回 0 還是 None」這種決定重複實作幾次就會分岔一次。
    #[must_use]
    pub fn from_checkpoint(checkpoint: Option<ProjectionCheckpoint>, now: DateTime<Utc>) -> Self {
        let lag_seconds = checkpoint
            .as_ref()
            .and_then(|cp| cp.last_source_at)
            .map(|at| (now - at).num_seconds());
        Self {
            checkpoint,
            lag_seconds,
        }
    }
}

/// 重建進行到哪裡。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RebuildState {
    /// 沒有重建紀錄，或上一次重建的紀錄已被清掉。
    #[default]
    Idle,
    Running,
    Completed,
    Failed,
}

/// 最近一次（或正在進行的）重建狀態。
///
/// 存在的理由是 SPEC_V0.2 §27 的 Operations Center 要回答「上次 reindex 是什麼時候、
/// 寫了幾筆、失敗了嗎」。這個資訊**只存在於投影端**：canonical store 不知道有人跑過重建。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebuildStatus {
    pub projection: String,
    pub state: RebuildState,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub scanned: u64,
    pub written: u64,
    pub failed: u64,
    /// 失敗原因。**寫入前必須過 [`StorageError::sanitize`]**：重建的錯誤訊息常常
    /// 含後端 URL 或 DSN，而這一列會被 Operations Center 顯示出來。
    pub last_error: Option<String>,
}

impl RebuildStatus {
    /// 「沒有任何重建紀錄」的狀態。[`ProjectionStore::rebuild_status`] 查不到記錄時回這個。
    #[must_use]
    pub fn idle(projection: impl Into<String>) -> Self {
        Self {
            projection: projection.into(),
            state: RebuildState::Idle,
            started_at: None,
            finished_at: None,
            scanned: 0,
            written: 0,
            failed: 0,
            last_error: None,
        }
    }
}

/// 投影的進度與重建狀態（`STORAGE_ARCHITECTURE.md` §12、SPEC_V0.2「Projection Storage
/// Contracts」）。OpenSearch 與（V0.2 之後的）Neo4j adapter 都實作它。
///
/// # 為什麼這裡沒有 upsert／delete
///
/// `STORAGE_ARCHITECTURE.md` §7 的示意寫了 `upsert_projection` / `delete_projection`，
/// 那一段明說是 illustrative。實際上「把一個物件寫進投影」已經由各後端自己的能力介面
/// 涵蓋了（搜尋投影是 [`SearchStore::index`]／[`SearchStore::bulk_index`]／
/// [`SearchStore::delete`]，圖投影會是 `GraphStore`）。再疊一套後端中立的
/// `ProjectionObject` 寫入介面，代價是**兩條寫入路徑**：
///
/// * bulk 的逐筆失敗（[`BulkFailure`]）、mapping 衝突、nested 欄位這些東西在中立介面裡
///   無處可放，只能退化成「成功／失敗」——那正是 [`BulkIndexResult`] 的註解在講的
///   靜默丟資料。
/// * 兩條路徑遲早分岔，而分岔的那一條只在其中一個呼叫端上跑，最難被測到。
///
/// 所以這個 trait 只管**狀態**：checkpoint、lag、rebuild 狀態、重置。
/// 「寫入」由 capability 專屬介面負責。差異已記在
/// `docs/architecture/STORAGE_ARCHITECTURE.md` §7 與 `docs/developer/storage-adapters.md`。
#[async_trait]
pub trait ProjectionStore: HealthProvider {
    /// 這個投影的 checkpoint。從來沒寫過的投影回 `None`（不是零值 checkpoint——
    /// 「沒寫過」與「寫過但來源時間戳是 epoch」必須分得出來）。
    async fn checkpoint(
        &self,
        projection: &str,
    ) -> Result<Option<ProjectionCheckpoint>, StorageError>;

    /// 覆寫 checkpoint。
    ///
    /// **累加與「只前進」的合併邏輯不在這裡**，由呼叫端先用
    /// [`ProjectionCheckpoint::advance`] 算好再寫；adapter 只負責存。
    /// 把合併放進 adapter 會讓每個後端各實作一次同一套規則。
    ///
    /// 寫入必須是「寫完就讀得到」（OpenSearch adapter 用 `refresh=true`；
    /// `wait_for` 也正確但會等滿一個 refresh 週期，實測數字見該 adapter 的註解）：
    /// 投影 worker 的下一批會先讀 checkpoint 再累加，讀到舊值等於計數永遠停在原地。
    async fn save_checkpoint(&self, checkpoint: &ProjectionCheckpoint) -> Result<(), StorageError>;

    /// `now - last_source_at`。`now` 由呼叫端傳入而不是 adapter 取 `Utc::now()`，
    /// 否則測試無法斷言，批次查詢多個投影時每個的基準時間也會不同。
    async fn projection_lag(
        &self,
        projection: &str,
        now: DateTime<Utc>,
    ) -> Result<ProjectionLag, StorageError>;

    /// 最近一次重建狀態。**沒有記錄時回 [`RebuildStatus::idle`]，不是 `Err`**：
    /// 「還沒有人跑過重建」是正常狀態，讓它變成錯誤會逼每個呼叫端去分辨
    /// 「查不到」與「真的壞了」，而那兩者在錯誤型別上長得一樣。
    async fn rebuild_status(&self, projection: &str) -> Result<RebuildStatus, StorageError>;

    /// 覆寫重建狀態。`last_error` 必須已經過 [`StorageError::sanitize`]。
    async fn set_rebuild_status(&self, status: &RebuildStatus) -> Result<(), StorageError>;

    /// 清掉這個投影的 checkpoint 與重建狀態（`--drop` 重建時用）。
    ///
    /// 不存在時也回 `Ok`（冪等）。**這是唯一會讓狀態消失的操作**——
    /// 刪掉投影本身（drop index）不會動到狀態，理由見
    /// `docs/developer/storage-adapters.md` 的「為什麼狀態放獨立 index」。
    async fn reset_projection(&self, projection: &str) -> Result<(), StorageError>;
}

// ---------------------------------------------------------------------------
// GraphStore（V0.2 Phase 0g）
// ---------------------------------------------------------------------------

/// 圖投影上的一個節點。
///
/// `entity_type` 用 `String` 而不是 [`core_model::EntityType`]：圖投影的節點
/// 不保證都來自那份封閉列舉（之後 STIX 匯入、文件節點都可能進來），
/// Neo4j label 本身也是字串。過濾條件 [`GraphTraversalOptions::entity_types`]
/// 同樣是字串，呼叫端自己負責與寫入時一致。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphNode {
    pub entity_id: EntityId,
    pub entity_type: String,
    pub display_name: String,
    pub attributes: Value,
}

/// 圖投影上的一條邊。對應 canonical 的 [`Relationship`]，但只帶投影查詢需要的欄位。
///
/// PostgreSQL 仍是 relationship truth（SPEC_V0.2 §8）；這裡是投影視圖，
/// 重建後必須能從 canonical 完整還原。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphEdge {
    pub relationship_id: RelationshipId,
    pub source: EntityId,
    pub target: EntityId,
    pub relationship_type: String,
    pub confidence: f64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// 圖遍歷的共用過濾條件。對齊 SPEC_V0.2 §9：one hop / multi hop、
/// relation type filter、entity type filter、confidence threshold、time range。
///
/// `max_hops` 沒有後端中立的上限，但**不可無界**——adapter 必須自己夾一個
/// 硬上限（mock 夾 32）。`u32::MAX` 丟進 Neo4j 會變成一次掃完整張圖。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphTraversalOptions {
    /// `1` = 一跳鄰居。`0` 表示不走邊（neighbors／relationships 回空）。
    pub max_hops: u32,
    pub relationship_types: Option<Vec<String>>,
    pub entity_types: Option<Vec<String>>,
    pub min_confidence: Option<f64>,
    /// 邊的觀測區間必須與這個閉區間**重疊**（`first_seen <= to && last_seen >= from`）。
    /// 用 `last_seen` 落在區間內會漏掉「很早就出現、一直活到現在」的邊。
    pub time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
}

impl GraphTraversalOptions {
    /// 一跳、不過濾。Graph API 的預設。
    #[must_use]
    pub fn one_hop() -> Self {
        Self {
            max_hops: 1,
            relationship_types: None,
            entity_types: None,
            min_confidence: None,
            time_range: None,
        }
    }
}

impl Default for GraphTraversalOptions {
    fn default() -> Self {
        Self::one_hop()
    }
}

/// `POST /graph/query` 的結構化查詢形狀。
///
/// # 為什麼不是字串
///
/// 理由同 [`SearchQuery`]：使用者輸入的 Cypher／Gremlin **永遠不能**進到後端。
/// 那條路徑等於把整張圖的任意讀（以及部分寫，視授權）交給呼叫端，
/// 而且每個 adapter 的查詢語言不同，domain 一旦依賴字串就再也換不了後端。
/// 呼叫端把意圖編成這棵樹，adapter 再翻成 Cypher／Gremlin。
///
/// 沒有「原始查詢字串」的後門方法。運維臨時查詢走 Neo4j Browser，不走這個 trait。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphQuery {
    /// 遍歷起點。空的回 [`StorageError::ConstraintViolation`]——「從哪裡開始」
    /// 是查詢的一部分，缺了就不是查詢。
    pub starts: Vec<EntityId>,
    pub pattern: GraphPattern,
    pub options: GraphTraversalOptions,
}

/// 結構化圖查詢要走的形狀。對齊 SPEC_V0.2 §9 的四種讀 API。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum GraphPattern {
    /// 從 `starts` 出發、最多 `max_hops` 跳的鄰居節點。每個結果 path 是
    /// `[start, …, neighbor]`。
    Neighbors,
    /// 同上，但呼叫端要的是邊上的資料而不是節點。每個結果 path 至少含一條邊。
    Relationships,
    /// 從 `starts` 的每一點走到 `to` 的最短路徑。找不到回空 vec，不是 `Err`。
    ShortestPath { to: EntityId },
    /// 有界散步：從 `starts` 出發、最多 `max_hops` 跳。`end` 有值時只保留
    /// 停在那些節點的 path。
    BoundedWalk { end: Option<Vec<EntityId>> },
}

/// 一條遍歷結果。`nodes[i] --edges[i]--> nodes[i+1]`，所以
/// `edges.len() + 1 == nodes.len()`（單節點、沒有邊的 path 也合法，
/// 例如 `shortest_path(a, a)`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GraphPath {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
}

/// 圖投影的寫入與查詢（SPEC_V0.2 §8／§9）。
///
/// PostgreSQL 仍是 relationship truth；Neo4j 只是投影。寫入面對應
/// graph-worker 同步進來的實體／關係，查詢面對應 Graph API 的五個端點
/// （rebuild 走 [`ProjectionStore`]，不在這裡）。
///
/// # 與 `ProjectionStore` 的關係
///
/// Neo4j adapter（V0.2 Phase 2 的 `storage-neo4j`）**通常也需要實作
/// [`ProjectionStore`]**（見 Phase 0f）：checkpoint／lag／rebuild 狀態
/// 是投影契約，跟圖遍歷是兩件事。同一個 adapter 實作兩個 trait，
/// 但呼叫端（core-api 的 `GET /graph/...`）不需要也不該碰得到
/// `reset_projection`。
///
/// # 邊的方向
///
/// `neighbors`／`relationships`／`shortest_path`／`query` 都把邊當**無向**
/// 來看——情報圖問「這個實體連到誰」時，incoming 與 outgoing 同等重要。
/// 過濾 `relationship_types` 看的是型別字串，不看方向。
#[async_trait]
pub trait GraphStore: HealthProvider {
    async fn upsert_node(&self, node: &GraphNode) -> Result<(), StorageError>;
    async fn upsert_edge(&self, edge: &GraphEdge) -> Result<(), StorageError>;
    /// 刪節點，並**連帶刪掉**所有以它為端點的邊。
    ///
    /// 不連帶刪的話，圖上會留下指向不存在節點的邊，shortest path 與
    /// rebuild 對帳都會 silently 算錯。不存在時也回 `Ok`（冪等）。
    async fn delete_node(&self, entity_id: &EntityId) -> Result<(), StorageError>;
    /// 刪單一邊。不存在時也回 `Ok`（冪等）。
    async fn delete_edge(&self, relationship_id: &RelationshipId) -> Result<(), StorageError>;
    async fn neighbors(
        &self,
        entity_id: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphNode>, StorageError>;
    async fn relationships(
        &self,
        entity_id: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Vec<GraphEdge>, StorageError>;
    /// 找不到路徑回 `Ok(None)`，不是 `Err`——「這兩點沒連上」是查詢結果，
    /// 不是儲存故障。起點或終點節點不存在也回 `None`（同一理由）。
    async fn shortest_path(
        &self,
        from: &EntityId,
        to: &EntityId,
        options: &GraphTraversalOptions,
    ) -> Result<Option<GraphPath>, StorageError>;
    /// `POST /graph/query` 對應的自由查詢面。吃 [`GraphQuery`]，
    /// **不吃**使用者原始 Cypher／Gremlin 字串。
    async fn query(&self, query: &GraphQuery) -> Result<Vec<GraphPath>, StorageError>;
    /// 刪掉所有 `:Entity` 節點與邊，**不碰** `:ProjectionState`（那是
    /// [`ProjectionStore::reset_projection`] 的事）。給 `--rebuild --drop` 用。
    ///
    /// Entity 在 Postgres 被刪之後，Neo4j 裡對應的節點不會自動消失；只有 upsert
    /// 沒有 diff-delete，所以 `--drop` 必須能真正從零開始。空圖也回 `Ok`（冪等）。
    async fn wipe(&self) -> Result<(), StorageError>;
}

// ---------------------------------------------------------------------------
// EmbeddingProvider（V0.2 Phase 0g）
// ---------------------------------------------------------------------------

/// 這段文字在非對稱 embedding 裡扮演哪一端。
///
/// e5 系列的模型卡要求查詢加 `query: `、被索引的文件加 `passage: `。
/// MiniLM **不加**前綴。要不要加、加哪個，是實作依選中的模型決定的——
/// 呼叫端只宣告意圖，不要自己拼前綴（拼錯或漏拼會靜默降低召回率，
/// 見 `docs/developer/embedding.md` §5.3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingKind {
    Query,
    Passage,
}

/// 一次 embedding 請求。
///
/// `language` 是 **BCP 47 主語言**（`en`／`zh`／`zh-Hant`），由**呼叫端**偵測後傳入。
/// 本 trait **不做語言偵測**——OpenSearch 沒有內建偵測 processor，偵測放在
/// Core（Rust）端；provider 只負責「這個語言用哪個模型」。
///
/// `None` 的語意是「語言未知」→ 走多語模型（目前是 e5），**不是**英文 MiniLM。
/// MiniLM 的中文檢索 top-1 只有 2/5（`embedding.md` §5.2），未知語言丟給它
/// 會靜默得到無意義的鄰居。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingRequest {
    pub text: String,
    pub kind: EmbeddingKind,
    pub language: Option<String>,
}

/// 目前會處理這個語言的模型識別。給呼叫端在**送出推論之前**決定
/// 向量要寫進哪個 k-NN 欄位／index。
///
/// # 向量空間不相通
///
/// MiniLM 與 e5 維度都是 384，mapping 可以共用，但**兩個模型的向量空間
/// 不相通**，混在同一個 k-NN 欄位會得到無意義的鄰居。呼叫端必須用
/// [`EmbeddingProvider::model_for`]（或回傳的 [`EmbeddingVector::model`]）
/// 決定目的地，不能假設「維度相同就能比」。k-NN 欄位要怎麼拆仍是
/// SPEC §14 的未決事項；本 trait 只保證「你一定問得到是哪個模型」。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingModelRef {
    /// 例如 `"huggingface/sentence-transformers/all-MiniLM-L6-v2"` 或
    /// `"intfloat/multilingual-e5-small-int8"`。
    pub model: String,
    /// 上游版本。ml-commons 的 `model_version` 欄位是它自己的遞增序號，
    /// **不是**上游版本（`embedding.md` §2：註冊 `1.0.2` 之後文件裡是 `"1"`）。
    /// 上游沒有可讀版本時，用內容雜湊（`model_content_hash_value`）頂替，
    /// 不要填 `"1"`——那會讓 re-generate 判斷永遠命中不了該換模型的紀錄。
    pub model_version: String,
    /// 這個模型產出的維度。不要假設等於 [`EmbeddingProvider::dimensions`]。
    pub dimensions: usize,
}

/// 一次推論的完整結果。欄位對齊 SPEC_V0.2 §11 Embedding record
/// （`model`／`model_version`／`dimensions`／`content_hash`）——必須夠寫進
/// 那筆 record，**不是**只回一個裸 `Vec<f32>`。
///
/// `content_hash` 是**原始文字**的 SHA-256（見 [`embedding_content_hash`]），
/// 不含前綴、不含模型名。re-generate 比的是「這段內容有沒有變」，
/// 把前綴或模型算進去會讓換前綴策略／換模型時所有 hash 一起失效，
/// 看起來像整庫內容都被改過。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingVector {
    pub model: String,
    pub model_version: String,
    pub dimensions: usize,
    pub content_hash: String,
    pub vector: Vec<f32>,
}

/// SPEC §11 `content_hash` 的**唯一**定義：SHA-256（小寫十六進位）打在
/// 原始文字的 UTF-8 bytes 上，不做正規化。
///
/// 放在這裡而不是各呼叫端自己算，理由同 [`core_model::content_hash`]：
/// 兩份實作遲早分岔，分岔之後 re-generate 會靜默比不中。
///
/// **不做** `normalize_content` 那種空白折疊——embedding 對空白敏感
/// （前綴後面的那個空格就是語意的一部分），正規化會讓「加不加前綴」
/// 與「原文有沒有變」纏在一起。
#[must_use]
pub fn embedding_content_hash(text: &str) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(text.as_bytes()))
}

/// Stage 5 與 embedding-worker 共用的 Redis key 前綴。帶版本號，改 JSON
/// 格式時換 `v2`，不要覆寫舊資料。
pub const EMBEDDING_CACHE_KEY_PREFIX: &str = "embedding-cache:v1:";

/// Stage 5 寫入、embedding-worker 讀取的 Redis key。
///
/// 兩邊各自用 [`embedding_content_hash`] 算出同一把 key，不互相傳遞。
/// key **不含模型名**：同一段文字在同一語言路由下兩個服務會用同一個模型；
/// 讀取端仍必須核對 `EmbeddingVector.model`，對不上就當 miss。
#[must_use]
pub fn embedding_cache_key(content_hash: &str) -> String {
    format!("{EMBEDDING_CACHE_KEY_PREFIX}{content_hash}")
}

/// 「文字 → 向量」的能力（SPEC_V0.2 §11–§14）。
///
/// 這不是資料庫 port。生產實作會包 OpenSearch ml-commons 的 `_predict`
/// （或之後換的 runtime）；Phase 1 的 semantic similarity 可先注入
/// [`crate::mock::MockEmbeddingProvider`]，或設成回
/// [`StorageError::UnsupportedCapability`] 測「還沒接語意」的路徑。
///
/// # 硬約束（不要在實作裡簡化掉）
///
/// 1. **per-language 路由**：英文走 MiniLM、中文／其他走 e5。呼叫
///    [`EmbeddingProvider::model_for`] 問，不要綁死一個 `model_id`。
/// 2. **維度可問、不可寫死**：兩個模型目前都是 384，但換模型就會變。
///    [`EmbeddingProvider::dimensions`] 回的是「這個 provider 目前的預設維度」，
///    [`EmbeddingProvider::dimensions_for`] 回指定語言那一個。
/// 3. **非對稱前綴**：[`EmbeddingKind`] 區分查詢與文件；實作依模型決定
///    加不加。MiniLM 不加，e5 加。
/// 4. **回傳必須能寫進 Embedding record**：見 [`EmbeddingVector`]。
/// 5. **向量空間不相通**：見 [`EmbeddingModelRef`]。
#[async_trait]
pub trait EmbeddingProvider: HealthProvider {
    /// 這個 provider 目前的預設維度（語言未知時會用的那個模型）。
    ///
    /// **不是**編譯期常數。呼叫端建 k-NN mapping 或配置向量欄位時問這裡，
    /// 不要寫 `384`。
    fn dimensions(&self) -> usize;

    /// 指定語言會用的模型的維度。`language` 語意同 [`EmbeddingRequest::language`]。
    fn dimensions_for(&self, language: Option<&str>) -> usize;

    /// 指定語言會用哪個模型。呼叫端在推論**之前**就要知道，才能決定
    /// 向量寫進哪個欄位／index。
    fn model_for(&self, language: Option<&str>) -> EmbeddingModelRef;

    async fn embed(&self, request: &EmbeddingRequest) -> Result<EmbeddingVector, StorageError>;
    async fn embed_batch(
        &self,
        requests: &[EmbeddingRequest],
    ) -> Result<Vec<EmbeddingVector>, StorageError>;
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_757_000_000 + secs, 0).unwrap()
    }

    #[test]
    fn lag_without_checkpoint_is_none_not_zero() {
        // 0 會被讀成「完全沒落後」。「從沒寫過」必須長得跟「剛寫過」不一樣，
        // 否則一個從來沒啟動過的投影在儀表板上看起來是健康的。
        let lag = ProjectionLag::from_checkpoint(None, ts(0));
        assert_eq!(lag.lag_seconds, None);
        assert!(lag.checkpoint.is_none());
    }

    #[test]
    fn lag_with_checkpoint_but_no_source_timestamp_is_also_none() {
        // 投影寫過東西、但來源物件沒有可用的時間戳。仍然算不出 lag。
        let lag = ProjectionLag::from_checkpoint(
            Some(ProjectionCheckpoint::empty("osint-documents", ts(0))),
            ts(600),
        );
        assert_eq!(lag.lag_seconds, None);
        assert!(lag.checkpoint.is_some(), "checkpoint 本身要照樣回傳");
    }

    #[test]
    fn lag_is_now_minus_last_source_at() {
        let mut cp = ProjectionCheckpoint::empty("osint-documents", ts(0));
        cp.advance(Some(ts(100)), Some(ObjectId::nil()), 1, ts(100));
        let lag = ProjectionLag::from_checkpoint(Some(cp), ts(460));
        assert_eq!(lag.lag_seconds, Some(360));
    }

    #[test]
    fn advance_accumulates_written_count() {
        let mut cp = ProjectionCheckpoint::empty("osint-documents", ts(0));
        cp.advance(Some(ts(10)), Some(ObjectId::nil()), 200, ts(10));
        cp.advance(Some(ts(20)), Some(ObjectId::nil()), 150, ts(20));
        assert_eq!(
            cp.objects_written, 350,
            "計數要累加，不是覆寫成最後一批的量"
        );
        assert_eq!(cp.updated_at, ts(20));
    }

    #[test]
    fn advance_never_moves_the_source_timestamp_backwards() {
        // rebuild 是 id DESC 掃過來的，第二頁比第一頁舊。覆寫的話 checkpoint 會停在
        // 最舊那一頁，lag 看起來像落後好幾個月，而投影其實是完整的。
        let newer = ObjectId::from_u128(2);
        let older = ObjectId::from_u128(1);
        let mut cp = ProjectionCheckpoint::empty("osint-documents", ts(0));
        cp.advance(Some(ts(1_000)), Some(newer), 100, ts(1_000));
        cp.advance(Some(ts(10)), Some(older), 100, ts(1_010));

        assert_eq!(cp.last_source_at, Some(ts(1_000)));
        assert_eq!(
            cp.last_object_id,
            Some(newer),
            "id 必須跟著 last_source_at 一起留在同一筆物件上"
        );
        assert_eq!(cp.objects_written, 200, "時間戳不前進，但計數照樣累加");
    }

    #[test]
    fn advance_without_a_source_timestamp_only_counts() {
        let mut cp = ProjectionCheckpoint::empty("osint-documents", ts(0));
        cp.advance(Some(ts(500)), Some(ObjectId::from_u128(7)), 1, ts(500));
        cp.advance(None, None, 3, ts(600));
        assert_eq!(cp.last_source_at, Some(ts(500)));
        assert_eq!(cp.last_object_id, Some(ObjectId::from_u128(7)));
        assert_eq!(cp.objects_written, 4);
    }

    #[test]
    fn rebuild_status_default_is_idle_with_no_history() {
        let status = RebuildStatus::idle("osint-documents");
        assert_eq!(status.state, RebuildState::Idle);
        assert_eq!(status.started_at, None);
        assert_eq!(status.finished_at, None);
        assert_eq!((status.scanned, status.written, status.failed), (0, 0, 0));
        assert_eq!(status.last_error, None);
        assert_eq!(RebuildState::default(), RebuildState::Idle);
    }

    #[test]
    fn rebuild_state_serialises_as_snake_case() {
        // 存進投影後端的是這個字串。改掉它等於讓既有的狀態列讀不回來。
        assert_eq!(
            serde_json::to_value(RebuildState::Completed).unwrap(),
            Value::String("completed".into())
        );
        assert_eq!(
            serde_json::from_value::<RebuildState>(Value::String("running".into())).unwrap(),
            RebuildState::Running
        );
    }

    #[test]
    fn embedding_cache_key_is_versioned_prefix_plus_hash() {
        let hash = embedding_content_hash("hello");
        assert_eq!(
            embedding_cache_key(&hash),
            format!("{EMBEDDING_CACHE_KEY_PREFIX}{hash}")
        );
        assert!(embedding_cache_key(&hash).starts_with("embedding-cache:v1:"));
    }
}
