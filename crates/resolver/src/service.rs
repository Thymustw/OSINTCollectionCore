//! [`ResolverService`]：對一個 Entity 跑目前已實作的 resolution method。

use chrono::Utc;
use core_model::{
    Entity, EntityId, EntityType, RESOLUTION_METHODS, ResolutionCandidate, ResolutionStatus,
};
use serde_json::json;
use storage_core::{EmbeddingProvider, GraphStore, RelationalStore, StorageError};
use tracing::info;
use uuid::Uuid;

use crate::error::ResolverError;
use crate::graph_context::check_graph_context;
use crate::identifier_methods::{check_alias, check_domain};
use crate::semantic::check_semantic_similarity;

/// SPEC §10 的 13 種 EntityType。新增變體時下面的 match 會編譯失敗，
/// 強迫 resolver 決定要不要把新型別納入 `normalized_name` 掃描。
pub const ALL_ENTITY_TYPES: [EntityType; 13] = [
    EntityType::Person,
    EntityType::Organization,
    EntityType::Account,
    EntityType::Domain,
    EntityType::Hostname,
    EntityType::Ip,
    EntityType::Url,
    EntityType::Email,
    EntityType::Vulnerability,
    EntityType::Software,
    EntityType::Repository,
    EntityType::Hash,
    EntityType::Location,
];

/// 新增 [`EntityType`] 變體時這個 match 會編譯失敗，強迫更新 [`ALL_ENTITY_TYPES`]。
const fn assert_entity_type_known(t: EntityType) {
    match t {
        EntityType::Person
        | EntityType::Organization
        | EntityType::Account
        | EntityType::Domain
        | EntityType::Hostname
        | EntityType::Ip
        | EntityType::Url
        | EntityType::Email
        | EntityType::Vulnerability
        | EntityType::Software
        | EntityType::Repository
        | EntityType::Hash
        | EntityType::Location => {}
    }
}

/// 跨 type、相同 `normalized_name` 的分數。
///
/// 同 type 同名在 UUID v5 自然鍵下去重後已經是同一個 Entity，走不到這裡。
/// 跨 type 同名（Person「acme」vs Organization「acme」）只是弱訊號，
/// 0.40 夠進 Review，遠不到自動合併。
pub const NORMALIZED_NAME_CROSS_TYPE_SCORE: f64 = 0.40;

/// `check_semantic_similarity` 的 cosine 門檻。
///
/// 先給合理預設，之後應該可設定，不是最終定案。
pub const SEMANTIC_SIMILARITY_THRESHOLD: f64 = 0.85;

/// `check_graph_context` 的 Jaccard 門檻。
///
/// 先給合理預設，之後應該可設定，不是最終定案。
pub const GRAPH_CONTEXT_THRESHOLD: f64 = 0.5;

const METHOD_NORMALIZED_NAME: &str = RESOLUTION_METHODS[1];

/// 對 Entity 跑 resolution method、把新候選寫進 store。
pub struct ResolverService<S: RelationalStore, E: EmbeddingProvider, G: GraphStore> {
    store: S,
    embedder: E,
    graph: G,
}

impl<S: RelationalStore, E: EmbeddingProvider, G: GraphStore> ResolverService<S, E, G> {
    #[must_use]
    pub fn new(store: S, embedder: E, graph: G) -> Self {
        Self {
            store,
            embedder,
            graph,
        }
    }

    /// 依序跑所有目前已實作的掃描方法，回傳**這次新寫入**的 candidate。
    ///
    /// 聚合順序：`normalized_name` → `alias` → `domain` → `semantic_similarity`
    /// → `graph_context`。某一筆候選已經存在（`StorageError::Conflict`）視為
    /// 已處理，記一行 log 後繼續，不讓整次 resolve 失敗。底層 storage 錯誤
    /// （含 semantic／graph 路徑）用 `?` 往上傳播，只有 persist 那一層吞 Conflict。
    pub async fn resolve_entity(
        &self,
        entity_id: EntityId,
    ) -> Result<Vec<ResolutionCandidate>, ResolverError> {
        let entity = self
            .store
            .get_entity(entity_id)
            .await?
            .ok_or(ResolverError::EntityNotFound { entity_id })?;

        let mut candidates = self.check_normalized_name(&entity).await?;
        candidates.extend(check_alias(&self.store, &entity).await?);
        candidates.extend(check_domain(&self.store, &entity).await?);
        candidates.extend(
            check_semantic_similarity(
                &self.store,
                &self.embedder,
                &entity,
                SEMANTIC_SIMILARITY_THRESHOLD,
            )
            .await?,
        );
        candidates
            .extend(check_graph_context(&self.graph, entity_id, GRAPH_CONTEXT_THRESHOLD).await?);

        let mut written = Vec::new();
        for candidate in candidates {
            if let Some(kept) = self.persist_candidate(candidate).await? {
                written.push(kept);
            }
        }
        Ok(written)
    }

    /// SPEC §6「normalized name」：對每個 EntityType 查相同 `normalized_name`。
    ///
    /// 命中且不是 entity 自己 → 建一筆 pending candidate
    /// （`method="normalized_name"`，跨 type 分數 0.40）。
    ///
    /// **跳過 entity 自己的 entity_type**：同 type 同名在既有 UUID v5 去重下
    /// 已經是同一個 Entity，不會走到這裡。仍做防禦性檢查——查到的
    /// `entity.id == 傳入的 entity.id` 也跳過，不要對自己產生 candidate。
    ///
    /// 這個方法**不寫入 store**，只組出候選；寫入由 [`Self::resolve_entity`]
    /// 負責，方便單測直接斷言組出來的內容。
    pub async fn check_normalized_name(
        &self,
        entity: &Entity,
    ) -> Result<Vec<ResolutionCandidate>, ResolverError> {
        let mut out = Vec::new();
        for entity_type in ALL_ENTITY_TYPES {
            assert_entity_type_known(entity_type);
            if entity_type == entity.entity_type {
                continue;
            }
            let Some(other) = self
                .store
                .find_entity_by_normalized_name(entity_type, &entity.normalized_name)
                .await?
            else {
                continue;
            };
            if other.id == entity.id {
                continue;
            }
            out.push(normalized_name_candidate(entity, &other));
        }
        Ok(out)
    }

    async fn persist_candidate(
        &self,
        candidate: ResolutionCandidate,
    ) -> Result<Option<ResolutionCandidate>, ResolverError> {
        match self.store.put_resolution_candidate(&candidate).await {
            Ok(()) => Ok(Some(candidate)),
            Err(StorageError::Conflict { message }) => {
                info!(
                    entity_a_id = %candidate.entity_a_id,
                    entity_b_id = %candidate.entity_b_id,
                    method = %candidate.method,
                    %message,
                    "resolution candidate 已存在，沿用既有列，不中斷本次 resolve"
                );
                Ok(None)
            }
            Err(err) => Err(ResolverError::Storage(err)),
        }
    }
}

fn normalized_name_candidate(entity: &Entity, other: &Entity) -> ResolutionCandidate {
    let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(entity.id, other.id);
    let (entity_a_type, entity_b_type) = if entity_a_id == entity.id {
        (entity.entity_type, other.entity_type)
    } else {
        (other.entity_type, entity.entity_type)
    };
    ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id,
        entity_b_id,
        score: NORMALIZED_NAME_CROSS_TYPE_SCORE,
        method: METHOD_NORMALIZED_NAME.to_string(),
        evidence: json!({
            "method": METHOD_NORMALIZED_NAME,
            "normalized_name": entity.normalized_name,
            "entity_a_type": entity_a_type,
            "entity_b_type": entity_b_type,
            "match_type": "cross_type_exact",
        }),
        status: ResolutionStatus::Pending,
        created_at: Utc::now(),
        reviewed_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use core_model::{
        Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DocumentType,
        DuplicateGroup, DuplicateGroupId, EntityAlias, EntityAliasId, EntityExtraction,
        EntityExtractionId, EntityIdentifier, EntityIdentifierId, Event, EventId, FailedEvent,
        FailedEventId, Job, JobId, JobStatus, MergeHistory, MergeHistoryId, NetworkRule,
        NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence, RawEvidenceId,
        Relationship, RelationshipEvidence, RelationshipEvidenceId, RelationshipId,
        RelationshipType, ResolutionCandidateId, Source, SourceId,
    };
    use serde_json::json;
    use storage_core::health::{HealthProvider, StorageHealth};
    use storage_core::mock::{MockEmbeddingProvider, MockGraphStore};
    use storage_core::traits::SimhashCandidate;

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 12, 8, 0, 0).unwrap()
    }

    fn entity(id: EntityId, entity_type: EntityType, normalized_name: &str) -> Entity {
        Entity {
            id,
            entity_type,
            name: normalized_name.into(),
            normalized_name: normalized_name.into(),
            description: None,
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
            merged_into: None,
            attributes: json!({}),
        }
    }

    /// resolver 單元測試用的最小 RelationalStore。
    ///
    /// 實作 `get_entity`／`find_entity_by_normalized_name`／
    /// `put_resolution_candidate`／alias 查詢／`list_relationships_by_object`；
    /// 其餘方法回 `UnsupportedCapability`。**不要**把這個當生產 adapter。
    struct MemoryStore {
        inner: Mutex<Inner>,
    }

    struct Inner {
        entities: HashMap<EntityId, Entity>,
        /// `(entity_type, normalized_name)` → Entity。同鍵後寫覆蓋。
        by_name: HashMap<(EntityType, String), Entity>,
        candidates: HashMap<(EntityId, EntityId, String), ResolutionCandidate>,
        aliases: Vec<EntityAlias>,
        relationships: Vec<Relationship>,
        queried_types: Vec<EntityType>,
        /// 下一次 `put_resolution_candidate` 強制回 Conflict，測完自動清掉。
        force_conflict: bool,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self {
                inner: Mutex::new(Inner {
                    entities: HashMap::new(),
                    by_name: HashMap::new(),
                    candidates: HashMap::new(),
                    aliases: Vec::new(),
                    relationships: Vec::new(),
                    queried_types: Vec::new(),
                    force_conflict: false,
                }),
            }
        }

        fn seed(&self, e: Entity) {
            let mut inner = self.inner.lock().expect("mutex");
            inner
                .by_name
                .insert((e.entity_type, e.normalized_name.clone()), e.clone());
            inner.entities.insert(e.id, e);
        }

        fn seed_alias(&self, row: EntityAlias) {
            self.inner.lock().expect("mutex").aliases.push(row);
        }

        fn force_next_put_conflict(&self) {
            self.inner.lock().expect("mutex").force_conflict = true;
        }

        fn skip_semantic_and_graph() -> (MockEmbeddingProvider, MockGraphStore) {
            (MockEmbeddingProvider::unsupported(), MockGraphStore::new())
        }

        fn queried_types(&self) -> Vec<EntityType> {
            self.inner.lock().expect("mutex").queried_types.clone()
        }

        fn unsupported<T>(capability: &'static str) -> Result<T, StorageError> {
            Err(StorageError::UnsupportedCapability {
                backend: "memory-resolver-test",
                capability,
            })
        }
    }

    fn alias(entity_id: EntityId, text: &str, confidence: f64) -> EntityAlias {
        EntityAlias {
            id: Uuid::now_v7(),
            entity_id,
            alias: text.into(),
            alias_type: "name".into(),
            source_id: None,
            confidence,
            first_seen: ts(),
            last_seen: ts(),
        }
    }

    fn svc(
        store: MemoryStore,
    ) -> ResolverService<MemoryStore, MockEmbeddingProvider, MockGraphStore> {
        let (embedder, graph) = MemoryStore::skip_semantic_and_graph();
        ResolverService::new(store, embedder, graph)
    }

    #[async_trait]
    impl HealthProvider for MemoryStore {
        async fn health(&self) -> Result<StorageHealth, StorageError> {
            Ok(StorageHealth::ok(
                "memory-resolver-test",
                "resolver 單元測試用記憶體 store",
            ))
        }
    }

    #[async_trait]
    impl RelationalStore for MemoryStore {
        async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>, StorageError> {
            Ok(self.inner.lock().expect("mutex").entities.get(&id).cloned())
        }

        async fn find_entity_by_normalized_name(
            &self,
            entity_type: EntityType,
            normalized_name: &str,
        ) -> Result<Option<Entity>, StorageError> {
            let mut inner = self.inner.lock().expect("mutex");
            inner.queried_types.push(entity_type);
            Ok(inner
                .by_name
                .get(&(entity_type, normalized_name.to_string()))
                .cloned())
        }

        async fn put_resolution_candidate(
            &self,
            candidate: &ResolutionCandidate,
        ) -> Result<(), StorageError> {
            if candidate.entity_a_id >= candidate.entity_b_id {
                return Err(StorageError::ConstraintViolation {
                    message: "entity_a_id 必須小於 entity_b_id".into(),
                });
            }
            let mut inner = self.inner.lock().expect("mutex");
            if inner.force_conflict {
                inner.force_conflict = false;
                return Err(StorageError::Conflict {
                    message: "測試注入：同一對同一方法已存在".into(),
                });
            }
            let key = (
                candidate.entity_a_id,
                candidate.entity_b_id,
                candidate.method.clone(),
            );
            if inner.candidates.contains_key(&key) {
                return Err(StorageError::Conflict {
                    message: "同一對同一方法已存在".into(),
                });
            }
            inner.candidates.insert(key, candidate.clone());
            Ok(())
        }

        async fn put_source(&self, _: &Source) -> Result<(), StorageError> {
            Self::unsupported("put_source")
        }
        async fn get_source(&self, _: SourceId) -> Result<Option<Source>, StorageError> {
            Self::unsupported("get_source")
        }
        async fn delete_source(&self, _: SourceId) -> Result<bool, StorageError> {
            Self::unsupported("delete_source")
        }
        async fn list_sources(
            &self,
            _: Option<SourceId>,
            _: u32,
        ) -> Result<Vec<Source>, StorageError> {
            Self::unsupported("list_sources")
        }
        async fn put_network_rule(&self, _: &NetworkRule) -> Result<(), StorageError> {
            Self::unsupported("put_network_rule")
        }
        async fn get_network_rule(
            &self,
            _: NetworkRuleId,
        ) -> Result<Option<NetworkRule>, StorageError> {
            Self::unsupported("get_network_rule")
        }
        async fn list_network_rules(&self, _: SourceId) -> Result<Vec<NetworkRule>, StorageError> {
            Self::unsupported("list_network_rules")
        }
        async fn delete_network_rule(&self, _: NetworkRuleId) -> Result<bool, StorageError> {
            Self::unsupported("delete_network_rule")
        }
        async fn put_connector(&self, _: &Connector) -> Result<(), StorageError> {
            Self::unsupported("put_connector")
        }
        async fn get_connector(&self, _: ConnectorId) -> Result<Option<Connector>, StorageError> {
            Self::unsupported("get_connector")
        }
        async fn delete_connector(&self, _: ConnectorId) -> Result<bool, StorageError> {
            Self::unsupported("delete_connector")
        }
        async fn list_enabled_connectors(&self) -> Result<Vec<Connector>, StorageError> {
            Self::unsupported("list_enabled_connectors")
        }
        async fn list_connectors(
            &self,
            _: Option<ConnectorId>,
            _: u32,
        ) -> Result<Vec<Connector>, StorageError> {
            Self::unsupported("list_connectors")
        }
        async fn list_connectors_by_enabled(
            &self,
            _: Option<bool>,
            _: Option<ConnectorId>,
            _: u32,
        ) -> Result<Vec<Connector>, StorageError> {
            Self::unsupported("list_connectors_by_enabled")
        }
        async fn put_collection(&self, _: &Collection) -> Result<(), StorageError> {
            Self::unsupported("put_collection")
        }
        async fn get_collection(
            &self,
            _: CollectionId,
        ) -> Result<Option<Collection>, StorageError> {
            Self::unsupported("get_collection")
        }
        async fn delete_collection(&self, _: CollectionId) -> Result<bool, StorageError> {
            Self::unsupported("delete_collection")
        }
        async fn list_collections(
            &self,
            _: Option<CollectionId>,
            _: u32,
        ) -> Result<Vec<Collection>, StorageError> {
            Self::unsupported("list_collections")
        }
        async fn link_collection_source(
            &self,
            _: CollectionId,
            _: SourceId,
        ) -> Result<(), StorageError> {
            Self::unsupported("link_collection_source")
        }
        async fn link_collection_connector(
            &self,
            _: CollectionId,
            _: ConnectorId,
        ) -> Result<(), StorageError> {
            Self::unsupported("link_collection_connector")
        }
        async fn link_collection_object(
            &self,
            _: CollectionId,
            _: ObjectId,
        ) -> Result<(), StorageError> {
            Self::unsupported("link_collection_object")
        }
        async fn list_collection_sources(
            &self,
            _: CollectionId,
            _: u32,
        ) -> Result<Vec<SourceId>, StorageError> {
            Self::unsupported("list_collection_sources")
        }
        async fn list_collection_connectors(
            &self,
            _: CollectionId,
            _: u32,
        ) -> Result<Vec<ConnectorId>, StorageError> {
            Self::unsupported("list_collection_connectors")
        }
        async fn list_collection_objects(
            &self,
            _: CollectionId,
            _: u32,
        ) -> Result<Vec<ObjectId>, StorageError> {
            Self::unsupported("list_collection_objects")
        }
        async fn insert_raw_evidence(&self, _: &RawEvidence) -> Result<(), StorageError> {
            Self::unsupported("insert_raw_evidence")
        }
        async fn get_raw_evidence(
            &self,
            _: RawEvidenceId,
        ) -> Result<Option<RawEvidence>, StorageError> {
            Self::unsupported("get_raw_evidence")
        }
        async fn list_raw_evidence(
            &self,
            _: Option<RawEvidenceId>,
            _: u32,
        ) -> Result<Vec<RawEvidence>, StorageError> {
            Self::unsupported("list_raw_evidence")
        }
        async fn list_raw_evidence_by_source(
            &self,
            _: SourceId,
            _: Option<RawEvidenceId>,
            _: u32,
        ) -> Result<Vec<RawEvidence>, StorageError> {
            Self::unsupported("list_raw_evidence_by_source")
        }
        async fn put_document(&self, _: &Document) -> Result<(), StorageError> {
            Self::unsupported("put_document")
        }
        async fn get_document(&self, _: DocumentId) -> Result<Option<Document>, StorageError> {
            Self::unsupported("get_document")
        }
        async fn delete_document(&self, _: DocumentId) -> Result<bool, StorageError> {
            Self::unsupported("delete_document")
        }
        async fn list_documents(
            &self,
            _: Option<DocumentId>,
            _: u32,
        ) -> Result<Vec<Document>, StorageError> {
            Self::unsupported("list_documents")
        }
        async fn list_documents_filtered(
            &self,
            _: Option<DocumentType>,
            _: bool,
            _: Option<DocumentId>,
            _: u32,
        ) -> Result<Vec<Document>, StorageError> {
            Self::unsupported("list_documents_filtered")
        }
        async fn put_entity(&self, _: &Entity) -> Result<(), StorageError> {
            Self::unsupported("put_entity")
        }
        async fn delete_entity(&self, _: EntityId) -> Result<bool, StorageError> {
            Self::unsupported("delete_entity")
        }
        async fn list_entities(
            &self,
            _: Option<EntityId>,
            _: u32,
        ) -> Result<Vec<Entity>, StorageError> {
            Self::unsupported("list_entities")
        }
        async fn list_entities_by_type(
            &self,
            _: Option<EntityType>,
            _: Option<EntityId>,
            _: u32,
        ) -> Result<Vec<Entity>, StorageError> {
            Self::unsupported("list_entities_by_type")
        }
        async fn put_relationship(&self, _: &Relationship) -> Result<(), StorageError> {
            Self::unsupported("put_relationship")
        }
        async fn get_relationship(
            &self,
            _: RelationshipId,
        ) -> Result<Option<Relationship>, StorageError> {
            Self::unsupported("get_relationship")
        }
        async fn delete_relationship(&self, _: RelationshipId) -> Result<bool, StorageError> {
            Self::unsupported("delete_relationship")
        }
        async fn list_relationships_by_object(
            &self,
            object_id: ObjectId,
            limit: u32,
        ) -> Result<Vec<Relationship>, StorageError> {
            let inner = self.inner.lock().expect("mutex");
            let mut items: Vec<Relationship> = inner
                .relationships
                .iter()
                .filter(|r| r.source_object_id == object_id || r.target_object_id == object_id)
                .cloned()
                .collect();
            items.sort_by_key(|a| std::cmp::Reverse(a.id));
            items.truncate(limit as usize);
            Ok(items)
        }
        async fn list_relationships(
            &self,
            _: Option<RelationshipId>,
            _: u32,
        ) -> Result<Vec<Relationship>, StorageError> {
            Self::unsupported("list_relationships")
        }
        async fn list_relationships_by_type(
            &self,
            _: Option<RelationshipType>,
            _: Option<RelationshipId>,
            _: u32,
        ) -> Result<Vec<Relationship>, StorageError> {
            Self::unsupported("list_relationships_by_type")
        }
        async fn put_relationship_evidence(
            &self,
            _: &RelationshipEvidence,
        ) -> Result<(), StorageError> {
            Self::unsupported("put_relationship_evidence")
        }
        async fn get_relationship_evidence(
            &self,
            _: RelationshipEvidenceId,
        ) -> Result<Option<RelationshipEvidence>, StorageError> {
            Self::unsupported("get_relationship_evidence")
        }
        async fn list_relationship_evidence(
            &self,
            _: RelationshipId,
            _: u32,
        ) -> Result<Vec<RelationshipEvidence>, StorageError> {
            Self::unsupported("list_relationship_evidence")
        }
        async fn put_event(&self, _: &Event) -> Result<(), StorageError> {
            Self::unsupported("put_event")
        }
        async fn get_event(&self, _: EventId) -> Result<Option<Event>, StorageError> {
            Self::unsupported("get_event")
        }
        async fn delete_event(&self, _: EventId) -> Result<bool, StorageError> {
            Self::unsupported("delete_event")
        }
        async fn list_events(
            &self,
            _: Option<EventId>,
            _: u32,
        ) -> Result<Vec<Event>, StorageError> {
            Self::unsupported("list_events")
        }
        async fn put_provenance(&self, _: &Provenance) -> Result<(), StorageError> {
            Self::unsupported("put_provenance")
        }
        async fn get_provenance(
            &self,
            _: ProvenanceId,
        ) -> Result<Option<Provenance>, StorageError> {
            Self::unsupported("get_provenance")
        }
        async fn list_provenance_by_raw_evidence(
            &self,
            _: RawEvidenceId,
        ) -> Result<Vec<Provenance>, StorageError> {
            Self::unsupported("list_provenance_by_raw_evidence")
        }
        async fn list_provenance_by_subject(
            &self,
            _: ObjectId,
        ) -> Result<Vec<Provenance>, StorageError> {
            Self::unsupported("list_provenance_by_subject")
        }
        async fn put_job(&self, _: &Job) -> Result<(), StorageError> {
            Self::unsupported("put_job")
        }
        async fn get_job(&self, _: JobId) -> Result<Option<Job>, StorageError> {
            Self::unsupported("get_job")
        }
        async fn delete_job(&self, _: JobId) -> Result<bool, StorageError> {
            Self::unsupported("delete_job")
        }
        async fn list_jobs(&self, _: Option<JobId>, _: u32) -> Result<Vec<Job>, StorageError> {
            Self::unsupported("list_jobs")
        }
        async fn list_jobs_by_status(
            &self,
            _: JobStatus,
            _: Option<JobId>,
            _: u32,
        ) -> Result<Vec<Job>, StorageError> {
            Self::unsupported("list_jobs_by_status")
        }
        async fn find_document_ids_by_external_key(
            &self,
            _: &str,
            _: DocumentId,
            _: u32,
        ) -> Result<Vec<DocumentId>, StorageError> {
            Self::unsupported("find_document_ids_by_external_key")
        }
        async fn find_document_ids_by_canonical_url(
            &self,
            _: &str,
            _: DocumentId,
            _: u32,
        ) -> Result<Vec<DocumentId>, StorageError> {
            Self::unsupported("find_document_ids_by_canonical_url")
        }
        async fn find_document_ids_by_content_hash(
            &self,
            _: &str,
            _: DocumentId,
            _: u32,
        ) -> Result<Vec<DocumentId>, StorageError> {
            Self::unsupported("find_document_ids_by_content_hash")
        }
        async fn find_simhash_candidates(
            &self,
            _: i64,
            _: u32,
            _: DocumentId,
            _: u32,
        ) -> Result<Vec<SimhashCandidate>, StorageError> {
            Self::unsupported("find_simhash_candidates")
        }
        async fn put_duplicate_group(&self, _: &DuplicateGroup) -> Result<(), StorageError> {
            Self::unsupported("put_duplicate_group")
        }
        async fn get_duplicate_group(
            &self,
            _: DuplicateGroupId,
        ) -> Result<Option<DuplicateGroup>, StorageError> {
            Self::unsupported("get_duplicate_group")
        }
        async fn get_duplicate_group_by_member(
            &self,
            _: ObjectId,
        ) -> Result<Option<DuplicateGroup>, StorageError> {
            Self::unsupported("get_duplicate_group_by_member")
        }
        async fn list_duplicate_groups_by_canonical(
            &self,
            _: ObjectId,
            _: u32,
        ) -> Result<Vec<DuplicateGroup>, StorageError> {
            Self::unsupported("list_duplicate_groups_by_canonical")
        }
        async fn put_entity_extraction(&self, _: &EntityExtraction) -> Result<(), StorageError> {
            Self::unsupported("put_entity_extraction")
        }
        async fn get_entity_extraction(
            &self,
            _: EntityExtractionId,
        ) -> Result<Option<EntityExtraction>, StorageError> {
            Self::unsupported("get_entity_extraction")
        }
        async fn list_entity_extractions_by_object(
            &self,
            _: ObjectId,
            _: u32,
        ) -> Result<Vec<EntityExtraction>, StorageError> {
            Self::unsupported("list_entity_extractions_by_object")
        }
        async fn list_entity_extractions_by_entity(
            &self,
            _: EntityId,
            _: u32,
        ) -> Result<Vec<EntityExtraction>, StorageError> {
            Self::unsupported("list_entity_extractions_by_entity")
        }
        async fn put_entity_alias(&self, _: &EntityAlias) -> Result<(), StorageError> {
            Self::unsupported("put_entity_alias")
        }
        async fn get_entity_alias(
            &self,
            _: EntityAliasId,
        ) -> Result<Option<EntityAlias>, StorageError> {
            Self::unsupported("get_entity_alias")
        }
        async fn list_entity_aliases_by_entity(
            &self,
            entity_id: EntityId,
            limit: u32,
        ) -> Result<Vec<EntityAlias>, StorageError> {
            let inner = self.inner.lock().expect("mutex");
            let mut items: Vec<EntityAlias> = inner
                .aliases
                .iter()
                .filter(|a| a.entity_id == entity_id)
                .cloned()
                .collect();
            items.sort_by_key(|a| a.id);
            items.truncate(limit as usize);
            Ok(items)
        }
        async fn find_entity_aliases_by_text(
            &self,
            alias: &str,
            limit: u32,
        ) -> Result<Vec<EntityAlias>, StorageError> {
            let inner = self.inner.lock().expect("mutex");
            let mut items: Vec<EntityAlias> = inner
                .aliases
                .iter()
                .filter(|a| a.alias == alias)
                .cloned()
                .collect();
            items.sort_by_key(|a| a.id);
            items.truncate(limit as usize);
            Ok(items)
        }
        async fn put_entity_identifier(&self, _: &EntityIdentifier) -> Result<(), StorageError> {
            Self::unsupported("put_entity_identifier")
        }
        async fn get_entity_identifier(
            &self,
            _: EntityIdentifierId,
        ) -> Result<Option<EntityIdentifier>, StorageError> {
            Self::unsupported("get_entity_identifier")
        }
        async fn list_entity_identifiers_by_entity(
            &self,
            _: EntityId,
            _: u32,
        ) -> Result<Vec<EntityIdentifier>, StorageError> {
            Self::unsupported("list_entity_identifiers_by_entity")
        }
        async fn find_entity_identifier_owner(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<EntityIdentifier>, StorageError> {
            Self::unsupported("find_entity_identifier_owner")
        }
        async fn get_resolution_candidate(
            &self,
            _: ResolutionCandidateId,
        ) -> Result<Option<ResolutionCandidate>, StorageError> {
            Self::unsupported("get_resolution_candidate")
        }
        async fn list_resolution_candidates(
            &self,
            _: Option<ResolutionStatus>,
            _: Option<ResolutionCandidateId>,
            _: u32,
        ) -> Result<Vec<ResolutionCandidate>, StorageError> {
            Self::unsupported("list_resolution_candidates")
        }
        async fn put_merge_history(&self, _: &MergeHistory) -> Result<(), StorageError> {
            Self::unsupported("put_merge_history")
        }
        async fn get_merge_history(
            &self,
            _: MergeHistoryId,
        ) -> Result<Option<MergeHistory>, StorageError> {
            Self::unsupported("get_merge_history")
        }
        async fn list_merge_history_by_entity(
            &self,
            _: EntityId,
            _: u32,
        ) -> Result<Vec<MergeHistory>, StorageError> {
            Self::unsupported("list_merge_history_by_entity")
        }
        async fn put_failed_event(&self, _: &FailedEvent) -> Result<FailedEvent, StorageError> {
            Self::unsupported("put_failed_event")
        }
        async fn get_failed_event(
            &self,
            _: FailedEventId,
        ) -> Result<Option<FailedEvent>, StorageError> {
            Self::unsupported("get_failed_event")
        }
        async fn list_failed_events(
            &self,
            _: Option<FailedEventId>,
            _: u32,
        ) -> Result<Vec<FailedEvent>, StorageError> {
            Self::unsupported("list_failed_events")
        }
        async fn mark_replayed(
            &self,
            _: FailedEventId,
            _: chrono::DateTime<Utc>,
        ) -> Result<bool, StorageError> {
            Self::unsupported("mark_replayed")
        }
    }

    #[tokio::test]
    async fn check_normalized_name_cross_type_hit() {
        let store = MemoryStore::new();
        let person_id = Uuid::from_u128(0x1111);
        let org_id = Uuid::from_u128(0x2222);
        let person = entity(person_id, EntityType::Person, "acme");
        let org = entity(org_id, EntityType::Organization, "acme");
        store.seed(person.clone());
        store.seed(org);
        let svc = svc(store);
        let hits = svc.check_normalized_name(&person).await.expect("check");
        assert_eq!(hits.len(), 1);
        let c = &hits[0];
        assert_eq!(c.entity_a_id, person_id);
        assert_eq!(c.entity_b_id, org_id);
        assert!(c.entity_a_id < c.entity_b_id);
        assert_eq!(c.score, NORMALIZED_NAME_CROSS_TYPE_SCORE);
        assert_eq!(c.method, "normalized_name");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "normalized_name");
        assert_eq!(c.evidence["normalized_name"], "acme");
        assert_eq!(c.evidence["match_type"], "cross_type_exact");
        assert_eq!(c.evidence["entity_a_type"], "person");
        assert_eq!(c.evidence["entity_b_type"], "organization");
    }

    #[tokio::test]
    async fn check_normalized_name_no_other_type_hit_returns_empty() {
        let store = MemoryStore::new();
        let person = entity(Uuid::from_u128(1), EntityType::Person, "lonely");
        store.seed(person.clone());
        let svc = svc(store);
        let hits = svc.check_normalized_name(&person).await.expect("check");
        assert!(hits.is_empty(), "{hits:?}");
    }

    #[tokio::test]
    async fn check_normalized_name_skips_own_entity_type() {
        let store = MemoryStore::new();
        let person_id = Uuid::from_u128(0xaaaa);
        let twin_id = Uuid::from_u128(0xbbbb);
        let person = entity(person_id, EntityType::Person, "acme");
        // 同 type 同名的另一個 Entity——生產環境被自然鍵擋住，這裡刻意種進去，
        // 用來證明跳過是程式邏輯，不是「根本查不到第二個 Person」。
        let twin = entity(twin_id, EntityType::Person, "acme");
        store.seed(person.clone());
        store.seed(twin);
        let svc = svc(store);
        let hits = svc.check_normalized_name(&person).await.expect("check");
        assert!(hits.is_empty(), "同 type 不該產生 candidate，得到 {hits:?}");
        let queried = svc.store.queried_types();
        assert!(
            !queried.contains(&EntityType::Person),
            "不該對自己的 entity_type 發查詢，實際查了 {queried:?}"
        );
        assert_eq!(
            queried.len(),
            ALL_ENTITY_TYPES.len() - 1,
            "應掃過其餘 12 種 type，實際 {queried:?}"
        );
    }

    #[tokio::test]
    async fn resolve_entity_conflict_does_not_abort() {
        let store = MemoryStore::new();
        let person_id = Uuid::from_u128(0x1111);
        let org_id = Uuid::from_u128(0x2222);
        let loc_id = Uuid::from_u128(0x3333);
        let person = entity(person_id, EntityType::Person, "acme");
        store.seed(person);
        store.seed(entity(org_id, EntityType::Organization, "acme"));
        store.seed(entity(loc_id, EntityType::Location, "acme"));
        // ALL_ENTITY_TYPES 裡 Organization 在 Location 前面，第一筆會 Conflict。
        store.force_next_put_conflict();
        let svc = svc(store);
        let written = svc.resolve_entity(person_id).await.expect("resolve");
        assert_eq!(
            written.len(),
            1,
            "第一筆 Conflict 不該讓第二筆（Location）消失：{written:?}"
        );
        assert_eq!(written[0].entity_b_id, loc_id);
        assert_eq!(written[0].method, "normalized_name");
    }

    #[tokio::test]
    async fn resolve_entity_missing_is_error() {
        let store = MemoryStore::new();
        let svc = svc(store);
        let missing = Uuid::from_u128(0xdead);
        let err = svc.resolve_entity(missing).await.expect_err("missing");
        match err {
            ResolverError::EntityNotFound { entity_id } => assert_eq!(entity_id, missing),
            other => panic!("預期 EntityNotFound，得到 {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_entity_aggregates_normalized_name_and_alias() {
        let store = MemoryStore::new();
        let person_id = Uuid::from_u128(0x1111);
        let org_id = Uuid::from_u128(0x2222);
        let person = entity(person_id, EntityType::Person, "acme");
        let org = entity(org_id, EntityType::Organization, "acme");
        store.seed(person);
        store.seed(org);
        store.seed_alias(alias(person_id, "微軟", 0.9));
        store.seed_alias(alias(org_id, "微軟", 0.7));
        let svc = svc(store);
        let written = svc.resolve_entity(person_id).await.expect("resolve");
        let methods: Vec<&str> = written.iter().map(|c| c.method.as_str()).collect();
        assert!(
            methods.contains(&"normalized_name"),
            "跨 type 同名應命中 normalized_name，實際 {written:?}"
        );
        assert!(
            methods.contains(&"alias"),
            "共用別名應命中 alias，實際 {written:?}"
        );
        assert_eq!(
            written
                .iter()
                .filter(|c| c.method == "normalized_name" || c.method == "alias")
                .count(),
            2,
            "兩種方法各應寫入一筆，實際 {written:?}"
        );
        for c in &written {
            assert_eq!(c.entity_a_id, person_id);
            assert_eq!(c.entity_b_id, org_id);
            assert_eq!(c.status, ResolutionStatus::Pending);
        }
    }
}
