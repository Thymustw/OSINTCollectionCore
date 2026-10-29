//! collector 單元測試用的記憶體 RelationalStore。
//!
//! 只實作 source／connector 的 get／put 與 `update_connector_run_state`；
//! 其餘方法回 `UnsupportedCapability`。`put_connector` 是整列覆寫，
//! `update_connector_run_state` 才是部分更新——用來抓「為了改執行狀態
//! 卻走 put_connector」的迴歸。

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Utc;
use core_model::*;
use storage_core::{
    ConnectorRunStateUpdate, HealthProvider, RelationalStore, SimhashCandidate, StorageError,
    StorageHealth,
};

#[derive(Clone, Default)]
pub struct MemoryConnectorStore {
    inner: Arc<Mutex<Inner>>,
    put_connector_calls: Arc<AtomicUsize>,
}

#[derive(Default)]
struct Inner {
    sources: HashMap<SourceId, Source>,
    connectors: HashMap<ConnectorId, Connector>,
}

impl MemoryConnectorStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn unused<T>() -> Result<T, StorageError> {
        Err(StorageError::UnsupportedCapability {
            backend: "collector-test",
            capability: "unused",
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|err| err.into_inner())
    }

    pub fn put_connector_call_count(&self) -> usize {
        self.put_connector_calls.load(Ordering::SeqCst)
    }

    pub fn seed_connector(&self, connector: Connector) {
        self.lock().connectors.insert(connector.id, connector);
    }

    #[allow(dead_code)]
    pub fn seed_source(&self, source: Source) {
        self.lock().sources.insert(source.id, source);
    }
}

#[async_trait]
impl HealthProvider for MemoryConnectorStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        Ok(StorageHealth::ok(
            "collector-test",
            "collector 單元測試用記憶體 store",
        ))
    }
}

#[async_trait]
impl RelationalStore for MemoryConnectorStore {
    async fn put_source(&self, source: &Source) -> Result<(), StorageError> {
        self.lock().sources.insert(source.id, source.clone());
        Ok(())
    }

    async fn get_source(&self, id: SourceId) -> Result<Option<Source>, StorageError> {
        Ok(self.lock().sources.get(&id).cloned())
    }

    async fn put_connector(&self, connector: &Connector) -> Result<(), StorageError> {
        self.put_connector_calls.fetch_add(1, Ordering::SeqCst);
        self.lock()
            .connectors
            .insert(connector.id, connector.clone());
        Ok(())
    }

    async fn get_connector(&self, id: ConnectorId) -> Result<Option<Connector>, StorageError> {
        Ok(self.lock().connectors.get(&id).cloned())
    }

    async fn update_connector_run_state(
        &self,
        id: ConnectorId,
        update: &ConnectorRunStateUpdate,
    ) -> Result<bool, StorageError> {
        update.validate()?;
        let mut inner = self.lock();
        let Some(connector) = inner.connectors.get_mut(&id) else {
            return Ok(false);
        };
        update.apply_to(connector);
        Ok(true)
    }

    async fn get_entity(&self, _: EntityId) -> Result<Option<Entity>, StorageError> {
        Self::unused()
    }
    async fn find_entity_by_normalized_name(
        &self,
        _: EntityType,
        _: &str,
    ) -> Result<Option<Entity>, StorageError> {
        Self::unused()
    }
    async fn put_resolution_candidate(&self, _: &ResolutionCandidate) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn delete_source(&self, _: SourceId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_sources(&self, _: Option<SourceId>, _: u32) -> Result<Vec<Source>, StorageError> {
        Self::unused()
    }
    async fn put_network_rule(&self, _: &NetworkRule) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_network_rule(
        &self,
        _: NetworkRuleId,
    ) -> Result<Option<NetworkRule>, StorageError> {
        Self::unused()
    }
    async fn list_network_rules(&self, _: SourceId) -> Result<Vec<NetworkRule>, StorageError> {
        Self::unused()
    }
    async fn delete_network_rule(&self, _: NetworkRuleId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn delete_connector(&self, _: ConnectorId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_enabled_connectors(&self) -> Result<Vec<Connector>, StorageError> {
        Self::unused()
    }
    async fn list_connectors(
        &self,
        _: Option<ConnectorId>,
        _: u32,
    ) -> Result<Vec<Connector>, StorageError> {
        Self::unused()
    }
    async fn list_connectors_by_enabled(
        &self,
        _: Option<bool>,
        _: Option<ConnectorId>,
        _: u32,
    ) -> Result<Vec<Connector>, StorageError> {
        Self::unused()
    }
    async fn put_collection(&self, _: &Collection) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_collection(&self, _: CollectionId) -> Result<Option<Collection>, StorageError> {
        Self::unused()
    }
    async fn delete_collection(&self, _: CollectionId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_collections(
        &self,
        _: Option<CollectionId>,
        _: u32,
    ) -> Result<Vec<Collection>, StorageError> {
        Self::unused()
    }
    async fn link_collection_source(
        &self,
        _: CollectionId,
        _: SourceId,
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn link_collection_connector(
        &self,
        _: CollectionId,
        _: ConnectorId,
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn link_collection_object(
        &self,
        _: CollectionId,
        _: ObjectId,
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn list_collection_sources(
        &self,
        _: CollectionId,
        _: u32,
    ) -> Result<Vec<SourceId>, StorageError> {
        Self::unused()
    }
    async fn list_collection_connectors(
        &self,
        _: CollectionId,
        _: u32,
    ) -> Result<Vec<ConnectorId>, StorageError> {
        Self::unused()
    }
    async fn list_collection_objects(
        &self,
        _: CollectionId,
        _: u32,
    ) -> Result<Vec<ObjectId>, StorageError> {
        Self::unused()
    }
    async fn list_collections_by_source(
        &self,
        _: SourceId,
        _: u32,
    ) -> Result<Vec<CollectionId>, StorageError> {
        Self::unused()
    }
    async fn list_collections_by_connector(
        &self,
        _: ConnectorId,
        _: u32,
    ) -> Result<Vec<CollectionId>, StorageError> {
        Self::unused()
    }
    async fn list_collections_by_raw_evidence(
        &self,
        _: RawEvidenceId,
        _: u32,
    ) -> Result<Vec<CollectionId>, StorageError> {
        Self::unused()
    }
    async fn unlink_collection_source(
        &self,
        _: CollectionId,
        _: SourceId,
    ) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn unlink_collection_connector(
        &self,
        _: CollectionId,
        _: ConnectorId,
    ) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn link_collection_members(
        &self,
        _: CollectionId,
        _: &[SourceId],
        _: &[ConnectorId],
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn create_collection_with_members(
        &self,
        _: &Collection,
        _: &[SourceId],
        _: &[ConnectorId],
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn insert_raw_evidence(
        &self,
        _: &RawEvidence,
        _: &[CollectionId],
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_raw_evidence(
        &self,
        _: RawEvidenceId,
    ) -> Result<Option<RawEvidence>, StorageError> {
        Self::unused()
    }
    async fn list_raw_evidence(
        &self,
        _: Option<RawEvidenceId>,
        _: u32,
    ) -> Result<Vec<RawEvidence>, StorageError> {
        Self::unused()
    }
    async fn list_raw_evidence_by_source(
        &self,
        _: SourceId,
        _: Option<RawEvidenceId>,
        _: u32,
    ) -> Result<Vec<RawEvidence>, StorageError> {
        Self::unused()
    }
    async fn put_document(&self, _: &Document) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_document(&self, _: DocumentId) -> Result<Option<Document>, StorageError> {
        Self::unused()
    }
    async fn delete_document(&self, _: DocumentId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_documents(
        &self,
        _: Option<DocumentId>,
        _: u32,
    ) -> Result<Vec<Document>, StorageError> {
        Self::unused()
    }
    async fn list_documents_filtered(
        &self,
        _: Option<DocumentType>,
        _: bool,
        _: Option<DocumentId>,
        _: u32,
    ) -> Result<Vec<Document>, StorageError> {
        Self::unused()
    }
    async fn put_entity(&self, _: &Entity) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn delete_entity(&self, _: EntityId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_entities(
        &self,
        _: Option<EntityId>,
        _: u32,
    ) -> Result<Vec<Entity>, StorageError> {
        Self::unused()
    }
    async fn list_entities_by_type(
        &self,
        _: Option<EntityType>,
        _: Option<EntityId>,
        _: u32,
    ) -> Result<Vec<Entity>, StorageError> {
        Self::unused()
    }
    async fn put_relationship(&self, _: &Relationship) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_relationship(
        &self,
        _: RelationshipId,
    ) -> Result<Option<Relationship>, StorageError> {
        Self::unused()
    }
    async fn delete_relationship(&self, _: RelationshipId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_relationships_by_object(
        &self,
        _: ObjectId,
        _: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        Self::unused()
    }
    async fn list_relationships(
        &self,
        _: Option<RelationshipId>,
        _: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        Self::unused()
    }
    async fn list_relationships_by_type(
        &self,
        _: Option<RelationshipType>,
        _: Option<RelationshipId>,
        _: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        Self::unused()
    }
    async fn put_relationship_evidence(
        &self,
        _: &RelationshipEvidence,
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_relationship_evidence(
        &self,
        _: RelationshipEvidenceId,
    ) -> Result<Option<RelationshipEvidence>, StorageError> {
        Self::unused()
    }
    async fn list_relationship_evidence(
        &self,
        _: RelationshipId,
        _: u32,
    ) -> Result<Vec<RelationshipEvidence>, StorageError> {
        Self::unused()
    }
    async fn put_event(&self, _: &Event) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_event(&self, _: EventId) -> Result<Option<Event>, StorageError> {
        Self::unused()
    }
    async fn delete_event(&self, _: EventId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_events(&self, _: Option<EventId>, _: u32) -> Result<Vec<Event>, StorageError> {
        Self::unused()
    }
    async fn put_provenance(&self, _: &Provenance) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_provenance(&self, _: ProvenanceId) -> Result<Option<Provenance>, StorageError> {
        Self::unused()
    }
    async fn list_provenance_by_raw_evidence(
        &self,
        _: RawEvidenceId,
    ) -> Result<Vec<Provenance>, StorageError> {
        Self::unused()
    }
    async fn list_provenance_by_subject(
        &self,
        _: ObjectId,
    ) -> Result<Vec<Provenance>, StorageError> {
        Self::unused()
    }
    async fn put_job(&self, _: &Job) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_job(&self, _: JobId) -> Result<Option<Job>, StorageError> {
        Self::unused()
    }
    async fn delete_job(&self, _: JobId) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn list_jobs(&self, _: Option<JobId>, _: u32) -> Result<Vec<Job>, StorageError> {
        Self::unused()
    }
    async fn list_jobs_by_status(
        &self,
        _: JobStatus,
        _: Option<JobId>,
        _: u32,
    ) -> Result<Vec<Job>, StorageError> {
        Self::unused()
    }
    async fn find_document_ids_by_external_key(
        &self,
        _: &str,
        _: DocumentId,
        _: u32,
    ) -> Result<Vec<DocumentId>, StorageError> {
        Self::unused()
    }
    async fn find_document_ids_by_canonical_url(
        &self,
        _: &str,
        _: DocumentId,
        _: u32,
    ) -> Result<Vec<DocumentId>, StorageError> {
        Self::unused()
    }
    async fn find_document_ids_by_content_hash(
        &self,
        _: &str,
        _: DocumentId,
        _: u32,
    ) -> Result<Vec<DocumentId>, StorageError> {
        Self::unused()
    }
    async fn find_simhash_candidates(
        &self,
        _: i64,
        _: u32,
        _: DocumentId,
        _: u32,
    ) -> Result<Vec<SimhashCandidate>, StorageError> {
        Self::unused()
    }
    async fn put_duplicate_group(&self, _: &DuplicateGroup) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_duplicate_group(
        &self,
        _: DuplicateGroupId,
    ) -> Result<Option<DuplicateGroup>, StorageError> {
        Self::unused()
    }
    async fn get_duplicate_group_by_member(
        &self,
        _: ObjectId,
    ) -> Result<Option<DuplicateGroup>, StorageError> {
        Self::unused()
    }
    async fn list_duplicate_groups_by_canonical(
        &self,
        _: ObjectId,
        _: u32,
    ) -> Result<Vec<DuplicateGroup>, StorageError> {
        Self::unused()
    }
    async fn put_entity_extraction(&self, _: &EntityExtraction) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_entity_extraction(
        &self,
        _: EntityExtractionId,
    ) -> Result<Option<EntityExtraction>, StorageError> {
        Self::unused()
    }
    async fn list_entity_extractions_by_object(
        &self,
        _: ObjectId,
        _: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError> {
        Self::unused()
    }
    async fn list_entity_extractions_by_entity(
        &self,
        _: EntityId,
        _: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError> {
        Self::unused()
    }
    async fn list_sources_by_entity(
        &self,
        _: EntityId,
        _: u32,
    ) -> Result<Vec<SourceId>, StorageError> {
        Self::unused()
    }
    async fn put_entity_alias(&self, _: &EntityAlias) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_entity_alias(
        &self,
        _: EntityAliasId,
    ) -> Result<Option<EntityAlias>, StorageError> {
        Self::unused()
    }
    async fn list_entity_aliases_by_entity(
        &self,
        _: EntityId,
        _: u32,
    ) -> Result<Vec<EntityAlias>, StorageError> {
        Self::unused()
    }
    async fn find_entity_aliases_by_text(
        &self,
        _: &str,
        _: u32,
    ) -> Result<Vec<EntityAlias>, StorageError> {
        Self::unused()
    }
    async fn put_entity_identifier(&self, _: &EntityIdentifier) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_entity_identifier(
        &self,
        _: EntityIdentifierId,
    ) -> Result<Option<EntityIdentifier>, StorageError> {
        Self::unused()
    }
    async fn list_entity_identifiers_by_entity(
        &self,
        _: EntityId,
        _: u32,
    ) -> Result<Vec<EntityIdentifier>, StorageError> {
        Self::unused()
    }
    async fn find_entity_identifier_owner(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Option<EntityIdentifier>, StorageError> {
        Self::unused()
    }
    async fn find_entity_identifiers_by_normalized_value(
        &self,
        _: &str,
        _: u32,
    ) -> Result<Vec<EntityIdentifier>, StorageError> {
        Self::unused()
    }
    async fn get_resolution_candidate(
        &self,
        _: ResolutionCandidateId,
    ) -> Result<Option<ResolutionCandidate>, StorageError> {
        Self::unused()
    }
    async fn list_resolution_candidates(
        &self,
        _: Option<ResolutionStatus>,
        _: Option<ResolutionCandidateId>,
        _: u32,
    ) -> Result<Vec<ResolutionCandidate>, StorageError> {
        Self::unused()
    }
    async fn list_resolution_candidates_by_entity(
        &self,
        _: EntityId,
        _: Option<ResolutionStatus>,
        _: Option<ResolutionCandidateId>,
        _: u32,
    ) -> Result<Vec<ResolutionCandidate>, StorageError> {
        Self::unused()
    }
    async fn update_resolution_candidate_status(
        &self,
        _: ResolutionCandidateId,
        _: ResolutionStatus,
        _: chrono::DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn put_seed(&self, _: &Seed) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_seed(&self, _: SeedId) -> Result<Option<Seed>, StorageError> {
        Self::unused()
    }
    async fn list_seeds(
        &self,
        _: Option<&str>,
        _: Option<SeedId>,
        _: u32,
    ) -> Result<Vec<Seed>, StorageError> {
        Self::unused()
    }
    async fn list_seeds_by_collection(
        &self,
        _: CollectionId,
        _: Option<&str>,
        _: Option<SeedId>,
        _: u32,
    ) -> Result<Vec<Seed>, StorageError> {
        Self::unused()
    }
    async fn update_seed_status(&self, _: SeedId, _: &str) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn put_candidate(&self, _: &Candidate) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_candidate(&self, _: CandidateId) -> Result<Option<Candidate>, StorageError> {
        Self::unused()
    }
    async fn list_candidates(
        &self,
        _: Option<CandidateStatus>,
        _: Option<CandidateId>,
        _: u32,
    ) -> Result<Vec<Candidate>, StorageError> {
        Self::unused()
    }
    async fn list_candidates_by_collection(
        &self,
        _: CollectionId,
        _: Option<CandidateStatus>,
        _: Option<CandidateId>,
        _: u32,
    ) -> Result<Vec<Candidate>, StorageError> {
        Self::unused()
    }
    async fn update_candidate_status(
        &self,
        _: CandidateId,
        _: CandidateStatus,
        _: chrono::DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn put_candidate_evidence(&self, _: &CandidateEvidence) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn record_candidate_discovery(
        &self,
        _: &Candidate,
        _: &CandidateEvidence,
    ) -> Result<storage_core::CandidateDiscoveryOutcome, StorageError> {
        Self::unused()
    }
    async fn get_candidate_evidence(
        &self,
        _: CandidateEvidenceId,
    ) -> Result<Option<CandidateEvidence>, StorageError> {
        Self::unused()
    }
    async fn list_candidate_evidence_by_candidate(
        &self,
        _: CandidateId,
        _: u32,
    ) -> Result<Vec<CandidateEvidence>, StorageError> {
        Self::unused()
    }
    async fn put_ai_run(&self, _: &AiRun) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_ai_run(&self, _: AiRunId) -> Result<Option<AiRun>, StorageError> {
        Self::unused()
    }
    async fn list_ai_runs(
        &self,
        _: Option<&str>,
        _: Option<AiRunId>,
        _: u32,
    ) -> Result<Vec<AiRun>, StorageError> {
        Self::unused()
    }
    async fn put_collection_budget(
        &self,
        _: &core_model::CollectionBudget,
    ) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_collection_budget(
        &self,
        _: CollectionId,
    ) -> Result<Option<core_model::CollectionBudget>, StorageError> {
        Self::unused()
    }
    async fn try_consume_daily_request_budget(
        &self,
        _: CollectionId,
        _: chrono::NaiveDate,
        _: i64,
        _: i64,
    ) -> Result<storage_core::BudgetConsumption, StorageError> {
        Self::unused()
    }
    async fn try_consume_daily_ai_budget(
        &self,
        _: CollectionId,
        _: chrono::NaiveDate,
        _: i64,
        _: i64,
    ) -> Result<storage_core::BudgetConsumption, StorageError> {
        Self::unused()
    }
    async fn get_daily_usage(
        &self,
        _: CollectionId,
        _: chrono::NaiveDate,
    ) -> Result<storage_core::DailyUsage, StorageError> {
        Self::unused()
    }
    async fn put_merge_history(&self, _: &MergeHistory) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn get_merge_history(
        &self,
        _: MergeHistoryId,
    ) -> Result<Option<MergeHistory>, StorageError> {
        Self::unused()
    }
    async fn list_merge_history_by_entity(
        &self,
        _: EntityId,
        _: u32,
    ) -> Result<Vec<MergeHistory>, StorageError> {
        Self::unused()
    }
    async fn put_failed_event(&self, _: &FailedEvent) -> Result<FailedEvent, StorageError> {
        Self::unused()
    }
    async fn get_failed_event(
        &self,
        _: FailedEventId,
    ) -> Result<Option<FailedEvent>, StorageError> {
        Self::unused()
    }
    async fn list_failed_events(
        &self,
        _: Option<FailedEventId>,
        _: u32,
    ) -> Result<Vec<FailedEvent>, StorageError> {
        Self::unused()
    }
    async fn list_failed_events_filtered(
        &self,
        _: Option<&str>,
        _: bool,
        _: Option<FailedEventId>,
        _: u32,
    ) -> Result<Vec<FailedEvent>, StorageError> {
        Self::unused()
    }
    async fn mark_replayed(
        &self,
        _: FailedEventId,
        _: chrono::DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        Self::unused()
    }
    async fn put_embedding(&self, _: &Embedding) -> Result<(), StorageError> {
        Self::unused()
    }
    async fn find_embedding(
        &self,
        _: ObjectId,
        _: EmbeddingTarget,
        _: &str,
        _: &str,
    ) -> Result<Option<Embedding>, StorageError> {
        Self::unused()
    }
    async fn list_embeddings_by_target(
        &self,
        _: ObjectId,
        _: EmbeddingTarget,
        _: u32,
    ) -> Result<Vec<Embedding>, StorageError> {
        Self::unused()
    }
}
