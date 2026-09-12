//! SPEC §6「semantic similarity」：同 type 的 Entity 用 embedding cosine 比對。
//!
//! 由 [`crate::ResolverService::resolve_entity`] 聚合呼叫（門檻
//! [`crate::SEMANTIC_SIMILARITY_THRESHOLD`]）；也仍以自由函式公開。
//! **不寫 candidate 表**。還沒接真實 ml-commons adapter 時，注入
//! [`storage_core::mock::MockEmbeddingProvider`] 或 `unsupported()` 即可測 plumbing。

use chrono::Utc;
use core_model::{Entity, EntityType, RESOLUTION_METHODS, ResolutionCandidate, ResolutionStatus};
use serde_json::json;
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, RelationalStore, StorageError,
};
use tracing::info;
use uuid::Uuid;

use crate::error::ResolverError;

/// 這個函式目前是暴力比對：一次最多拉 100 筆同 type Entity 逐一 embed。
///
/// 真實規模需要 ANN／embedding index；100 是效能取捨，不是「系統裡最多
/// 100 個 Entity」的語意。超過的列這次看不到，靜默漏比，不要把空結果
/// 讀成「沒有相似實體」。
const BRUTE_FORCE_CANDIDATE_LIMIT: u32 = 100;

const METHOD_SEMANTIC_SIMILARITY: &str = RESOLUTION_METHODS[8];

/// cosine 轉 candidate score 的係數。語意相似是弱於 exact identifier 的訊號，
/// 即使向量完全相同（cosine = 1.0）也只給 0.80，夠進 Review、不到自動合併。
const SEMANTIC_SCORE_SCALE: f64 = 0.8;

/// 對一個 Entity 找同 type、語意相近的其他 Entity，組出 pending candidate。
///
/// # 跳過 identity-type
///
/// `Domain`／`Hostname`／`Ip`／`Url`／`Email`／`Hash` 的 name 本身就是識別碼，
/// 「192.168.1.1」跟「192.168.1.2」語意相近不代表同一實體。這類直接回空 Vec，
/// **不呼叫** embedder、也不查 store。
///
/// # 語言
///
/// `EmbeddingRequest.language` 固定 `None`（走多語模型）。語言偵測不在這個
/// 函式的範圍——這是簡化，不是遺漏；呼叫端之後可以擴充成先偵測再傳入。
///
/// # `UnsupportedCapability`
///
/// embedder 回 [`StorageError::UnsupportedCapability`] 視為「這次沒有語意
/// 能力」，回空 Vec，**不是錯誤**。其他 storage 錯誤往上傳播。
pub async fn check_semantic_similarity<S, E>(
    store: &S,
    embedder: &E,
    entity: &Entity,
    threshold: f64,
) -> Result<Vec<ResolutionCandidate>, ResolverError>
where
    S: RelationalStore,
    E: EmbeddingProvider,
{
    if is_identity_type(entity.entity_type) {
        return Ok(Vec::new());
    }

    let source_text = embedding_text(entity);
    let source_vec = match embed_passage(embedder, &source_text).await {
        Ok(vector) => vector,
        Err(StorageError::UnsupportedCapability {
            backend,
            capability,
        }) => {
            info!(
                %backend,
                %capability,
                entity_id = %entity.id,
                "語意相似度暫時跳過：embedding provider 不支援此 capability，回空結果而不是失敗"
            );
            return Ok(Vec::new());
        }
        Err(err) => return Err(ResolverError::Storage(err)),
    };

    let others = store
        .list_entities_by_type(Some(entity.entity_type), None, BRUTE_FORCE_CANDIDATE_LIMIT)
        .await?;

    let model = embedder.model_for(None).model;
    let mut out = Vec::new();
    for other in others {
        if other.id == entity.id {
            continue;
        }
        let other_text = embedding_text(&other);
        let other_vec = match embed_passage(embedder, &other_text).await {
            Ok(vector) => vector,
            Err(StorageError::UnsupportedCapability {
                backend,
                capability,
            }) => {
                info!(
                    %backend,
                    %capability,
                    entity_id = %entity.id,
                    other_id = %other.id,
                    "語意相似度暫時跳過：比對中途 embedding 變為不支援，回目前已組出的候選"
                );
                break;
            }
            Err(err) => return Err(ResolverError::Storage(err)),
        };
        let cosine = cosine_similarity(&source_vec, &other_vec);
        if cosine < threshold {
            continue;
        }
        out.push(semantic_candidate(entity, &other, cosine, &model));
    }
    Ok(out)
}

/// 兩個 f32 向量的 cosine。不假設已單位化，仍除以兩邊 L2 norm。
///
/// 長度不同或任一邊 norm 為 0 時回 `0.0`（沒有可定義的夾角，不當命中）。
#[must_use]
pub(crate) fn cosine_similarity(a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f64;
    let mut norm_a = 0.0f64;
    let mut norm_b = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let x = f64::from(*x);
        let y = f64::from(*y);
        dot += x * y;
        norm_a += x * x;
        norm_b += y * y;
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 { 0.0 } else { dot / denom }
}

fn is_identity_type(entity_type: EntityType) -> bool {
    match entity_type {
        EntityType::Domain
        | EntityType::Hostname
        | EntityType::Ip
        | EntityType::Url
        | EntityType::Email
        | EntityType::Hash => true,
        EntityType::Person
        | EntityType::Organization
        | EntityType::Account
        | EntityType::Vulnerability
        | EntityType::Software
        | EntityType::Repository
        | EntityType::Location => false,
    }
}

fn embedding_text(entity: &Entity) -> String {
    match entity.description.as_deref() {
        Some(description) => format!("{}: {}", entity.name, description),
        None => entity.name.clone(),
    }
}

async fn embed_passage<E: EmbeddingProvider>(
    embedder: &E,
    text: &str,
) -> Result<Vec<f32>, StorageError> {
    let request = EmbeddingRequest {
        text: text.to_string(),
        kind: EmbeddingKind::Passage,
        language: None,
    };
    Ok(embedder.embed(&request).await?.vector)
}

fn semantic_candidate(
    entity: &Entity,
    other: &Entity,
    cosine: f64,
    model: &str,
) -> ResolutionCandidate {
    let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(entity.id, other.id);
    ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id,
        entity_b_id,
        score: cosine * SEMANTIC_SCORE_SCALE,
        method: METHOD_SEMANTIC_SIMILARITY.to_string(),
        evidence: json!({
            "method": METHOD_SEMANTIC_SIMILARITY,
            "cosine_similarity": cosine,
            "model": model,
            "embedding_kind": "passage",
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use core_model::{
        Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DocumentType,
        DuplicateGroup, DuplicateGroupId, EntityAlias, EntityAliasId, EntityExtraction,
        EntityExtractionId, EntityId, EntityIdentifier, EntityIdentifierId, Event, EventId,
        FailedEvent, FailedEventId, Job, JobId, JobStatus, MergeHistory, MergeHistoryId,
        NetworkRule, NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence, RawEvidenceId,
        Relationship, RelationshipEvidence, RelationshipEvidenceId, RelationshipId,
        RelationshipType, ResolutionCandidateId, Source, SourceId,
    };
    use serde_json::json;
    use storage_core::health::{HealthProvider, StorageHealth};
    use storage_core::mock::MockEmbeddingProvider;
    use storage_core::traits::{EmbeddingModelRef, EmbeddingVector, SimhashCandidate};

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 13, 8, 0, 0).unwrap()
    }

    fn entity(id: EntityId, entity_type: EntityType, name: &str) -> Entity {
        Entity {
            id,
            entity_type,
            name: name.into(),
            normalized_name: name.into(),
            description: None,
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
            attributes: json!({}),
        }
    }

    /// 只實作 `list_entities_by_type` 的記憶體 store。其餘方法回
    /// `UnsupportedCapability`。另記 list 被呼叫次數，用來證明 identity-type
    /// 路徑連 store 都沒碰。
    struct MemoryStore {
        inner: Mutex<HashMap<EntityId, Entity>>,
        list_calls: AtomicUsize,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self {
                inner: Mutex::new(HashMap::new()),
                list_calls: AtomicUsize::new(0),
            }
        }

        fn seed(&self, e: Entity) {
            self.inner.lock().expect("mutex").insert(e.id, e);
        }

        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }

        fn unsupported<T>(capability: &'static str) -> Result<T, StorageError> {
            Err(StorageError::UnsupportedCapability {
                backend: "memory-semantic-test",
                capability,
            })
        }
    }

    #[async_trait]
    impl HealthProvider for MemoryStore {
        async fn health(&self) -> Result<StorageHealth, StorageError> {
            Ok(StorageHealth::ok(
                "memory-semantic-test",
                "semantic 單元測試用記憶體 store",
            ))
        }
    }

    /// 包一層計數，斷言 identity-type 路徑真的沒呼叫 embed，不是碰巧沒觸發。
    struct CountingEmbedder {
        inner: MockEmbeddingProvider,
        embeds: AtomicUsize,
    }

    impl CountingEmbedder {
        fn new(inner: MockEmbeddingProvider) -> Self {
            Self {
                inner,
                embeds: AtomicUsize::new(0),
            }
        }

        fn embed_calls(&self) -> usize {
            self.embeds.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl HealthProvider for CountingEmbedder {
        async fn health(&self) -> Result<StorageHealth, StorageError> {
            self.inner.health().await
        }
    }

    #[async_trait]
    impl EmbeddingProvider for CountingEmbedder {
        fn dimensions(&self) -> usize {
            self.inner.dimensions()
        }

        fn dimensions_for(&self, language: Option<&str>) -> usize {
            self.inner.dimensions_for(language)
        }

        fn model_for(&self, language: Option<&str>) -> EmbeddingModelRef {
            self.inner.model_for(language)
        }

        async fn embed(&self, request: &EmbeddingRequest) -> Result<EmbeddingVector, StorageError> {
            self.embeds.fetch_add(1, Ordering::SeqCst);
            self.inner.embed(request).await
        }

        async fn embed_batch(
            &self,
            requests: &[EmbeddingRequest],
        ) -> Result<Vec<EmbeddingVector>, StorageError> {
            self.embeds.fetch_add(requests.len(), Ordering::SeqCst);
            self.inner.embed_batch(requests).await
        }
    }

    #[async_trait]
    impl RelationalStore for MemoryStore {
        async fn list_entities_by_type(
            &self,
            entity_type: Option<EntityType>,
            after: Option<EntityId>,
            limit: u32,
        ) -> Result<Vec<Entity>, StorageError> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            let inner = self.inner.lock().expect("mutex");
            let mut items: Vec<Entity> = inner
                .values()
                .filter(|e| entity_type.is_none_or(|t| e.entity_type == t))
                .cloned()
                .collect();
            items.sort_by_key(|a| std::cmp::Reverse(a.id));
            if let Some(after) = after {
                items.retain(|e| e.id < after);
            }
            items.truncate(limit as usize);
            Ok(items)
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
        async fn get_entity(&self, _: EntityId) -> Result<Option<Entity>, StorageError> {
            Self::unsupported("get_entity")
        }
        async fn delete_entity(&self, _: EntityId) -> Result<bool, StorageError> {
            Self::unsupported("delete_entity")
        }
        async fn find_entity_by_normalized_name(
            &self,
            _: EntityType,
            _: &str,
        ) -> Result<Option<Entity>, StorageError> {
            Self::unsupported("find_entity_by_normalized_name")
        }
        async fn list_entities(
            &self,
            _: Option<EntityId>,
            _: u32,
        ) -> Result<Vec<Entity>, StorageError> {
            Self::unsupported("list_entities")
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
            _: ObjectId,
            _: u32,
        ) -> Result<Vec<Relationship>, StorageError> {
            Self::unsupported("list_relationships_by_object")
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
            _: EntityId,
            _: u32,
        ) -> Result<Vec<EntityAlias>, StorageError> {
            Self::unsupported("list_entity_aliases_by_entity")
        }
        async fn find_entity_aliases_by_text(
            &self,
            _: &str,
            _: u32,
        ) -> Result<Vec<EntityAlias>, StorageError> {
            Self::unsupported("find_entity_aliases_by_text")
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
        async fn put_resolution_candidate(
            &self,
            _: &ResolutionCandidate,
        ) -> Result<(), StorageError> {
            Self::unsupported("put_resolution_candidate")
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

    #[test]
    fn cosine_similarity_known_vectors() {
        assert!((cosine_similarity(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-9);
        assert!((cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]) - 0.0).abs() < 1e-9);
        assert!((cosine_similarity(&[1.0, 0.0], &[-1.0, 0.0]) + 1.0).abs() < 1e-9);
        assert_eq!(cosine_similarity(&[1.0], &[1.0, 0.0]), 0.0);
        assert_eq!(cosine_similarity(&[], &[]), 0.0);
        assert_eq!(cosine_similarity(&[0.0, 0.0], &[1.0, 0.0]), 0.0);
    }

    #[tokio::test]
    async fn identical_names_yield_unit_cosine_and_scaled_score() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Person, "alice example");
        let b = entity(b_id, EntityType::Person, "alice example");
        store.seed(a.clone());
        store.seed(b);
        let embedder = MockEmbeddingProvider::new();
        let hits = check_semantic_similarity(&store, &embedder, &a, 0.5)
            .await
            .expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        assert_eq!(c.entity_a_id, a_id);
        assert_eq!(c.entity_b_id, b_id);
        assert!(c.entity_a_id < c.entity_b_id);
        let cosine = c.evidence["cosine_similarity"]
            .as_f64()
            .expect("cosine_similarity");
        assert!(
            cosine > 0.999,
            "相同文字應得到接近 1.0 的 cosine，實際 {cosine}"
        );
        assert!((c.score - cosine * 0.8).abs() < 1e-9);
        assert!((c.score - 0.8).abs() < 0.001);
        assert_eq!(c.method, "semantic_similarity");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "semantic_similarity");
        assert_eq!(c.evidence["embedding_kind"], "passage");
        assert_eq!(c.evidence["model"], embedder.model_for(None).model);
    }

    #[tokio::test]
    async fn different_names_follow_computed_cosine_not_semantic_claim() {
        let store = MemoryStore::new();
        let a = entity(Uuid::from_u128(0x1111), EntityType::Person, "alice");
        let b = entity(Uuid::from_u128(0x2222), EntityType::Person, "totally-other");
        store.seed(a.clone());
        store.seed(b.clone());
        let embedder = MockEmbeddingProvider::new();

        let va = embedder
            .embed(&EmbeddingRequest {
                text: a.name.clone(),
                kind: EmbeddingKind::Passage,
                language: None,
            })
            .await
            .expect("embed a");
        let vb = embedder
            .embed(&EmbeddingRequest {
                text: b.name.clone(),
                kind: EmbeddingKind::Passage,
                language: None,
            })
            .await
            .expect("embed b");
        let expected = cosine_similarity(&va.vector, &vb.vector);

        let hits = check_semantic_similarity(&store, &embedder, &a, expected - 1e-6)
            .await
            .expect("check at computed cosine");
        assert_eq!(
            hits.len(),
            1,
            "threshold 略低於實際 cosine ({expected}) 應命中，得到 {hits:?}"
        );
        let reported = hits[0].evidence["cosine_similarity"]
            .as_f64()
            .expect("cosine");
        assert!(
            (reported - expected).abs() < 1e-9,
            "函式算出的 cosine {reported} 應等於直接 embed 的 {expected}"
        );

        let high = check_semantic_similarity(&store, &embedder, &a, 0.999)
            .await
            .expect("high threshold");
        if expected >= 0.999 {
            assert_eq!(high.len(), 1, "極小機率：mock 向量碰巧幾乎同向");
        } else {
            assert!(
                high.is_empty(),
                "不同文字在 0.999 門檻不應命中（實際 cosine {expected}）：{high:?}"
            );
        }
    }

    #[tokio::test]
    async fn unsupported_embedder_returns_empty_not_error() {
        let store = MemoryStore::new();
        let a = entity(Uuid::from_u128(0x1111), EntityType::Person, "alice");
        store.seed(a.clone());
        store.seed(entity(Uuid::from_u128(0x2222), EntityType::Person, "alice"));
        let embedder = MockEmbeddingProvider::unsupported();
        let hits = check_semantic_similarity(&store, &embedder, &a, 0.5)
            .await
            .expect("unsupported 不該當錯誤");
        assert!(hits.is_empty(), "{hits:?}");
        assert_eq!(
            store.list_calls(),
            0,
            "embed 一開始就不支援時不該再去掃 Entity"
        );
    }

    #[tokio::test]
    async fn identity_type_skips_embedder_and_store() {
        let store = MemoryStore::new();
        let domain = entity(Uuid::from_u128(0x1111), EntityType::Domain, "example.com");
        store.seed(domain.clone());
        store.seed(entity(
            Uuid::from_u128(0x2222),
            EntityType::Domain,
            "example.com",
        ));
        let embedder = CountingEmbedder::new(MockEmbeddingProvider::new());
        let hits = check_semantic_similarity(&store, &embedder, &domain, 0.0)
            .await
            .expect("check");
        assert!(hits.is_empty(), "identity-type 應直接回空，得到 {hits:?}");
        assert_eq!(
            embedder.embed_calls(),
            0,
            "Domain 不該呼叫 embedder，實際呼叫了 {} 次",
            embedder.embed_calls()
        );
        assert_eq!(
            store.list_calls(),
            0,
            "Domain 不該呼叫 list_entities_by_type"
        );
    }

    #[tokio::test]
    async fn skips_self_even_when_same_name_is_in_the_page() {
        let store = MemoryStore::new();
        let only = entity(Uuid::from_u128(0x1111), EntityType::Organization, "acme");
        store.seed(only.clone());
        let embedder = MockEmbeddingProvider::new();
        let hits = check_semantic_similarity(&store, &embedder, &only, 0.0)
            .await
            .expect("check");
        assert!(hits.is_empty(), "不該對自己產生 candidate，得到 {hits:?}");
    }
}
