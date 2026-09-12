//! SPEC §6 的 `alias`／`domain`／`url` 三條 resolution method。
//!
//! **自由函式，不寫入 store、不接進 [`crate::ResolverService::resolve_entity`]。**
//! 呼叫端自己決定要不要 `put_resolution_candidate`。之後的整合任務才會把它們
//! 接進聚合呼叫。
//!
//! # 資料可能是空的——這是預期，不是沒命中
//!
//! 這三個函式都讀 `entity_aliases`／`entity_identifiers`。entity-worker 的
//! 寫入者是平行交付（Phase 1c-0-data）；在那完成之前，真實 store 通常是空表，
//! 這裡會回空 `Vec`。空結果代表「這次沒資料可比」，不要讀成「確定沒有同一實體」。

use std::collections::HashSet;

use chrono::Utc;
use core_model::{Entity, RESOLUTION_METHODS, ResolutionCandidate, ResolutionStatus};
use serde_json::json;
use storage_core::RelationalStore;
use uuid::Uuid;

use crate::error::ResolverError;

/// 共用 alias 的分數。同一個顯示名稱出現在兩個 Entity 上是中等訊號，
/// 0.55 夠進 Review，不到自動合併。
pub const ALIAS_SCORE: f64 = 0.55;

/// 共用 domain identifier 的分數。同一個網域被兩個 Entity 宣稱是弱訊號
/// （共用主機、轉售、資料髒了都常見），0.30 只夠進 Review。
///
/// 目前 `entity_identifiers` 通常是空的（寫入者是另一個平行任務），
/// 這個函式邏輯要對，但實測可能永遠拿到空結果。
pub const DOMAIN_SCORE: f64 = 0.30;

/// 共用 URL identifier 的分數。正規化後同一個 URL 比 domain 硬一點，
/// 0.60 夠進 Review，仍不到自動合併。
pub const URL_SCORE: f64 = 0.60;

const METHOD_ALIAS: &str = RESOLUTION_METHODS[2];
const METHOD_DOMAIN: &str = RESOLUTION_METHODS[3];
const METHOD_URL: &str = RESOLUTION_METHODS[4];
const LIST_LIMIT: u32 = 100;

/// SPEC §6「alias」：這個 Entity 的 alias 文字，有沒有被別的 Entity 用過。
///
/// 同一對因多個共用 alias 命中多次時只留第一筆；`evidence` 不合併所有 alias。
pub async fn check_alias<S: RelationalStore>(
    store: &S,
    entity: &Entity,
) -> Result<Vec<ResolutionCandidate>, ResolverError> {
    let own_aliases = store
        .list_entity_aliases_by_entity(entity.id, LIST_LIMIT)
        .await?;
    let mut seen: HashSet<(core_model::EntityId, core_model::EntityId)> = HashSet::new();
    let mut out = Vec::new();
    for own in own_aliases {
        let hits = store
            .find_entity_aliases_by_text(&own.alias, LIST_LIMIT)
            .await?;
        for other in hits {
            if other.entity_id == entity.id {
                continue;
            }
            let (entity_a_id, entity_b_id) =
                ResolutionCandidate::ordered_pair(entity.id, other.entity_id);
            if !seen.insert((entity_a_id, entity_b_id)) {
                continue;
            }
            let (entity_a_alias_confidence, entity_b_alias_confidence) = if entity_a_id == entity.id
            {
                (own.confidence, other.confidence)
            } else {
                (other.confidence, own.confidence)
            };
            out.push(ResolutionCandidate {
                id: Uuid::now_v7(),
                entity_a_id,
                entity_b_id,
                score: ALIAS_SCORE,
                method: METHOD_ALIAS.to_string(),
                evidence: json!({
                    "method": METHOD_ALIAS,
                    "shared_alias": own.alias,
                    "entity_a_alias_confidence": entity_a_alias_confidence,
                    "entity_b_alias_confidence": entity_b_alias_confidence,
                }),
                status: ResolutionStatus::Pending,
                created_at: Utc::now(),
                reviewed_at: None,
            });
        }
    }
    Ok(out)
}

/// SPEC §6「domain」：這個 Entity 的 `namespace="domain"` identifier，
/// 目前的 owner 是不是別人。
///
/// # 空表是預期
///
/// entity-worker 還沒寫 `entity_identifiers` 時，這個函式會回空 `Vec`。
/// 那是「沒資料」，不是「沒有共用網域」。
pub async fn check_domain<S: RelationalStore>(
    store: &S,
    entity: &Entity,
) -> Result<Vec<ResolutionCandidate>, ResolverError> {
    let identifiers = store
        .list_entity_identifiers_by_entity(entity.id, LIST_LIMIT)
        .await?;
    let mut seen: HashSet<(core_model::EntityId, core_model::EntityId)> = HashSet::new();
    let mut out = Vec::new();
    for ident in identifiers {
        if ident.namespace != "domain" {
            continue;
        }
        let Some(owner) = store
            .find_entity_identifier_owner("domain", &ident.normalized_value)
            .await?
        else {
            continue;
        };
        if owner.entity_id == entity.id {
            continue;
        }
        let (entity_a_id, entity_b_id) =
            ResolutionCandidate::ordered_pair(entity.id, owner.entity_id);
        if !seen.insert((entity_a_id, entity_b_id)) {
            continue;
        }
        out.push(ResolutionCandidate {
            id: Uuid::now_v7(),
            entity_a_id,
            entity_b_id,
            score: DOMAIN_SCORE,
            method: METHOD_DOMAIN.to_string(),
            evidence: json!({
                "method": METHOD_DOMAIN,
                "domain": ident.normalized_value,
            }),
            status: ResolutionStatus::Pending,
            created_at: Utc::now(),
            reviewed_at: None,
        });
    }
    Ok(out)
}

/// SPEC §6「url」：這個 Entity 的 `namespace="url"` identifier，正規化後
/// 查目前 owner 是不是別人。
///
/// 正規化規則見 [`normalize_url_for_resolution`]：只做 scheme／host 小寫、
/// 去掉 fragment、去掉結尾多餘的 `/`。**不是** [`core_model::url_norm::canonicalize`]——
/// 那套還會刪追蹤參數、排序 query，是 Stage 2 文件去重用的，語意不同。
pub async fn check_url<S: RelationalStore>(
    store: &S,
    entity: &Entity,
) -> Result<Vec<ResolutionCandidate>, ResolverError> {
    let identifiers = store
        .list_entity_identifiers_by_entity(entity.id, LIST_LIMIT)
        .await?;
    let mut seen: HashSet<(core_model::EntityId, core_model::EntityId)> = HashSet::new();
    let mut out = Vec::new();
    for ident in identifiers {
        if ident.namespace != "url" {
            continue;
        }
        let Some(normalized) = normalize_url_for_resolution(&ident.value)
            .or_else(|| normalize_url_for_resolution(&ident.normalized_value))
        else {
            continue;
        };
        let Some(owner) = store
            .find_entity_identifier_owner("url", &normalized)
            .await?
        else {
            continue;
        };
        if owner.entity_id == entity.id {
            continue;
        }
        let (entity_a_id, entity_b_id) =
            ResolutionCandidate::ordered_pair(entity.id, owner.entity_id);
        if !seen.insert((entity_a_id, entity_b_id)) {
            continue;
        }
        out.push(ResolutionCandidate {
            id: Uuid::now_v7(),
            entity_a_id,
            entity_b_id,
            score: URL_SCORE,
            method: METHOD_URL.to_string(),
            evidence: json!({
                "method": METHOD_URL,
                "matched_url": normalized,
            }),
            status: ResolutionStatus::Pending,
            created_at: Utc::now(),
            reviewed_at: None,
        });
    }
    Ok(out)
}

/// 給 resolution 用的輕量 URL 正規化。
///
/// - scheme 小寫、host 小寫（`url` crate 解析時就會做）
/// - 去掉 fragment `#...`
/// - 去掉路徑結尾多餘的 `/`（根路徑 `/` 保留，否則會變成非法 URL）
///
/// 解析失敗回 `None`，呼叫端跳過該筆 identifier，不把原字串拿去比——
/// 那會讓「正規化過的 URL」與「沒正規化的字串」混在同一個鍵裡。
#[must_use]
pub fn normalize_url_for_resolution(raw: &str) -> Option<String> {
    let mut parsed = url::Url::parse(raw.trim()).ok()?;
    parsed.set_fragment(None);
    let path = parsed.path().to_string();
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        parsed.set_path("/");
    } else if trimmed != path {
        parsed.set_path(trimmed);
    }
    Some(parsed.to_string())
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
        EntityExtractionId, EntityId, EntityIdentifier, EntityIdentifierId, EntityType, Event,
        EventId, FailedEvent, FailedEventId, Job, JobId, JobStatus, MergeHistory, MergeHistoryId,
        NetworkRule, NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence, RawEvidenceId,
        Relationship, RelationshipEvidence, RelationshipEvidenceId, RelationshipId,
        RelationshipType, ResolutionCandidateId, Source, SourceId,
    };
    use serde_json::json;
    use storage_core::health::{HealthProvider, StorageHealth};
    use storage_core::{StorageError, traits::SimhashCandidate};

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

    fn identifier(
        owner: EntityId,
        namespace: &str,
        value: &str,
        normalized_value: &str,
    ) -> EntityIdentifier {
        EntityIdentifier {
            id: Uuid::now_v7(),
            entity_id: owner,
            namespace: namespace.into(),
            value: value.into(),
            normalized_value: normalized_value.into(),
            confidence: 1.0,
            source_id: None,
            first_seen: ts(),
            last_seen: ts(),
        }
    }

    /// identifier／alias 單元測試用的最小 RelationalStore。
    ///
    /// 只實作 `list_entity_aliases_by_entity`／`find_entity_aliases_by_text`／
    /// `list_entity_identifiers_by_entity`／`find_entity_identifier_owner`。
    /// listed identifier 與 owner 索引分開種，才能測「自己列上看得到、
    /// owner 卻是別人」——真實 UNIQUE 不允許這種列，測試必須能種。
    struct MemoryStore {
        inner: Mutex<Inner>,
    }

    struct Inner {
        aliases: Vec<EntityAlias>,
        listed_identifiers: Vec<EntityIdentifier>,
        owners: HashMap<(String, String), EntityIdentifier>,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self {
                inner: Mutex::new(Inner {
                    aliases: Vec::new(),
                    listed_identifiers: Vec::new(),
                    owners: HashMap::new(),
                }),
            }
        }

        fn seed_alias(&self, row: EntityAlias) {
            self.inner.lock().expect("mutex").aliases.push(row);
        }

        fn seed_listed_identifier(&self, row: EntityIdentifier) {
            self.inner
                .lock()
                .expect("mutex")
                .listed_identifiers
                .push(row);
        }

        fn seed_owner(&self, row: EntityIdentifier) {
            let mut inner = self.inner.lock().expect("mutex");
            inner
                .owners
                .insert((row.namespace.clone(), row.normalized_value.clone()), row);
        }

        fn unsupported<T>(capability: &'static str) -> Result<T, StorageError> {
            Err(StorageError::UnsupportedCapability {
                backend: "memory-identifier-test",
                capability,
            })
        }
    }

    #[async_trait]
    impl HealthProvider for MemoryStore {
        async fn health(&self) -> Result<StorageHealth, StorageError> {
            Ok(StorageHealth::ok(
                "memory-identifier-test",
                "identifier_methods 單元測試用記憶體 store",
            ))
        }
    }

    #[async_trait]
    impl RelationalStore for MemoryStore {
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
            entity_id: EntityId,
            limit: u32,
        ) -> Result<Vec<EntityIdentifier>, StorageError> {
            let inner = self.inner.lock().expect("mutex");
            let mut items: Vec<EntityIdentifier> = inner
                .listed_identifiers
                .iter()
                .filter(|i| i.entity_id == entity_id)
                .cloned()
                .collect();
            items.sort_by_key(|a| a.id);
            items.truncate(limit as usize);
            Ok(items)
        }
        async fn find_entity_identifier_owner(
            &self,
            namespace: &str,
            normalized_value: &str,
        ) -> Result<Option<EntityIdentifier>, StorageError> {
            let inner = self.inner.lock().expect("mutex");
            Ok(inner
                .owners
                .get(&(namespace.to_string(), normalized_value.to_string()))
                .cloned())
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
    fn normalize_url_lowercases_scheme_host_strips_fragment_and_trailing_slash() {
        assert_eq!(
            normalize_url_for_resolution("HTTPS://Example.INVALID/a/#section"),
            normalize_url_for_resolution("https://example.invalid/a"),
        );
        assert_eq!(
            normalize_url_for_resolution("https://example.invalid/a/"),
            Some("https://example.invalid/a".into()),
        );
        assert_eq!(
            normalize_url_for_resolution("https://example.invalid/"),
            Some("https://example.invalid/".into()),
            "根路徑的 `/` 不是多餘的，拿掉會變成非法 URL"
        );
        assert!(
            normalize_url_for_resolution("not a url").is_none(),
            "解析失敗應回 None，不要退回原字串"
        );
    }

    #[tokio::test]
    async fn check_alias_shared_alias_yields_one_candidate() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Organization, "microsoft");
        store.seed_alias(alias(a_id, "微軟", 0.9));
        store.seed_alias(alias(b_id, "微軟", 0.7));
        let hits = check_alias(&store, &a).await.expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        assert_eq!(c.entity_a_id, a_id);
        assert_eq!(c.entity_b_id, b_id);
        assert!(c.entity_a_id < c.entity_b_id);
        assert_eq!(c.score, ALIAS_SCORE);
        assert_eq!(c.method, "alias");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "alias");
        assert_eq!(c.evidence["shared_alias"], "微軟");
        assert_eq!(c.evidence["entity_a_alias_confidence"], 0.9);
        assert_eq!(c.evidence["entity_b_alias_confidence"], 0.7);
    }

    #[tokio::test]
    async fn check_alias_only_self_returns_empty() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let a = entity(a_id, EntityType::Organization, "lonely");
        store.seed_alias(alias(a_id, "孤單公司", 1.0));
        let hits = check_alias(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "只有自己有 alias 不該產生候選，得到 {hits:?}"
        );
    }

    #[tokio::test]
    async fn check_alias_two_shared_aliases_dedup_to_one_pair() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Organization, "microsoft");
        store.seed_alias(alias(a_id, "微軟", 0.9));
        store.seed_alias(alias(a_id, "Microsoft Corp", 0.8));
        store.seed_alias(alias(b_id, "微軟", 0.7));
        store.seed_alias(alias(b_id, "Microsoft Corp", 0.6));
        let hits = check_alias(&store, &a).await.expect("check");
        assert_eq!(
            hits.len(),
            1,
            "同一對因兩個共用 alias 命中兩次應只留一筆，得到 {hits:?}"
        );
        assert_eq!(
            hits[0].evidence["shared_alias"], "微軟",
            "應保留第一個命中的 alias"
        );
    }

    #[tokio::test]
    async fn check_domain_owner_is_other_yields_candidate() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Organization, "acme");
        store.seed_listed_identifier(identifier(a_id, "domain", "Example.COM", "example.com"));
        store.seed_owner(identifier(b_id, "domain", "example.com", "example.com"));
        let hits = check_domain(&store, &a).await.expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        assert_eq!(c.entity_a_id, a_id);
        assert_eq!(c.entity_b_id, b_id);
        assert_eq!(c.score, DOMAIN_SCORE);
        assert_eq!(c.method, "domain");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "domain");
        assert_eq!(c.evidence["domain"], "example.com");
    }

    #[tokio::test]
    async fn check_domain_owner_is_self_returns_empty() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let a = entity(a_id, EntityType::Domain, "example.com");
        let row = identifier(a_id, "domain", "example.com", "example.com");
        store.seed_listed_identifier(row.clone());
        store.seed_owner(row);
        let hits = check_domain(&store, &a).await.expect("check");
        assert!(hits.is_empty(), "owner 是自己不該產生候選，得到 {hits:?}");
    }

    #[tokio::test]
    async fn check_domain_no_identifier_returns_empty() {
        let store = MemoryStore::new();
        let a = entity(Uuid::from_u128(0x1111), EntityType::Person, "alice");
        store.seed_listed_identifier(identifier(
            a.id,
            "email",
            "alice@example.com",
            "alice@example.com",
        ));
        let hits = check_domain(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "沒有 domain identifier 應回空，email 不該被當成 domain：{hits:?}"
        );
    }

    #[tokio::test]
    async fn check_url_equivalent_after_normalization_hits() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Url, "https://example.invalid/a/");
        store.seed_listed_identifier(identifier(
            a_id,
            "url",
            "HTTPS://Example.INVALID/a/#section",
            "HTTPS://Example.INVALID/a/#section",
        ));
        let matched =
            normalize_url_for_resolution("https://example.invalid/a/").expect("正規化應成功");
        store.seed_owner(identifier(
            b_id,
            "url",
            "https://example.invalid/a",
            &matched,
        ));
        let hits = check_url(&store, &a).await.expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        assert_eq!(c.entity_a_id, a_id);
        assert_eq!(c.entity_b_id, b_id);
        assert_eq!(c.score, URL_SCORE);
        assert_eq!(c.method, "url");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "url");
        assert_eq!(c.evidence["matched_url"], matched);
    }

    #[tokio::test]
    async fn check_url_different_after_normalization_does_not_hit() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Url, "https://example.invalid/a");
        store.seed_listed_identifier(identifier(
            a_id,
            "url",
            "https://example.invalid/a",
            "https://example.invalid/a",
        ));
        let other =
            normalize_url_for_resolution("https://example.invalid/b").expect("正規化應成功");
        store.seed_owner(identifier(b_id, "url", "https://example.invalid/b", &other));
        let hits = check_url(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "正規化後仍不同的 URL 不該命中，得到 {hits:?}"
        );
    }

    #[tokio::test]
    async fn check_url_owner_is_self_returns_empty() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let a = entity(a_id, EntityType::Url, "https://example.invalid/a");
        let matched =
            normalize_url_for_resolution("https://example.invalid/a").expect("正規化應成功");
        store.seed_listed_identifier(identifier(
            a_id,
            "url",
            "https://example.invalid/a/",
            "https://example.invalid/a/",
        ));
        store.seed_owner(identifier(
            a_id,
            "url",
            "https://example.invalid/a",
            &matched,
        ));
        let hits = check_url(&store, &a).await.expect("check");
        assert!(hits.is_empty(), "owner 是自己不該產生候選，得到 {hits:?}");
    }
}
