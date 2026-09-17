//! SPEC §6 的 `alias`／`domain`／`account_handle` resolution method。
//!
//! 三個函式都由 [`crate::ResolverService::resolve_entity`] 聚合呼叫；也仍以
//! 自由函式公開，方便單測直接斷言組出來的候選。**不寫 candidate 表**——
//! 寫入由 `resolve_entity` 經 [`crate::persist::persist_candidate`] 負責。
//!
//! # `alias` 的空表是預期
//!
//! `check_alias` 讀 `entity_aliases`。V0.2 還沒有生產寫入者，真實 store
//! 通常是空表，會回空 `Vec`。空結果代表「這次沒資料可比」，不要讀成
//! 「確定沒有同一實體」。
//!
//! # `domain` 為什麼不走 `entity_identifiers`
//!
//! 舊版對自己列上的 `namespace="domain"` identifier 做
//! `find_entity_identifier_owner`。`(namespace, normalized_value)` UNIQUE
//! 保證一個值只有一個 owner，而這個 owner 一定就是查詢者自己——
//! **已用真實 PostgreSQL 驗證為結構性死碼**，不是測試 double 種不出資料。
//!
//! 真正該比的訊號已經在 `relationships` 表：entity-worker 抽到 Email／URL
//! 時會衍生 Domain Entity，並寫 `Email --AssociatedWith--> Domain` 或
//! `URL --BelongsTo--> Domain`。兩份文件抽到指向同一個網域的 Email／URL
//! 會因 UUID v5 自然鍵收斂到同一個 Domain Entity，所以「共享網域」這件事
//! 已經完整記錄，resolver 只要走 Relationship 即可。
//!
//! `AssociatedWith`／`BelongsTo` 目前**不是**專屬於 domain 衍生
//! （`RelationshipType` 是共用列舉）。V0.2 只有 entity-worker 會寫這兩種
//! type；之後有其他寫入者要再檢視，否則會把非 domain 的關聯誤當成共用網域。

use std::collections::{HashMap, HashSet};

use chrono::Utc;
use core_model::{
    Entity, EntityId, RESOLUTION_METHODS, Relationship, RelationshipType, ResolutionCandidate,
    ResolutionStatus,
};
use serde_json::json;
use storage_core::RelationalStore;
use uuid::Uuid;

use crate::error::ResolverError;

/// 共用 alias 的分數。同一個顯示名稱出現在兩個 Entity 上是中等訊號，
/// 0.55 夠進 Review，不到自動合併。
pub const ALIAS_SCORE: f64 = 0.55;

/// 共用衍生 Domain Entity 的分數。共用網域基礎設施不代表同一實體
/// （共用主機、轉售、資料髒了都常見），0.30 只夠進 Review。
pub const DOMAIN_SCORE: f64 = 0.30;

/// 跨平台同 handle 的分數。
///
/// 放在 `domain`（0.30）之上、`alias`（0.55）之下：同一個 username 出現在
/// GitHub 與 Twitter 比「剛好共用一台主機」稍強一點，但仍遠不到能自動合併。
/// SPEC §6 明文禁止只因同 username 就判定同一真實人物——0.35 只夠進 Review，
/// evidence 也帶這句警告。
pub const ACCOUNT_HANDLE_SCORE: f64 = 0.35;

const METHOD_ALIAS: &str = RESOLUTION_METHODS[2];
const METHOD_DOMAIN: &str = RESOLUTION_METHODS[3];
const METHOD_ACCOUNT_HANDLE: &str = RESOLUTION_METHODS[5];
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

/// SPEC §6「domain」：兩個 Entity 是否都連到同一個衍生 Domain Entity。
///
/// 走 `list_relationships_by_object`，**不碰** `entity_identifiers`。
/// 舊版 identifier 反查在 UNIQUE 限制下結構性永遠回空（已用真實 PostgreSQL
/// 驗證）；URL 共用也由同一條衍生 Domain 邊表達，所以 `check_url` 已退場。
///
/// 同一對因多個共用 Domain 命中多次時只留第一筆（同 `check_alias`）。
pub async fn check_domain<S: RelationalStore>(
    store: &S,
    entity: &Entity,
) -> Result<Vec<ResolutionCandidate>, ResolverError> {
    let own_rels = store
        .list_relationships_by_object(entity.id, LIST_LIMIT)
        .await?;
    // 同一個 Domain 被兩種 type 連兩次時，後寫入的 type 覆蓋——V0.2 不會發生，
    // 且只影響 evidence 裡「自己那一段」記哪種 type，不影響是否命中。
    let domain_links: HashMap<EntityId, RelationshipType> = own_rels
        .iter()
        .filter(|r| is_domain_link(r.relationship_type))
        .filter_map(|r| other_end(r, entity.id).map(|id| (id, r.relationship_type)))
        .collect();

    let mut seen: HashSet<(EntityId, EntityId)> = HashSet::new();
    let mut out = Vec::new();
    for (domain_id, own_rel_type) in domain_links {
        let domain_rels = store
            .list_relationships_by_object(domain_id, LIST_LIMIT)
            .await?;
        for r in domain_rels {
            if !is_domain_link(r.relationship_type) {
                continue;
            }
            let Some(other_id) = other_end(&r, domain_id) else {
                continue;
            };
            if other_id == entity.id {
                continue;
            }
            let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(entity.id, other_id);
            if !seen.insert((entity_a_id, entity_b_id)) {
                continue;
            }
            out.push(domain_candidate(
                entity.id,
                other_id,
                domain_id,
                own_rel_type,
                r.relationship_type,
            ));
        }
    }
    Ok(out)
}

fn is_domain_link(t: RelationshipType) -> bool {
    matches!(
        t,
        RelationshipType::AssociatedWith | RelationshipType::BelongsTo
    )
}

/// relationship 的兩端，回傳不是 `id` 的那一端；兩端都不是 `id`（不該發生）回 `None`。
fn other_end(r: &Relationship, id: EntityId) -> Option<EntityId> {
    if r.source_object_id == id {
        Some(r.target_object_id)
    } else if r.target_object_id == id {
        Some(r.source_object_id)
    } else {
        None
    }
}

/// 新增 [`RelationshipType`] 變體時這個 match 會編譯失敗。
const fn relationship_type_str(t: RelationshipType) -> &'static str {
    match t {
        RelationshipType::Mentions => "mentions",
        RelationshipType::References => "references",
        RelationshipType::PublishedBy => "published_by",
        RelationshipType::AuthoredBy => "authored_by",
        RelationshipType::LinksTo => "links_to",
        RelationshipType::Affects => "affects",
        RelationshipType::BelongsTo => "belongs_to",
        RelationshipType::MemberOf => "member_of",
        RelationshipType::Owns => "owns",
        RelationshipType::Uses => "uses",
        RelationshipType::LocatedAt => "located_at",
        RelationshipType::AssociatedWith => "associated_with",
        RelationshipType::DerivedFrom => "derived_from",
        RelationshipType::Indicates => "indicates",
        RelationshipType::AttributedTo => "attributed_to",
        RelationshipType::Targets => "targets",
        RelationshipType::Mitigates => "mitigates",
    }
}

/// SPEC §6「account_handle」：同一個 handle 出現在不同平台。
///
/// 只對 [`EntityType::Account`] 有意義。非 Account 直接回空，**不查 store**。
///
/// 步驟：
/// 1. 列出自己的 identifier，只留 namespace 以 `_handle` 結尾的
///    （`github_handle`／`twitter_handle`／`telegram_handle`）。
/// 2. 對每個 handle 的 `normalized_value` 做不限 namespace 的反查。
/// 3. 丟掉自己、丟掉同 namespace（同平台不是這個方法要抓的訊號；
///    生產路徑上 UNIQUE 本來就擋得掉，記憶體 double 沒有 UNIQUE，所以這裡顯式排除）。
/// 4. 丟掉對方 namespace 不是 `_handle` 的列（`email`／`cve` 碰巧同字串不算帳號）。
/// 5. 同一對因多個平台命中多次時只留第一筆。
pub async fn check_account_handle<S: RelationalStore>(
    store: &S,
    entity: &Entity,
) -> Result<Vec<ResolutionCandidate>, ResolverError> {
    if entity.entity_type != core_model::EntityType::Account {
        return Ok(Vec::new());
    }
    let own = store
        .list_entity_identifiers_by_entity(entity.id, LIST_LIMIT)
        .await?;
    let mut seen: HashSet<(EntityId, EntityId)> = HashSet::new();
    let mut out = Vec::new();
    for own_id in own
        .into_iter()
        .filter(|i| is_handle_namespace(&i.namespace))
    {
        let hits = store
            .find_entity_identifiers_by_normalized_value(&own_id.normalized_value, LIST_LIMIT)
            .await?;
        for other in hits {
            if other.entity_id == entity.id {
                continue;
            }
            if other.namespace == own_id.namespace {
                continue;
            }
            if !is_handle_namespace(&other.namespace) {
                continue;
            }
            let (entity_a_id, entity_b_id) =
                ResolutionCandidate::ordered_pair(entity.id, other.entity_id);
            if !seen.insert((entity_a_id, entity_b_id)) {
                continue;
            }
            let (entity_a_namespace, entity_b_namespace) = if entity_a_id == entity.id {
                (own_id.namespace.clone(), other.namespace.clone())
            } else {
                (other.namespace.clone(), own_id.namespace.clone())
            };
            out.push(ResolutionCandidate {
                id: Uuid::now_v7(),
                entity_a_id,
                entity_b_id,
                score: ACCOUNT_HANDLE_SCORE,
                method: METHOD_ACCOUNT_HANDLE.to_string(),
                evidence: json!({
                    "method": METHOD_ACCOUNT_HANDLE,
                    "handle": own_id.normalized_value,
                    "entity_a_namespace": entity_a_namespace,
                    "entity_b_namespace": entity_b_namespace,
                    "warning": "SPEC §6 禁止僅依 username 判定同人",
                }),
                status: ResolutionStatus::Pending,
                created_at: Utc::now(),
                reviewed_at: None,
            });
        }
    }
    Ok(out)
}

fn is_handle_namespace(namespace: &str) -> bool {
    namespace.ends_with("_handle")
}

fn domain_candidate(
    entity_id: EntityId,
    other_id: EntityId,
    domain_id: EntityId,
    own_rel_type: RelationshipType,
    other_rel_type: RelationshipType,
) -> ResolutionCandidate {
    let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(entity_id, other_id);
    ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id,
        entity_b_id,
        score: DOMAIN_SCORE,
        method: METHOD_DOMAIN.to_string(),
        evidence: json!({
            "method": METHOD_DOMAIN,
            "shared_domain_entity_id": domain_id,
            "relationship_types": [
                relationship_type_str(own_rel_type),
                relationship_type_str(other_rel_type),
            ],
        }),
        status: ResolutionStatus::Pending,
        created_at: Utc::now(),
        reviewed_at: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use chrono::{TimeZone, Utc};
    use core_model::{
        AiRun, AiRunId, Candidate, CandidateEvidence, CandidateEvidenceId, CandidateId,
        CandidateStatus, Collection, CollectionId, Connector, ConnectorId, Document, DocumentId,
        DocumentType, DuplicateGroup, DuplicateGroupId, Embedding, EmbeddingTarget, EntityAlias,
        EntityAliasId, EntityExtraction, EntityExtractionId, EntityId, EntityIdentifier,
        EntityIdentifierId, EntityType, Event, EventId, FailedEvent, FailedEventId, Job, JobId,
        JobStatus, MergeHistory, MergeHistoryId, NetworkRule, NetworkRuleId, ObjectId, Provenance,
        ProvenanceId, RawEvidence, RawEvidenceId, Relationship, RelationshipEvidence,
        RelationshipEvidenceId, RelationshipId, RelationshipType, ResolutionCandidateId, Seed,
        SeedId, Source, SourceId,
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
            merged_into: None,
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

    fn relationship(
        source: EntityId,
        rel_type: RelationshipType,
        target: EntityId,
    ) -> Relationship {
        Relationship {
            id: Uuid::now_v7(),
            source_object_id: source,
            relationship_type: rel_type,
            target_object_id: target,
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
            evidence_count: 1,
            created_at: ts(),
            updated_at: ts(),
        }
    }

    /// alias／domain／account_handle 單元測試用的最小 RelationalStore。
    ///
    /// 只實作 alias／relationship／identifier 相關查詢。其餘方法回
    /// `UnsupportedCapability`。
    struct MemoryStore {
        inner: Mutex<Inner>,
    }

    struct Inner {
        aliases: Vec<EntityAlias>,
        relationships: Vec<Relationship>,
        identifiers: Vec<EntityIdentifier>,
        list_identifier_calls: u32,
        find_by_value_calls: u32,
    }

    impl MemoryStore {
        fn new() -> Self {
            Self {
                inner: Mutex::new(Inner {
                    aliases: Vec::new(),
                    relationships: Vec::new(),
                    identifiers: Vec::new(),
                    list_identifier_calls: 0,
                    find_by_value_calls: 0,
                }),
            }
        }

        fn seed_alias(&self, row: EntityAlias) {
            self.inner.lock().expect("mutex").aliases.push(row);
        }

        fn seed_relationship(&self, row: Relationship) {
            self.inner.lock().expect("mutex").relationships.push(row);
        }

        fn seed_identifier(&self, row: EntityIdentifier) {
            self.inner.lock().expect("mutex").identifiers.push(row);
        }

        fn identifier_query_counts(&self) -> (u32, u32) {
            let inner = self.inner.lock().expect("mutex");
            (inner.list_identifier_calls, inner.find_by_value_calls)
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
            entity_id: EntityId,
            limit: u32,
        ) -> Result<Vec<EntityIdentifier>, StorageError> {
            let mut inner = self.inner.lock().expect("mutex");
            inner.list_identifier_calls += 1;
            let mut items: Vec<EntityIdentifier> = inner
                .identifiers
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
            _: &str,
            _: &str,
        ) -> Result<Option<EntityIdentifier>, StorageError> {
            Self::unsupported("find_entity_identifier_owner")
        }
        async fn find_entity_identifiers_by_normalized_value(
            &self,
            normalized_value: &str,
            limit: u32,
        ) -> Result<Vec<EntityIdentifier>, StorageError> {
            let mut inner = self.inner.lock().expect("mutex");
            inner.find_by_value_calls += 1;
            let mut items: Vec<EntityIdentifier> = inner
                .identifiers
                .iter()
                .filter(|i| i.normalized_value == normalized_value)
                .cloned()
                .collect();
            items.sort_by_key(|a| a.id);
            items.truncate(limit as usize);
            Ok(items)
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
        async fn list_resolution_candidates_by_entity(
            &self,
            _: EntityId,
            _: Option<ResolutionStatus>,
            _: Option<ResolutionCandidateId>,
            _: u32,
        ) -> Result<Vec<ResolutionCandidate>, StorageError> {
            Self::unsupported("list_resolution_candidates_by_entity")
        }
        async fn update_resolution_candidate_status(
            &self,
            _: ResolutionCandidateId,
            _: ResolutionStatus,
            _: chrono::DateTime<Utc>,
        ) -> Result<bool, StorageError> {
            Self::unsupported("update_resolution_candidate_status")
        }
        async fn put_seed(&self, _: &Seed) -> Result<(), StorageError> {
            Self::unsupported("put_seed")
        }
        async fn get_seed(&self, _: SeedId) -> Result<Option<Seed>, StorageError> {
            Self::unsupported("get_seed")
        }
        async fn list_seeds(
            &self,
            _: Option<&str>,
            _: Option<SeedId>,
            _: u32,
        ) -> Result<Vec<Seed>, StorageError> {
            Self::unsupported("list_seeds")
        }
        async fn list_seeds_by_collection(
            &self,
            _: CollectionId,
            _: Option<&str>,
            _: Option<SeedId>,
            _: u32,
        ) -> Result<Vec<Seed>, StorageError> {
            Self::unsupported("list_seeds_by_collection")
        }
        async fn update_seed_status(&self, _: SeedId, _: &str) -> Result<bool, StorageError> {
            Self::unsupported("update_seed_status")
        }
        async fn put_candidate(&self, _: &Candidate) -> Result<(), StorageError> {
            Self::unsupported("put_candidate")
        }
        async fn get_candidate(&self, _: CandidateId) -> Result<Option<Candidate>, StorageError> {
            Self::unsupported("get_candidate")
        }
        async fn list_candidates(
            &self,
            _: Option<CandidateStatus>,
            _: Option<CandidateId>,
            _: u32,
        ) -> Result<Vec<Candidate>, StorageError> {
            Self::unsupported("list_candidates")
        }
        async fn list_candidates_by_collection(
            &self,
            _: CollectionId,
            _: Option<CandidateStatus>,
            _: Option<CandidateId>,
            _: u32,
        ) -> Result<Vec<Candidate>, StorageError> {
            Self::unsupported("list_candidates_by_collection")
        }
        async fn update_candidate_status(
            &self,
            _: CandidateId,
            _: CandidateStatus,
            _: chrono::DateTime<Utc>,
        ) -> Result<bool, StorageError> {
            Self::unsupported("update_candidate_status")
        }
        async fn put_candidate_evidence(&self, _: &CandidateEvidence) -> Result<(), StorageError> {
            Self::unsupported("put_candidate_evidence")
        }
        async fn get_candidate_evidence(
            &self,
            _: CandidateEvidenceId,
        ) -> Result<Option<CandidateEvidence>, StorageError> {
            Self::unsupported("get_candidate_evidence")
        }
        async fn list_candidate_evidence_by_candidate(
            &self,
            _: CandidateId,
            _: u32,
        ) -> Result<Vec<CandidateEvidence>, StorageError> {
            Self::unsupported("list_candidate_evidence_by_candidate")
        }
        async fn put_ai_run(&self, _: &AiRun) -> Result<(), StorageError> {
            Self::unsupported("put_ai_run")
        }
        async fn get_ai_run(&self, _: AiRunId) -> Result<Option<AiRun>, StorageError> {
            Self::unsupported("get_ai_run")
        }
        async fn list_ai_runs(
            &self,
            _: Option<&str>,
            _: Option<AiRunId>,
            _: u32,
        ) -> Result<Vec<AiRun>, StorageError> {
            Self::unsupported("list_ai_runs")
        }
        async fn put_collection_budget(
            &self,
            _: &core_model::CollectionBudget,
        ) -> Result<(), StorageError> {
            Self::unsupported("put_collection_budget")
        }
        async fn get_collection_budget(
            &self,
            _: CollectionId,
        ) -> Result<Option<core_model::CollectionBudget>, StorageError> {
            Self::unsupported("get_collection_budget")
        }
        async fn try_consume_daily_request_budget(
            &self,
            _: CollectionId,
            _: chrono::NaiveDate,
            _: i64,
            _: i64,
        ) -> Result<storage_core::BudgetConsumption, StorageError> {
            Self::unsupported("try_consume_daily_request_budget")
        }
        async fn try_consume_daily_ai_budget(
            &self,
            _: CollectionId,
            _: chrono::NaiveDate,
            _: i64,
            _: i64,
        ) -> Result<storage_core::BudgetConsumption, StorageError> {
            Self::unsupported("try_consume_daily_ai_budget")
        }
        async fn get_daily_usage(
            &self,
            _: CollectionId,
            _: chrono::NaiveDate,
        ) -> Result<storage_core::DailyUsage, StorageError> {
            Self::unsupported("get_daily_usage")
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
        async fn put_embedding(&self, _: &Embedding) -> Result<(), StorageError> {
            Self::unsupported("put_embedding")
        }
        async fn find_embedding(
            &self,
            _: ObjectId,
            _: EmbeddingTarget,
            _: &str,
            _: &str,
        ) -> Result<Option<Embedding>, StorageError> {
            Self::unsupported("find_embedding")
        }
        async fn list_embeddings_by_target(
            &self,
            _: ObjectId,
            _: EmbeddingTarget,
            _: u32,
        ) -> Result<Vec<Embedding>, StorageError> {
            Self::unsupported("list_embeddings_by_target")
        }
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
    async fn check_domain_two_emails_sharing_domain_yields_candidate() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let domain_id = Uuid::from_u128(0xdddd);
        let a = entity(a_id, EntityType::Email, "alice@example.com");
        store.seed_relationship(relationship(
            a_id,
            RelationshipType::AssociatedWith,
            domain_id,
        ));
        store.seed_relationship(relationship(
            b_id,
            RelationshipType::AssociatedWith,
            domain_id,
        ));
        let hits = check_domain(&store, &a).await.expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        assert_eq!(c.entity_a_id, a_id);
        assert_eq!(c.entity_b_id, b_id);
        assert!(c.entity_a_id < c.entity_b_id);
        assert_eq!(c.score, DOMAIN_SCORE);
        assert_eq!(c.method, "domain");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert!(c.reviewed_at.is_none());
        assert_eq!(c.evidence["method"], "domain");
        assert_eq!(c.evidence["shared_domain_entity_id"], domain_id.to_string());
        assert_eq!(
            c.evidence["relationship_types"],
            json!(["associated_with", "associated_with"])
        );
    }

    #[tokio::test]
    async fn check_domain_only_self_linked_returns_empty() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let domain_id = Uuid::from_u128(0xdddd);
        let a = entity(a_id, EntityType::Email, "lonely@example.com");
        store.seed_relationship(relationship(
            a_id,
            RelationshipType::AssociatedWith,
            domain_id,
        ));
        let hits = check_domain(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "只有自己連到 Domain 不該產生候選，得到 {hits:?}"
        );
    }

    #[tokio::test]
    async fn check_domain_mentions_is_not_a_domain_link() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let domain_id = Uuid::from_u128(0xdddd);
        let a = entity(a_id, EntityType::Email, "alice@example.com");
        store.seed_relationship(relationship(a_id, RelationshipType::Mentions, domain_id));
        store.seed_relationship(relationship(b_id, RelationshipType::Mentions, domain_id));
        let hits = check_domain(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "Mentions 不該被當成 domain 衍生邊，得到 {hits:?}"
        );
    }

    #[tokio::test]
    async fn check_domain_email_and_url_sharing_domain_records_both_types() {
        let store = MemoryStore::new();
        let email_id = Uuid::from_u128(0x1111);
        let url_id = Uuid::from_u128(0x2222);
        let domain_id = Uuid::from_u128(0xdddd);
        let email = entity(email_id, EntityType::Email, "alice@example.com");
        store.seed_relationship(relationship(
            email_id,
            RelationshipType::AssociatedWith,
            domain_id,
        ));
        store.seed_relationship(relationship(url_id, RelationshipType::BelongsTo, domain_id));
        let hits = check_domain(&store, &email).await.expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        assert_eq!(
            hits[0].evidence["relationship_types"],
            json!(["associated_with", "belongs_to"]),
            "第一段是查詢端（Email），第二段是另一端（URL）"
        );
    }

    fn identifier(entity_id: EntityId, namespace: &str, handle: &str) -> EntityIdentifier {
        EntityIdentifier {
            id: Uuid::now_v7(),
            entity_id,
            namespace: namespace.into(),
            value: handle.into(),
            normalized_value: handle.to_ascii_lowercase(),
            confidence: 0.75,
            source_id: None,
            first_seen: ts(),
            last_seen: ts(),
        }
    }

    #[tokio::test]
    async fn check_account_handle_cross_platform_same_handle_yields_candidate() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Account, "github:alice");
        store.seed_identifier(identifier(a_id, "github_handle", "alice"));
        store.seed_identifier(identifier(b_id, "twitter_handle", "alice"));
        let hits = check_account_handle(&store, &a).await.expect("check");
        assert_eq!(hits.len(), 1, "{hits:?}");
        let c = &hits[0];
        assert_eq!(c.entity_a_id, a_id);
        assert_eq!(c.entity_b_id, b_id);
        assert_eq!(c.score, ACCOUNT_HANDLE_SCORE);
        assert_eq!(c.method, "account_handle");
        assert_eq!(c.status, ResolutionStatus::Pending);
        assert_eq!(c.evidence["method"], "account_handle");
        assert_eq!(c.evidence["handle"], "alice");
        assert_eq!(c.evidence["entity_a_namespace"], "github_handle");
        assert_eq!(c.evidence["entity_b_namespace"], "twitter_handle");
        assert_eq!(c.evidence["warning"], "SPEC §6 禁止僅依 username 判定同人");
    }

    #[tokio::test]
    async fn check_account_handle_same_entity_two_platforms_is_not_a_candidate() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let a = entity(a_id, EntityType::Account, "github:alice");
        store.seed_identifier(identifier(a_id, "github_handle", "alice"));
        store.seed_identifier(identifier(a_id, "twitter_handle", "alice"));
        let hits = check_account_handle(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "同一個 Entity 自己跨平台掛同一個 handle 不算候選，得到 {hits:?}"
        );
    }

    #[tokio::test]
    async fn check_account_handle_non_account_does_not_query_store() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let a = entity(a_id, EntityType::Person, "alice");
        store.seed_identifier(identifier(a_id, "github_handle", "alice"));
        let hits = check_account_handle(&store, &a).await.expect("check");
        assert!(hits.is_empty(), "非 Account 應直接回空，得到 {hits:?}");
        assert_eq!(
            store.identifier_query_counts(),
            (0, 0),
            "非 Account 連 identifier 查詢都不該發，否則空結果可能只是碰巧沒資料"
        );
    }

    #[tokio::test]
    async fn check_account_handle_same_namespace_is_not_cross_platform() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Account, "github:alice");
        store.seed_identifier(identifier(a_id, "github_handle", "alice"));
        store.seed_identifier(identifier(b_id, "github_handle", "alice"));
        let hits = check_account_handle(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "同平台同 handle 不是 account_handle 要抓的訊號（生產路徑由 UNIQUE 擋）；\
             記憶體 double 沒有 UNIQUE，必須顯式排除，得到 {hits:?}"
        );
    }

    #[tokio::test]
    async fn check_account_handle_ignores_non_handle_namespaces() {
        let store = MemoryStore::new();
        let a_id = Uuid::from_u128(0x1111);
        let b_id = Uuid::from_u128(0x2222);
        let a = entity(a_id, EntityType::Account, "github:alice");
        store.seed_identifier(identifier(a_id, "github_handle", "alice"));
        store.seed_identifier(identifier(b_id, "email", "alice"));
        let hits = check_account_handle(&store, &a).await.expect("check");
        assert!(
            hits.is_empty(),
            "email namespace 碰巧同字串不算跨平台帳號，得到 {hits:?}"
        );
    }
}
