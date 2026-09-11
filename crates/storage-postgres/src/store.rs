use std::time::Duration;

use async_trait::async_trait;
use core_model::{
    Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DuplicateGroup,
    DuplicateGroupId, Entity, EntityExtraction, EntityExtractionId, EntityId, Event, EventId, Job,
    JobId, NetworkRule, NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence,
    RawEvidenceId, Relationship, RelationshipEvidence, RelationshipEvidenceId, RelationshipId,
    Source, SourceId,
};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::{PgPoolOptions, PgRow};
use storage_core::codec::encode_enum;
use storage_core::{
    CanonicalStore, CapabilityDescriptor, HealthProvider, RelationalStore, SimhashCandidate,
    StorageAdapter, StorageError, StorageHealth,
};

use crate::error::map_sqlx;
use crate::mapping;

/// list cursor 分頁的每頁上限。呼叫端傳 0 或超大值都夾回 1..=100，避免無界查詢。
fn clamp_limit(limit: u32) -> i64 {
    i64::from(limit.clamp(1, 100))
}

/// Dedup Stage 4 的掃描上限。比 `clamp_limit` 寬（SimHash 沒有等值索引可走，
/// 掃描範圍太小會漏掉候選），但仍然是硬上限，不接受「不限」。
fn clamp_scan_limit(limit: u32) -> i64 {
    i64::from(limit.clamp(1, 5_000))
}

fn ports_json(ports: Option<&[u16]>) -> Result<Option<Value>, StorageError> {
    match ports {
        None => Ok(None),
        Some(ports) => serde_json::to_value(ports)
            .map(Some)
            .map_err(|err| StorageError::Unknown {
                backend: "postgres",
                message: format!("序列化 source_network_rules.ports 失敗：{err}"),
            }),
    }
}

/// PostgreSQL canonical + relational store。
#[derive(Debug, Clone)]
pub struct PostgresCanonicalStore {
    pool: PgPool,
}

impl PostgresCanonicalStore {
    /// 建立有界連線池。不會自動跑 migration。
    pub async fn connect(dsn: &str, pool_max: u32) -> Result<Self, StorageError> {
        if pool_max == 0 {
            return Err(StorageError::Configuration {
                message: "storage.canonical.pool_max 不可為 0".into(),
            });
        }
        let pool = PgPoolOptions::new()
            .max_connections(pool_max)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Some(Duration::from_secs(60)))
            .connect(dsn)
            .await
            .map_err(map_sqlx)?;
        Ok(Self { pool })
    }

    /// 跑 `migrations/postgres`。已套用過的版本會被 sqlx 跳過。
    pub async fn migrate(&self) -> Result<(), StorageError> {
        sqlx::migrate!("../../migrations/postgres")
            .run(&self.pool)
            .await
            .map_err(|err| StorageError::MigrationRequired {
                message: format!("Postgres migration 失敗：{err}。請確認 migrations/postgres 存在且帳號有 DDL 權限"),
            })
    }

    #[must_use]
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    async fn fetch_optional_mapped<T, F>(
        &self,
        sql: &'static str,
        id: uuid::Uuid,
        map: F,
    ) -> Result<Option<T>, StorageError>
    where
        F: Fn(&PgRow) -> Result<T, StorageError>,
    {
        let row = sqlx::query(sql)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|r| map(&r)).transpose()
    }

    async fn delete_id(&self, sql: &'static str, id: uuid::Uuid) -> Result<bool, StorageError> {
        let result = sqlx::query(sql)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected() > 0)
    }

    /// Stage 1／2／3 的候選查詢共用：一個字串鍵 + 「只看 id 比 before 小的」+ 上限。
    async fn dedup_candidate_ids(
        &self,
        sql: &'static str,
        key: &str,
        before: uuid::Uuid,
        limit: u32,
    ) -> Result<Vec<uuid::Uuid>, StorageError> {
        let rows: Vec<(uuid::Uuid,)> = sqlx::query_as(sql)
            .bind(key)
            .bind(before)
            .bind(clamp_limit(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    async fn link(
        &self,
        sql: &'static str,
        left: uuid::Uuid,
        right: uuid::Uuid,
    ) -> Result<(), StorageError> {
        sqlx::query(sql)
            .bind(left)
            .bind(right)
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }
}

#[async_trait]
impl HealthProvider for PostgresCanonicalStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let (one,): (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)?;
        if one != 1 {
            return Ok(StorageHealth::down("postgres", "SELECT 1 沒有回 1"));
        }
        Ok(
            StorageHealth::ok("postgres", "SELECT 1 成功").with_details(serde_json::json!({
                "pool_size": self.pool.size(),
                "pool_idle": self.pool.num_idle(),
            })),
        )
    }
}

impl StorageAdapter for PostgresCanonicalStore {
    fn descriptor(&self) -> CapabilityDescriptor {
        CapabilityDescriptor::new(
            "postgres",
            env!("CARGO_PKG_VERSION"),
            &["canonical", "relational"],
        )
        .with_feature("json", Value::Bool(true))
        .with_feature("high_concurrent_write", Value::Bool(true))
        .with_feature("bulk_write", Value::Bool(true))
    }
}

#[async_trait]
impl CanonicalStore for PostgresCanonicalStore {
    fn canonical_backend_id(&self) -> &'static str {
        "postgres"
    }
}

#[async_trait]
impl RelationalStore for PostgresCanonicalStore {
    async fn put_source(&self, source: &Source) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO sources (
                id, name, source_type, platform, base_url, description, language, country,
                enabled, collection_policy, created_at, updated_at, last_seen
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                source_type = EXCLUDED.source_type,
                platform = EXCLUDED.platform,
                base_url = EXCLUDED.base_url,
                description = EXCLUDED.description,
                language = EXCLUDED.language,
                country = EXCLUDED.country,
                enabled = EXCLUDED.enabled,
                collection_policy = EXCLUDED.collection_policy,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                last_seen = EXCLUDED.last_seen
            "#,
        )
        .bind(source.id)
        .bind(&source.name)
        .bind(encode_enum(&source.source_type)?)
        .bind(&source.platform)
        .bind(&source.base_url)
        .bind(&source.description)
        .bind(&source.language)
        .bind(&source.country)
        .bind(source.enabled)
        .bind(&source.collection_policy)
        .bind(source.created_at)
        .bind(source.updated_at)
        .bind(source.last_seen)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_source(&self, id: SourceId) -> Result<Option<Source>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM sources WHERE id = $1", id, mapping::source)
            .await
    }

    async fn delete_source(&self, id: SourceId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM sources WHERE id = $1", id)
            .await
    }

    async fn list_sources(
        &self,
        after: Option<SourceId>,
        limit: u32,
    ) -> Result<Vec<Source>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM sources
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::source).collect()
    }

    async fn put_network_rule(&self, rule: &NetworkRule) -> Result<(), StorageError> {
        let ports = ports_json(rule.ports.as_deref())?;
        sqlx::query(
            r#"
            INSERT INTO source_network_rules (
                id, source_id, cidr_or_host, ports, reason, approved_by,
                expires_at, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                source_id = EXCLUDED.source_id,
                cidr_or_host = EXCLUDED.cidr_or_host,
                ports = EXCLUDED.ports,
                reason = EXCLUDED.reason,
                approved_by = EXCLUDED.approved_by,
                expires_at = EXCLUDED.expires_at,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(rule.id)
        .bind(rule.source_id)
        .bind(&rule.cidr_or_host)
        .bind(&ports)
        .bind(&rule.reason)
        .bind(&rule.approved_by)
        .bind(rule.expires_at)
        .bind(rule.created_at)
        .bind(rule.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_network_rule(
        &self,
        id: NetworkRuleId,
    ) -> Result<Option<NetworkRule>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM source_network_rules WHERE id = $1",
            id,
            mapping::network_rule,
        )
        .await
    }

    async fn list_network_rules(
        &self,
        source_id: SourceId,
    ) -> Result<Vec<NetworkRule>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM source_network_rules WHERE source_id = $1 ORDER BY created_at, id",
        )
        .bind(source_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::network_rule).collect()
    }

    async fn delete_network_rule(&self, id: NetworkRuleId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM source_network_rules WHERE id = $1", id)
            .await
    }

    async fn put_connector(&self, connector: &Connector) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO connectors (
                id, source_id, name, "type", version, enabled, configuration, credential_reference,
                schedule, rate_limit, timeout, proxy_reference, checkpoint, last_run, last_success,
                status, error_count
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
            ON CONFLICT (id) DO UPDATE SET
                source_id = EXCLUDED.source_id,
                name = EXCLUDED.name,
                "type" = EXCLUDED."type",
                version = EXCLUDED.version,
                enabled = EXCLUDED.enabled,
                configuration = EXCLUDED.configuration,
                credential_reference = EXCLUDED.credential_reference,
                schedule = EXCLUDED.schedule,
                rate_limit = EXCLUDED.rate_limit,
                timeout = EXCLUDED.timeout,
                proxy_reference = EXCLUDED.proxy_reference,
                checkpoint = EXCLUDED.checkpoint,
                last_run = EXCLUDED.last_run,
                last_success = EXCLUDED.last_success,
                status = EXCLUDED.status,
                error_count = EXCLUDED.error_count
            "#,
        )
        .bind(connector.id)
        .bind(connector.source_id)
        .bind(&connector.name)
        .bind(&connector.connector_type)
        .bind(&connector.version)
        .bind(connector.enabled)
        .bind(&connector.configuration)
        .bind(&connector.credential_reference)
        .bind(&connector.schedule)
        .bind(&connector.rate_limit)
        .bind(&connector.timeout)
        .bind(&connector.proxy_reference)
        .bind(&connector.checkpoint)
        .bind(connector.last_run)
        .bind(connector.last_success)
        .bind(&connector.status)
        .bind(connector.error_count)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_connector(&self, id: ConnectorId) -> Result<Option<Connector>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM connectors WHERE id = $1",
            id,
            mapping::connector,
        )
        .await
    }

    async fn delete_connector(&self, id: ConnectorId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM connectors WHERE id = $1", id)
            .await
    }

    async fn list_enabled_connectors(&self) -> Result<Vec<Connector>, StorageError> {
        let rows = sqlx::query("SELECT * FROM connectors WHERE enabled = TRUE ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(map_sqlx)?;
        rows.iter().map(mapping::connector).collect()
    }

    async fn list_connectors(
        &self,
        after: Option<ConnectorId>,
        limit: u32,
    ) -> Result<Vec<Connector>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM connectors
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::connector).collect()
    }

    async fn put_collection(&self, collection: &Collection) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO collections (
                id, workspace_id, name, description, status, priority, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                workspace_id = EXCLUDED.workspace_id,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                priority = EXCLUDED.priority,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(collection.id)
        .bind(collection.workspace_id)
        .bind(&collection.name)
        .bind(&collection.description)
        .bind(&collection.status)
        .bind(collection.priority)
        .bind(collection.created_at)
        .bind(collection.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_collection(&self, id: CollectionId) -> Result<Option<Collection>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM collections WHERE id = $1",
            id,
            mapping::collection,
        )
        .await
    }

    async fn delete_collection(&self, id: CollectionId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM collections WHERE id = $1", id)
            .await
    }

    async fn link_collection_source(
        &self,
        collection_id: CollectionId,
        source_id: SourceId,
    ) -> Result<(), StorageError> {
        self.link(
            r#"
            INSERT INTO collection_sources (collection_id, source_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
            collection_id,
            source_id,
        )
        .await
    }

    async fn link_collection_connector(
        &self,
        collection_id: CollectionId,
        connector_id: ConnectorId,
    ) -> Result<(), StorageError> {
        self.link(
            r#"
            INSERT INTO collection_connectors (collection_id, connector_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
            collection_id,
            connector_id,
        )
        .await
    }

    async fn link_collection_object(
        &self,
        collection_id: CollectionId,
        object_id: ObjectId,
    ) -> Result<(), StorageError> {
        self.link(
            r#"
            INSERT INTO collection_objects (collection_id, object_id)
            VALUES ($1, $2)
            ON CONFLICT DO NOTHING
            "#,
            collection_id,
            object_id,
        )
        .await
    }

    async fn insert_raw_evidence(&self, evidence: &RawEvidence) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO raw_evidence (
                id, source_id, connector_id, collection_id, external_id, source_url, retrieved_at,
                content_type, mime_type, content_length, sha256, storage_path, http_status,
                http_headers, metadata, collector_version
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
            "#,
        )
        .bind(evidence.id)
        .bind(evidence.source_id)
        .bind(evidence.connector_id)
        .bind(evidence.collection_id)
        .bind(&evidence.external_id)
        .bind(&evidence.source_url)
        .bind(evidence.retrieved_at)
        .bind(&evidence.content_type)
        .bind(&evidence.mime_type)
        .bind(evidence.content_length)
        .bind(&evidence.sha256)
        .bind(&evidence.storage_path)
        .bind(evidence.http_status)
        .bind(&evidence.http_headers)
        .bind(&evidence.metadata)
        .bind(&evidence.collector_version)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_raw_evidence(
        &self,
        id: RawEvidenceId,
    ) -> Result<Option<RawEvidence>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM raw_evidence WHERE id = $1",
            id,
            mapping::raw_evidence,
        )
        .await
    }

    async fn list_raw_evidence(
        &self,
        after: Option<RawEvidenceId>,
        limit: u32,
    ) -> Result<Vec<RawEvidence>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM raw_evidence
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::raw_evidence).collect()
    }

    async fn list_raw_evidence_by_source(
        &self,
        source_id: SourceId,
        after: Option<RawEvidenceId>,
        limit: u32,
    ) -> Result<Vec<RawEvidence>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM raw_evidence
            WHERE source_id = $1 AND ($2::uuid IS NULL OR id < $2)
            ORDER BY id DESC
            LIMIT $3
            "#,
        )
        .bind(source_id)
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::raw_evidence).collect()
    }

    async fn put_document(&self, document: &Document) -> Result<(), StorageError> {
        let labels =
            serde_json::to_value(&document.labels).map_err(|err| StorageError::Unknown {
                backend: "postgres",
                message: format!("序列化 documents.labels 失敗：{err}"),
            })?;
        sqlx::query(
            r#"
            INSERT INTO documents (
                id, object_type, schema_version, title, body, summary, language, author,
                published_at, modified_at, observed_at, collected_at, source_url, canonical_url,
                normalized_content_hash, confidence, labels, attributes,
                external_key, simhash, duplicate_of
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)
            ON CONFLICT (id) DO UPDATE SET
                object_type = EXCLUDED.object_type,
                schema_version = EXCLUDED.schema_version,
                title = EXCLUDED.title,
                body = EXCLUDED.body,
                summary = EXCLUDED.summary,
                language = EXCLUDED.language,
                author = EXCLUDED.author,
                published_at = EXCLUDED.published_at,
                modified_at = EXCLUDED.modified_at,
                observed_at = EXCLUDED.observed_at,
                collected_at = EXCLUDED.collected_at,
                source_url = EXCLUDED.source_url,
                canonical_url = EXCLUDED.canonical_url,
                normalized_content_hash = EXCLUDED.normalized_content_hash,
                confidence = EXCLUDED.confidence,
                labels = EXCLUDED.labels,
                attributes = EXCLUDED.attributes,
                external_key = EXCLUDED.external_key,
                simhash = EXCLUDED.simhash,
                duplicate_of = EXCLUDED.duplicate_of
            "#,
        )
        .bind(document.id)
        .bind(encode_enum(&document.object_type)?)
        .bind(&document.schema_version)
        .bind(&document.title)
        .bind(&document.body)
        .bind(&document.summary)
        .bind(&document.language)
        .bind(&document.author)
        .bind(document.published_at)
        .bind(document.modified_at)
        .bind(document.observed_at)
        .bind(document.collected_at)
        .bind(&document.source_url)
        .bind(&document.canonical_url)
        .bind(&document.normalized_content_hash)
        .bind(document.confidence)
        .bind(labels)
        .bind(&document.attributes)
        .bind(&document.external_key)
        .bind(document.simhash)
        .bind(document.duplicate_of)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_document(&self, id: DocumentId) -> Result<Option<Document>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM documents WHERE id = $1",
            id,
            mapping::document,
        )
        .await
    }

    async fn delete_document(&self, id: DocumentId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM documents WHERE id = $1", id)
            .await
    }

    async fn list_documents(
        &self,
        after: Option<DocumentId>,
        limit: u32,
    ) -> Result<Vec<Document>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM documents
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::document).collect()
    }

    async fn put_entity(&self, entity: &Entity) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO entities (
                id, entity_type, name, normalized_name, description, confidence,
                first_seen, last_seen, attributes
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                name = EXCLUDED.name,
                normalized_name = EXCLUDED.normalized_name,
                description = EXCLUDED.description,
                confidence = EXCLUDED.confidence,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen,
                attributes = EXCLUDED.attributes
            "#,
        )
        .bind(entity.id)
        .bind(encode_enum(&entity.entity_type)?)
        .bind(&entity.name)
        .bind(&entity.normalized_name)
        .bind(&entity.description)
        .bind(entity.confidence)
        .bind(entity.first_seen)
        .bind(entity.last_seen)
        .bind(&entity.attributes)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM entities WHERE id = $1", id, mapping::entity)
            .await
    }

    async fn delete_entity(&self, id: EntityId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM entities WHERE id = $1", id)
            .await
    }

    async fn put_relationship(&self, relationship: &Relationship) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO relationships (
                id, source_object_id, relationship_type, target_object_id, confidence,
                first_seen, last_seen, evidence_count, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (id) DO UPDATE SET
                source_object_id = EXCLUDED.source_object_id,
                relationship_type = EXCLUDED.relationship_type,
                target_object_id = EXCLUDED.target_object_id,
                confidence = EXCLUDED.confidence,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen,
                evidence_count = EXCLUDED.evidence_count,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(relationship.id)
        .bind(relationship.source_object_id)
        .bind(encode_enum(&relationship.relationship_type)?)
        .bind(relationship.target_object_id)
        .bind(relationship.confidence)
        .bind(relationship.first_seen)
        .bind(relationship.last_seen)
        .bind(relationship.evidence_count)
        .bind(relationship.created_at)
        .bind(relationship.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_relationship(
        &self,
        id: RelationshipId,
    ) -> Result<Option<Relationship>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM relationships WHERE id = $1",
            id,
            mapping::relationship,
        )
        .await
    }

    async fn delete_relationship(&self, id: RelationshipId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM relationships WHERE id = $1", id)
            .await
    }

    async fn put_relationship_evidence(
        &self,
        evidence: &RelationshipEvidence,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO relationship_evidence (
                id, relationship_id, object_id, raw_evidence_id, excerpt, confidence, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (id) DO UPDATE SET
                relationship_id = EXCLUDED.relationship_id,
                object_id = EXCLUDED.object_id,
                raw_evidence_id = EXCLUDED.raw_evidence_id,
                excerpt = EXCLUDED.excerpt,
                confidence = EXCLUDED.confidence,
                created_at = EXCLUDED.created_at
            "#,
        )
        .bind(evidence.id)
        .bind(evidence.relationship_id)
        .bind(evidence.object_id)
        .bind(evidence.raw_evidence_id)
        .bind(&evidence.excerpt)
        .bind(evidence.confidence)
        .bind(evidence.created_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_relationship_evidence(
        &self,
        id: RelationshipEvidenceId,
    ) -> Result<Option<RelationshipEvidence>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM relationship_evidence WHERE id = $1",
            id,
            mapping::relationship_evidence,
        )
        .await
    }

    async fn put_event(&self, event: &Event) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO events (
                id, event_type, title, description, start_time, end_time, confidence,
                status, attributes, created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (id) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                confidence = EXCLUDED.confidence,
                status = EXCLUDED.status,
                attributes = EXCLUDED.attributes,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(event.id)
        .bind(&event.event_type)
        .bind(&event.title)
        .bind(&event.description)
        .bind(event.start_time)
        .bind(event.end_time)
        .bind(event.confidence)
        .bind(&event.status)
        .bind(&event.attributes)
        .bind(event.created_at)
        .bind(event.updated_at)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_event(&self, id: EventId) -> Result<Option<Event>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM events WHERE id = $1", id, mapping::event)
            .await
    }

    async fn delete_event(&self, id: EventId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM events WHERE id = $1", id).await
    }

    async fn put_provenance(&self, provenance: &Provenance) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO provenance (
                id, subject_id, action, parent_id, raw_evidence_id, processor,
                processor_version, timestamp, metadata
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                subject_id = EXCLUDED.subject_id,
                action = EXCLUDED.action,
                parent_id = EXCLUDED.parent_id,
                raw_evidence_id = EXCLUDED.raw_evidence_id,
                processor = EXCLUDED.processor,
                processor_version = EXCLUDED.processor_version,
                timestamp = EXCLUDED.timestamp,
                metadata = EXCLUDED.metadata
            "#,
        )
        .bind(provenance.id)
        .bind(provenance.subject_id)
        .bind(&provenance.action)
        .bind(provenance.parent_id)
        .bind(provenance.raw_evidence_id)
        .bind(&provenance.processor)
        .bind(&provenance.processor_version)
        .bind(provenance.timestamp)
        .bind(&provenance.metadata)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_provenance(&self, id: ProvenanceId) -> Result<Option<Provenance>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM provenance WHERE id = $1",
            id,
            mapping::provenance,
        )
        .await
    }

    async fn list_provenance_by_raw_evidence(
        &self,
        raw_evidence_id: RawEvidenceId,
    ) -> Result<Vec<Provenance>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM provenance WHERE raw_evidence_id = $1 ORDER BY timestamp, id",
        )
        .bind(raw_evidence_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::provenance).collect()
    }

    async fn list_provenance_by_subject(
        &self,
        subject_id: ObjectId,
    ) -> Result<Vec<Provenance>, StorageError> {
        let rows =
            sqlx::query("SELECT * FROM provenance WHERE subject_id = $1 ORDER BY timestamp, id")
                .bind(subject_id)
                .fetch_all(&self.pool)
                .await
                .map_err(map_sqlx)?;
        rows.iter().map(mapping::provenance).collect()
    }

    async fn put_job(&self, job: &Job) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO jobs (
                id, "type", status, correlation_id, created_at, started_at, completed_at,
                retry_count, error
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                "type" = EXCLUDED."type",
                status = EXCLUDED.status,
                correlation_id = EXCLUDED.correlation_id,
                created_at = EXCLUDED.created_at,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                retry_count = EXCLUDED.retry_count,
                error = EXCLUDED.error
            "#,
        )
        .bind(job.id)
        .bind(&job.job_type)
        .bind(encode_enum(&job.status)?)
        .bind(job.correlation_id)
        .bind(job.created_at)
        .bind(job.started_at)
        .bind(job.completed_at)
        .bind(job.retry_count)
        .bind(&job.error)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_job(&self, id: JobId) -> Result<Option<Job>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM jobs WHERE id = $1", id, mapping::job)
            .await
    }

    async fn delete_job(&self, id: JobId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM jobs WHERE id = $1", id).await
    }

    async fn list_jobs(&self, after: Option<JobId>, limit: u32) -> Result<Vec<Job>, StorageError> {
        let limit = i64::from(limit.clamp(1, 100));
        let rows = sqlx::query(
            r#"
            SELECT * FROM jobs
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::job).collect()
    }

    async fn find_document_ids_by_external_key(
        &self,
        external_key: &str,
        before: DocumentId,
        limit: u32,
    ) -> Result<Vec<DocumentId>, StorageError> {
        self.dedup_candidate_ids(
            r#"
            SELECT id FROM documents
            WHERE external_key = $1 AND id < $2
            ORDER BY id ASC
            LIMIT $3
            "#,
            external_key,
            before,
            limit,
        )
        .await
    }

    async fn find_document_ids_by_canonical_url(
        &self,
        canonical_url: &str,
        before: DocumentId,
        limit: u32,
    ) -> Result<Vec<DocumentId>, StorageError> {
        self.dedup_candidate_ids(
            r#"
            SELECT id FROM documents
            WHERE canonical_url = $1 AND id < $2
            ORDER BY id ASC
            LIMIT $3
            "#,
            canonical_url,
            before,
            limit,
        )
        .await
    }

    async fn find_document_ids_by_content_hash(
        &self,
        content_hash: &str,
        before: DocumentId,
        limit: u32,
    ) -> Result<Vec<DocumentId>, StorageError> {
        self.dedup_candidate_ids(
            r#"
            SELECT id FROM documents
            WHERE normalized_content_hash = $1 AND id < $2
            ORDER BY id ASC
            LIMIT $3
            "#,
            content_hash,
            before,
            limit,
        )
        .await
    }

    async fn find_simhash_candidates(
        &self,
        fingerprint: i64,
        max_distance: u32,
        before: DocumentId,
        scan_limit: u32,
    ) -> Result<Vec<SimhashCandidate>, StorageError> {
        // 內層子查詢先把掃描範圍夾成「最近 scan_limit 筆有 fingerprint 的 Document」
        // （走 idx_documents_simhash_recent），外層才算 Hamming 距離。
        // `#` 是 PostgreSQL 的位元 XOR；bit_count 只吃 bit／bytea，所以要先 ::bit(64)。
        // 距離計算留在 DB 端，不把整個掃描範圍搬回程式。
        let rows = sqlx::query(
            r#"
            SELECT c.id, c.simhash
            FROM (
                SELECT id, simhash FROM documents
                WHERE simhash IS NOT NULL AND id < $1
                ORDER BY id DESC
                LIMIT $4
            ) AS c
            WHERE bit_count((c.simhash # $2)::bit(64)) <= $3
            ORDER BY c.id ASC
            "#,
        )
        .bind(before)
        .bind(fingerprint)
        .bind(i64::from(max_distance))
        .bind(clamp_scan_limit(scan_limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::simhash_candidate).collect()
    }

    async fn put_duplicate_group(&self, group: &DuplicateGroup) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO duplicate_groups (
                id, canonical_object_id, member_object_id, member_raw_evidence_id,
                method, similarity, first_seen
            ) VALUES ($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (id) DO UPDATE SET
                canonical_object_id = EXCLUDED.canonical_object_id,
                member_object_id = EXCLUDED.member_object_id,
                member_raw_evidence_id = EXCLUDED.member_raw_evidence_id,
                method = EXCLUDED.method,
                similarity = EXCLUDED.similarity,
                first_seen = EXCLUDED.first_seen
            "#,
        )
        .bind(group.id)
        .bind(group.canonical_object_id)
        .bind(group.member_object_id)
        .bind(group.member_raw_evidence_id)
        .bind(&group.method)
        .bind(group.similarity)
        .bind(group.first_seen)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_duplicate_group(
        &self,
        id: DuplicateGroupId,
    ) -> Result<Option<DuplicateGroup>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM duplicate_groups WHERE id = $1",
            id,
            mapping::duplicate_group,
        )
        .await
    }

    async fn get_duplicate_group_by_member(
        &self,
        member_object_id: ObjectId,
    ) -> Result<Option<DuplicateGroup>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM duplicate_groups WHERE member_object_id = $1",
            member_object_id,
            mapping::duplicate_group,
        )
        .await
    }

    async fn list_duplicate_groups_by_canonical(
        &self,
        canonical_object_id: ObjectId,
        limit: u32,
    ) -> Result<Vec<DuplicateGroup>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM duplicate_groups
            WHERE canonical_object_id = $1
            ORDER BY first_seen ASC, id ASC
            LIMIT $2
            "#,
        )
        .bind(canonical_object_id)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::duplicate_group).collect()
    }

    async fn put_entity_extraction(
        &self,
        extraction: &EntityExtraction,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO entity_extractions (
                id, object_id, entity_id, extractor, extractor_version, confidence,
                text_offset, excerpt
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                object_id = EXCLUDED.object_id,
                entity_id = EXCLUDED.entity_id,
                extractor = EXCLUDED.extractor,
                extractor_version = EXCLUDED.extractor_version,
                confidence = EXCLUDED.confidence,
                text_offset = EXCLUDED.text_offset,
                excerpt = EXCLUDED.excerpt
            "#,
        )
        .bind(extraction.id)
        .bind(extraction.object_id)
        .bind(extraction.entity_id)
        .bind(&extraction.extractor)
        .bind(&extraction.extractor_version)
        .bind(extraction.confidence)
        .bind(extraction.text_offset)
        .bind(&extraction.excerpt)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_entity_extraction(
        &self,
        id: EntityExtractionId,
    ) -> Result<Option<EntityExtraction>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM entity_extractions WHERE id = $1",
            id,
            mapping::entity_extraction,
        )
        .await
    }
}
