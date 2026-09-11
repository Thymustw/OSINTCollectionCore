use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use core_model::{
    Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DuplicateGroup,
    DuplicateGroupId, Entity, EntityExtraction, EntityExtractionId, EntityId, Event, EventId, Job,
    JobId, NetworkRule, NetworkRuleId, ObjectId, Provenance, ProvenanceId, RawEvidence,
    RawEvidenceId, Relationship, RelationshipEvidence, RelationshipEvidenceId, RelationshipId,
    Source, SourceId,
};
use serde_json::Value;
use sqlx::SqlitePool;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
};
use storage_core::codec::encode_enum;
use storage_core::conformance::assert_sqlite_path_safe;
use storage_core::{
    CapabilityDescriptor, EmbeddedStore, HealthProvider, RelationalStore, StorageAdapter,
    StorageError, StorageHealth,
};
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::mapping;

/// SQLite embedded + relational store。
#[derive(Debug, Clone)]
pub struct SqliteEmbeddedStore {
    pool: SqlitePool,
    path: PathBuf,
}

fn rfc3339(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn opt_rfc3339(ts: Option<DateTime<Utc>>) -> Option<String> {
    ts.map(rfc3339)
}

fn json_text(value: &Value) -> String {
    value.to_string()
}

fn uuid_text(id: Uuid) -> String {
    id.to_string()
}

fn opt_uuid_text(id: Option<Uuid>) -> Option<String> {
    id.map(|v| v.to_string())
}

fn bool_int(value: bool) -> i64 {
    i64::from(value)
}

/// list cursor 分頁的每頁上限。呼叫端傳 0 或超大值都夾回 1..=100，避免無界查詢。
fn clamp_limit(limit: u32) -> i64 {
    i64::from(limit.clamp(1, 100))
}

fn ports_text(ports: Option<&[u16]>) -> Result<Option<String>, StorageError> {
    match ports {
        None => Ok(None),
        Some(ports) => {
            serde_json::to_string(ports)
                .map(Some)
                .map_err(|err| StorageError::Unknown {
                    backend: "sqlite",
                    message: format!("序列化 source_network_rules.ports 失敗：{err}"),
                })
        }
    }
}

impl SqliteEmbeddedStore {
    /// 開啟（或建立）SQLite 檔。會設 WAL / foreign_keys / busy_timeout。
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        assert_sqlite_path_safe(path)?;
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(|err| StorageError::Unavailable {
                    backend: "sqlite",
                    message: format!(
                        "無法建立 SQLite 目錄 `{}`：{err}。請確認路徑可寫",
                        parent.display()
                    ),
                })?;
            }
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_millis(5_000));

        // SQLite 寫入串行；維持小池子給讀取。不要假設 Postgres 等級的寫入吞吐。
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(options)
            .await
            .map_err(map_sqlx)?;

        Ok(Self {
            pool,
            path: path.to_path_buf(),
        })
    }

    pub async fn migrate(&self) -> Result<(), StorageError> {
        sqlx::migrate!("../../migrations/sqlite")
            .run(&self.pool)
            .await
            .map_err(|err| StorageError::MigrationRequired {
                message: format!("SQLite migration 失敗：{err}。請確認檔案可寫且 schema 目錄存在"),
            })
    }

    async fn fetch_optional_mapped<T, F>(
        &self,
        sql: &'static str,
        id: Uuid,
        map: F,
    ) -> Result<Option<T>, StorageError>
    where
        F: Fn(&SqliteRow) -> Result<T, StorageError>,
    {
        let row = sqlx::query(sql)
            .bind(uuid_text(id))
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx)?;
        row.map(|r| map(&r)).transpose()
    }

    async fn delete_id(&self, sql: &'static str, id: Uuid) -> Result<bool, StorageError> {
        let result = sqlx::query(sql)
            .bind(uuid_text(id))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected() > 0)
    }

    async fn link(&self, sql: &'static str, left: Uuid, right: Uuid) -> Result<(), StorageError> {
        sqlx::query(sql)
            .bind(uuid_text(left))
            .bind(uuid_text(right))
            .execute(&self.pool)
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }
}

#[async_trait]
impl HealthProvider for SqliteEmbeddedStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let (one,): (i64,) = sqlx::query_as("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(map_sqlx)?;
        if one != 1 {
            return Ok(StorageHealth::down("sqlite", "SELECT 1 沒有回 1"));
        }
        Ok(
            StorageHealth::ok("sqlite", "SELECT 1 成功").with_details(serde_json::json!({
                "path": self.path.display().to_string(),
            })),
        )
    }
}

impl StorageAdapter for SqliteEmbeddedStore {
    fn descriptor(&self) -> CapabilityDescriptor {
        CapabilityDescriptor::new(
            "sqlite",
            env!("CARGO_PKG_VERSION"),
            &["embedded", "relational"],
        )
        .with_feature("json", Value::Bool(true))
        .with_feature("high_concurrent_write", Value::Bool(false))
        .with_feature("bulk_write", Value::Bool(true))
    }
}

#[async_trait]
impl EmbeddedStore for SqliteEmbeddedStore {
    fn database_path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl RelationalStore for SqliteEmbeddedStore {
    async fn put_source(&self, source: &Source) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO sources (
                id, name, source_type, platform, base_url, description, language, country,
                enabled, collection_policy, created_at, updated_at, last_seen
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                name = excluded.name,
                source_type = excluded.source_type,
                platform = excluded.platform,
                base_url = excluded.base_url,
                description = excluded.description,
                language = excluded.language,
                country = excluded.country,
                enabled = excluded.enabled,
                collection_policy = excluded.collection_policy,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                last_seen = excluded.last_seen
            "#,
        )
        .bind(uuid_text(source.id))
        .bind(&source.name)
        .bind(encode_enum(&source.source_type)?)
        .bind(&source.platform)
        .bind(&source.base_url)
        .bind(&source.description)
        .bind(&source.language)
        .bind(&source.country)
        .bind(bool_int(source.enabled))
        .bind(json_text(&source.collection_policy))
        .bind(rfc3339(source.created_at))
        .bind(rfc3339(source.updated_at))
        .bind(opt_rfc3339(source.last_seen))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_source(&self, id: SourceId) -> Result<Option<Source>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM sources WHERE id = ?", id, mapping::source)
            .await
    }

    async fn delete_source(&self, id: SourceId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM sources WHERE id = ?", id).await
    }

    async fn list_sources(
        &self,
        after: Option<SourceId>,
        limit: u32,
    ) -> Result<Vec<Source>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM sources
            WHERE (?1 IS NULL OR id < ?1)
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )
        .bind(opt_uuid_text(after))
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::source).collect()
    }

    async fn put_network_rule(&self, rule: &NetworkRule) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO source_network_rules (
                id, source_id, cidr_or_host, ports, reason, approved_by,
                expires_at, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                source_id = excluded.source_id,
                cidr_or_host = excluded.cidr_or_host,
                ports = excluded.ports,
                reason = excluded.reason,
                approved_by = excluded.approved_by,
                expires_at = excluded.expires_at,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(uuid_text(rule.id))
        .bind(uuid_text(rule.source_id))
        .bind(&rule.cidr_or_host)
        .bind(ports_text(rule.ports.as_deref())?)
        .bind(&rule.reason)
        .bind(&rule.approved_by)
        .bind(opt_rfc3339(rule.expires_at))
        .bind(rfc3339(rule.created_at))
        .bind(rfc3339(rule.updated_at))
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
            "SELECT * FROM source_network_rules WHERE id = ?",
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
            "SELECT * FROM source_network_rules WHERE source_id = ? ORDER BY created_at, id",
        )
        .bind(uuid_text(source_id))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::network_rule).collect()
    }

    async fn delete_network_rule(&self, id: NetworkRuleId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM source_network_rules WHERE id = ?", id)
            .await
    }

    async fn put_connector(&self, connector: &Connector) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO connectors (
                id, source_id, name, "type", version, enabled, configuration, credential_reference,
                schedule, rate_limit, timeout, proxy_reference, checkpoint, last_run, last_success,
                status, error_count
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                source_id = excluded.source_id,
                name = excluded.name,
                "type" = excluded."type",
                version = excluded.version,
                enabled = excluded.enabled,
                configuration = excluded.configuration,
                credential_reference = excluded.credential_reference,
                schedule = excluded.schedule,
                rate_limit = excluded.rate_limit,
                timeout = excluded.timeout,
                proxy_reference = excluded.proxy_reference,
                checkpoint = excluded.checkpoint,
                last_run = excluded.last_run,
                last_success = excluded.last_success,
                status = excluded.status,
                error_count = excluded.error_count
            "#,
        )
        .bind(uuid_text(connector.id))
        .bind(uuid_text(connector.source_id))
        .bind(&connector.name)
        .bind(&connector.connector_type)
        .bind(&connector.version)
        .bind(bool_int(connector.enabled))
        .bind(json_text(&connector.configuration))
        .bind(&connector.credential_reference)
        .bind(&connector.schedule)
        .bind(json_text(&connector.rate_limit))
        .bind(json_text(&connector.timeout))
        .bind(&connector.proxy_reference)
        .bind(json_text(&connector.checkpoint))
        .bind(opt_rfc3339(connector.last_run))
        .bind(opt_rfc3339(connector.last_success))
        .bind(&connector.status)
        .bind(i64::from(connector.error_count))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_connector(&self, id: ConnectorId) -> Result<Option<Connector>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM connectors WHERE id = ?",
            id,
            mapping::connector,
        )
        .await
    }

    async fn delete_connector(&self, id: ConnectorId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM connectors WHERE id = ?", id)
            .await
    }

    async fn list_enabled_connectors(&self) -> Result<Vec<Connector>, StorageError> {
        let rows = sqlx::query("SELECT * FROM connectors WHERE enabled = 1 ORDER BY id")
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
            WHERE (?1 IS NULL OR id < ?1)
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )
        .bind(opt_uuid_text(after))
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
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                workspace_id = excluded.workspace_id,
                name = excluded.name,
                description = excluded.description,
                status = excluded.status,
                priority = excluded.priority,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(uuid_text(collection.id))
        .bind(opt_uuid_text(collection.workspace_id))
        .bind(&collection.name)
        .bind(&collection.description)
        .bind(&collection.status)
        .bind(i64::from(collection.priority))
        .bind(rfc3339(collection.created_at))
        .bind(rfc3339(collection.updated_at))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_collection(&self, id: CollectionId) -> Result<Option<Collection>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM collections WHERE id = ?",
            id,
            mapping::collection,
        )
        .await
    }

    async fn delete_collection(&self, id: CollectionId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM collections WHERE id = ?", id)
            .await
    }

    async fn link_collection_source(
        &self,
        collection_id: CollectionId,
        source_id: SourceId,
    ) -> Result<(), StorageError> {
        self.link(
            "INSERT INTO collection_sources (collection_id, source_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
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
            "INSERT INTO collection_connectors (collection_id, connector_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
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
            "INSERT INTO collection_objects (collection_id, object_id) VALUES (?, ?) ON CONFLICT DO NOTHING",
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
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            "#,
        )
        .bind(uuid_text(evidence.id))
        .bind(uuid_text(evidence.source_id))
        .bind(uuid_text(evidence.connector_id))
        .bind(opt_uuid_text(evidence.collection_id))
        .bind(&evidence.external_id)
        .bind(&evidence.source_url)
        .bind(rfc3339(evidence.retrieved_at))
        .bind(&evidence.content_type)
        .bind(&evidence.mime_type)
        .bind(evidence.content_length)
        .bind(&evidence.sha256)
        .bind(&evidence.storage_path)
        .bind(evidence.http_status.map(i64::from))
        .bind(json_text(&evidence.http_headers))
        .bind(json_text(&evidence.metadata))
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
            "SELECT * FROM raw_evidence WHERE id = ?",
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
            WHERE (?1 IS NULL OR id < ?1)
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )
        .bind(opt_uuid_text(after))
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
            WHERE source_id = ?1 AND (?2 IS NULL OR id < ?2)
            ORDER BY id DESC
            LIMIT ?3
            "#,
        )
        .bind(uuid_text(source_id))
        .bind(opt_uuid_text(after))
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::raw_evidence).collect()
    }

    async fn put_document(&self, document: &Document) -> Result<(), StorageError> {
        let labels =
            serde_json::to_string(&document.labels).map_err(|err| StorageError::Unknown {
                backend: "sqlite",
                message: format!("序列化 documents.labels 失敗：{err}"),
            })?;
        sqlx::query(
            r#"
            INSERT INTO documents (
                id, object_type, schema_version, title, body, summary, language, author,
                published_at, modified_at, observed_at, collected_at, source_url, canonical_url,
                normalized_content_hash, confidence, labels, attributes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                object_type = excluded.object_type,
                schema_version = excluded.schema_version,
                title = excluded.title,
                body = excluded.body,
                summary = excluded.summary,
                language = excluded.language,
                author = excluded.author,
                published_at = excluded.published_at,
                modified_at = excluded.modified_at,
                observed_at = excluded.observed_at,
                collected_at = excluded.collected_at,
                source_url = excluded.source_url,
                canonical_url = excluded.canonical_url,
                normalized_content_hash = excluded.normalized_content_hash,
                confidence = excluded.confidence,
                labels = excluded.labels,
                attributes = excluded.attributes
            "#,
        )
        .bind(uuid_text(document.id))
        .bind(encode_enum(&document.object_type)?)
        .bind(&document.schema_version)
        .bind(&document.title)
        .bind(&document.body)
        .bind(&document.summary)
        .bind(&document.language)
        .bind(&document.author)
        .bind(opt_rfc3339(document.published_at))
        .bind(opt_rfc3339(document.modified_at))
        .bind(rfc3339(document.observed_at))
        .bind(rfc3339(document.collected_at))
        .bind(&document.source_url)
        .bind(&document.canonical_url)
        .bind(&document.normalized_content_hash)
        .bind(document.confidence)
        .bind(labels)
        .bind(json_text(&document.attributes))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_document(&self, id: DocumentId) -> Result<Option<Document>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM documents WHERE id = ?",
            id,
            mapping::document,
        )
        .await
    }

    async fn delete_document(&self, id: DocumentId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM documents WHERE id = ?", id)
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
            WHERE (?1 IS NULL OR id < ?1)
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )
        .bind(opt_uuid_text(after))
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
            ) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                entity_type = excluded.entity_type,
                name = excluded.name,
                normalized_name = excluded.normalized_name,
                description = excluded.description,
                confidence = excluded.confidence,
                first_seen = excluded.first_seen,
                last_seen = excluded.last_seen,
                attributes = excluded.attributes
            "#,
        )
        .bind(uuid_text(entity.id))
        .bind(encode_enum(&entity.entity_type)?)
        .bind(&entity.name)
        .bind(&entity.normalized_name)
        .bind(&entity.description)
        .bind(entity.confidence)
        .bind(rfc3339(entity.first_seen))
        .bind(rfc3339(entity.last_seen))
        .bind(json_text(&entity.attributes))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_entity(&self, id: EntityId) -> Result<Option<Entity>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM entities WHERE id = ?", id, mapping::entity)
            .await
    }

    async fn delete_entity(&self, id: EntityId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM entities WHERE id = ?", id)
            .await
    }

    async fn put_relationship(&self, relationship: &Relationship) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO relationships (
                id, source_object_id, relationship_type, target_object_id, confidence,
                first_seen, last_seen, evidence_count, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                source_object_id = excluded.source_object_id,
                relationship_type = excluded.relationship_type,
                target_object_id = excluded.target_object_id,
                confidence = excluded.confidence,
                first_seen = excluded.first_seen,
                last_seen = excluded.last_seen,
                evidence_count = excluded.evidence_count,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(uuid_text(relationship.id))
        .bind(uuid_text(relationship.source_object_id))
        .bind(encode_enum(&relationship.relationship_type)?)
        .bind(uuid_text(relationship.target_object_id))
        .bind(relationship.confidence)
        .bind(rfc3339(relationship.first_seen))
        .bind(rfc3339(relationship.last_seen))
        .bind(i64::from(relationship.evidence_count))
        .bind(rfc3339(relationship.created_at))
        .bind(rfc3339(relationship.updated_at))
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
            "SELECT * FROM relationships WHERE id = ?",
            id,
            mapping::relationship,
        )
        .await
    }

    async fn delete_relationship(&self, id: RelationshipId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM relationships WHERE id = ?", id)
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
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                relationship_id = excluded.relationship_id,
                object_id = excluded.object_id,
                raw_evidence_id = excluded.raw_evidence_id,
                excerpt = excluded.excerpt,
                confidence = excluded.confidence,
                created_at = excluded.created_at
            "#,
        )
        .bind(uuid_text(evidence.id))
        .bind(uuid_text(evidence.relationship_id))
        .bind(uuid_text(evidence.object_id))
        .bind(opt_uuid_text(evidence.raw_evidence_id))
        .bind(&evidence.excerpt)
        .bind(evidence.confidence)
        .bind(rfc3339(evidence.created_at))
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
            "SELECT * FROM relationship_evidence WHERE id = ?",
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
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                event_type = excluded.event_type,
                title = excluded.title,
                description = excluded.description,
                start_time = excluded.start_time,
                end_time = excluded.end_time,
                confidence = excluded.confidence,
                status = excluded.status,
                attributes = excluded.attributes,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(uuid_text(event.id))
        .bind(&event.event_type)
        .bind(&event.title)
        .bind(&event.description)
        .bind(opt_rfc3339(event.start_time))
        .bind(opt_rfc3339(event.end_time))
        .bind(event.confidence)
        .bind(&event.status)
        .bind(json_text(&event.attributes))
        .bind(rfc3339(event.created_at))
        .bind(rfc3339(event.updated_at))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_event(&self, id: EventId) -> Result<Option<Event>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM events WHERE id = ?", id, mapping::event)
            .await
    }

    async fn delete_event(&self, id: EventId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM events WHERE id = ?", id).await
    }

    async fn put_provenance(&self, provenance: &Provenance) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO provenance (
                id, subject_id, action, parent_id, raw_evidence_id, processor,
                processor_version, timestamp, metadata
            ) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                subject_id = excluded.subject_id,
                action = excluded.action,
                parent_id = excluded.parent_id,
                raw_evidence_id = excluded.raw_evidence_id,
                processor = excluded.processor,
                processor_version = excluded.processor_version,
                timestamp = excluded.timestamp,
                metadata = excluded.metadata
            "#,
        )
        .bind(uuid_text(provenance.id))
        .bind(uuid_text(provenance.subject_id))
        .bind(&provenance.action)
        .bind(opt_uuid_text(provenance.parent_id))
        .bind(opt_uuid_text(provenance.raw_evidence_id))
        .bind(&provenance.processor)
        .bind(&provenance.processor_version)
        .bind(rfc3339(provenance.timestamp))
        .bind(json_text(&provenance.metadata))
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_provenance(&self, id: ProvenanceId) -> Result<Option<Provenance>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM provenance WHERE id = ?",
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
            "SELECT * FROM provenance WHERE raw_evidence_id = ? ORDER BY timestamp, id",
        )
        .bind(uuid_text(raw_evidence_id))
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
            sqlx::query("SELECT * FROM provenance WHERE subject_id = ? ORDER BY timestamp, id")
                .bind(uuid_text(subject_id))
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
            ) VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                "type" = excluded."type",
                status = excluded.status,
                correlation_id = excluded.correlation_id,
                created_at = excluded.created_at,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                retry_count = excluded.retry_count,
                error = excluded.error
            "#,
        )
        .bind(uuid_text(job.id))
        .bind(&job.job_type)
        .bind(encode_enum(&job.status)?)
        .bind(opt_uuid_text(job.correlation_id))
        .bind(rfc3339(job.created_at))
        .bind(opt_rfc3339(job.started_at))
        .bind(opt_rfc3339(job.completed_at))
        .bind(i64::from(job.retry_count))
        .bind(&job.error)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_job(&self, id: JobId) -> Result<Option<Job>, StorageError> {
        self.fetch_optional_mapped("SELECT * FROM jobs WHERE id = ?", id, mapping::job)
            .await
    }

    async fn delete_job(&self, id: JobId) -> Result<bool, StorageError> {
        self.delete_id("DELETE FROM jobs WHERE id = ?", id).await
    }

    async fn list_jobs(&self, after: Option<JobId>, limit: u32) -> Result<Vec<Job>, StorageError> {
        let limit = i64::from(limit.clamp(1, 100));
        let after_text = after.map(|id| id.to_string());
        let rows = sqlx::query(
            r#"
            SELECT * FROM jobs
            WHERE (?1 IS NULL OR id < ?1)
            ORDER BY id DESC
            LIMIT ?2
            "#,
        )
        .bind(after_text)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::job).collect()
    }

    async fn put_duplicate_group(&self, group: &DuplicateGroup) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO duplicate_groups (
                id, canonical_object_id, member_object_id, member_raw_evidence_id,
                method, similarity, first_seen
            ) VALUES (?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                canonical_object_id = excluded.canonical_object_id,
                member_object_id = excluded.member_object_id,
                member_raw_evidence_id = excluded.member_raw_evidence_id,
                method = excluded.method,
                similarity = excluded.similarity,
                first_seen = excluded.first_seen
            "#,
        )
        .bind(uuid_text(group.id))
        .bind(uuid_text(group.canonical_object_id))
        .bind(opt_uuid_text(group.member_object_id))
        .bind(opt_uuid_text(group.member_raw_evidence_id))
        .bind(&group.method)
        .bind(group.similarity)
        .bind(rfc3339(group.first_seen))
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
            "SELECT * FROM duplicate_groups WHERE id = ?",
            id,
            mapping::duplicate_group,
        )
        .await
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
            ) VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT (id) DO UPDATE SET
                object_id = excluded.object_id,
                entity_id = excluded.entity_id,
                extractor = excluded.extractor,
                extractor_version = excluded.extractor_version,
                confidence = excluded.confidence,
                text_offset = excluded.text_offset,
                excerpt = excluded.excerpt
            "#,
        )
        .bind(uuid_text(extraction.id))
        .bind(uuid_text(extraction.object_id))
        .bind(uuid_text(extraction.entity_id))
        .bind(&extraction.extractor)
        .bind(&extraction.extractor_version)
        .bind(extraction.confidence)
        .bind(extraction.text_offset.map(i64::from))
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
            "SELECT * FROM entity_extractions WHERE id = ?",
            id,
            mapping::entity_extraction,
        )
        .await
    }
}
