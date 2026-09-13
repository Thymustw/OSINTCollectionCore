use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_model::{
    Collection, CollectionId, Connector, ConnectorId, Document, DocumentId, DocumentType,
    DuplicateGroup, DuplicateGroupId, Embedding, EmbeddingTarget, Entity, EntityAlias,
    EntityAliasId, EntityExtraction, EntityExtractionId, EntityId, EntityIdentifier,
    EntityIdentifierId, EntityType, Event, EventId, FailedEvent, FailedEventId, Job, JobId,
    JobStatus, MergeHistory, MergeHistoryId, NetworkRule, NetworkRuleId, ObjectId, Provenance,
    ProvenanceId, RawEvidence, RawEvidenceId, Relationship, RelationshipEvidence,
    RelationshipEvidenceId, RelationshipId, RelationshipType, ResolutionCandidate,
    ResolutionCandidateId, ResolutionStatus, Source, SourceId,
};
use serde_json::Value;
use sqlx::PgPool;
use sqlx::postgres::{PgPoolOptions, PgRow};
use storage_core::codec::encode_enum;
use storage_core::{
    CanonicalStore, CapabilityDescriptor, HealthProvider, RelationalStore, SimhashCandidate,
    StorageAdapter, StorageError, StorageHealth, Transaction, TransactionalStore,
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

/// 一次查詢要用的連線。
///
/// 兩個變體是 adapter 能同時服務「連線池」與「進行中的交易」的關鍵：
/// `RelationalStore` 的整份實作只寫一次，執行對象在這裡切換。
/// 沒有這層的話，交易路徑就得複製一份 80 個方法的 SQL，兩份遲早分岔。
enum PgConn<'a> {
    /// 從池子臨時借一條。語意與先前直接把 `&PgPool` 當 executor 相同
    /// （sqlx 對 `&Pool` 的 `Executor` 實作本來就是「借一條、用完還」）。
    Pooled(sqlx::pool::PoolConnection<sqlx::Postgres>),
    /// 這條交易自己的連線。整個交易期間都是同一條。
    Tx(tokio::sync::MutexGuard<'a, sqlx::Transaction<'static, sqlx::Postgres>>),
}

impl PgConn<'_> {
    fn as_mut(&mut self) -> &mut sqlx::PgConnection {
        match self {
            PgConn::Pooled(conn) => conn,
            PgConn::Tx(tx) => tx,
        }
    }
}

/// PostgreSQL canonical + relational store。
#[derive(Debug, Clone)]
pub struct PostgresCanonicalStore {
    pool: PgPool,
    /// `Some` 代表這個 handle 綁在一條進行中的交易上（由
    /// [`PostgresCanonicalStore::begin`] 產生，外部拿不到這種 handle）。
    /// 交易是**序列**的，`Mutex` 只是為了在 `&self` 介面下取得 `&mut Transaction`，
    /// 不是為了讓多個工作同時用同一條交易——那本來就是錯的用法。
    tx: Option<std::sync::Arc<tokio::sync::Mutex<sqlx::Transaction<'static, sqlx::Postgres>>>>,
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
        Ok(Self { pool, tx: None })
    }

    /// 這次查詢要用哪條連線：綁了交易就用交易那條，否則向池子借。
    async fn conn(&self) -> Result<PgConn<'_>, StorageError> {
        match &self.tx {
            Some(tx) => Ok(PgConn::Tx(tx.lock().await)),
            None => Ok(PgConn::Pooled(self.pool.acquire().await.map_err(map_sqlx)?)),
        }
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
            .fetch_optional(self.conn().await?.as_mut())
            .await
            .map_err(map_sqlx)?;
        row.map(|r| map(&r)).transpose()
    }

    async fn delete_id(&self, sql: &'static str, id: uuid::Uuid) -> Result<bool, StorageError> {
        let result = sqlx::query(sql)
            .bind(id)
            .execute(self.conn().await?.as_mut())
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected() > 0)
    }

    /// `entity_extractions` 的兩個反查共用：一個 UUID 鍵 + 上限。
    async fn entity_extractions(
        &self,
        sql: &'static str,
        key: uuid::Uuid,
        limit: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError> {
        let rows = sqlx::query(sql)
            .bind(key)
            .bind(clamp_limit(limit))
            .fetch_all(self.conn().await?.as_mut())
            .await
            .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity_extraction).collect()
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
            .fetch_all(self.conn().await?.as_mut())
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
            .execute(self.conn().await?.as_mut())
            .await
            .map_err(map_sqlx)?;
        Ok(())
    }

    /// collection 三個關聯表的反查共用：一個 collection_id + 上限，回一欄 UUID。
    async fn linked_ids(
        &self,
        sql: &'static str,
        collection_id: uuid::Uuid,
        limit: u32,
    ) -> Result<Vec<uuid::Uuid>, StorageError> {
        let rows: Vec<(uuid::Uuid,)> = sqlx::query_as(sql)
            .bind(collection_id)
            .bind(clamp_limit(limit))
            .fetch_all(self.conn().await?.as_mut())
            .await
            .map_err(map_sqlx)?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

#[async_trait]
impl HealthProvider for PostgresCanonicalStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let (one,): (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(self.conn().await?.as_mut())
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
            &["canonical", "relational", "transactional"],
        )
        .with_feature("json", Value::Bool(true))
        .with_feature("high_concurrent_write", Value::Bool(true))
        .with_feature("bulk_write", Value::Bool(true))
        .with_feature("transactions", Value::Bool(true))
    }
}

#[async_trait]
impl CanonicalStore for PostgresCanonicalStore {
    fn canonical_backend_id(&self) -> &'static str {
        "postgres"
    }
}

#[async_trait]
impl TransactionalStore for PostgresCanonicalStore {
    async fn begin(&self) -> Result<Box<dyn Transaction>, StorageError> {
        if self.tx.is_some() {
            // sqlx 其實能用 SAVEPOINT 疊，但巢狀交易的語意（內層 rollback 之後外層
            // 還能不能繼續）需要呼叫端明確決定。V0.2 沒有這種需求，先擋掉——
            // 讓它「看起來能用」但語意沒定義，比直接不支援危險。
            return Err(StorageError::UnsupportedCapability {
                backend: "postgres",
                capability: "nested_transaction",
            });
        }
        let tx = self.pool.begin().await.map_err(map_sqlx)?;
        Ok(Box::new(PostgresTransaction {
            store: PostgresCanonicalStore {
                pool: self.pool.clone(),
                tx: Some(std::sync::Arc::new(tokio::sync::Mutex::new(tx))),
            },
        }))
    }
}

/// 一條進行中的 PostgreSQL 交易。
///
/// 內含的 `store` 是一個**綁在這條交易上**的 `PostgresCanonicalStore`：
/// 交易內的 CRUD 走的是同一份 `RelationalStore` 實作，只是執行對象換成交易的連線。
///
/// **沒有 commit 就 drop 會回滾**：drop 時 sqlx 的 `Transaction::drop` 會排一個
/// ROLLBACK，連線歸還池子前執行。所以「處理到一半 return 了」不會留下半套資料。
pub struct PostgresTransaction {
    store: PostgresCanonicalStore,
}

impl std::fmt::Debug for PostgresTransaction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresTransaction")
            .finish_non_exhaustive()
    }
}

impl PostgresTransaction {
    /// 取回底層 sqlx 交易。呼叫後這個 handle 就沒了（commit／rollback 各用一次）。
    fn into_inner(self) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, StorageError> {
        let arc = self.store.tx.ok_or_else(|| StorageError::Unknown {
            backend: "postgres",
            message: "交易 handle 沒有帶交易連線。這是 adapter 內部錯誤，請回報".into(),
        })?;
        std::sync::Arc::try_unwrap(arc)
            .map(tokio::sync::Mutex::into_inner)
            .map_err(|_| StorageError::Unknown {
                backend: "postgres",
                message: "交易仍被其他 handle 持有，無法結束。請確認沒有把交易 handle 複製出去"
                    .into(),
            })
    }
}

#[async_trait]
impl Transaction for PostgresTransaction {
    fn store(&self) -> &dyn RelationalStore {
        &self.store
    }

    async fn commit(self: Box<Self>) -> Result<(), StorageError> {
        (*self).into_inner()?.commit().await.map_err(map_sqlx)
    }

    async fn rollback(self: Box<Self>) -> Result<(), StorageError> {
        (*self).into_inner()?.rollback().await.map_err(map_sqlx)
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
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
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
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
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
        .execute(self.conn().await?.as_mut())
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
            .fetch_all(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::connector).collect()
    }

    async fn list_connectors_by_enabled(
        &self,
        enabled: Option<bool>,
        after: Option<ConnectorId>,
        limit: u32,
    ) -> Result<Vec<Connector>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM connectors
            WHERE ($1::uuid IS NULL OR id < $1)
              AND ($2::boolean IS NULL OR enabled = $2)
            ORDER BY id DESC
            LIMIT $3
            "#,
        )
        .bind(after)
        .bind(enabled)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
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
        .execute(self.conn().await?.as_mut())
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

    async fn list_collections(
        &self,
        after: Option<CollectionId>,
        limit: u32,
    ) -> Result<Vec<Collection>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM collections
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::collection).collect()
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

    async fn list_collection_sources(
        &self,
        collection_id: CollectionId,
        limit: u32,
    ) -> Result<Vec<SourceId>, StorageError> {
        self.linked_ids(
            r#"
            SELECT source_id AS id FROM collection_sources
            WHERE collection_id = $1
            ORDER BY source_id ASC
            LIMIT $2
            "#,
            collection_id,
            limit,
        )
        .await
    }

    async fn list_collection_connectors(
        &self,
        collection_id: CollectionId,
        limit: u32,
    ) -> Result<Vec<ConnectorId>, StorageError> {
        self.linked_ids(
            r#"
            SELECT connector_id AS id FROM collection_connectors
            WHERE collection_id = $1
            ORDER BY connector_id ASC
            LIMIT $2
            "#,
            collection_id,
            limit,
        )
        .await
    }

    async fn list_collection_objects(
        &self,
        collection_id: CollectionId,
        limit: u32,
    ) -> Result<Vec<ObjectId>, StorageError> {
        self.linked_ids(
            r#"
            SELECT object_id AS id FROM collection_objects
            WHERE collection_id = $1
            ORDER BY object_id ASC
            LIMIT $2
            "#,
            collection_id,
            limit,
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
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
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
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::document).collect()
    }

    async fn list_documents_filtered(
        &self,
        object_type: Option<DocumentType>,
        include_duplicates: bool,
        after: Option<DocumentId>,
        limit: u32,
    ) -> Result<Vec<Document>, StorageError> {
        let object_type = object_type.as_ref().map(encode_enum).transpose()?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM documents
            WHERE ($1::uuid IS NULL OR id < $1)
              AND ($2::text IS NULL OR object_type = $2)
              AND ($3::boolean OR duplicate_of IS NULL)
            ORDER BY id DESC
            LIMIT $4
            "#,
        )
        .bind(after)
        .bind(object_type)
        .bind(include_duplicates)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::document).collect()
    }

    async fn put_entity(&self, entity: &Entity) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO entities (
                id, entity_type, name, normalized_name, description, confidence,
                first_seen, last_seen, merged_into, attributes
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
            ON CONFLICT (id) DO UPDATE SET
                entity_type = EXCLUDED.entity_type,
                name = EXCLUDED.name,
                normalized_name = EXCLUDED.normalized_name,
                description = EXCLUDED.description,
                confidence = EXCLUDED.confidence,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen,
                merged_into = EXCLUDED.merged_into,
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
        .bind(entity.merged_into)
        .bind(&entity.attributes)
        .execute(self.conn().await?.as_mut())
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

    async fn find_entity_by_normalized_name(
        &self,
        entity_type: EntityType,
        normalized_name: &str,
    ) -> Result<Option<Entity>, StorageError> {
        let row =
            sqlx::query("SELECT * FROM entities WHERE entity_type = $1 AND normalized_name = $2")
                .bind(encode_enum(&entity_type)?)
                .bind(normalized_name)
                .fetch_optional(self.conn().await?.as_mut())
                .await
                .map_err(map_sqlx)?;
        row.as_ref().map(mapping::entity).transpose()
    }

    async fn list_entities(
        &self,
        after: Option<EntityId>,
        limit: u32,
    ) -> Result<Vec<Entity>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM entities
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity).collect()
    }

    async fn list_entities_by_type(
        &self,
        entity_type: Option<EntityType>,
        after: Option<EntityId>,
        limit: u32,
    ) -> Result<Vec<Entity>, StorageError> {
        let entity_type = entity_type.as_ref().map(encode_enum).transpose()?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM entities
            WHERE ($1::uuid IS NULL OR id < $1)
              AND ($2::text IS NULL OR entity_type = $2)
            ORDER BY id DESC
            LIMIT $3
            "#,
        )
        .bind(after)
        .bind(entity_type)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity).collect()
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
        .execute(self.conn().await?.as_mut())
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

    async fn list_relationships_by_object(
        &self,
        object_id: ObjectId,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM relationships
            WHERE source_object_id = $1 OR target_object_id = $1
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(object_id)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::relationship).collect()
    }

    async fn list_relationships(
        &self,
        after: Option<RelationshipId>,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM relationships
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::relationship).collect()
    }

    async fn list_relationships_by_type(
        &self,
        relationship_type: Option<RelationshipType>,
        after: Option<RelationshipId>,
        limit: u32,
    ) -> Result<Vec<Relationship>, StorageError> {
        let relationship_type = relationship_type.as_ref().map(encode_enum).transpose()?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM relationships
            WHERE ($1::uuid IS NULL OR id < $1)
              AND ($2::text IS NULL OR relationship_type = $2)
            ORDER BY id DESC
            LIMIT $3
            "#,
        )
        .bind(after)
        .bind(relationship_type)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::relationship).collect()
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
        .execute(self.conn().await?.as_mut())
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

    async fn list_relationship_evidence(
        &self,
        relationship_id: RelationshipId,
        limit: u32,
    ) -> Result<Vec<RelationshipEvidence>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM relationship_evidence
            WHERE relationship_id = $1
            ORDER BY created_at ASC, id ASC
            LIMIT $2
            "#,
        )
        .bind(relationship_id)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::relationship_evidence).collect()
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
        .execute(self.conn().await?.as_mut())
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

    async fn list_events(
        &self,
        after: Option<EventId>,
        limit: u32,
    ) -> Result<Vec<Event>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM events
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::event).collect()
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
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
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
                .fetch_all(self.conn().await?.as_mut())
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
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::job).collect()
    }

    async fn list_jobs_by_status(
        &self,
        status: JobStatus,
        after: Option<JobId>,
        limit: u32,
    ) -> Result<Vec<Job>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM jobs
            WHERE status = $1 AND ($2::uuid IS NULL OR id < $2)
            ORDER BY id DESC
            LIMIT $3
            "#,
        )
        .bind(encode_enum(&status)?)
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::simhash_candidate).collect()
    }

    async fn put_duplicate_group(&self, group: &DuplicateGroup) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO duplicate_groups (
                id, canonical_object_id, member_object_id, member_raw_evidence_id,
                method, similarity, first_seen, model
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                canonical_object_id = EXCLUDED.canonical_object_id,
                member_object_id = EXCLUDED.member_object_id,
                member_raw_evidence_id = EXCLUDED.member_raw_evidence_id,
                method = EXCLUDED.method,
                similarity = EXCLUDED.similarity,
                first_seen = EXCLUDED.first_seen,
                model = EXCLUDED.model
            "#,
        )
        .bind(group.id)
        .bind(group.canonical_object_id)
        .bind(group.member_object_id)
        .bind(group.member_raw_evidence_id)
        .bind(&group.method)
        .bind(group.similarity)
        .bind(group.first_seen)
        .bind(&group.model)
        .execute(self.conn().await?.as_mut())
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
        .fetch_all(self.conn().await?.as_mut())
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
        .execute(self.conn().await?.as_mut())
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

    async fn list_entity_extractions_by_object(
        &self,
        object_id: ObjectId,
        limit: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError> {
        self.entity_extractions(
            "SELECT * FROM entity_extractions WHERE object_id = $1 ORDER BY id ASC LIMIT $2",
            object_id,
            limit,
        )
        .await
    }

    async fn list_entity_extractions_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<EntityExtraction>, StorageError> {
        self.entity_extractions(
            "SELECT * FROM entity_extractions WHERE entity_id = $1 ORDER BY id ASC LIMIT $2",
            entity_id,
            limit,
        )
        .await
    }

    // ===== V0.2 =====

    async fn put_entity_alias(&self, alias: &EntityAlias) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO entity_aliases (
                id, entity_id, alias, alias_type, source_id, confidence, first_seen, last_seen
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (id) DO UPDATE SET
                entity_id = EXCLUDED.entity_id,
                alias = EXCLUDED.alias,
                alias_type = EXCLUDED.alias_type,
                source_id = EXCLUDED.source_id,
                confidence = EXCLUDED.confidence,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen
            "#,
        )
        .bind(alias.id)
        .bind(alias.entity_id)
        .bind(&alias.alias)
        .bind(&alias.alias_type)
        .bind(alias.source_id)
        .bind(alias.confidence)
        .bind(alias.first_seen)
        .bind(alias.last_seen)
        .execute(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_entity_alias(
        &self,
        id: EntityAliasId,
    ) -> Result<Option<EntityAlias>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM entity_aliases WHERE id = $1",
            id,
            mapping::entity_alias,
        )
        .await
    }

    async fn list_entity_aliases_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<EntityAlias>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM entity_aliases WHERE entity_id = $1 ORDER BY id ASC LIMIT $2",
        )
        .bind(entity_id)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity_alias).collect()
    }

    async fn find_entity_aliases_by_text(
        &self,
        alias: &str,
        limit: u32,
    ) -> Result<Vec<EntityAlias>, StorageError> {
        let rows =
            sqlx::query("SELECT * FROM entity_aliases WHERE alias = $1 ORDER BY id ASC LIMIT $2")
                .bind(alias)
                .bind(clamp_limit(limit))
                .fetch_all(self.conn().await?.as_mut())
                .await
                .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity_alias).collect()
    }

    async fn put_entity_identifier(
        &self,
        identifier: &EntityIdentifier,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO entity_identifiers (
                id, entity_id, namespace, value, normalized_value, confidence,
                source_id, first_seen, last_seen
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                entity_id = EXCLUDED.entity_id,
                namespace = EXCLUDED.namespace,
                value = EXCLUDED.value,
                normalized_value = EXCLUDED.normalized_value,
                confidence = EXCLUDED.confidence,
                source_id = EXCLUDED.source_id,
                first_seen = EXCLUDED.first_seen,
                last_seen = EXCLUDED.last_seen
            "#,
        )
        .bind(identifier.id)
        .bind(identifier.entity_id)
        .bind(&identifier.namespace)
        .bind(&identifier.value)
        .bind(&identifier.normalized_value)
        .bind(identifier.confidence)
        .bind(identifier.source_id)
        .bind(identifier.first_seen)
        .bind(identifier.last_seen)
        .execute(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_entity_identifier(
        &self,
        id: EntityIdentifierId,
    ) -> Result<Option<EntityIdentifier>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM entity_identifiers WHERE id = $1",
            id,
            mapping::entity_identifier,
        )
        .await
    }

    async fn list_entity_identifiers_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<EntityIdentifier>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM entity_identifiers WHERE entity_id = $1 ORDER BY id ASC LIMIT $2",
        )
        .bind(entity_id)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity_identifier).collect()
    }

    async fn find_entity_identifier_owner(
        &self,
        namespace: &str,
        normalized_value: &str,
    ) -> Result<Option<EntityIdentifier>, StorageError> {
        // `fetch_optional_mapped` 只綁一個 UUID 主鍵；這條走自然鍵兩欄字串，
        // 寫法同 `find_entity_by_normalized_name`。
        let row = sqlx::query(
            "SELECT * FROM entity_identifiers WHERE namespace = $1 AND normalized_value = $2",
        )
        .bind(namespace)
        .bind(normalized_value)
        .fetch_optional(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(mapping::entity_identifier).transpose()
    }

    async fn find_entity_identifiers_by_normalized_value(
        &self,
        normalized_value: &str,
        limit: u32,
    ) -> Result<Vec<EntityIdentifier>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM entity_identifiers WHERE normalized_value = $1 ORDER BY id ASC LIMIT $2",
        )
        .bind(normalized_value)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::entity_identifier).collect()
    }

    async fn put_resolution_candidate(
        &self,
        candidate: &ResolutionCandidate,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO resolution_candidates (
                id, entity_a_id, entity_b_id, score, method, evidence, status,
                created_at, reviewed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                entity_a_id = EXCLUDED.entity_a_id,
                entity_b_id = EXCLUDED.entity_b_id,
                score = EXCLUDED.score,
                method = EXCLUDED.method,
                evidence = EXCLUDED.evidence,
                status = EXCLUDED.status,
                created_at = EXCLUDED.created_at,
                reviewed_at = EXCLUDED.reviewed_at
            "#,
        )
        .bind(candidate.id)
        .bind(candidate.entity_a_id)
        .bind(candidate.entity_b_id)
        .bind(candidate.score)
        .bind(&candidate.method)
        .bind(&candidate.evidence)
        .bind(encode_enum(&candidate.status)?)
        .bind(candidate.created_at)
        .bind(candidate.reviewed_at)
        .execute(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_resolution_candidate(
        &self,
        id: ResolutionCandidateId,
    ) -> Result<Option<ResolutionCandidate>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM resolution_candidates WHERE id = $1",
            id,
            mapping::resolution_candidate,
        )
        .await
    }

    async fn list_resolution_candidates(
        &self,
        status: Option<ResolutionStatus>,
        after: Option<ResolutionCandidateId>,
        limit: u32,
    ) -> Result<Vec<ResolutionCandidate>, StorageError> {
        let status = status.as_ref().map(encode_enum).transpose()?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM resolution_candidates
            WHERE ($1::uuid IS NULL OR id < $1)
              AND ($2::text IS NULL OR status = $2)
            ORDER BY id DESC
            LIMIT $3
            "#,
        )
        .bind(after)
        .bind(status)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::resolution_candidate).collect()
    }

    async fn list_resolution_candidates_by_entity(
        &self,
        entity_id: EntityId,
        status: Option<ResolutionStatus>,
        after: Option<ResolutionCandidateId>,
        limit: u32,
    ) -> Result<Vec<ResolutionCandidate>, StorageError> {
        let status = status.as_ref().map(encode_enum).transpose()?;
        let rows = sqlx::query(
            r#"
            SELECT * FROM resolution_candidates
            WHERE (entity_a_id = $1 OR entity_b_id = $1)
              AND ($2::text IS NULL OR status = $2)
              AND ($3::uuid IS NULL OR id < $3)
            ORDER BY id DESC
            LIMIT $4
            "#,
        )
        .bind(entity_id)
        .bind(status)
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::resolution_candidate).collect()
    }

    async fn put_merge_history(&self, history: &MergeHistory) -> Result<(), StorageError> {
        let repointed = serde_json::to_value(&history.repointed_references).map_err(|err| {
            StorageError::Unknown {
                backend: "postgres",
                message: format!("序列化 merge_history.repointed_references 失敗：{err}"),
            }
        })?;
        let merged_relationships =
            serde_json::to_value(&history.merged_relationships).map_err(|err| {
                StorageError::Unknown {
                    backend: "postgres",
                    message: format!("序列化 merge_history.merged_relationships 失敗：{err}"),
                }
            })?;
        sqlx::query(
            r#"
            INSERT INTO merge_history (
                id, survivor_id, merged_id, reason, operator, timestamp,
                repointed_references, merged_relationships, undone_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (id) DO UPDATE SET
                survivor_id = EXCLUDED.survivor_id,
                merged_id = EXCLUDED.merged_id,
                reason = EXCLUDED.reason,
                operator = EXCLUDED.operator,
                timestamp = EXCLUDED.timestamp,
                repointed_references = EXCLUDED.repointed_references,
                merged_relationships = EXCLUDED.merged_relationships,
                undone_at = EXCLUDED.undone_at
            "#,
        )
        .bind(history.id)
        .bind(history.survivor_id)
        .bind(history.merged_id)
        .bind(&history.reason)
        .bind(&history.operator)
        .bind(history.timestamp)
        .bind(repointed)
        .bind(merged_relationships)
        .bind(history.undone_at)
        .execute(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn get_merge_history(
        &self,
        id: MergeHistoryId,
    ) -> Result<Option<MergeHistory>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM merge_history WHERE id = $1",
            id,
            mapping::merge_history,
        )
        .await
    }

    async fn list_merge_history_by_entity(
        &self,
        entity_id: EntityId,
        limit: u32,
    ) -> Result<Vec<MergeHistory>, StorageError> {
        // 兩端都算。idx_merge_history_survivor 與 idx_merge_history_merged 都在，
        // PostgreSQL 會走 bitmap OR（理由同 list_relationships_by_object）。
        let rows = sqlx::query(
            r#"
            SELECT * FROM merge_history
            WHERE survivor_id = $1 OR merged_id = $1
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(entity_id)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::merge_history).collect()
    }

    async fn put_failed_event(&self, event: &FailedEvent) -> Result<FailedEvent, StorageError> {
        // 自然鍵是 (topic, partition, offset)，不是 id。衝突時 attempt_count 由 DB 累加，
        // id / first_seen 保留既有列的值——所以必須 RETURNING 把實際存下來的列帶回去，
        // 呼叫端拿自己產生的 id 是查不到東西的。
        let row = sqlx::query(
            r#"
            INSERT INTO failed_events (
                id, topic, partition, "offset", consumer_group, failure_reason,
                attempt_count, envelope, first_seen, last_seen, replayed_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
            ON CONFLICT (topic, partition, "offset") DO UPDATE SET
                consumer_group = EXCLUDED.consumer_group,
                failure_reason = EXCLUDED.failure_reason,
                attempt_count = failed_events.attempt_count + 1,
                envelope = EXCLUDED.envelope,
                last_seen = EXCLUDED.last_seen,
                replayed_at = EXCLUDED.replayed_at
            RETURNING *
            "#,
        )
        .bind(event.id)
        .bind(&event.topic)
        .bind(event.partition)
        .bind(event.offset)
        .bind(&event.consumer_group)
        .bind(&event.failure_reason)
        .bind(event.attempt_count)
        .bind(&event.envelope)
        .bind(event.first_seen)
        .bind(event.last_seen)
        .bind(event.replayed_at)
        .fetch_one(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        mapping::failed_event(&row)
    }

    async fn get_failed_event(
        &self,
        id: FailedEventId,
    ) -> Result<Option<FailedEvent>, StorageError> {
        self.fetch_optional_mapped(
            "SELECT * FROM failed_events WHERE id = $1",
            id,
            mapping::failed_event,
        )
        .await
    }

    async fn list_failed_events(
        &self,
        after: Option<FailedEventId>,
        limit: u32,
    ) -> Result<Vec<FailedEvent>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM failed_events
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::failed_event).collect()
    }

    async fn mark_replayed(
        &self,
        id: FailedEventId,
        replayed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query("UPDATE failed_events SET replayed_at = $2 WHERE id = $1")
            .bind(id)
            .bind(replayed_at)
            .execute(self.conn().await?.as_mut())
            .await
            .map_err(map_sqlx)?;
        Ok(result.rows_affected() > 0)
    }

    // ===== V0.2 Phase 3 Step 1：Embedding metadata =====

    async fn put_embedding(&self, embedding: &Embedding) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO embeddings (
                id, target_id, target_type, model, model_version, dimensions,
                content_hash, created_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            "#,
        )
        .bind(embedding.id)
        .bind(embedding.target_id)
        .bind(encode_enum(&embedding.target_type)?)
        .bind(&embedding.model)
        .bind(&embedding.model_version)
        .bind(embedding.dimensions)
        .bind(&embedding.content_hash)
        .bind(embedding.created_at)
        .execute(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        Ok(())
    }

    async fn find_embedding(
        &self,
        target_id: ObjectId,
        target_type: EmbeddingTarget,
        model: &str,
        content_hash: &str,
    ) -> Result<Option<Embedding>, StorageError> {
        let row = sqlx::query(
            r#"
            SELECT * FROM embeddings
            WHERE target_id = $1 AND target_type = $2 AND model = $3 AND content_hash = $4
            "#,
        )
        .bind(target_id)
        .bind(encode_enum(&target_type)?)
        .bind(model)
        .bind(content_hash)
        .fetch_optional(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        row.as_ref().map(mapping::embedding).transpose()
    }

    async fn list_embeddings_by_target(
        &self,
        target_id: ObjectId,
        target_type: EmbeddingTarget,
        limit: u32,
    ) -> Result<Vec<Embedding>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM embeddings
            WHERE target_id = $1 AND target_type = $2
            ORDER BY id ASC
            LIMIT $3
            "#,
        )
        .bind(target_id)
        .bind(encode_enum(&target_type)?)
        .bind(clamp_limit(limit))
        .fetch_all(self.conn().await?.as_mut())
        .await
        .map_err(map_sqlx)?;
        rows.iter().map(mapping::embedding).collect()
    }
}
