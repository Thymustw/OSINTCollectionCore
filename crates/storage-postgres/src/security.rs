//! `core_security::AuditLog` 與 `ApiTokenStore` 的 PostgreSQL 實作。
//!
//! # 為什麼放在 storage-postgres，而不是 core-security 或一個新 crate
//!
//! 三個選項都想過：
//!
//! 1. **放進 `core-security`** ——要讓 `core-security` 相依 `sqlx`。不行：
//!    `core-security` 現在被 `connector-sdk`、`collector`、`osint-cli` 等
//!    一整排不碰資料庫的 crate 相依，加一個 sqlx + PostgreSQL driver 進去
//!    等於讓每支 binary 都背上連線池與 TLS 堆疊。
//!
//! 2. **另開 `security-postgres` crate** ——乾淨，但為了兩個 impl 多一個 crate、
//!    多一份 Cargo.toml 與 deny/audit 面，而且它會需要跟 `storage-postgres`
//!    各自維護一份連線池與 `map_sqlx`。
//!
//! 3. **放進 `storage-postgres`（採用）** ——這正是 CLAUDE.md §13 的
//!    「capability interface → concrete adapter」形狀：`AuditLog` 與
//!    `ApiTokenStore` 是能力介面（定義在 `core-security`，不知道後端），
//!    這裡是 PostgreSQL adapter。相依方向是
//!    `storage-postgres → core-security`，**沒有環**（`core-security`
//!    不相依任何 storage crate，這一點不能破壞）。
//!    `audit_log` / `api_tokens` 兩張表也本來就在
//!    `migrations/postgres/` 底下，由這個 crate 的 `migrate()` 負責。
//!
//! # 錯誤型別
//!
//! 這兩個 trait 回的是 `SecurityError`，不是 `StorageError`。所以這裡把
//! `StorageError` 轉成 `SecurityError::Audit` / `SecurityError::TokenHash`
//! 之前會先過 `StorageError::sanitize`——稽核失敗的訊息會被寫進 log，
//! 不可以夾帶 DSN 或表結構細節。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use core_security::{ApiTokenRecord, ApiTokenStore, AuditEntry, AuditLog, Role, SecurityError};
use serde_json::Value;
use sqlx::postgres::PgRow;
use sqlx::{PgPool, Row};
use storage_core::StorageError;
use uuid::Uuid;

use crate::error::map_sqlx;
use crate::store::PostgresCanonicalStore;

/// list cursor 每頁上限。語意同 `store::clamp_limit`：0 或超大值都夾回 1..=100。
fn clamp_limit(limit: u32) -> i64 {
    i64::from(limit.clamp(1, 100))
}

/// 稽核寫入／查詢失敗一律變成 `SecurityError::Audit`，訊息先消毒過。
fn audit_error(err: StorageError) -> SecurityError {
    SecurityError::Audit {
        message: StorageError::sanitize(&err.to_string()),
    }
}

/// token store 的錯誤沒有專屬 variant，借用 `Audit`——它的語意是
/// 「安全平面的持久化失敗」，訊息會明講是 token 還是稽核。
fn token_error(what: &str, err: StorageError) -> SecurityError {
    SecurityError::Audit {
        message: format!(
            "{what} 讀寫 Postgres 失敗：{}。請確認 Postgres 在跑且已跑過 migration 0006",
            StorageError::sanitize(&err.to_string())
        ),
    }
}

// ---------------------------------------------------------------------------
// AuditLog
// ---------------------------------------------------------------------------

/// 落地到 `audit_log` 表的稽核。
#[derive(Debug, Clone)]
pub struct PostgresAuditLog {
    pool: PgPool,
}

impl PostgresAuditLog {
    /// 共用既有 canonical store 的連線池。
    ///
    /// 刻意**不另開一個池**：稽核與業務寫入在同一個 Postgres，多一個池只是多一份
    /// 連線配額，在共用工作站上（CLAUDE.md §7）沒有理由這樣花。
    #[must_use]
    pub fn new(store: &PostgresCanonicalStore) -> Self {
        Self {
            pool: store.pool().clone(),
        }
    }
}

/// `audit_log` 一列 → `AuditEntry`。
///
/// ⚠️ 欄位 `details` 對應 `AuditEntry.metadata`（名字不同，見 migration 0006 的註解）。
fn audit_row(row: &PgRow) -> Result<AuditEntry, StorageError> {
    Ok(AuditEntry {
        id: row.try_get("id").map_err(map_sqlx)?,
        timestamp: row.try_get("timestamp").map_err(map_sqlx)?,
        actor: row.try_get("actor").map_err(map_sqlx)?,
        action: row.try_get("action").map_err(map_sqlx)?,
        resource_type: row.try_get("resource_type").map_err(map_sqlx)?,
        resource_id: row.try_get("resource_id").map_err(map_sqlx)?,
        outcome: row.try_get("outcome").map_err(map_sqlx)?,
        ip: row.try_get("ip").map_err(map_sqlx)?,
        metadata: row.try_get::<Value, _>("details").map_err(map_sqlx)?,
    })
}

#[async_trait]
impl AuditLog for PostgresAuditLog {
    async fn append(&self, entry: AuditEntry) -> Result<(), SecurityError> {
        // metadata 可能是 Value::Null（AuditEntry::new 的預設）。欄位是 NOT NULL，
        // 所以在這裡折成 `{}`——不是為了通過約束，而是因為之後查詢時
        // `details->>'x'` 對 JSON null 與 `{}` 的行為不同，統一成物件比較好推理。
        let details = match entry.metadata {
            Value::Null => Value::Object(serde_json::Map::new()),
            other => other,
        };
        sqlx::query(
            r#"
            INSERT INTO audit_log (
                id, timestamp, actor, action, resource_type, resource_id, details, ip, outcome
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            "#,
        )
        .bind(entry.id)
        .bind(entry.timestamp)
        .bind(&entry.actor)
        .bind(&entry.action)
        .bind(&entry.resource_type)
        .bind(&entry.resource_id)
        .bind(&details)
        .bind(&entry.ip)
        .bind(&entry.outcome)
        .execute(&self.pool)
        .await
        .map_err(|err| audit_error(map_sqlx(err)))?;
        Ok(())
    }

    async fn list(
        &self,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<AuditEntry>, SecurityError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM audit_log
            WHERE ($1::uuid IS NULL OR id < $1)
            ORDER BY id DESC
            LIMIT $2
            "#,
        )
        .bind(after)
        .bind(clamp_limit(limit))
        .fetch_all(&self.pool)
        .await
        .map_err(|err| audit_error(map_sqlx(err)))?;
        rows.iter()
            .map(audit_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(audit_error)
    }

    async fn list_by_resource(
        &self,
        resource_type: &str,
        resource_id: &str,
    ) -> Result<Vec<AuditEntry>, SecurityError> {
        let rows = sqlx::query(
            r#"
            SELECT * FROM audit_log
            WHERE resource_type = $1 AND resource_id = $2
            ORDER BY id DESC
            LIMIT 100
            "#,
        )
        .bind(resource_type)
        .bind(resource_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|err| audit_error(map_sqlx(err)))?;
        rows.iter()
            .map(audit_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(audit_error)
    }
}

// ---------------------------------------------------------------------------
// ApiTokenStore
// ---------------------------------------------------------------------------

/// 落地到 `api_tokens` 表的 token store。
#[derive(Debug, Clone)]
pub struct PostgresApiTokenStore {
    pool: PgPool,
}

impl PostgresApiTokenStore {
    #[must_use]
    pub fn new(store: &PostgresCanonicalStore) -> Self {
        Self {
            pool: store.pool().clone(),
        }
    }
}

fn token_row(row: &PgRow) -> Result<ApiTokenRecord, StorageError> {
    let role_text: String = row.try_get("role").map_err(map_sqlx)?;
    // 角色是授權決策的依據。資料庫裡出現不認得的字串時**不可以**猜一個預設值——
    // fallback 成 viewer 看起來「安全」，但那會讓一把 admin token 在 schema
    // 改動後靜默降級；fallback 成 admin 則是直接的提權。兩者都不能接受，所以報錯。
    let role =
        Role::from_str_strict(&role_text).ok_or_else(|| StorageError::CorruptionSuspected {
            message: format!(
                "api_tokens.role 是不認得的值 `{role_text}`（只接受 viewer／operator／admin）。\
             請修正該列或撤銷這把 token"
            ),
        })?;
    Ok(ApiTokenRecord {
        id: row.try_get("id").map_err(map_sqlx)?,
        name: row.try_get("name").map_err(map_sqlx)?,
        role,
        secret_hash: row.try_get("token_hash").map_err(map_sqlx)?,
        created_by: row.try_get("created_by").map_err(map_sqlx)?,
        created_at: row.try_get("created_at").map_err(map_sqlx)?,
        expires_at: row.try_get("expires_at").map_err(map_sqlx)?,
        last_used_at: row.try_get("last_used_at").map_err(map_sqlx)?,
        revoked_at: row.try_get("revoked_at").map_err(map_sqlx)?,
    })
}

#[async_trait]
impl ApiTokenStore for PostgresApiTokenStore {
    async fn insert(&self, record: &ApiTokenRecord) -> Result<(), SecurityError> {
        // 刻意用純 INSERT，不是 upsert：同一個 token id 被寫第二次代表
        // 上游 UUID 生成或重送出了問題，應該當場失敗，不是靜默覆寫一把已發行的
        // token（那會讓原本那把持有者的 token 在無預警下換掉）。
        sqlx::query(
            r#"
            INSERT INTO api_tokens (
                id, name, token_hash, role, created_by, created_at,
                expires_at, revoked_at, last_used_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            "#,
        )
        .bind(record.id)
        .bind(&record.name)
        .bind(&record.secret_hash)
        .bind(record.role.as_str())
        .bind(&record.created_by)
        .bind(record.created_at)
        .bind(record.expires_at)
        .bind(record.revoked_at)
        .bind(record.last_used_at)
        .execute(&self.pool)
        .await
        .map_err(|err| token_error("api_tokens insert", map_sqlx(err)))?;
        Ok(())
    }

    async fn get(&self, id: Uuid) -> Result<Option<ApiTokenRecord>, SecurityError> {
        let row = sqlx::query("SELECT * FROM api_tokens WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| token_error("api_tokens get", map_sqlx(err)))?;
        row.as_ref()
            .map(token_row)
            .transpose()
            .map_err(|err| token_error("api_tokens get", err))
    }

    async fn list(&self) -> Result<Vec<ApiTokenRecord>, SecurityError> {
        // 含已撤銷的（理由見 trait 文件）。上限 100：token 數量級遠小於這個，
        // 但介面不留無界查詢。
        let rows = sqlx::query("SELECT * FROM api_tokens ORDER BY id DESC LIMIT 100")
            .fetch_all(&self.pool)
            .await
            .map_err(|err| token_error("api_tokens list", map_sqlx(err)))?;
        rows.iter()
            .map(token_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| token_error("api_tokens list", err))
    }

    async fn revoke(&self, id: Uuid, at: DateTime<Utc>) -> Result<bool, SecurityError> {
        // `revoked_at IS NULL` 讓重複撤銷不覆寫第一次的時間，同時仍然回 true
        // （下面用 EXISTS 判斷「這個 id 存不存在」，而不是用 rows_affected）。
        let row = sqlx::query(
            r#"
            WITH updated AS (
                UPDATE api_tokens SET revoked_at = $2
                WHERE id = $1 AND revoked_at IS NULL
                RETURNING id
            )
            SELECT EXISTS (SELECT 1 FROM api_tokens WHERE id = $1) AS found
            "#,
        )
        .bind(id)
        .bind(at)
        .fetch_one(&self.pool)
        .await
        .map_err(|err| token_error("api_tokens revoke", map_sqlx(err)))?;
        row.try_get::<bool, _>("found")
            .map_err(|err| token_error("api_tokens revoke", map_sqlx(err)))
    }

    async fn touch_last_used(&self, id: Uuid, at: DateTime<Utc>) -> Result<(), SecurityError> {
        sqlx::query("UPDATE api_tokens SET last_used_at = $2 WHERE id = $1")
            .bind(id)
            .bind(at)
            .execute(&self.pool)
            .await
            .map_err(|err| token_error("api_tokens touch_last_used", map_sqlx(err)))?;
        Ok(())
    }
}
