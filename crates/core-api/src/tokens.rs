//! `/api/v1/tokens`：API token 的發行、列出、撤銷。**admin-only**。
//!
//! 這是 V0.1 第一組真正需要 admin 角色的路由。在這之前 `Role::Admin` 只是
//! `Permission::Admin` 的唯一持有者，但沒有任何東西用到 `Permission::Admin`——
//! RBAC 矩陣裡有一整欄是空的。
//!
//! # 明文只出現一次
//!
//! `POST` 的回應是**唯一**能拿到 token 明文的地方。資料庫只存 argon2id hash，
//! 所以「重新顯示一次」在結構上不可能——弄丟了就撤銷重發。
//! `GET` 與稽核紀錄都不含明文、也不含 hash。

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::{Duration, Utc};
use core_security::{AuditEntry, Permission, Principal, Role, issue_api_token};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::ApiError;
use crate::state::AppState;

/// 稽核 action。改動要同步改 `docs/developer/security.md` 的動作清單。
pub const AUDIT_TOKEN_ISSUE: &str = "token.issue";
pub const AUDIT_TOKEN_LIST: &str = "token.list";
pub const AUDIT_TOKEN_REVOKE: &str = "token.revoke";

/// 稽核與回應裡的資源種類。
const RESOURCE: &str = "api_token";

/// `expires_in_days` 的上限。
///
/// 沒有上限的話「不會過期的 operator token」會變成預設用法——那正是最常在
/// CI 設定檔裡躺三年的東西。要長期憑證得明確傳 `null`，而且會被稽核記下來。
const MAX_EXPIRES_IN_DAYS: i64 = 365;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssueTokenBody {
    /// 給人看的用途名稱，會出現在 `Principal.subject`（`token:<name>`）與稽核裡。
    pub name: String,
    pub role: Role,
    /// 幾天後到期。省略或 `null` 代表不自動到期（只能撤銷）。
    #[serde(default)]
    pub expires_in_days: Option<i64>,
}

/// `GET` 回的摘要。**沒有** `secret_hash` 欄位——不是忘了加，是刻意不送：
/// hash 外流雖然不能直接用，但等於把離線暴力破解的門檻交出去。
#[derive(Debug, Clone, Serialize)]
pub struct TokenSummary {
    pub id: Uuid,
    pub name: String,
    pub role: Role,
    pub created_by: Option<String>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_used_at: Option<chrono::DateTime<chrono::Utc>>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    /// 便於前端直接判斷，等同 `revoked_at.is_none() && expires_at > now`。
    pub active: bool,
}

/// `POST /api/v1/tokens`。
pub async fn issue_token(
    State(state): State<AppState>,
    principal: Principal,
    Json(body): Json<IssueTokenBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    // 路由層已經有 require_admin；handler 再擋一次，被搬到別的 router 也不會失去保護。
    principal.role.require(Permission::Admin)?;

    let name = body.name.trim().to_string();
    let requested_role = body.role;
    match issue(&state, &principal, name.clone(), body).await {
        Ok((id, response)) => {
            audit(
                &state,
                &principal,
                AUDIT_TOKEN_ISSUE,
                Some(id.to_string()),
                "success",
                json!({ "name": name, "role": requested_role }),
            )
            .await;
            Ok((StatusCode::CREATED, Json(response)))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                AUDIT_TOKEN_ISSUE,
                None,
                "rejected",
                json!({
                    "name": name,
                    "role": requested_role,
                    "status_code": err.status.as_u16(),
                    "error": err.error,
                }),
            )
            .await;
            Err(err)
        }
    }
}

async fn issue(
    state: &AppState,
    principal: &Principal,
    name: String,
    body: IssueTokenBody,
) -> Result<(Uuid, Value), ApiError> {
    if name.is_empty() || name.chars().count() > 64 {
        return Err(ApiError::bad_request(
            "name 必須是 1..=64 個字元。它會出現在稽核紀錄裡，請填看得出用途的名稱，例如 ci-indexer",
        ));
    }
    // 控制字元會被原樣寫進稽核與 log。
    if name.chars().any(char::is_control) {
        return Err(ApiError::bad_request(
            "name 不可含控制字元。請用一般文字，例如 ci-indexer",
        ));
    }

    let expires_at = match body.expires_in_days {
        None => None,
        Some(days) if (1..=MAX_EXPIRES_IN_DAYS).contains(&days) => {
            Some(Utc::now() + Duration::days(days))
        }
        Some(days) => {
            return Err(ApiError::bad_request(format!(
                "expires_in_days 是 {days}，必須落在 1..={MAX_EXPIRES_IN_DAYS}。\
                 要發不會自動到期的 token 請把這個欄位整個省略（或填 null），\
                 那會被單獨記進稽核"
            )));
        }
    };

    // argon2 是 CPU-heavy（理由同 middleware 的 verify）。發行走的是同一條規則。
    let created_by = principal.subject.clone();
    let role = body.role;
    let issued = tokio::task::spawn_blocking(move || {
        issue_api_token(name, role, Some(created_by), expires_at)
    })
    .await
    .map_err(|err| {
        ApiError::internal(format!(
            "發行 API token 的工作執行緒中止：{err}。沒有任何 token 被寫入，請重試"
        ))
    })??;

    state.auth.tokens.insert(&issued.record).await?;

    Ok((
        issued.record.id,
        json!({
            "id": issued.record.id,
            "name": issued.record.name,
            "role": issued.record.role,
            "created_by": issued.record.created_by,
            "created_at": issued.record.created_at,
            "expires_at": issued.record.expires_at,
            // 只有這一次。
            "token": issued.plaintext,
            "message": "請立刻保存 token：伺服器只留 argon2 雜湊，這個明文不會再出現第二次。\
                        弄丟了請用 DELETE /api/v1/tokens/{id} 撤銷後重新發行",
        }),
    ))
}

/// `GET /api/v1/tokens`。含已撤銷的（撤銷紀錄本身就是要看的東西）。
pub async fn list_tokens(
    State(state): State<AppState>,
    principal: Principal,
) -> Result<Json<Value>, ApiError> {
    principal.role.require(Permission::Admin)?;
    let now = Utc::now();
    let items: Vec<TokenSummary> = state
        .auth
        .tokens
        .list()
        .await?
        .into_iter()
        .map(|record| TokenSummary {
            active: record.ensure_usable(now).is_ok(),
            id: record.id,
            name: record.name,
            role: record.role,
            created_by: record.created_by,
            created_at: record.created_at,
            expires_at: record.expires_at,
            last_used_at: record.last_used_at,
            revoked_at: record.revoked_at,
        })
        .collect();

    // 列出憑證清單本身就是值得留痕的動作（事後要能回答「洩漏前誰看過這份清單」）。
    audit(
        &state,
        &principal,
        AUDIT_TOKEN_LIST,
        None,
        "success",
        json!({ "count": items.len() }),
    )
    .await;

    // 刻意不做 cursor 分頁：store 端已經硬夾在 100 筆內，而 token 的數量級
    // 本來就遠小於這個。真的需要分頁時再加，不要先長出一個沒人用的 cursor。
    Ok(Json(json!({ "items": items })))
}

/// `DELETE /api/v1/tokens/{id}`。撤銷後該 token 立刻無法認證。
pub async fn revoke_token(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    principal.role.require(Permission::Admin)?;
    let found = state.auth.tokens.revoke(id, Utc::now()).await?;
    audit(
        &state,
        &principal,
        AUDIT_TOKEN_REVOKE,
        Some(id.to_string()),
        if found { "success" } else { "not_found" },
        json!({}),
    )
    .await;
    if !found {
        return Err(ApiError::not_found(format!(
            "找不到 API token `{id}`。請用 GET /api/v1/tokens 確認 id"
        )));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn audit(
    state: &AppState,
    principal: &Principal,
    action: &str,
    resource_id: Option<String>,
    outcome: &str,
    metadata: Value,
) {
    let entry = AuditEntry::new(
        principal.subject.clone(),
        action,
        RESOURCE,
        resource_id,
        outcome,
    )
    .with_metadata(metadata);
    if let Err(err) = state.audit.append(entry).await {
        tracing::error!(error = %err, %action, "寫入 API token 稽核紀錄失敗");
    }
}
