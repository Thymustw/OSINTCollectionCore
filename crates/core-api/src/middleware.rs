//! Auth 與 RBAC middleware。
//!
//! # 認證／授權失敗一定要寫稽核
//!
//! Phase 6a 之前只有 `POST /api/v1/import` 會寫稽核，失敗的認證完全沒有留痕——
//! 也就是說「有人拿著撤銷的 token 敲了三千次」這件事在系統裡查不到。
//! 現在 401 與 403 都各寫一列。
//!
//! ⚠️ **已知取捨**：未認證的請求任何人都能發，所以 `auth.failed` 這個 action
//! 的寫入速率等同於外部可控的請求速率（全域 rate limit 之內）。V0.1 沒有稽核
//! 保留策略（沒有分區、沒有清除排程），長期跑會讓 `audit_log` 一直長。
//! 這一點寫在 `docs/developer/security.md`，要在 V0.2 用保留策略處理。

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, State};
use axum::http::{Request, header};
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;
use serde_json::json;

use core_security::{
    AuditEntry, AuthMethod, Permission, Principal, Role, parse_presented_token, verify_secret,
};

use crate::error::ApiError;
use crate::state::AppState;

/// 認證失敗的稽核 action。
pub const AUDIT_AUTH_FAILED: &str = "auth.failed";
/// 授權（RBAC）拒絕的稽核 action。
pub const AUDIT_AUTHZ_DENIED: &str = "authz.denied";

/// JWT Bearer 或 API token。失敗回 401 JSON，並寫一列 `auth.failed`。
///
/// ⚠️ **不要在 `await` 之前持有 `&Request`**（連唯讀借用都不行）。
/// axum 的 `Body` 不是 `Sync`，借用跨過 await 點會讓整個 future 失去 `Send`，
/// 而編譯器報出來的是 `FromFn<…>: Service<…>` 不滿足——訊息完全指不到真因。
/// 所以這裡先同步把需要的東西（憑證字串、IP、路徑）抄出來再進 async。
pub async fn authenticate(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let ip = peer_ip(&request);
    let path = request.uri().path().to_string();
    let credential = extract_bearer(&request);

    let principal = match resolve_principal(&state, credential).await {
        Ok(principal) => principal,
        Err(err) => {
            // 不記 token 本身（連前綴都不記）——稽核表不該存任何可用來重放的東西。
            // 只記「用的是哪一類憑證」與失敗原因的分類字串。
            audit(
                &state,
                AuditEntry::new("anonymous", AUDIT_AUTH_FAILED, "auth", None, "denied")
                    .with_ip(ip)
                    .with_metadata(json!({
                        "path": path,
                        "status": err.status.as_u16(),
                        "reason": err.error,
                    })),
            )
            .await;
            return Err(err);
        }
    };

    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

/// 同步取出 `Authorization: Bearer <token>` 的 token 部分。
///
/// 三種失敗（沒有 header／不是 Bearer／空字串）各給不同訊息，因為使用者
/// 要做的事不一樣。回傳 `Err` 代表憑證格式本身就不對，還沒開始驗。
fn extract_bearer(request: &Request<axum::body::Body>) -> Result<String, ApiError> {
    let header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| {
            ApiError::unauthorized(
                "缺少 Authorization header。請用 Bearer <jwt> 或 Bearer <api-token>",
            )
        })?;
    let token = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .ok_or_else(|| {
            ApiError::unauthorized(
                "Authorization 必須是 Bearer。請改成 `Authorization: Bearer <token>`",
            )
        })?
        .trim();
    if token.is_empty() {
        return Err(ApiError::unauthorized("Bearer token 是空的"));
    }
    Ok(token.to_string())
}

async fn resolve_principal(
    state: &AppState,
    credential: Result<String, ApiError>,
) -> Result<Principal, ApiError> {
    let token = credential?;
    if token.starts_with("osint_") {
        authenticate_api_token(state, &token).await
    } else {
        authenticate_jwt(state, &token)
    }
}

fn authenticate_jwt(state: &AppState, token: &str) -> Result<Principal, ApiError> {
    let claims = state.auth.jwt.verify(token)?;
    Ok(Principal {
        subject: claims.sub,
        role: claims.role,
        auth_method: AuthMethod::Jwt,
    })
}

async fn authenticate_api_token(state: &AppState, token: &str) -> Result<Principal, ApiError> {
    let presented = parse_presented_token(token)?;
    let record = state.auth.tokens.get(presented.id).await?.ok_or_else(|| {
        // 刻意**不用** `SecurityError::TokenNotFound`（那會被映成 404）。
        // 在認證路徑上區分「這個 id 不存在」與「secret 不對」等於送出一個
        // token id 的枚舉 oracle：攻擊者可以用回應碼掃出哪些 id 是真的。
        // 兩種情況都回同一句 401。
        ApiError::unauthorized("API token 無效。請確認複製完整 token，或請管理者重新發行")
    })?;
    record.ensure_usable(Utc::now())?;

    // argon2 是刻意設計成 CPU-heavy 的（預設參數在本機約數十毫秒）。
    // 直接在 async context 裡跑會佔住 Tokio executor 執行緒，違反 CLAUDE.md §5
    // 「CPU-heavy work must not block Tokio executor threads」——高併發下
    // 這一行會讓**所有**路由的延遲一起變差，而不只是認證。
    let secret = presented.secret.clone();
    let hash = record.secret_hash.clone();
    let verified = tokio::task::spawn_blocking(move || verify_secret(&secret, &hash))
        .await
        .map_err(|err| {
            ApiError::internal(format!(
                "驗證 API token 的工作執行緒中止：{err}。請重試；持續發生請看 osint-api 記錄檔"
            ))
        })??;
    if !verified {
        return Err(ApiError::unauthorized(
            "API token 無效。請確認複製完整 token，或請管理者重新發行",
        ));
    }

    // 失敗只記 log：token 是對的，不該因為更新「最後使用時間」失敗就擋下請求。
    if let Err(err) = state
        .auth
        .tokens
        .touch_last_used(presented.id, Utc::now())
        .await
    {
        tracing::warn!(error = %err, "更新 api_tokens.last_used_at 失敗");
    }
    Ok(Principal {
        subject: format!("token:{}", record.name),
        role: record.role,
        auth_method: AuthMethod::ApiToken,
    })
}

/// 需要 Write 權限。
pub async fn require_write(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let checked = check(&request, Permission::Write);
    enforce(&state, checked).await?;
    Ok(next.run(request).await)
}

/// 需要 Admin 權限。V0.1 的 admin-only 路由是 `/api/v1/tokens`。
pub async fn require_admin(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    let checked = check(&request, Permission::Admin);
    enforce(&state, checked).await?;
    Ok(next.run(request).await)
}

/// RBAC 判斷的結果。`Denied` 帶著寫稽核需要的全部欄位，
/// 這樣 `enforce` 就不必再碰 `Request`（理由見 `authenticate` 的註解）。
enum RbacCheck {
    Allowed,
    Denied {
        subject: String,
        role: Role,
        permission: Permission,
        path: String,
        ip: Option<String>,
    },
    /// 沒有 `Principal` extension，代表 router 組裝有誤。
    NotAuthenticated,
}

/// 同步做完 RBAC 判斷，不 await。
fn check(request: &Request<axum::body::Body>, permission: Permission) -> RbacCheck {
    let Some(principal) = request.extensions().get::<Principal>() else {
        return RbacCheck::NotAuthenticated;
    };
    if principal.role.allows(permission) {
        return RbacCheck::Allowed;
    }
    RbacCheck::Denied {
        subject: principal.subject.clone(),
        role: principal.role,
        permission,
        path: request.uri().path().to_string(),
        ip: peer_ip(request),
    }
}

async fn enforce(state: &AppState, checked: RbacCheck) -> Result<(), ApiError> {
    match checked {
        RbacCheck::Allowed => Ok(()),
        RbacCheck::NotAuthenticated => Err(ApiError::unauthorized(
            "尚未認證。若這是內部路由組裝問題，請確認 require_write／require_admin 掛在 authenticate 之後",
        )),
        RbacCheck::Denied {
            subject,
            role,
            permission,
            path,
            ip,
        } => {
            audit(
                state,
                AuditEntry::new(subject, AUDIT_AUTHZ_DENIED, "auth", None, "denied")
                    .with_ip(ip)
                    .with_metadata(json!({
                        "path": path,
                        "role": role.as_str(),
                        "required_permission": permission.as_str(),
                    })),
            )
            .await;
            Err(role.require(permission).unwrap_err().into())
        }
    }
}

/// 取呼叫端 IP。
///
/// 只認 `ConnectInfo`（TCP peer）。**刻意不讀 `X-Forwarded-For`**：那是呼叫端
/// 完全可控的字串，在沒有可信任的 reverse proxy 把它覆寫掉之前採信它，
/// 等於讓任何人都能往稽核表裡寫任意 IP——那比沒有 IP 更糟，因為它看起來像證據。
/// 之後要支援 proxy 佈署時，必須連同「哪些 proxy 可信」一起設計。
///
/// 測試用 `oneshot` 直接呼叫 router，沒有 `ConnectInfo`，所以會是 `None`。
fn peer_ip(request: &Request<axum::body::Body>) -> Option<String> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|info| info.0.ip().to_string())
}

async fn audit(state: &AppState, entry: AuditEntry) {
    let action = entry.action.clone();
    if let Err(err) = state.audit.append(entry).await {
        // 稽核寫不進去不會擋下請求（那會讓 DB 抖一下就變成全面 503），
        // 但一定要在 log 裡看得見——這是「有動作但沒留痕」的唯一線索。
        tracing::error!(error = %err, %action, "寫入稽核紀錄失敗");
    }
}
