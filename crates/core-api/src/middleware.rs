//! Auth 與 RBAC middleware。

use axum::extract::State;
use axum::http::{Request, header};
use axum::middleware::Next;
use axum::response::Response;
use chrono::Utc;

use core_security::{
    AuthMethod, Permission, Principal, Role, SecurityError, parse_presented_token, verify_secret,
};

use crate::error::ApiError;
use crate::state::AppState;

/// JWT Bearer 或 API token。失敗回 401 JSON。
pub async fn authenticate(
    State(state): State<AppState>,
    mut request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
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

    let principal = if token.starts_with("osint_") {
        authenticate_api_token(&state, token).await?
    } else {
        authenticate_jwt(&state, token)?
    };
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
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
    let record =
        state
            .auth
            .tokens
            .get(presented.id)
            .await?
            .ok_or(SecurityError::TokenNotFound {
                id: presented.id.to_string(),
            })?;
    if record.revoked_at.is_some() {
        return Err(SecurityError::TokenRevoked.into());
    }
    if !verify_secret(&presented.secret, &record.secret_hash)? {
        return Err(ApiError::unauthorized(
            "API token secret 不符。請確認複製完整 token，或重新發行",
        ));
    }
    let _ = state
        .auth
        .tokens
        .touch_last_used(presented.id, Utc::now())
        .await;
    Ok(Principal {
        subject: format!("token:{}", record.name),
        role: record.role,
        auth_method: AuthMethod::ApiToken,
    })
}

/// 需要 Write 權限。
pub async fn require_write(
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    require_permission(&request, Permission::Write)?;
    Ok(next.run(request).await)
}

/// 需要 Admin 權限。V0.1 skeleton 尚無 admin-only 路由，留給後續版本。
#[allow(dead_code)]
pub async fn require_admin(
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, ApiError> {
    require_permission(&request, Permission::Admin)?;
    Ok(next.run(request).await)
}

fn require_permission(
    request: &Request<axum::body::Body>,
    permission: Permission,
) -> Result<(), ApiError> {
    let Some(principal) = request.extensions().get::<Principal>() else {
        return Err(ApiError::unauthorized("尚未認證"));
    };
    principal.role.require(permission)?;
    Ok(())
}

/// 給 handler 直接呼叫。
#[allow(dead_code)]
pub fn check_role(role: Role, permission: Permission) -> Result<(), ApiError> {
    role.require(permission).map_err(ApiError::from)
}
