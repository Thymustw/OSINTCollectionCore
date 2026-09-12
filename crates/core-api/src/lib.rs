//! Axum API skeleton。

mod error;
mod extractors;
mod import;
mod jobs;
mod middleware;
mod pagination;
mod rate_limit;
mod ready;
mod routes;
mod search;
mod state;

pub use error::{ApiError, ErrorBody};
pub use import::{AUDIT_ACTION as IMPORT_AUDIT_ACTION, ImportRequest};
pub use pagination::{CursorPage, Pagination};
pub use rate_limit::RateLimiter;
pub use ready::{PostgresReady, ReadyCheck, ReadyProbe};
pub use routes::router;
pub use search::{EntitySummary, SearchHitBody, SearchResponse};
pub use state::{AppState, AuthState, ImportState, SearchState};

/// 不接下游的 readiness（測試／Postgres 掛掉時仍讓行程活著）。
#[must_use]
pub fn ready_always() -> ReadyProbe {
    ReadyProbe::always_ready()
}

use axum::Router;
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use std::sync::Arc;

/// 測試與本機不接 DB 時的最小 app（記憶體 token store）。
pub fn test_app(jwt: JwtService) -> Router {
    test_app_parts(jwt).0
}

/// 同 `test_app`，另外回傳稽核紀錄，讓測試可以驗證「真的有寫」。
pub fn test_app_parts(jwt: JwtService) -> (Router, MemoryAuditLog) {
    let audit = MemoryAuditLog::new();
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(audit.clone()),
        jobs: None,
        import: None,
        // 測試 app 不接 OpenSearch：`POST /api/v1/search` 回 503。
        // 認證／RBAC 的測試仍然有效——middleware 在 handler 之前就擋下來了。
        search: None,
        ready: ready::ReadyProbe::always_ready(),
        rate_limit_per_second: 100,
        request_body_limit_bytes: 1_048_576,
        // 測試用小上限：不需要為了驗證 413 真的傳 10 MiB 進來。
        import_config: core_config::ImportSection {
            max_upload_bytes: 4_096,
            ..core_config::ImportSection::default()
        },
        rate_limiter: rate_limit::RateLimiter::new(100),
    };
    (router(state), audit)
}

/// 從 secret bytes 建測試 JWT。
pub fn test_jwt() -> JwtService {
    JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("test jwt")
}

/// 測試用 admin token。
pub fn issue_test_jwt(role: Role) -> (JwtService, String) {
    let jwt = test_jwt();
    let token = jwt.issue("test-user", role).expect("issue");
    (jwt, token)
}
