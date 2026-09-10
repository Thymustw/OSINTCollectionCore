//! Axum API skeleton。

mod error;
mod extractors;
mod jobs;
mod middleware;
mod pagination;
mod rate_limit;
mod ready;
mod routes;
mod state;

pub use error::{ApiError, ErrorBody};
pub use pagination::{CursorPage, Pagination};
pub use rate_limit::RateLimiter;
pub use ready::{PostgresReady, ReadyCheck, ReadyProbe};
pub use routes::router;
pub use state::{AppState, AuthState};

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
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        jobs: None,
        ready: ready::ReadyProbe::always_ready(),
        rate_limit_per_second: 100,
        request_body_limit_bytes: 1_048_576,
        rate_limiter: rate_limit::RateLimiter::new(100),
    };
    router(state)
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
