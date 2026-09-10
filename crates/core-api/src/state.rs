//! 共享狀態。JobService 可選，沒有 Postgres 時 health 仍可活。

use std::sync::Arc;

use core_jobs::JobService;
use core_observability::MetricsRegistry;
use core_security::{ApiTokenStore, AuditLog, JwtService};
use storage_postgres::PostgresCanonicalStore;

use crate::ready::ReadyProbe;

pub type SharedTokenStore = Arc<dyn ApiTokenStore>;
pub type SharedAudit = Arc<dyn AuditLog>;
pub type SharedJobService = Arc<JobService<PostgresCanonicalStore>>;

/// 認證相關。
#[derive(Clone)]
pub struct AuthState {
    pub jwt: Arc<JwtService>,
    pub tokens: SharedTokenStore,
}

/// 整個 API 的狀態。
#[derive(Clone)]
pub struct AppState {
    pub metrics: MetricsRegistry,
    pub auth: AuthState,
    pub audit: SharedAudit,
    pub jobs: Option<SharedJobService>,
    pub ready: ReadyProbe,
    pub rate_limit_per_second: u32,
    pub request_body_limit_bytes: u32,
    pub rate_limiter: crate::rate_limit::RateLimiter,
}
