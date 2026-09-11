//! 共享狀態。JobService 可選，沒有 Postgres 時 health 仍可活。

use std::sync::Arc;

use connector_sdk::EvidenceSink;
use core_config::ImportSection;
use core_events::EventProducer;
use core_jobs::JobService;
use core_observability::MetricsRegistry;
use core_security::{ApiTokenStore, AuditLog, JwtService};
use storage_postgres::PostgresCanonicalStore;

use crate::ready::ReadyProbe;

pub type SharedTokenStore = Arc<dyn ApiTokenStore>;
pub type SharedAudit = Arc<dyn AuditLog>;
pub type SharedJobService = Arc<JobService<PostgresCanonicalStore>>;

/// 匯入路徑要用到的下游。沒接上時 `POST /api/v1/import` 回 503，其他路由不受影響。
///
/// `sink` 直接用 `connector-sdk` 的 `EvidenceSink`：push 與 pull 兩條路徑寫 RawEvidence
/// 的方式必須是同一套（同樣的 sha256、同樣的 storage_path、同樣的失敗回滾），
/// 不能各寫各的。
#[derive(Clone)]
pub struct ImportState {
    pub store: PostgresCanonicalStore,
    pub sink: Arc<dyn EvidenceSink>,
    pub producer: Option<Arc<EventProducer>>,
}

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
    pub import: Option<Arc<ImportState>>,
    pub ready: ReadyProbe,
    pub rate_limit_per_second: u32,
    pub request_body_limit_bytes: u32,
    pub import_config: ImportSection,
    pub rate_limiter: crate::rate_limit::RateLimiter,
}
