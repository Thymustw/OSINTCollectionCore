//! Axum API skeleton。
//!
//! # 這裡沒有測試用的 app 與密鑰
//!
//! Phase 6a 之前 `test_jwt()` / `test_app()` / `issue_test_jwt()` 就放在這個檔，
//! 用一個「32 個相同位元組」的字面量當 JWT 密鑰，而且**沒有 `#[cfg(test)]`**——
//! 也就是說那段硬編碼密鑰會被編進 `libcore_api` 與 `osint-api` 的 release binary。
//! 它們已經搬到 `tests/common/mod.rs`（整合測試的 crate 裡，不會進生產產物）。
//!
//! `tests/no_test_secret_in_src.rs` 會掃這個目錄，防止同樣的東西被放回來。
//! （那支測試是純字串比對，所以這裡連舉例都不能寫出那個字面量。）
//!
//! 沒有改用 `#[cfg(feature = "test-util")]` 的理由：那需要 crate 自己
//! dev-depend 自己才能在整合測試裡打開 feature，會出現「同一個 crate 兩份」
//! 的型別不相容問題。搬到 `tests/common/` 沒有這個風險，
//! 而且讓「生產 build 不含測試密鑰」變成結構上必然，不是靠設定正確。

mod error;
mod extractors;
mod import;
mod jobs;
mod middleware;
mod ops;
mod pagination;
mod rate_limit;
mod ready;
mod resources;
mod routes;
mod search;
mod state;
mod tokens;

pub use error::{ApiError, ErrorBody};
pub use extractors::ClientIp;
pub use import::{AUDIT_ACTION as IMPORT_AUDIT_ACTION, ImportRequest};
pub use jobs::{AUDIT_JOB_CREATE, AUDIT_JOB_DISPATCH, AUDIT_JOB_RETRY, AUDIT_JOB_TRANSITION};
pub use middleware::{AUDIT_AUTH_FAILED, AUDIT_AUTHZ_DENIED};
pub use ops::{
    BackendCheck, BrokerCheck, ConnectorHealth, ConnectorsView, DlqView, GraphCheck, OpsHealth,
    ProcessMetrics, QueueBinding, QueueEntry, QueueInspector, QueueSummary,
};
pub use pagination::{CursorPage, Pagination};
pub use rate_limit::RateLimiter;
pub use ready::{PostgresReady, ReadyCheck, ReadyProbe};
pub use resources::collections::CollectionDetail;
pub use resources::entities::EntityDetail;
pub use resources::objects::{ExtractedEntity, ObjectDetail};
pub use resources::relationships::RelationshipDetail;
pub use resources::{
    AUDIT_COLLECTION_CREATE, AUDIT_CONNECTOR_CREATE, AUDIT_CONNECTOR_UPDATE, AUDIT_ENTITY_MERGE,
    AUDIT_ENTITY_RESOLVE, AUDIT_MERGE_UNDO, AUDIT_OBJECT_CREATE, AUDIT_SOURCE_CREATE,
    AUDIT_SOURCE_UPDATE,
};
pub use routes::router;
pub use search::{EntitySummary, SearchHitBody, SearchResponse};
pub use state::{
    AppState, AuthState, ImportState, SearchState, SharedObjects, SharedStore, SharedTokenStore,
};
pub use tokens::{
    AUDIT_TOKEN_ISSUE, AUDIT_TOKEN_LIST, AUDIT_TOKEN_REVOKE, IssueTokenBody, TokenSummary,
};

/// 不接下游的 readiness（測試／Postgres 掛掉時仍讓行程活著）。
#[must_use]
pub fn ready_always() -> ReadyProbe {
    ReadyProbe::always_ready()
}
