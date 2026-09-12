//! 整合測試共用的最小 app。
//!
//! # 為什麼在這裡，不在 `src/lib.rs`
//!
//! 這個檔案裡有一把**硬編碼的 JWT 密鑰**。它放在 `tests/` 底下，所以只會被編進
//! 測試 binary，永遠不會進 `libcore_api` 或 `osint-api` 的 release 產物。
//!
//! 在 Phase 6a 之前同樣的東西放在 `src/lib.rs` 且沒有 `#[cfg(test)]` gate——
//! 那把 `tttt…` 密鑰會被編進生產 binary。它不會讓誰直接登入（生產的 `JwtService`
//! 用的是 `JWT_SECRET`），但一把靜態密鑰出現在發佈產物裡就是不能接受的：
//! 只要有一條路徑不小心用到 `test_jwt()`，任何人都能簽出 admin token。
//!
//! **不要把這個檔案的任何東西搬回 `src/`。**

#![allow(dead_code)] // 每支測試只用得到其中幾個 helper。

use std::sync::Arc;

use core_api::{AppState, AuthState, RateLimiter, ReadyCheck, ReadyProbe, ready_always, router};
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};

/// 測試用 JWT 密鑰。**只在測試 binary 裡**（見模組註解）。
const TEST_JWT_SECRET: &[u8; 32] = &[b't'; 32];

/// 從固定 secret 建測試 JWT service。
pub fn test_jwt() -> JwtService {
    JwtService::new(TEST_JWT_SECRET, "osint-core", chrono::Duration::hours(1)).expect("test jwt")
}

/// 建一把指定角色的 JWT，連同簽它的 service 一起回傳。
pub fn issue_test_jwt(role: Role) -> (JwtService, String) {
    let jwt = test_jwt();
    let token = jwt.issue("test-user", role).expect("issue");
    (jwt, token)
}

/// 測試與本機不接 DB 時的最小 app（記憶體 token store 與稽核）。
pub fn test_app(jwt: JwtService) -> axum::Router {
    test_app_parts(jwt).0
}

/// 同 [`test_app`]，另外回傳稽核與 token store，讓測試可以驗證「真的有寫」。
pub fn test_app_parts(jwt: JwtService) -> (axum::Router, MemoryAuditLog, Arc<MemoryApiTokenStore>) {
    test_app_with_backends(jwt, Vec::new(), DEFAULT_MISSING.to_vec())
}

/// 預設「什麼後端都沒接」。
///
/// 這份清單要跟 `core-api/src/main.rs` 會 push 進 `missing` 的後端維持一致——
/// 少一個的話，`ops_health_separates_not_configured_from_broken` 就不再是在驗
/// 「全部沒接」，而是在驗「五個沒接、一個不知道去哪了」。
/// neo4j 是 V0.2 phase 0b 加的。
const DEFAULT_MISSING: [&str; 6] = [
    "postgres",
    "object_store",
    "redis",
    "opensearch",
    "redpanda",
    "neo4j",
];

/// 同 [`test_app_parts`]，但可以指定 `/api/v1/ops/health` 要跑哪些檢查。
///
/// 驗「某個後端掛掉時回 503 且指得出是哪一個」時用這個：預設的空清單是
/// healthy（空集合的 `all()` 是 true），放一個會 down 的檢查進來才測得到那條路徑。
pub fn test_app_with_backends(
    jwt: JwtService,
    checks: Vec<Arc<dyn ReadyCheck>>,
    missing: Vec<&'static str>,
) -> (axum::Router, MemoryAuditLog, Arc<MemoryApiTokenStore>) {
    let audit = MemoryAuditLog::new();
    let tokens = Arc::new(MemoryApiTokenStore::new());
    let state = AppState {
        metrics: core_observability::MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: tokens.clone(),
        },
        audit: Arc::new(audit.clone()),
        // 不接 Postgres／MinIO：資源類 handler 回 503，
        // 但認證／RBAC／稽核的測試仍然有效——middleware 在 handler 之前就跑完了。
        store: None,
        objects: None,
        jobs: None,
        import: None,
        // 不接 OpenSearch：`POST /api/v1/search` 回 503。
        search: None,
        ready: ready_always(),
        backends: ReadyProbe::new(checks),
        // 這支測試不驗 /ops/queues。
        queues: None,
        backends_missing: missing,
        rate_limit_per_second: 100,
        request_body_limit_bytes: 1_048_576,
        object_bucket: String::new(),
        // 測試用小上限：不需要為了驗證 413 真的傳 10 MiB 進來。
        import_config: core_config::ImportSection {
            max_upload_bytes: 4_096,
            ..core_config::ImportSection::default()
        },
        rate_limiter: RateLimiter::new(100),
    };
    (router(state), audit, tokens)
}
