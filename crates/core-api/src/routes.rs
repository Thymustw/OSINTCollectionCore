//! Router 組裝。

use std::time::Duration;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use core_observability::HealthStatus;
use core_security::Principal;

use crate::error::ApiError;
use crate::import;
use crate::jobs;
use crate::middleware as auth_mw;
use crate::rate_limit;
use crate::search;
use crate::state::AppState;
use crate::tokens;

/// 組出完整 router。
pub fn router(state: AppState) -> Router {
    // `/metrics` 是公開的。取捨與部署要求見 `metrics` handler 的註解與
    // docs/developer/observability.md。
    let public = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics));

    let write = Router::new()
        .route("/api/v1/jobs", post(jobs::create_job))
        .route("/api/v1/jobs/{id}/transition", post(jobs::transition_job))
        .route("/api/v1/jobs/{id}/dispatch", post(jobs::dispatch_job))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_mw::require_write,
        ));

    // V0.1 唯一的 admin-only 群組。RBAC 矩陣裡 admin 那一欄在這之前是空的。
    let admin = Router::new()
        .route(
            "/api/v1/tokens",
            post(tokens::issue_token).get(tokens::list_tokens),
        )
        .route("/api/v1/tokens/{id}", delete(tokens::revoke_token))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_mw::require_admin,
        ));

    let protected = Router::new()
        .route("/api/v1/jobs", get(jobs::list_jobs))
        .route("/api/v1/jobs/{id}", get(jobs::get_job))
        .route("/api/v1/whoami", get(whoami))
        // 搜尋是唯讀的（viewer 以上），但用 POST：查詢條件有巢狀結構
        // （entity 物件、日期、布林語法），塞進 query string 會需要多層編碼，
        // 而且長查詢會撞到 URL 長度上限。這與 SPEC §19 的 `POST /search` 一致。
        .route("/api/v1/search", post(search::search))
        .merge(write)
        .merge(admin)
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_mw::authenticate,
        ));

    let body_limit = state.request_body_limit_bytes as usize;
    let upload_limit = usize::try_from(state.import_config.max_upload_bytes).unwrap_or(usize::MAX);
    // multipart 的 boundary／header 比檔案本身多一些。外層後盾放寬這點餘裕，
    // 讓 handler 的串流檢查先觸發——那樣回的訊息會指出「是 max_upload_bytes 擋的」，
    // 而不是只有一句籠統的太大。
    let upload_envelope = upload_limit.saturating_add(64 * 1024);

    // 檔案上傳自己一層較寬的上限，不去放寬其他路由的 body limit。
    let import_routes = Router::new()
        .route("/api/v1/import", post(import::import_upload))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_mw::require_write,
        ))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_mw::authenticate,
        ))
        // 這裡刻意用 axum 的 DefaultBodyLimit 當硬性後盾，而不是 tower-http 的
        // RequestBodyLimitLayer：後者的錯誤是 BoxError，multer 認不出它是「超過長度」，
        // 會變成 IncompleteStream → 400「multipart 格式錯誤」，把使用者指向錯的方向。
        // DefaultBodyLimit 的 LengthLimitError 是 axum 自己的型別，
        // MultipartError::status() 認得，我們才能回正確的 413。
        .layer(DefaultBodyLimit::max(upload_envelope));

    public
        .merge(protected)
        .layer(RequestBodyLimitLayer::new(body_limit))
        .layer(DefaultBodyLimit::max(body_limit))
        .merge(import_routes)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit::rate_limit,
        ))
        .layer(
            ServiceBuilder::new()
                .layer(CatchPanicLayer::new())
                .layer(TraceLayer::new_for_http())
                // Timeout 必須包 Route（Body: Default），不能包 RequestBodyLimit：
                // tower_http::limit::ResponseBody 沒有 Default，逾時無法組 408。
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(30),
                )),
        )
        .with_state(state)
}

async fn health() -> Json<HealthStatus> {
    Json(HealthStatus::alive())
}

async fn ready(
    State(state): State<AppState>,
) -> Result<Json<core_observability::ReadyStatus>, ApiError> {
    let status = state.ready.status().await;
    if status.ready {
        return Ok(Json(status));
    }
    let checks = serde_json::to_string(&status.checks).unwrap_or_default();
    Err(ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "not_ready",
        format!("尚未就緒。請看 checks 找出失敗的依賴，確認 compose 服務與 DATABASE_URL：{checks}"),
    ))
}

/// Prometheus text 輸出。**不需要認證**。
///
/// # 為什麼維持公開
///
/// 1. Prometheus 的預設 scrape 設定不帶憑證。要求 JWT 等於讓每個佈署都得先
///    處理「誰來簽一把永不過期的 scraper token」——那會生出一把比 `/metrics`
///    本身更值得保護的長期憑證。
/// 2. 這裡輸出的是**聚合計數器**：收了幾筆、重複率、佇列深度、各類錯誤數、
///    延遲總和。沒有 source 名稱、沒有 URL、沒有實體內容，也沒有任何識別碼
///    （`MetricsRegistry::render_prometheus` 的全部欄位見
///    crates/core-observability/src/metrics.rs）。外洩的是量體與健康度，
///    不是情報內容。
/// 3. 預設 `[http].bind` 是 `127.0.0.1:18080`，不對外。
///
/// # 因此部署時必須做的事
///
/// **量體本身仍是情報**（「今天收集量突然掉到零」對觀察者有意義），所以
/// `/metrics` 必須靠網路層限制，不是靠「反正沒人知道路徑」：
/// 綁 loopback 或內網介面，或放在只允許 Prometheus 來源 IP 的 reverse proxy 後面。
/// 這條要求寫在 docs/developer/observability.md 與 docs/operations/OPERATIONS.md。
///
/// 之後若真的要加認證，Prometheus 支援 `authorization.credentials_file`，
/// 屆時應該做成設定開關而不是硬性要求——不要讓本機開發也得先簽 token。
async fn metrics(
    State(state): State<AppState>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        state.metrics.render_prometheus(),
    )
}

async fn whoami(principal: Principal) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "subject": principal.subject,
        "role": principal.role,
        "auth_method": format!("{:?}", principal.auth_method),
    }))
}
