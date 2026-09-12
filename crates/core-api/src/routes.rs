//! Router 組裝。

use std::time::Duration;

use axum::extract::{DefaultBodyLimit, State};
use axum::http::StatusCode;
use axum::middleware;
use axum::routing::{get, post};
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

/// 組出完整 router。
pub fn router(state: AppState) -> Router {
    let public = Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics));

    let write = Router::new()
        .route("/api/v1/jobs", post(jobs::create_job))
        .route("/api/v1/jobs/{id}/transition", post(jobs::transition_job))
        .route("/api/v1/jobs/{id}/dispatch", post(jobs::dispatch_job))
        .route_layer(middleware::from_fn(auth_mw::require_write));

    let protected = Router::new()
        .route("/api/v1/jobs", get(jobs::list_jobs))
        .route("/api/v1/jobs/{id}", get(jobs::get_job))
        .route("/api/v1/whoami", get(whoami))
        // 搜尋是唯讀的（viewer 以上），但用 POST：查詢條件有巢狀結構
        // （entity 物件、日期、布林語法），塞進 query string 會需要多層編碼，
        // 而且長查詢會撞到 URL 長度上限。這與 SPEC §19 的 `POST /search` 一致。
        .route("/api/v1/search", post(search::search))
        .merge(write)
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
        .route_layer(middleware::from_fn(auth_mw::require_write))
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
