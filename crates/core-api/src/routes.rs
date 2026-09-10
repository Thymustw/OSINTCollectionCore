//! Router 組裝。

use std::time::Duration;

use axum::extract::State;
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
use crate::jobs;
use crate::middleware as auth_mw;
use crate::rate_limit;
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
        .merge(write)
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            auth_mw::authenticate,
        ));

    let body_limit = state.request_body_limit_bytes as usize;

    public
        .merge(protected)
        .layer(middleware::from_fn_with_state(
            state.clone(),
            rate_limit::rate_limit,
        ))
        .layer(
            ServiceBuilder::new()
                .layer(CatchPanicLayer::new())
                .layer(TraceLayer::new_for_http())
                .layer(RequestBodyLimitLayer::new(body_limit))
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
