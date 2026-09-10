//! Job HTTP handlers。沒有接 Postgres 時回 503。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use uuid::Uuid;

use core_model::{Job, JobStatus};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::pagination::{CursorPage, Pagination};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct CreateJobBody {
    #[serde(rename = "type")]
    pub job_type: String,
    pub correlation_id: Option<Uuid>,
    /// 預設 true：建立後立刻 produce `job.dispatched`。
    #[serde(default = "default_true")]
    pub dispatch: bool,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct TransitionBody {
    pub status: JobStatus,
    pub error: Option<String>,
}

fn jobs_or_unavailable(state: &AppState) -> Result<&crate::state::SharedJobService, ApiError> {
    state.jobs.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Job 系統未接上 Postgres。請設定 DATABASE_URL 並重啟 osint-api",
        )
    })
}

pub async fn list_jobs(
    State(state): State<AppState>,
    principal: Principal,
    Query(page): Query<Pagination>,
) -> Result<Json<CursorPage<Job>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let jobs = jobs_or_unavailable(&state)?;
    let (after, limit) = page.decode()?;
    let items = jobs.list(after, limit).await?;
    Ok(Json(CursorPage::from_items(items, limit, |job| job.id)))
}

pub async fn get_job(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, ApiError> {
    principal.role.require(Permission::Read)?;
    let jobs = jobs_or_unavailable(&state)?;
    Ok(Json(jobs.get(id).await?))
}

pub async fn create_job(
    State(state): State<AppState>,
    principal: Principal,
    Json(body): Json<CreateJobBody>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    principal.role.require(Permission::Write)?;
    if body.job_type.trim().is_empty() {
        return Err(ApiError::bad_request(
            "type 不可為空。請提供 job 種類，例如 collect",
        ));
    }
    let jobs = jobs_or_unavailable(&state)?;
    let job = if body.dispatch {
        jobs.create_and_dispatch(body.job_type, body.correlation_id)
            .await?
    } else {
        jobs.create(body.job_type, body.correlation_id).await?
    };
    Ok((StatusCode::CREATED, Json(job)))
}

pub async fn transition_job(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Json(body): Json<TransitionBody>,
) -> Result<Json<Job>, ApiError> {
    principal.role.require(Permission::Write)?;
    let jobs = jobs_or_unavailable(&state)?;
    Ok(Json(jobs.transition(id, body.status, body.error).await?))
}

pub async fn dispatch_job(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, ApiError> {
    principal.role.require(Permission::Write)?;
    let jobs = jobs_or_unavailable(&state)?;
    Ok(Json(jobs.dispatch(id).await?))
}
