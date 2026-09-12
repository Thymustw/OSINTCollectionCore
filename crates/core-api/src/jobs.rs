//! Job HTTP handlers。沒有接 Postgres 時回 503。
//!
//! 三個寫入動作（create／transition／dispatch）都寫稽核。**成功與失敗都寫**：
//! 只記成功的話，「誰一直試圖把一個已完成的 job 轉回 running」這種訊號完全看不到。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use core_model::{Job, JobStatus};
use core_security::{AuditEntry, Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::state::AppState;

/// 稽核 action。字串會進資料表，改動等於改稽核查詢條件——
/// `docs/developer/security.md` 的動作清單要一起改。
pub const AUDIT_JOB_CREATE: &str = "job.create";
pub const AUDIT_JOB_TRANSITION: &str = "job.transition";
pub const AUDIT_JOB_DISPATCH: &str = "job.dispatch";
/// SPEC §31「retry action is audited」直接對應這一列。
pub const AUDIT_JOB_RETRY: &str = "job.retry";

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

/// `GET /api/v1/jobs` 的 query。`status` 是 Operations Center 最常用的過濾條件
/// （「現在有哪些 job 掛了」）。
#[derive(Debug, Clone, Deserialize)]
pub struct ListJobsQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// `queued`／`running`／`completed`／`retrying`／`failed`／`cancelled`。
    pub status: Option<JobStatus>,
}

pub async fn list_jobs(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListJobsQuery>,
) -> Result<Json<CursorPage<Job>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let jobs = jobs_or_unavailable(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    // 有 status 就走 `list_jobs_by_status`——那個方法把過濾放進 SQL。
    // 先取一頁再在程式端 filter 是錯的：佇列裡有一萬筆 queued 時，
    // 取回最新 100 筆再濾出 failed 的會得到空頁，而「沒有失敗的 job」與
    // 「最新 100 筆裡沒有失敗的 job」是完全不同的兩件事。
    let items = match query.status {
        Some(status) => jobs.list_by_status(status, after, limit).await?,
        None => jobs.list(after, limit).await?,
    };
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
    ip: ClientIp,
    Json(body): Json<CreateJobBody>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    principal.role.require(Permission::Write)?;
    let job_type = body.job_type.clone();
    let dispatch = body.dispatch;
    let result = create_job_inner(&state, body).await;
    match &result {
        Ok(job) => {
            audit(
                &state,
                &principal,
                &ip,
                AUDIT_JOB_CREATE,
                Some(job.id.to_string()),
                "success",
                json!({ "type": job.job_type, "dispatch": dispatch, "status": job.status }),
            )
            .await;
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AUDIT_JOB_CREATE,
                None,
                "rejected",
                json!({ "type": job_type, "dispatch": dispatch, "status_code": err.status.as_u16(), "error": err.error }),
            )
            .await;
        }
    }
    result.map(|job| (StatusCode::CREATED, Json(job)))
}

async fn create_job_inner(state: &AppState, body: CreateJobBody) -> Result<Job, ApiError> {
    if body.job_type.trim().is_empty() {
        return Err(ApiError::bad_request(
            "type 不可為空。請提供 job 種類，例如 collect",
        ));
    }
    let jobs = jobs_or_unavailable(state)?;
    let job = if body.dispatch {
        jobs.create_and_dispatch(body.job_type, body.correlation_id)
            .await?
    } else {
        jobs.create(body.job_type, body.correlation_id).await?
    };
    Ok(job)
}

pub async fn transition_job(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
    Json(body): Json<TransitionBody>,
) -> Result<Json<Job>, ApiError> {
    principal.role.require(Permission::Write)?;
    let requested = body.status;
    let result = async {
        let jobs = jobs_or_unavailable(&state)?;
        jobs.transition(id, body.status, body.error)
            .await
            .map_err(ApiError::from)
    }
    .await;
    audit_outcome(
        &state,
        &principal,
        &ip,
        AUDIT_JOB_TRANSITION,
        id,
        &result,
        json!({ "requested_status": requested }),
    )
    .await;
    result.map(Json)
}

pub async fn dispatch_job(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, ApiError> {
    principal.role.require(Permission::Write)?;
    let result = async {
        let jobs = jobs_or_unavailable(&state)?;
        jobs.dispatch(id).await.map_err(ApiError::from)
    }
    .await;
    audit_outcome(
        &state,
        &principal,
        &ip,
        AUDIT_JOB_DISPATCH,
        id,
        &result,
        json!({}),
    )
    .await;
    result.map(Json)
}

/// `POST /api/v1/jobs/{id}/retry`（SPEC §31）。operator 以上。
///
/// 只對 `failed` 的 job 有效，其他狀態回 409。成功與失敗都寫稽核——
/// SPEC §31 的驗收條件之一就是「retry action is audited」，
/// 而「viewer 重試被擋下來」這件事同樣要查得到（那條在 middleware 寫 `authz.denied`）。
pub async fn retry_job(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Job>, ApiError> {
    principal.role.require(Permission::Write)?;
    let result = async {
        let jobs = jobs_or_unavailable(&state)?;
        jobs.retry(id).await.map_err(ApiError::from)
    }
    .await;
    audit_outcome(
        &state,
        &principal,
        &ip,
        AUDIT_JOB_RETRY,
        id,
        &result,
        json!({}),
    )
    .await;
    result.map(Json)
}

/// 對「已知 job id」的動作寫稽核。成功時補上結果狀態，失敗時補上狀態碼與錯誤碼。
async fn audit_outcome(
    state: &AppState,
    principal: &Principal,
    ip: &ClientIp,
    action: &str,
    id: Uuid,
    result: &Result<Job, ApiError>,
    mut metadata: Value,
) {
    let outcome = match result {
        Ok(job) => {
            merge(&mut metadata, "result_status", json!(job.status));
            "success"
        }
        Err(err) => {
            merge(&mut metadata, "status_code", json!(err.status.as_u16()));
            merge(&mut metadata, "error", json!(err.error));
            "rejected"
        }
    };
    audit(
        state,
        principal,
        ip,
        action,
        Some(id.to_string()),
        outcome,
        metadata,
    )
    .await;
}

fn merge(target: &mut Value, key: &str, value: Value) {
    if let Some(map) = target.as_object_mut() {
        map.insert(key.to_string(), value);
    }
}

async fn audit(
    state: &AppState,
    principal: &Principal,
    ip: &ClientIp,
    action: &str,
    resource_id: Option<String>,
    outcome: &str,
    metadata: Value,
) {
    let entry = AuditEntry::new(
        principal.subject.clone(),
        action,
        "job",
        resource_id,
        outcome,
    )
    .with_ip(ip.0.clone())
    .with_metadata(metadata);
    if let Err(err) = state.audit.append(entry).await {
        tracing::error!(error = %err, %action, "寫入 Job 稽核紀錄失敗");
    }
}
