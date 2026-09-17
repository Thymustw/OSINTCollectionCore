//! Seed／Candidate／AI Run（SPEC_V0.3 §2／§6／§7／§17）。
//!
//! `POST /discovery/run`／`POST /entities/{id}/discover` 不在這裡：那兩條需要
//! Discovery Engine（`discovery-worker`，Phase 3）才有消費者，現在做只是空殼
//! dispatch。這裡只做資料層 CRUD——建立 Seed、瀏覽/核准/拒絕 Candidate、
//! 瀏覽 AI Run。
//!
//! `CandidateEvidence` 沒有獨立 endpoint：`GET /candidates/{id}` 的
//! [`CandidateDetail::evidence`] 直接把「Why was this discovered?」
//! （Acceptance C）帶出來，不需要另一條路由。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use core_model::{
    AiRun, Candidate, CandidateEvidence, CandidateStatus, Seed, SeedOrigin, SeedType,
};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{
    AuditEvent, LINK_PAGE, audit, rejected_metadata, storage_error, store, validate_text,
};
use crate::state::AppState;

const RESOURCE_SEED: &str = "seed";
const RESOURCE_CANDIDATE: &str = "candidate";

/// `Seed::value` 上限：可能是 URL／domain／keyword，比照 `sources.rs` 的
/// `MAX_URL` 量級。
const MAX_SEED_VALUE: usize = 2_048;

pub const AUDIT_SEED_CREATE: &str = "seed.create";
pub const AUDIT_CANDIDATE_APPROVE: &str = "candidate.approve";
pub const AUDIT_CANDIDATE_REJECT: &str = "candidate.reject";

// ============================================================================
// Seed
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct SeedListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// 自由字串（`Seed::status` 沒有封閉列舉，見 core-model 檔頭原則）。
    pub status: Option<String>,
    /// 給了才走 [`storage_core::RelationalStore::list_seeds_by_collection`]，
    /// 否則走全域 [`storage_core::RelationalStore::list_seeds`]。
    pub collection_id: Option<Uuid>,
}

/// `GET /api/v1/seeds`。viewer 以上。
pub async fn list_seeds(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<SeedListQuery>,
) -> Result<Json<CursorPage<Seed>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let status = query.status.as_deref();
    let items = match query.collection_id {
        Some(collection_id) => {
            store
                .list_seeds_by_collection(collection_id, status, after, limit)
                .await
        }
        None => store.list_seeds(status, after, limit).await,
    }
    .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |s| s.id)))
}

/// `POST /api/v1/seeds` 的 body。
///
/// `priority`／`confidence`／`depth`／`status` 有預設值：SPEC §2 沒有規定
/// 建立時的初始值，這裡選最中性的起點——`priority: 0`（呼叫端要提高急迫度
/// 自己指定）、`confidence: 1.0`（明確透過 API 建立代表呼叫端完全相信這筆
/// seed，不是機率性推論出來的）、`depth: 0`（API 直接建立的 seed 是
/// Discovery 展開樹的根，不是展開出來的）、`status: "pending"`
/// （跟 conformance 測試已經確立的慣例一致）。
///
/// `origin` 沒有預設值——呼叫端必須明確聲明這筆 seed 是 manual／connector／
/// discovery／…，這裡不猜。**這一層目前不檢查呼叫端聲明的 origin 是否
/// 屬實**（例如任何呼叫者都能宣稱 `origin: "ai"`）；如果之後這變成稽核或
/// 信任邊界問題，屬於後續 Phase 要補的授權檢查，不是這次資料層 CRUD 的範圍。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSeedBody {
    #[serde(default)]
    pub id: Option<Uuid>,
    pub seed_type: SeedType,
    pub value: String,
    #[serde(default)]
    pub collection_id: Option<Uuid>,
    #[serde(default)]
    pub entity_id: Option<Uuid>,
    #[serde(default)]
    pub priority: i32,
    #[serde(default = "default_confidence")]
    pub confidence: f64,
    pub origin: SeedOrigin,
    #[serde(default = "default_seed_status")]
    pub status: String,
    #[serde(default)]
    pub depth: i32,
}

fn default_confidence() -> f64 {
    1.0
}

fn default_seed_status() -> String {
    "pending".into()
}

async fn create_seed_inner(state: &AppState, body: CreateSeedBody) -> Result<Seed, ApiError> {
    let store = store(state)?;
    let id = match body.id {
        None => Uuid::now_v7(),
        Some(id) => {
            if store.get_seed(id).await.map_err(storage_error)?.is_some() {
                return Err(ApiError::conflict(format!(
                    "Seed `{id}` 已經存在。POST 是建立，不會覆寫既有資料；\
                     請省略 id 讓伺服器產生新的"
                )));
            }
            id
        }
    };
    let seed = Seed {
        id,
        collection_id: body.collection_id,
        seed_type: body.seed_type,
        value: validate_text("value", &body.value, MAX_SEED_VALUE)?,
        entity_id: body.entity_id,
        priority: body.priority,
        confidence: body.confidence,
        origin: body.origin,
        status: body.status,
        depth: body.depth,
        created_at: Utc::now(),
    };
    store.put_seed(&seed).await.map_err(storage_error)?;
    Ok(seed)
}

/// `POST /api/v1/seeds`。operator 以上。成功 201。
pub async fn create_seed(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Json(body): Json<CreateSeedBody>,
) -> Result<(StatusCode, Json<Seed>), ApiError> {
    principal.role.require(Permission::Write)?;
    let requested_id = body.id;
    match create_seed_inner(&state, body).await {
        Ok(seed) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_SEED_CREATE,
                    resource_type: RESOURCE_SEED,
                    resource_id: Some(seed.id.to_string()),
                    outcome: "success",
                    metadata: json!({ "seed_type": seed.seed_type, "value": seed.value }),
                },
            )
            .await;
            Ok((StatusCode::CREATED, Json(seed)))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_SEED_CREATE,
                    resource_type: RESOURCE_SEED,
                    resource_id: requested_id.map(|id| id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&err, json!({})),
                },
            )
            .await;
            Err(err)
        }
    }
}

// ============================================================================
// Candidate
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct CandidateListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub status: Option<CandidateStatus>,
}

/// `GET /api/v1/candidates`。viewer 以上。
pub async fn list_candidates(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<CandidateListQuery>,
) -> Result<Json<CursorPage<Candidate>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_candidates(query.status, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |c| c.id)))
}

/// `GET /api/v1/collections/{id}/discovery`。viewer 以上。
///
/// 這個 collection 底下的 Candidate——SPEC §17 沒有規定回應形狀，
/// 比照其他 list endpoint 回 cursor page。
pub async fn collection_discovery(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<CandidateListQuery>,
) -> Result<Json<CursorPage<Candidate>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_candidates_by_collection(id, query.status, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |c| c.id)))
}

/// `GET /api/v1/candidates/{id}` 的回應。`evidence` 就是 Acceptance C
/// 「Why was this discovered?」的答案——不需要呼叫端再打第二個 endpoint。
#[derive(Debug, Clone, Serialize)]
pub struct CandidateDetail {
    #[serde(flatten)]
    pub candidate: Candidate,
    pub evidence: Vec<CandidateEvidence>,
    /// `true` 代表這個 candidate 的證據超過 [`crate::resources::LINK_PAGE`]，
    /// 只回了前面那一頁。
    pub evidence_truncated: bool,
}

/// `GET /api/v1/candidates/{id}`。viewer 以上。
pub async fn get_candidate(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<CandidateDetail>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let candidate = store
        .get_candidate(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Candidate `{id}`。請用 GET /api/v1/candidates 確認 id"
            ))
        })?;
    let evidence = store
        .list_candidate_evidence_by_candidate(id, LINK_PAGE)
        .await
        .map_err(storage_error)?;
    Ok(Json(CandidateDetail {
        evidence_truncated: evidence.len() as u32 >= LINK_PAGE,
        evidence,
        candidate,
    }))
}

/// 核准／拒絕共用：更新狀態後重新讀回最新的 Candidate。
///
/// `update_candidate_status` 回 `false` 代表 `id` 不存在，轉成 404。
/// 更新成功後緊接著的 `get_candidate` 理論上不可能讀不到（沒有刪除 API）；
/// 讀不到代表出現了沒設計到的併發狀況，用 500 讓它明確可見，不要吞掉。
async fn set_candidate_status(
    state: &AppState,
    id: Uuid,
    status: CandidateStatus,
) -> Result<Candidate, ApiError> {
    let store = store(state)?;
    let updated = store
        .update_candidate_status(id, status, Utc::now())
        .await
        .map_err(storage_error)?;
    if !updated {
        return Err(ApiError::not_found(format!(
            "找不到 Candidate `{id}`。請用 GET /api/v1/candidates 確認 id"
        )));
    }
    store.get_candidate(id).await.map_err(storage_error)?.ok_or_else(|| {
        ApiError::internal(
            "update_candidate_status 回 true 但緊接著讀不到這筆 candidate，這是不該發生的併發狀態",
        )
    })
}

/// `POST /api/v1/candidates/{id}/approve`。operator 以上。
pub async fn approve_candidate(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Candidate>, ApiError> {
    principal.role.require(Permission::Write)?;
    match set_candidate_status(&state, id, CandidateStatus::Approved).await {
        Ok(candidate) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CANDIDATE_APPROVE,
                    resource_type: RESOURCE_CANDIDATE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({}),
                },
            )
            .await;
            Ok(Json(candidate))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CANDIDATE_APPROVE,
                    resource_type: RESOURCE_CANDIDATE,
                    resource_id: Some(id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&err, json!({})),
                },
            )
            .await;
            Err(err)
        }
    }
}

/// `POST /api/v1/candidates/{id}/reject`。operator 以上。
pub async fn reject_candidate(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Candidate>, ApiError> {
    principal.role.require(Permission::Write)?;
    match set_candidate_status(&state, id, CandidateStatus::Rejected).await {
        Ok(candidate) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CANDIDATE_REJECT,
                    resource_type: RESOURCE_CANDIDATE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({}),
                },
            )
            .await;
            Ok(Json(candidate))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CANDIDATE_REJECT,
                    resource_type: RESOURCE_CANDIDATE,
                    resource_id: Some(id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&err, json!({})),
                },
            )
            .await;
            Err(err)
        }
    }
}

// ============================================================================
// AI Run
// ============================================================================

#[derive(Debug, Clone, Deserialize)]
pub struct AiRunListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    pub task_type: Option<String>,
}

/// `GET /api/v1/ai/runs`。viewer 以上。
pub async fn list_ai_runs(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<AiRunListQuery>,
) -> Result<Json<CursorPage<AiRun>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_ai_runs(query.task_type.as_deref(), after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |r| r.id)))
}

/// `GET /api/v1/ai/runs/{id}`。viewer 以上。
pub async fn get_ai_run(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<AiRun>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let run = store
        .get_ai_run(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 AI Run `{id}`。請用 GET /api/v1/ai/runs 確認 id"
            ))
        })?;
    Ok(Json(run))
}
