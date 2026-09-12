//! Entity resolve／merge／undo（SPEC_V0.2 §5／§6／§7）。
//!
//! 這五個 endpoint 只組裝既有的 [`MergeService`]／[`ResolverService`]，
//! 不重寫 merge／resolution 邏輯。
//!
//! # `POST /entities/{id}/resolve` 目前只會真的產出三種候選
//!
//! `ResolverService` 在 API 組裝時注入的是
//! [`MockEmbeddingProvider::unsupported`] 與空的 [`MockGraphStore`]：
//! `semantic_similarity`／`graph_context` **誠實回空**，不是假造相似度。
//! 等 Phase 2（`storage-neo4j`）與 ml-commons adapter 接上才會補齊。
//! 目前會真的產生候選的方法只有 `normalized_name`／`alias`／`domain`。
//!
//! [`MergeService`]: merge::MergeService
//! [`ResolverService`]: resolver::ResolverService
//! [`MockEmbeddingProvider::unsupported`]: storage_core::mock::MockEmbeddingProvider::unsupported
//! [`MockGraphStore`]: storage_core::mock::MockGraphStore

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use merge::MergeError;
use resolver::ResolverError;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use core_model::{MergeHistory, ResolutionCandidate, ResolutionStatus};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{AuditEvent, audit, rejected_metadata, storage_error, store};
use crate::state::{AppState, SharedMergeService, SharedResolverService};

const RESOURCE_ENTITY: &str = "entity";
const RESOURCE_MERGE: &str = "merge_history";

/// 稽核 action。字串會進 `audit_log.action`，改動等於改稽核查詢條件——
/// `docs/developer/security.md` 的動作清單要一起改。
pub const AUDIT_ENTITY_RESOLVE: &str = "entity.resolve";
pub const AUDIT_ENTITY_MERGE: &str = "entity.merge";
pub const AUDIT_MERGE_UNDO: &str = "merge.undo";

#[derive(Debug, Clone, Deserialize)]
pub struct ResolutionCandidateQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// `pending`／`confirmed`／`rejected`／`auto_confirmed`。省略 = 不過濾。
    pub status: Option<ResolutionStatus>,
}

/// `POST /api/v1/entities/merge` 的 body。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MergeEntitiesBody {
    pub survivor_id: Uuid,
    pub merged_id: Uuid,
    pub reason: String,
}

fn merge_service(state: &AppState) -> Result<&SharedMergeService, ApiError> {
    state.merge.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Entity merge 未接上 canonical store。請設定 DATABASE_URL 並重啟 osint-api",
        )
    })
}

fn resolver_service(state: &AppState) -> Result<&SharedResolverService, ApiError> {
    state.resolver.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Entity resolution 未接上 canonical store。請設定 DATABASE_URL 並重啟 osint-api",
        )
    })
}

fn merge_error(err: MergeError) -> ApiError {
    match err {
        MergeError::EntityNotFound { .. } | MergeError::HistoryNotFound { .. } => {
            ApiError::not_found(err.to_string())
        }
        MergeError::SelfMerge { .. } | MergeError::TypeMismatch { .. } => {
            ApiError::bad_request(err.to_string())
        }
        MergeError::AlreadyMerged { .. }
        | MergeError::AlreadyUndone { .. }
        | MergeError::SurvivorLaterMerged { .. } => ApiError::conflict(err.to_string()),
        MergeError::CollectionTruncated { .. }
        | MergeError::ReferenceMissing { .. }
        | MergeError::UnknownReferenceTable { .. }
        | MergeError::UnknownReferenceColumn { .. }
        | MergeError::MissingAbsorberSnapshot { .. } => ApiError::internal(err.to_string()),
        MergeError::Storage(storage) => storage_error(storage),
    }
}

fn resolver_error(err: ResolverError) -> ApiError {
    match err {
        ResolverError::EntityNotFound { .. } => ApiError::not_found(err.to_string()),
        ResolverError::Storage(storage) => storage_error(storage),
    }
}

/// `POST /api/v1/entities/{id}/resolve`。operator 以上。
///
/// 對這個 Entity 跑目前已實作的掃描方法，回傳**這次新寫入**的候選。
/// 已存在的候選（同一對同一方法）不會再出現在回應裡。
pub async fn resolve_entity(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<ResolutionCandidate>>, ApiError> {
    principal.role.require(Permission::Write)?;
    let resolver = resolver_service(&state)?;
    match resolver.resolve_entity(id).await {
        Ok(candidates) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_ENTITY_RESOLVE,
                    resource_type: RESOURCE_ENTITY,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "candidate_count": candidates.len() }),
                },
            )
            .await;
            Ok(Json(candidates))
        }
        Err(err) => {
            let api_err = resolver_error(err);
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_ENTITY_RESOLVE,
                    resource_type: RESOURCE_ENTITY,
                    resource_id: Some(id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&api_err, json!({})),
                },
            )
            .await;
            Err(api_err)
        }
    }
}

/// `POST /api/v1/entities/merge`。operator 以上。
///
/// `reason` 不可為空白：merge 是 audited 的決策，必須留下理由。
pub async fn merge_entities(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Json(body): Json<MergeEntitiesBody>,
) -> Result<Json<MergeHistory>, ApiError> {
    principal.role.require(Permission::Write)?;
    if body.reason.trim().is_empty() {
        let err =
            ApiError::bad_request("reason 不能是空字串——merge 必須是 audited 的決策，需要留下理由");
        audit(
            &state,
            &principal,
            &ip,
            AuditEvent {
                action: AUDIT_ENTITY_MERGE,
                resource_type: RESOURCE_ENTITY,
                resource_id: Some(body.survivor_id.to_string()),
                outcome: "rejected",
                metadata: rejected_metadata(
                    &err,
                    json!({
                        "survivor_id": body.survivor_id,
                        "merged_id": body.merged_id,
                    }),
                ),
            },
        )
        .await;
        return Err(err);
    }

    let merge = merge_service(&state)?;
    match merge
        .execute_merge(
            body.survivor_id,
            body.merged_id,
            body.reason.clone(),
            principal.subject.clone(),
        )
        .await
    {
        Ok(history) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_ENTITY_MERGE,
                    resource_type: RESOURCE_ENTITY,
                    resource_id: Some(history.id.to_string()),
                    outcome: "success",
                    metadata: json!({
                        "survivor_id": history.survivor_id,
                        "merged_id": history.merged_id,
                        "reason": history.reason,
                    }),
                },
            )
            .await;
            Ok(Json(history))
        }
        Err(err) => {
            let api_err = merge_error(err);
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_ENTITY_MERGE,
                    resource_type: RESOURCE_ENTITY,
                    resource_id: Some(body.survivor_id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(
                        &api_err,
                        json!({
                            "survivor_id": body.survivor_id,
                            "merged_id": body.merged_id,
                        }),
                    ),
                },
            )
            .await;
            Err(api_err)
        }
    }
}

/// `POST /api/v1/merge-history/{id}/undo`。operator 以上。成功 204。
pub async fn undo_merge(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    principal.role.require(Permission::Write)?;
    let merge = merge_service(&state)?;
    match merge.undo_merge(id).await {
        Ok(()) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_MERGE_UNDO,
                    resource_type: RESOURCE_MERGE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({}),
                },
            )
            .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(err) => {
            let api_err = merge_error(err);
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_MERGE_UNDO,
                    resource_type: RESOURCE_MERGE,
                    resource_id: Some(id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&api_err, json!({})),
                },
            )
            .await;
            Err(api_err)
        }
    }
}

/// `GET /api/v1/entities/{id}/resolution-candidates`。viewer 以上。
pub async fn list_resolution_candidates(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<ResolutionCandidateQuery>,
) -> Result<Json<CursorPage<ResolutionCandidate>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_resolution_candidates_by_entity(id, query.status, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |c| c.id)))
}

/// `GET /api/v1/entities/{id}/merge-history`。viewer 以上。
///
/// 已撤銷的 merge 照樣回傳（Acceptance C）。上限 [`crate::resources::LINK_PAGE`]。
pub async fn list_merge_history(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<MergeHistory>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let history = store
        .list_merge_history_by_entity(id, crate::resources::LINK_PAGE)
        .await
        .map_err(storage_error)?;
    Ok(Json(history))
}
