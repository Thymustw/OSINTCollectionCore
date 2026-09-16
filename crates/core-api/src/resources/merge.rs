//! Entity resolve／merge／undo（SPEC_V0.2 §5／§6／§7）。
//!
//! 這些 endpoint 只組裝既有的 [`MergeService`]／[`ResolverService`]／
//! [`GraphContextResolver`]，不重寫 merge／resolution 邏輯。
//!
//! # `POST /entities/{id}/resolve` 目前只會真的產出 Postgres 路徑的候選
//!
//! `ResolverService` 在 API 組裝時注入的是
//! [`MockEmbeddingProvider::unsupported`]：`semantic_similarity` **誠實回空**，
//! 不是假造相似度。等 ml-commons adapter 接上才會補齊。目前會真的產生候選的
//! 方法是 `normalized_name`／`alias`／`domain`／`account_handle`。
//!
//! `graph_context` 走獨立路由 `POST /entities/{id}/resolve/graph-context`，
//! 由 [`GraphContextResolver`] 處理。Neo4j 沒接上只有那條路由回 503，
//! 不影響 `resolve_entity`。
//!
//! [`MergeService`]: merge::MergeService
//! [`ResolverService`]: resolver::ResolverService
//! [`GraphContextResolver`]: resolver::GraphContextResolver
//! [`MockEmbeddingProvider::unsupported`]: storage_core::mock::MockEmbeddingProvider::unsupported

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use merge::MergeError;
use resolver::{ResolverError, group_candidates_for_auto_approval};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use core_model::{MergeHistory, ResolutionCandidate, ResolutionStatus};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{AuditEvent, audit, rejected_metadata, storage_error, store};
use crate::state::{
    AppState, SharedAutoApprovalState, SharedGraphContextResolver, SharedMergeService,
    SharedResolverService,
};

const RESOURCE_ENTITY: &str = "entity";
const RESOURCE_MERGE: &str = "merge_history";

/// 稽核 action。字串會進 `audit_log.action`，改動等於改稽核查詢條件——
/// `docs/developer/security.md` 的動作清單要一起改。
pub const AUDIT_ENTITY_RESOLVE: &str = "entity.resolve";
pub const AUDIT_ENTITY_RESOLVE_GRAPH_CONTEXT: &str = "entity.resolve_graph_context";
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

fn graph_resolver_service(state: &AppState) -> Result<&SharedGraphContextResolver, ApiError> {
    state.graph_resolver.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "graph_context 未接上 Neo4j。請確認 [storage.graph] 設定與 Neo4j 是否啟動；\
             這不影響 POST /entities/{id}/resolve 的其他方法",
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
///
/// 回應型別維持 `Vec<ResolutionCandidate>`：自動核准若真的 merge 了某一對，
/// 屬於該對、且原本就在這次新寫入集合裡的項目會被同步成 `AutoConfirmed`。
/// 自動核准本身評估的是「這個 Entity 目前全部 Pending 候選」（含 entity-worker
/// 預先寫入的 `exact_identifier`），不是只有這次新寫入的那幾筆——否則預設
/// 門檻下永遠碰不到高信心路徑。見 ADR-012 Step 4。
pub async fn resolve_entity(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<ResolutionCandidate>>, ApiError> {
    principal.role.require(Permission::Write)?;
    let resolver = resolver_service(&state)?;
    match resolver.resolve_entity(id).await {
        Ok(mut candidates) => {
            let (auto_merged_pairs, auto_merged_history_ids) =
                maybe_evaluate_auto_approval(&state, id, &mut candidates).await;
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_ENTITY_RESOLVE,
                    resource_type: RESOURCE_ENTITY,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({
                        "candidate_count": candidates.len(),
                        "auto_merged_pairs": auto_merged_pairs,
                        "auto_merged_history_ids": auto_merged_history_ids,
                    }),
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

/// 評估這個 Entity 目前全部 Pending 候選，必要時自動核准並 merge。
///
/// `state.auto_approval` 是 `None`（沒接 Postgres）時直接回 0。
/// `state.store` 是 `None` 但 `auto_approval` 是 `Some` 是組裝矛盾：記 error
/// 並跳過，不要讓整個 resolve 請求失敗。list Pending 失敗同樣跳過——
/// 自動核准是加值路徑，不能擋住 `resolve_entity` 已經寫進去的候選回應。
async fn maybe_evaluate_auto_approval(
    state: &AppState,
    entity_id: Uuid,
    candidates: &mut [ResolutionCandidate],
) -> (u32, Vec<Uuid>) {
    let Some(auto_approval) = state.auto_approval.as_ref() else {
        return (0, Vec::new());
    };
    let Some(store) = state.store.as_ref() else {
        tracing::error!(
            %entity_id,
            "AppState.auto_approval 有值但 AppState.store 是 None（組裝矛盾），\
             跳過自動核准評估"
        );
        return (0, Vec::new());
    };

    let existing_pending = match store
        .list_resolution_candidates_by_entity(entity_id, Some(ResolutionStatus::Pending), None, 100)
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            tracing::error!(
                error = %err,
                %entity_id,
                "列出 Pending 候選失敗，跳過自動核准評估"
            );
            return (0, Vec::new());
        }
    };

    evaluate_auto_approval_groups(auto_approval, entity_id, candidates, existing_pending).await
}

async fn evaluate_auto_approval_groups(
    auto_approval: &SharedAutoApprovalState,
    entity_id: Uuid,
    candidates: &mut [ResolutionCandidate],
    existing_pending: Vec<ResolutionCandidate>,
) -> (u32, Vec<Uuid>) {
    let mut by_id = std::collections::HashMap::new();
    for c in candidates.iter().cloned() {
        by_id.insert(c.id, c);
    }
    for c in existing_pending {
        by_id.entry(c.id).or_insert(c);
    }
    let all_pending: Vec<ResolutionCandidate> = by_id.into_values().collect();

    let groups = group_candidates_for_auto_approval(entity_id, &all_pending);
    let mut auto_merged_pairs = 0u32;
    let mut auto_merged_history_ids: Vec<Uuid> = Vec::new();

    for (other_id, group) in groups {
        if auto_merged_pairs >= auto_approval.max_auto_merges_per_resolve {
            break;
        }
        let outcome = auto_approval
            .evaluator
            .evaluate_pair(entity_id, other_id, &group, "resolver")
            .await;
        match outcome {
            resolver::AutoApprovalOutcome::Merged { merge_history_id } => {
                auto_merged_pairs += 1;
                auto_merged_history_ids.push(merge_history_id);
                let now = chrono::Utc::now();
                for c in candidates.iter_mut() {
                    if group.iter().any(|g| g.id == c.id) {
                        c.status = ResolutionStatus::AutoConfirmed;
                        c.reviewed_at = Some(now);
                    }
                }
            }
            resolver::AutoApprovalOutcome::Pending { .. }
            | resolver::AutoApprovalOutcome::Disabled => {}
        }
    }

    (auto_merged_pairs, auto_merged_history_ids)
}

/// `POST /api/v1/entities/{id}/resolve/graph-context`。operator 以上。
///
/// 跟 `resolve_entity` 分開的獨立 endpoint——Neo4j 沒接上只有這條路由回 503，
/// 不影響 `POST /entities/{id}/resolve` 的另外幾個方法。
pub async fn resolve_graph_context(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
) -> Result<Json<Vec<ResolutionCandidate>>, ApiError> {
    principal.role.require(Permission::Write)?;
    let resolver = graph_resolver_service(&state)?;
    match resolver.resolve(id).await {
        Ok(candidates) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_ENTITY_RESOLVE_GRAPH_CONTEXT,
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
                    action: AUDIT_ENTITY_RESOLVE_GRAPH_CONTEXT,
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
