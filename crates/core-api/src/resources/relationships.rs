//! `/api/v1/relationships`（SPEC §11／§12／§19）。
//!
//! SPEC §12：「任何 relationship 必須能回查 evidence」。`GET /relationships/{id}`
//! 因此一定帶 evidence 清單——少了它，那條要求只剩下資料表裡的一張表。

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use core_model::{Relationship, RelationshipEvidence, RelationshipType};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{LINK_PAGE, storage_error, store};
use crate::state::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// `mentions`／`references`／`affects`… 參數名依 SPEC §19 慣例是 `type`。
    #[serde(rename = "type")]
    pub relationship_type: Option<RelationshipType>,
}

/// `GET /relationships/{id}` 的明細。
#[derive(Debug, Clone, Serialize)]
pub struct RelationshipDetail {
    #[serde(flatten)]
    pub relationship: Relationship,
    /// SPEC §12 的證據列，依 `created_at` 升序。
    pub evidence: Vec<RelationshipEvidence>,
    pub evidence_truncated: bool,
}

/// `GET /api/v1/relationships`。viewer 以上。
pub async fn list_relationships(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Relationship>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_relationships_by_type(query.relationship_type, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |r| r.id)))
}

/// `GET /api/v1/relationships/{id}`。
pub async fn get_relationship(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<RelationshipDetail>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let relationship = store
        .get_relationship(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Relationship `{id}`。請用 GET /api/v1/relationships 確認 id"
            ))
        })?;

    let evidence = store
        .list_relationship_evidence(relationship.id, LINK_PAGE)
        .await
        .map_err(storage_error)?;

    Ok(Json(RelationshipDetail {
        evidence_truncated: evidence.len() as u32 >= LINK_PAGE,
        relationship,
        evidence,
    }))
}
