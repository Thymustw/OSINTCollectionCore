//! `/api/v1/entities`（SPEC §10／§17／§19）。

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use core_model::{Entity, EntityExtraction, EntityType};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{LINK_PAGE, storage_error, store};
use crate::state::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// `person`／`organization`／`domain`／`ip`／`url`／`email`／`vulnerability`／`hash`…
    pub entity_type: Option<EntityType>,
}

/// `GET /entities/{id}` 的明細。
#[derive(Debug, Clone, Serialize)]
pub struct EntityDetail {
    #[serde(flatten)]
    pub entity: Entity,
    /// 這個 Entity 參與的 Relationship 筆數（source 或 target 命中都算）。
    ///
    /// ⚠️ 這是**在 `LINK_PAGE` 範圍內數到的筆數**，不是全表 count。
    /// `relationships_truncated` 為 true 時代表「至少這麼多」，不是「就這麼多」——
    /// 把它當精確總數顯示會讓熱門 IOC 的關聯數永遠停在 100。
    pub relationship_count: usize,
    pub relationships_truncated: bool,
    /// 最近的抽取紀錄（SPEC §17）：這個 Entity 是從哪些 object 抽出來的。
    pub recent_extractions: Vec<EntityExtraction>,
    pub extractions_truncated: bool,
}

/// `GET /api/v1/entities`。viewer 以上。
pub async fn list_entities(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Entity>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_entities_by_type(query.entity_type, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |e| e.id)))
}

/// `GET /api/v1/entities/{id}`。
pub async fn get_entity(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<EntityDetail>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let entity = store
        .get_entity(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Entity `{id}`。請用 GET /api/v1/entities 確認 id"
            ))
        })?;

    let relationships = store
        .list_relationships_by_object(entity.id, LINK_PAGE)
        .await
        .map_err(storage_error)?;
    let recent_extractions = store
        .list_entity_extractions_by_entity(entity.id, LINK_PAGE)
        .await
        .map_err(storage_error)?;

    Ok(Json(EntityDetail {
        relationship_count: relationships.len(),
        relationships_truncated: relationships.len() as u32 >= LINK_PAGE,
        extractions_truncated: recent_extractions.len() as u32 >= LINK_PAGE,
        entity,
        recent_extractions,
    }))
}
