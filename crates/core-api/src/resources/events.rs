//! `/api/v1/events`（SPEC §13／§19）。
//!
//! ⚠️ 這裡的 Event 是 SPEC §13 的**領域事件物件**（存在 `events` 表裡的情報事件），
//! 不是 Redpanda 上的 `EventEnvelope`。兩者只是名字撞在一起。
//!
//! **V0.1 沒有任何寫入者**：SPEC §13 明寫「只建立基礎 event model，不做自動
//! Event Detection」。因此這兩個 endpoint 正常情況下就是回空清單與 404。
//! 那是正確行為，不是缺陷——不要為了「看起來有資料」去合成假的 Event。

use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use uuid::Uuid;

use core_model::Event;
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{storage_error, store};
use crate::state::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

/// `GET /api/v1/events`。viewer 以上。
pub async fn list_events(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Event>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_events(after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |e| e.id)))
}

/// `GET /api/v1/events/{id}`。
pub async fn get_event(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<Event>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    store
        .get_event(id)
        .await
        .map_err(storage_error)?
        .map(Json)
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Event `{id}`。V0.1 不做自動 Event Detection（SPEC §13），\
                 events 表目前沒有任何寫入者，所以這裡回 404 是正常的"
            ))
        })
}
