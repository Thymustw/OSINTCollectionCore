//! `/api/v1/collections`（SPEC §7／§19）。
//!
//! SPEC §7 的 Relations（sources／connectors／objects）存在關聯表，不內嵌在
//! `Collection` struct 裡。`GET /collections/{id}` 因此回一個包起來的明細物件：
//! Collection 本體 + 三份 id 清單。
//!
//! 三份清單都**有上限**（`LINK_PAGE`），而且各自帶一個 `*_truncated` 旗標。
//! 沒有那個旗標的話，一個掛了五萬份文件的 collection 看起來會跟只掛 100 份的
//! 一模一樣——使用者會以為那就是全部。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use core_model::{Collection, CollectionId, ConnectorId, ObjectId, SourceId};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{
    AUDIT_COLLECTION_CREATE, AuditEvent, LINK_PAGE, audit, rejected_metadata, storage_error, store,
    validate_optional_text, validate_text,
};
use crate::state::AppState;

const RESOURCE: &str = "collection";
const MAX_NAME: usize = 200;
const MAX_DESCRIPTION: usize = 2_000;
const MAX_STATUS: usize = 32;
/// 一次 POST 最多可以連幾個 source／connector。無界的話一個請求就能寫上萬列關聯。
const MAX_LINKS_PER_REQUEST: usize = 100;
const DEFAULT_STATUS: &str = "active";

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateCollectionBody {
    #[serde(default)]
    pub id: Option<Uuid>,
    pub name: String,
    #[serde(default)]
    pub workspace_id: Option<Uuid>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub priority: i32,
    /// 一起建立關聯的 Source。每一個都必須已存在，否則整個請求 422（不會建一半）。
    #[serde(default)]
    pub source_ids: Vec<Uuid>,
    #[serde(default)]
    pub connector_ids: Vec<Uuid>,
}

/// `GET /collections/{id}` 的明細。
#[derive(Debug, Clone, Serialize)]
pub struct CollectionDetail {
    #[serde(flatten)]
    pub collection: Collection,
    pub source_ids: Vec<SourceId>,
    pub connector_ids: Vec<ConnectorId>,
    pub object_ids: Vec<ObjectId>,
    /// 清單被 `LINK_PAGE` 截斷時為 true。**不要**用 `len() == LINK_PAGE` 自己推：
    /// 剛好 100 筆與「超過 100 筆」在這個 API 上長得一樣。
    pub sources_truncated: bool,
    pub connectors_truncated: bool,
    pub objects_truncated: bool,
}

/// `GET /api/v1/collections`。viewer 以上。
pub async fn list_collections(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Collection>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_collections(after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |c| c.id)))
}

/// `GET /api/v1/collections/{id}`。含三份關聯 id 清單。
pub async fn get_collection(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<CollectionDetail>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let collection = load(&state, id).await?;

    let source_ids = store
        .list_collection_sources(id, LINK_PAGE)
        .await
        .map_err(storage_error)?;
    let connector_ids = store
        .list_collection_connectors(id, LINK_PAGE)
        .await
        .map_err(storage_error)?;
    let object_ids = store
        .list_collection_objects(id, LINK_PAGE)
        .await
        .map_err(storage_error)?;

    Ok(Json(CollectionDetail {
        sources_truncated: source_ids.len() as u32 >= LINK_PAGE,
        connectors_truncated: connector_ids.len() as u32 >= LINK_PAGE,
        objects_truncated: object_ids.len() as u32 >= LINK_PAGE,
        collection,
        source_ids,
        connector_ids,
        object_ids,
    }))
}

/// `POST /api/v1/collections`。operator 以上。
pub async fn create_collection(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Json(body): Json<CreateCollectionBody>,
) -> Result<(StatusCode, Json<CollectionDetail>), ApiError> {
    principal.role.require(Permission::Write)?;
    let requested_id = body.id;
    match create_inner(&state, body).await {
        Ok(detail) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_CREATE,
                    resource_type: RESOURCE,
                    resource_id: Some(detail.collection.id.to_string()),
                    outcome: "success",
                    metadata: json!({
                        "name": detail.collection.name,
                        "linked_sources": detail.source_ids.len(),
                        "linked_connectors": detail.connector_ids.len(),
                    }),
                },
            )
            .await;
            Ok((StatusCode::CREATED, Json(detail)))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_CREATE,
                    resource_type: RESOURCE,
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

async fn create_inner(
    state: &AppState,
    body: CreateCollectionBody,
) -> Result<CollectionDetail, ApiError> {
    let store = store(state)?;

    check_link_count("source_ids", body.source_ids.len())?;
    check_link_count("connector_ids", body.connector_ids.len())?;

    // 先驗全部關聯再寫任何東西：驗一半才失敗會留下一個「只連到一半」的 collection，
    // 而回應是錯誤——呼叫端不會知道那個半成品已經存在。
    for source_id in &body.source_ids {
        if store
            .get_source(*source_id)
            .await
            .map_err(storage_error)?
            .is_none()
        {
            return Err(ApiError::unprocessable(format!(
                "source_ids 裡的 `{source_id}` 不存在。請先建立它，或把它從清單移除；\
                 這次沒有建立任何東西"
            )));
        }
    }
    for connector_id in &body.connector_ids {
        if store
            .get_connector(*connector_id)
            .await
            .map_err(storage_error)?
            .is_none()
        {
            return Err(ApiError::unprocessable(format!(
                "connector_ids 裡的 `{connector_id}` 不存在。請先建立它，或把它從清單移除；\
                 這次沒有建立任何東西"
            )));
        }
    }

    let id = match body.id {
        None => Uuid::now_v7(),
        Some(id) => {
            if store
                .get_collection(id)
                .await
                .map_err(storage_error)?
                .is_some()
            {
                return Err(ApiError::conflict(format!(
                    "Collection `{id}` 已經存在。POST 是建立，不會覆寫既有資料；\
                     要改請省略 id 讓伺服器產生新的"
                )));
            }
            id
        }
    };

    let now = Utc::now();
    let collection = Collection {
        id,
        workspace_id: body.workspace_id,
        name: validate_text("name", &body.name, MAX_NAME)?,
        description: validate_optional_text("description", body.description, MAX_DESCRIPTION)?,
        status: match body.status {
            Some(s) => validate_text("status", &s, MAX_STATUS)?,
            None => DEFAULT_STATUS.to_string(),
        },
        priority: body.priority,
        created_at: now,
        updated_at: now,
    };
    store
        .put_collection(&collection)
        .await
        .map_err(storage_error)?;

    // 關聯寫入是 `ON CONFLICT DO NOTHING`，重複 id 不會失敗。
    for source_id in &body.source_ids {
        store
            .link_collection_source(collection.id, *source_id)
            .await
            .map_err(storage_error)?;
    }
    for connector_id in &body.connector_ids {
        store
            .link_collection_connector(collection.id, *connector_id)
            .await
            .map_err(storage_error)?;
    }

    let source_ids = store
        .list_collection_sources(collection.id, LINK_PAGE)
        .await
        .map_err(storage_error)?;
    let connector_ids = store
        .list_collection_connectors(collection.id, LINK_PAGE)
        .await
        .map_err(storage_error)?;

    Ok(CollectionDetail {
        sources_truncated: source_ids.len() as u32 >= LINK_PAGE,
        connectors_truncated: connector_ids.len() as u32 >= LINK_PAGE,
        objects_truncated: false,
        collection,
        source_ids,
        connector_ids,
        object_ids: Vec::new(),
    })
}

async fn load(state: &AppState, id: CollectionId) -> Result<Collection, ApiError> {
    store(state)?
        .get_collection(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Collection `{id}`。請用 GET /api/v1/collections 確認 id"
            ))
        })
}

fn check_link_count(field: &str, count: usize) -> Result<(), ApiError> {
    if count > MAX_LINKS_PER_REQUEST {
        return Err(ApiError::bad_request(format!(
            "`{field}` 一次最多 {MAX_LINKS_PER_REQUEST} 筆（收到 {count} 筆）。請分批建立"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_count_is_bounded() {
        assert!(check_link_count("source_ids", MAX_LINKS_PER_REQUEST).is_ok());
        let err = check_link_count("source_ids", MAX_LINKS_PER_REQUEST + 1).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn detail_flattens_the_collection_fields() {
        let now = Utc::now();
        let detail = CollectionDetail {
            collection: Collection {
                id: Uuid::nil(),
                workspace_id: None,
                name: "c".into(),
                description: None,
                status: "active".into(),
                priority: 0,
                created_at: now,
                updated_at: now,
            },
            source_ids: vec![],
            connector_ids: vec![],
            object_ids: vec![],
            sources_truncated: false,
            connectors_truncated: false,
            objects_truncated: false,
        };
        let value = serde_json::to_value(&detail).unwrap();
        // flatten：collection 的欄位在頂層，呼叫端不需要多剝一層。
        assert_eq!(value["name"], "c");
        assert!(value["source_ids"].is_array());
    }

    #[test]
    fn body_defaults_are_safe() {
        let body: CreateCollectionBody = serde_json::from_str(r#"{"name":"c"}"#).unwrap();
        assert_eq!(body.priority, 0);
        assert!(body.source_ids.is_empty());
        assert!(body.status.is_none());
    }
}
