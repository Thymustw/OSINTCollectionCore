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

use core_model::{Collection, CollectionBudget, CollectionId, ConnectorId, ObjectId, SourceId};
use core_security::{Permission, Principal};
use storage_core::RelationalStore;

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{
    AUDIT_COLLECTION_BUDGET_UPDATE, AUDIT_COLLECTION_CREATE, AUDIT_COLLECTION_LINK_CONNECTOR,
    AUDIT_COLLECTION_LINK_SOURCE, AUDIT_COLLECTION_UNLINK_CONNECTOR,
    AUDIT_COLLECTION_UNLINK_SOURCE, AuditEvent, LINK_PAGE, audit, rejected_metadata, storage_error,
    store, validate_optional_text, validate_text,
};
use crate::state::AppState;

const RESOURCE: &str = "collection";
const MAX_NAME: usize = 200;
const MAX_DESCRIPTION: usize = 2_000;
const MAX_STATUS: usize = 32;
/// 一次 POST 最多可以連幾個 source／connector。無界的話一個請求就能寫上萬列關聯。
const MAX_LINKS_PER_REQUEST: usize = 100;
/// 一個來源最多掛在幾個集合。
///
/// collector 取「來源掛的 ∪ 連接器掛的」寫進原始證據，兩邊各讀最多
/// [`LINK_PAGE`]（100）筆；匯入明確給的 `collection_ids` 也是
/// [`MAX_LINKS_PER_REQUEST`]（100）。寫入端把每邊卡在 50，聯集最多 100，
/// 剛好等於讀取上限——collector、匯入省略 `collection_ids`、normalizer
/// 寫 `collection_objects` 三處讀取都拿得到完整清單，不會再靜默截斷第 101 個。
///
/// 已知競態：兩個並發請求可能同時通過檢查而讓實際數量略超過 50。
/// 要讓來源∪連接器超過讀取上限 100，需要大量並發同時掛上；這是已知、
/// 可接受的競態，不加鎖。
const MAX_COLLECTIONS_PER_SOURCE: usize = 50;
/// 一個連接器最多掛在幾個集合。理由同 [`MAX_COLLECTIONS_PER_SOURCE`]。
const MAX_COLLECTIONS_PER_CONNECTOR: usize = 50;
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

    // 關聯上限必須在寫入 collection 與任何關聯列之前檢查完：掛到一半才
    // 拒絕會留下半成品，而回應是錯誤——呼叫端不會知道那個半成品已經存在。
    check_membership_capacity(
        store.as_ref(),
        id,
        &body.source_ids,
        &body.connector_ids,
        "這次沒有建立任何東西",
    )
    .await?;

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

fn format_id_list(ids: &[Uuid]) -> String {
    ids.iter()
        .map(|id| format!("`{id}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// 列出超限的來源／連接器 id、上限，以及「為什麼是 50」。
fn membership_over_capacity_error(
    over_sources: &[Uuid],
    over_connectors: &[Uuid],
    nothing_written: &str,
) -> ApiError {
    let mut parts = Vec::new();
    if !over_sources.is_empty() {
        parts.push(format!(
            "以下來源已經各屬於 {MAX_COLLECTIONS_PER_SOURCE} 個集合，無法再掛上這個：{}",
            format_id_list(over_sources)
        ));
    }
    if !over_connectors.is_empty() {
        parts.push(format!(
            "以下連接器已經各屬於 {MAX_COLLECTIONS_PER_CONNECTOR} 個集合，無法再掛上這個：{}",
            format_id_list(over_connectors)
        ));
    }
    ApiError::unprocessable(format!(
        "{}。一個來源／連接器最多屬於 {MAX_COLLECTIONS_PER_SOURCE} 個集合，\
         因為收集時來源與連接器各最多讀 {LINK_PAGE} 個集合，寫入端把每邊卡在 \
         {MAX_COLLECTIONS_PER_SOURCE} 才能一次讀完全部。\
         請先從其他集合解除關聯（DELETE /api/v1/collections/{{id}}/sources/{{source_id}} \
         或 .../connectors/{{connector_id}}），或把超限的 id 從這次請求拿掉；{nothing_written}",
        parts.join("。")
    ))
}

/// 在寫入任何「集合↔來源／連接器」關聯之前，確認每個來源／連接器還沒滿。
///
/// 這個集合本來就已掛上的不算新增（冪等重送不能被上限擋）。
/// 全部超限的 id 一次列完再拒絕，不能掛到一半才回 422。
async fn check_membership_capacity(
    store: &dyn RelationalStore,
    collection_id: CollectionId,
    source_ids: &[Uuid],
    connector_ids: &[Uuid],
    nothing_written: &str,
) -> Result<(), ApiError> {
    let mut over_sources = Vec::new();
    let mut seen_sources = std::collections::BTreeSet::new();
    for source_id in source_ids {
        if !seen_sources.insert(*source_id) {
            continue;
        }
        let existing = store
            .list_collections_by_source(*source_id, LINK_PAGE)
            .await
            .map_err(storage_error)?;
        if existing.contains(&collection_id) {
            continue;
        }
        if existing.len() >= MAX_COLLECTIONS_PER_SOURCE {
            over_sources.push(*source_id);
        }
    }

    let mut over_connectors = Vec::new();
    let mut seen_connectors = std::collections::BTreeSet::new();
    for connector_id in connector_ids {
        if !seen_connectors.insert(*connector_id) {
            continue;
        }
        let existing = store
            .list_collections_by_connector(*connector_id, LINK_PAGE)
            .await
            .map_err(storage_error)?;
        if existing.contains(&collection_id) {
            continue;
        }
        if existing.len() >= MAX_COLLECTIONS_PER_CONNECTOR {
            over_connectors.push(*connector_id);
        }
    }

    if over_sources.is_empty() && over_connectors.is_empty() {
        return Ok(());
    }
    Err(membership_over_capacity_error(
        &over_sources,
        &over_connectors,
        nothing_written,
    ))
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkSourcesBody {
    pub source_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinkConnectorsBody {
    pub connector_ids: Vec<Uuid>,
}

/// `GET /collections/{id}/budget` 的回應。沒有明確設定時回保守預設，並標
/// `is_default = true`——呼叫端必須看得出「這不是人設的」。
#[derive(Debug, Clone, Serialize)]
pub struct CollectionBudgetView {
    #[serde(flatten)]
    pub budget: CollectionBudget,
    pub is_default: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertCollectionBudgetBody {
    pub max_candidates_per_run: i32,
    pub max_requests_per_run: i32,
    pub max_ai_calls_per_run: i32,
    pub max_depth: i32,
    pub daily_request_budget: i64,
    pub daily_ai_budget: i64,
}

/// 解析匯入要掛上的集合。
///
/// - 呼叫端有給 `collection_ids`：每個都必須存在，否則 422 列出缺的。
/// - 沒給：用來源目前掛上的集合（不查 connector——匯入當下的 connector 多半
///   是自動建的，通常還沒被任何集合掛上）。
/// - 明確給空陣列：不掛任何集合。
pub(crate) async fn resolve_import_collections(
    store: &dyn RelationalStore,
    source_id: SourceId,
    requested: Option<Vec<Uuid>>,
) -> Result<Vec<CollectionId>, ApiError> {
    match requested {
        None => {
            // 一個來源最多 50 個集合（`MAX_COLLECTIONS_PER_SOURCE`），
            // `LINK_PAGE`（100）讀得到完整清單。不要改成翻頁。
            store
                .list_collections_by_source(source_id, LINK_PAGE)
                .await
                .map_err(storage_error)
        }
        Some(ids) => {
            check_link_count("collection_ids", ids.len())?;
            let mut missing = Vec::new();
            let mut seen = std::collections::BTreeSet::new();
            for id in &ids {
                if !seen.insert(*id) {
                    continue;
                }
                if store
                    .get_collection(*id)
                    .await
                    .map_err(storage_error)?
                    .is_none()
                {
                    missing.push(*id);
                }
            }
            if !missing.is_empty() {
                let listed = missing
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(ApiError::unprocessable(format!(
                    "collection_ids 裡的 `{listed}` 不存在。請先建立集合，或把它從清單移除；\
                     這次沒有寫入任何原始證據"
                )));
            }
            Ok(ids)
        }
    }
}

/// `POST /api/v1/collections/{id}/sources`。operator 以上。
///
/// 只影響之後收進來的資料，不回填已經落地的原始證據／文件——回填等於改寫
/// 「這筆資料當時屬於哪個調查」，會讓歸屬紀錄不可信。
pub async fn link_sources(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
    Json(body): Json<LinkSourcesBody>,
) -> Result<Json<CollectionDetail>, ApiError> {
    principal.role.require(Permission::Write)?;
    match link_sources_inner(&state, id, body).await {
        Ok(detail) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_LINK_SOURCE,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "linked_sources": detail.source_ids.len() }),
                },
            )
            .await;
            Ok(Json(detail))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_LINK_SOURCE,
                    resource_type: RESOURCE,
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

async fn link_sources_inner(
    state: &AppState,
    collection_id: CollectionId,
    body: LinkSourcesBody,
) -> Result<CollectionDetail, ApiError> {
    let store = store(state)?;
    load(state, collection_id).await?;
    if body.source_ids.is_empty() {
        return Err(ApiError::bad_request(
            "`source_ids` 不可為空。請至少給一個已存在的 Source id",
        ));
    }
    check_link_count("source_ids", body.source_ids.len())?;
    let mut missing = Vec::new();
    for source_id in &body.source_ids {
        if store
            .get_source(*source_id)
            .await
            .map_err(storage_error)?
            .is_none()
        {
            missing.push(*source_id);
        }
    }
    if !missing.is_empty() {
        let listed = missing
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::unprocessable(format!(
            "source_ids 裡的 `{listed}` 不存在。請先建立它，或把它從清單移除；\
             這次沒有寫入任何關聯"
        )));
    }
    check_membership_capacity(
        store.as_ref(),
        collection_id,
        &body.source_ids,
        &[],
        "這次沒有寫入任何關聯",
    )
    .await?;
    for source_id in &body.source_ids {
        store
            .link_collection_source(collection_id, *source_id)
            .await
            .map_err(storage_error)?;
    }
    detail_of(state, collection_id).await
}

/// `DELETE /api/v1/collections/{id}/sources/{source_id}`。operator 以上。
///
/// 集合或來源不存在 → 404。本來就沒掛上 → 204（冪等）。不回填舊資料。
pub async fn unlink_source(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path((id, source_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    principal.role.require(Permission::Write)?;
    match unlink_source_inner(&state, id, source_id).await {
        Ok(()) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_UNLINK_SOURCE,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "source_id": source_id }),
                },
            )
            .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_UNLINK_SOURCE,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&err, json!({ "source_id": source_id })),
                },
            )
            .await;
            Err(err)
        }
    }
}

async fn unlink_source_inner(
    state: &AppState,
    collection_id: CollectionId,
    source_id: SourceId,
) -> Result<(), ApiError> {
    let store = store(state)?;
    load(state, collection_id).await?;
    if store
        .get_source(source_id)
        .await
        .map_err(storage_error)?
        .is_none()
    {
        return Err(ApiError::not_found(format!(
            "找不到 Source `{source_id}`。請用 GET /api/v1/sources 確認 id"
        )));
    }
    let _ = store
        .unlink_collection_source(collection_id, source_id)
        .await
        .map_err(storage_error)?;
    Ok(())
}

/// `POST /api/v1/collections/{id}/connectors`。operator 以上。不回填舊資料。
pub async fn link_connectors(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
    Json(body): Json<LinkConnectorsBody>,
) -> Result<Json<CollectionDetail>, ApiError> {
    principal.role.require(Permission::Write)?;
    match link_connectors_inner(&state, id, body).await {
        Ok(detail) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_LINK_CONNECTOR,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "linked_connectors": detail.connector_ids.len() }),
                },
            )
            .await;
            Ok(Json(detail))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_LINK_CONNECTOR,
                    resource_type: RESOURCE,
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

async fn link_connectors_inner(
    state: &AppState,
    collection_id: CollectionId,
    body: LinkConnectorsBody,
) -> Result<CollectionDetail, ApiError> {
    let store = store(state)?;
    load(state, collection_id).await?;
    if body.connector_ids.is_empty() {
        return Err(ApiError::bad_request(
            "`connector_ids` 不可為空。請至少給一個已存在的 Connector id",
        ));
    }
    check_link_count("connector_ids", body.connector_ids.len())?;
    let mut missing = Vec::new();
    for connector_id in &body.connector_ids {
        if store
            .get_connector(*connector_id)
            .await
            .map_err(storage_error)?
            .is_none()
        {
            missing.push(*connector_id);
        }
    }
    if !missing.is_empty() {
        let listed = missing
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        return Err(ApiError::unprocessable(format!(
            "connector_ids 裡的 `{listed}` 不存在。請先建立它，或把它從清單移除；\
             這次沒有寫入任何關聯"
        )));
    }
    check_membership_capacity(
        store.as_ref(),
        collection_id,
        &[],
        &body.connector_ids,
        "這次沒有寫入任何關聯",
    )
    .await?;
    for connector_id in &body.connector_ids {
        store
            .link_collection_connector(collection_id, *connector_id)
            .await
            .map_err(storage_error)?;
    }
    detail_of(state, collection_id).await
}

/// `DELETE /api/v1/collections/{id}/connectors/{connector_id}`。operator 以上。
/// 集合或連接器不存在 → 404。本來就沒掛上 → 204（冪等）。不回填舊資料。
pub async fn unlink_connector(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path((id, connector_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    principal.role.require(Permission::Write)?;
    match unlink_connector_inner(&state, id, connector_id).await {
        Ok(()) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_UNLINK_CONNECTOR,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "connector_id": connector_id }),
                },
            )
            .await;
            Ok(StatusCode::NO_CONTENT)
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_UNLINK_CONNECTOR,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "rejected",
                    metadata: rejected_metadata(&err, json!({ "connector_id": connector_id })),
                },
            )
            .await;
            Err(err)
        }
    }
}

async fn unlink_connector_inner(
    state: &AppState,
    collection_id: CollectionId,
    connector_id: ConnectorId,
) -> Result<(), ApiError> {
    let store = store(state)?;
    load(state, collection_id).await?;
    if store
        .get_connector(connector_id)
        .await
        .map_err(storage_error)?
        .is_none()
    {
        return Err(ApiError::not_found(format!(
            "找不到 Connector `{connector_id}`。請用 GET /api/v1/connectors 確認 id"
        )));
    }
    let _ = store
        .unlink_collection_connector(collection_id, connector_id)
        .await
        .map_err(storage_error)?;
    Ok(())
}

/// `GET /api/v1/collections/{id}/budget`。viewer 以上。
///
/// 集合不存在 → 404。沒有明確設定 → 保守預設 + `is_default=true`。
pub async fn get_budget(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<CollectionBudgetView>, ApiError> {
    principal.role.require(Permission::Read)?;
    load(&state, id).await?;
    let store = store(&state)?;
    match store
        .get_collection_budget(id)
        .await
        .map_err(storage_error)?
    {
        Some(budget) => Ok(Json(CollectionBudgetView {
            budget,
            is_default: false,
        })),
        None => Ok(Json(CollectionBudgetView {
            budget: CollectionBudget::conservative_default(id, Utc::now()),
            is_default: true,
        })),
    }
}

/// `PUT /api/v1/collections/{id}/budget`。operator 以上。
pub async fn put_budget(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
    Json(body): Json<UpsertCollectionBudgetBody>,
) -> Result<Json<CollectionBudgetView>, ApiError> {
    principal.role.require(Permission::Write)?;
    match put_budget_inner(&state, id, body).await {
        Ok(view) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_BUDGET_UPDATE,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({
                        "max_candidates_per_run": view.budget.max_candidates_per_run,
                        "daily_request_budget": view.budget.daily_request_budget,
                    }),
                },
            )
            .await;
            Ok(Json(view))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_COLLECTION_BUDGET_UPDATE,
                    resource_type: RESOURCE,
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

async fn put_budget_inner(
    state: &AppState,
    collection_id: CollectionId,
    body: UpsertCollectionBudgetBody,
) -> Result<CollectionBudgetView, ApiError> {
    load(state, collection_id).await?;
    let store = store(state)?;
    let now = Utc::now();
    let existing = store
        .get_collection_budget(collection_id)
        .await
        .map_err(storage_error)?;
    let created_at = existing.as_ref().map(|b| b.created_at).unwrap_or(now);
    let budget = CollectionBudget {
        collection_id,
        max_candidates_per_run: require_positive_i32(
            "max_candidates_per_run",
            body.max_candidates_per_run,
        )?,
        max_requests_per_run: require_positive_i32(
            "max_requests_per_run",
            body.max_requests_per_run,
        )?,
        max_ai_calls_per_run: require_positive_i32(
            "max_ai_calls_per_run",
            body.max_ai_calls_per_run,
        )?,
        max_depth: require_positive_i32("max_depth", body.max_depth)?,
        daily_request_budget: require_positive_i64(
            "daily_request_budget",
            body.daily_request_budget,
        )?,
        daily_ai_budget: require_positive_i64("daily_ai_budget", body.daily_ai_budget)?,
        created_at,
        updated_at: now,
    };
    store
        .put_collection_budget(&budget)
        .await
        .map_err(storage_error)?;
    Ok(CollectionBudgetView {
        budget,
        is_default: false,
    })
}

async fn detail_of(state: &AppState, id: CollectionId) -> Result<CollectionDetail, ApiError> {
    let store = store(state)?;
    let collection = load(state, id).await?;
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
    Ok(CollectionDetail {
        sources_truncated: source_ids.len() as u32 >= LINK_PAGE,
        connectors_truncated: connector_ids.len() as u32 >= LINK_PAGE,
        objects_truncated: object_ids.len() as u32 >= LINK_PAGE,
        collection,
        source_ids,
        connector_ids,
        object_ids,
    })
}

fn require_positive_i32(field: &str, value: i32) -> Result<i32, ApiError> {
    if value <= 0 {
        return Err(ApiError::bad_request(format!(
            "`{field}` 必須大於 0（收到 {value}）。設成 0 會讓 Discovery 完全跑不動，那是關閉不是防護"
        )));
    }
    Ok(value)
}

fn require_positive_i64(field: &str, value: i64) -> Result<i64, ApiError> {
    if value <= 0 {
        return Err(ApiError::bad_request(format!(
            "`{field}` 必須大於 0（收到 {value}）。設成 0 會讓 Discovery 完全跑不動，那是關閉不是防護"
        )));
    }
    Ok(value)
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
    fn membership_cap_fits_the_read_page() {
        assert_eq!(MAX_COLLECTIONS_PER_SOURCE, 50);
        assert_eq!(MAX_COLLECTIONS_PER_CONNECTOR, 50);
        assert_eq!(
            MAX_COLLECTIONS_PER_SOURCE + MAX_COLLECTIONS_PER_CONNECTOR,
            LINK_PAGE as usize,
            "來源 50 + 連接器 50 必須剛好等於讀取上限，三處讀取才保證完整"
        );
    }

    #[test]
    fn membership_over_capacity_lists_ids_and_the_limit() {
        let source = Uuid::now_v7();
        let connector = Uuid::now_v7();
        let err = membership_over_capacity_error(&[source], &[connector], "這次沒有寫入任何關聯");
        assert_eq!(err.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(err.message.contains(&source.to_string()), "{}", err.message);
        assert!(
            err.message.contains(&connector.to_string()),
            "{}",
            err.message
        );
        assert!(
            err.message
                .contains(&MAX_COLLECTIONS_PER_SOURCE.to_string()),
            "{}",
            err.message
        );
        assert!(
            err.message.contains("這次沒有寫入任何關聯"),
            "{}",
            err.message
        );
        assert!(
            err.message.contains("一次讀完全部") || err.message.contains("100"),
            "要說明為什麼是 50：{}",
            err.message
        );
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
