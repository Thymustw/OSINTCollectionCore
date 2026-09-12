//! `/api/v1/sources`（SPEC §5／§19）。
//!
//! `GET /sources/{id}` 不在 SPEC §19 的清單裡，但 list 與 PATCH 都在——
//! 沒有單筆讀取的話，PATCH 的呼叫端拿不到 `If-Match` 要用的 ETag，
//! 那條併發保護就形同只能用 `*` 繞過。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use core_model::{Source, SourceType};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{
    AUDIT_SOURCE_CREATE, AUDIT_SOURCE_UPDATE, AuditEvent, audit, check_if_match, double_option,
    etag_header, rejected_metadata, required_patch, storage_error, store, timestamp_version,
    validate_text,
};
use crate::state::AppState;

const RESOURCE: &str = "source";
/// `name` 上限。資料表是 TEXT，但無界字串會直接出現在稽核與 CLI 表格裡。
const MAX_NAME: usize = 200;
const MAX_DESCRIPTION: usize = 2_000;
const MAX_URL: usize = 2_048;
/// `language`／`country`：BCP-47 與 ISO 3166 的短代碼，不是自由文字。
const MAX_CODE: usize = 16;
const MAX_PLATFORM: usize = 64;

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl ListQuery {
    fn pagination(&self) -> Pagination {
        Pagination {
            cursor: self.cursor.clone(),
            limit: self.limit,
        }
    }
}

/// `POST /sources` 的 body。
///
/// `deny_unknown_fields`：欄位名打錯必須當場失敗。靜默忽略會讓使用者拿到
/// 「建立成功但設定沒生效」，那種錯誤要到採集失敗時才會發現。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSourceBody {
    /// 省略時由 server 產 UUID v7。指定且已存在 → 409（POST 是建立，不是 upsert）。
    #[serde(default)]
    pub id: Option<Uuid>,
    pub name: String,
    pub source_type: SourceType,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub language: Option<String>,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "empty_object")]
    pub collection_policy: Value,
}

/// `PATCH /sources/{id}` 的 body。全部欄位可省略；可為空的欄位傳 `null` 代表清除。
///
/// `id`／`created_at`／`updated_at` 不在這裡：前者改了就是另一個資源，
/// 後兩者由伺服器維護。傳進來會因為 `deny_unknown_fields` 直接被拒。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchSourceBody {
    #[serde(default, deserialize_with = "double_option")]
    pub name: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub source_type: Option<Option<SourceType>>,
    #[serde(default, deserialize_with = "double_option")]
    pub platform: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub base_url: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub description: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub language: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub country: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub enabled: Option<Option<bool>>,
    #[serde(default, deserialize_with = "double_option")]
    pub collection_policy: Option<Option<Value>>,
    #[serde(default, deserialize_with = "double_option")]
    pub last_seen: Option<Option<chrono::DateTime<Utc>>>,
}

fn default_true() -> bool {
    true
}

fn empty_object() -> Value {
    json!({})
}

/// `GET /api/v1/sources`。viewer 以上。
pub async fn list_sources(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Source>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = query.pagination().decode()?;
    let items = store
        .list_sources(after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |s| s.id)))
}

/// `GET /api/v1/sources/{id}`。回應帶 `ETag`，PATCH 時原樣放進 `If-Match`。
pub async fn get_source(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<([(axum::http::HeaderName, String); 1], Json<Source>), ApiError> {
    principal.role.require(Permission::Read)?;
    let source = load(&state, id).await?;
    Ok((
        etag_header(&timestamp_version(source.updated_at)),
        Json(source),
    ))
}

/// `POST /api/v1/sources`。operator 以上。
pub async fn create_source(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Json(body): Json<CreateSourceBody>,
) -> Result<
    (
        StatusCode,
        [(axum::http::HeaderName, String); 1],
        Json<Source>,
    ),
    ApiError,
> {
    principal.role.require(Permission::Write)?;
    let requested_id = body.id;
    match create_inner(&state, body).await {
        Ok(source) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_SOURCE_CREATE,
                    resource_type: RESOURCE,
                    resource_id: Some(source.id.to_string()),
                    outcome: "success",
                    metadata: json!({ "name": source.name, "source_type": source.source_type }),
                },
            )
            .await;
            Ok((
                StatusCode::CREATED,
                etag_header(&timestamp_version(source.updated_at)),
                Json(source),
            ))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_SOURCE_CREATE,
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

async fn create_inner(state: &AppState, body: CreateSourceBody) -> Result<Source, ApiError> {
    let store = store(state)?;
    let id = match body.id {
        // UUID v7：`list_sources` 依 id 排序當時間序，v4 會讓新資料排到隨機位置。
        None => Uuid::now_v7(),
        Some(id) => {
            if store.get_source(id).await.map_err(storage_error)?.is_some() {
                return Err(ApiError::conflict(format!(
                    "Source `{id}` 已經存在。POST 是建立，不會覆寫既有資料；\
                     要改請用 PATCH /api/v1/sources/{id}，或省略 id 讓伺服器產生新的"
                )));
            }
            id
        }
    };

    let now = Utc::now();
    let source = Source {
        id,
        name: validate_text("name", &body.name, MAX_NAME)?,
        source_type: body.source_type,
        platform: optional("platform", body.platform, MAX_PLATFORM)?,
        base_url: optional("base_url", body.base_url, MAX_URL)?,
        description: optional("description", body.description, MAX_DESCRIPTION)?,
        language: optional("language", body.language, MAX_CODE)?,
        country: optional("country", body.country, MAX_CODE)?,
        enabled: body.enabled,
        collection_policy: validate_object("collection_policy", body.collection_policy)?,
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    store.put_source(&source).await.map_err(storage_error)?;
    Ok(source)
}

/// `PATCH /api/v1/sources/{id}`。operator 以上，必須帶 `If-Match`。
pub async fn patch_source(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<PatchSourceBody>,
) -> Result<([(axum::http::HeaderName, String); 1], Json<Source>), ApiError> {
    principal.role.require(Permission::Write)?;
    match patch_inner(&state, id, &headers, body).await {
        Ok((source, changed)) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_SOURCE_UPDATE,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "fields": changed }),
                },
            )
            .await;
            Ok((
                etag_header(&timestamp_version(source.updated_at)),
                Json(source),
            ))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_SOURCE_UPDATE,
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

/// 回傳更新後的 Source 與「這次真的改了哪些欄位」（給稽核用）。
async fn patch_inner(
    state: &AppState,
    id: Uuid,
    headers: &HeaderMap,
    body: PatchSourceBody,
) -> Result<(Source, Vec<&'static str>), ApiError> {
    let store = store(state)?;
    let mut source = load(state, id).await?;
    check_if_match(headers, &timestamp_version(source.updated_at))?;

    let mut changed = Vec::new();
    if let Some(name) = required_patch("name", body.name)? {
        source.name = validate_text("name", &name, MAX_NAME)?;
        changed.push("name");
    }
    if let Some(source_type) = required_patch("source_type", body.source_type)? {
        source.source_type = source_type;
        changed.push("source_type");
    }
    if let Some(enabled) = required_patch("enabled", body.enabled)? {
        source.enabled = enabled;
        changed.push("enabled");
    }
    if let Some(policy) = required_patch("collection_policy", body.collection_policy)? {
        source.collection_policy = validate_object("collection_policy", policy)?;
        changed.push("collection_policy");
    }
    if let Some(value) = body.platform {
        source.platform = optional("platform", value, MAX_PLATFORM)?;
        changed.push("platform");
    }
    if let Some(value) = body.base_url {
        source.base_url = optional("base_url", value, MAX_URL)?;
        changed.push("base_url");
    }
    if let Some(value) = body.description {
        source.description = optional("description", value, MAX_DESCRIPTION)?;
        changed.push("description");
    }
    if let Some(value) = body.language {
        source.language = optional("language", value, MAX_CODE)?;
        changed.push("language");
    }
    if let Some(value) = body.country {
        source.country = optional("country", value, MAX_CODE)?;
        changed.push("country");
    }
    if let Some(value) = body.last_seen {
        source.last_seen = value;
        changed.push("last_seen");
    }

    if changed.is_empty() {
        return Err(ApiError::bad_request(
            "PATCH body 沒有任何可更新的欄位。請至少給一個欄位；\
             可更新的是 name／source_type／platform／base_url／description／language／country／\
             enabled／collection_policy／last_seen",
        ));
    }

    // `updated_at` 同時是下一個 ETag。不更新它的話樂觀鎖等於沒有：
    // 舊的 If-Match 會一直通過，第二個寫入者照樣蓋掉第一個。
    source.updated_at = Utc::now();
    store.put_source(&source).await.map_err(storage_error)?;
    Ok((source, changed))
}

async fn load(state: &AppState, id: Uuid) -> Result<Source, ApiError> {
    store(state)?
        .get_source(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Source `{id}`。請用 GET /api/v1/sources 確認 id"
            ))
        })
}

fn optional(field: &str, value: Option<String>, max: usize) -> Result<Option<String>, ApiError> {
    crate::resources::validate_optional_text(field, value, max)
}

/// JSONB 欄位必須是物件。給陣列或字串在寫入時才會失敗，那時錯誤訊息指不到欄位。
fn validate_object(field: &str, value: Value) -> Result<Value, ApiError> {
    if value.is_object() {
        return Ok(value);
    }
    Err(ApiError::bad_request(format!(
        "`{field}` 必須是 JSON 物件（例如 {{}}），目前是 {}",
        kind_of(&value)
    )))
}

fn kind_of(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "布林",
        Value::Number(_) => "數字",
        Value::String(_) => "字串",
        Value::Array(_) => "陣列",
        Value::Object(_) => "物件",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patch_body_distinguishes_absent_from_null() {
        let absent: PatchSourceBody = serde_json::from_str("{}").unwrap();
        assert!(absent.description.is_none(), "沒出現的欄位不該被動到");

        let cleared: PatchSourceBody = serde_json::from_str(r#"{"description": null}"#).unwrap();
        assert_eq!(
            cleared.description,
            Some(None),
            "傳 null 必須能與「沒提供」分開，否則清除欄位會靜默失敗"
        );

        let set: PatchSourceBody = serde_json::from_str(r#"{"description": "x"}"#).unwrap();
        assert_eq!(set.description, Some(Some("x".to_string())));
    }

    #[test]
    fn unknown_patch_fields_are_rejected() {
        // 打錯欄位名不能靜默忽略，否則使用者以為改了。
        let err = serde_json::from_str::<PatchSourceBody>(r#"{"nmae": "x"}"#);
        assert!(err.is_err());
        // id 與時間戳不可 PATCH。
        assert!(serde_json::from_str::<PatchSourceBody>(r#"{"id": "x"}"#).is_err());
        assert!(serde_json::from_str::<PatchSourceBody>(r#"{"updated_at": "x"}"#).is_err());
    }

    #[test]
    fn collection_policy_must_be_an_object() {
        assert!(validate_object("collection_policy", json!({"a": 1})).is_ok());
        let err = validate_object("collection_policy", json!([1, 2])).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("陣列"), "{}", err.message);
    }

    #[test]
    fn create_body_defaults_to_enabled_with_empty_policy() {
        let body: CreateSourceBody =
            serde_json::from_str(r#"{"name":"a","source_type":"rss"}"#).unwrap();
        assert!(body.enabled);
        assert_eq!(body.collection_policy, json!({}));
        assert!(body.id.is_none());
    }
}
