//! `/api/v1/connectors`（SPEC §6／§19）。
//!
//! # 憑證只收 SecretRef
//!
//! SPEC §6：「禁止儲存明文 password/token」。`credential_reference` 與
//! `proxy_reference` 一律先過 `core_config::SecretRef::parse`，
//! 明文（`postgres://user:pw@…`、裸字串）會在 API 層就被擋下來回 400。
//!
//! 在 API 擋而不是只靠寫入端自律，是因為一旦明文落進 `connectors` 資料表，
//! 它會出現在備份、log 與每一次 `GET /connectors` 的回應裡——事後清不乾淨。
//!
//! # 為什麼 `source_id` 不存在要回 422 而不是 400
//!
//! 400 是「請求本身壞了」（JSON 不合法、欄位型別錯）。這裡 JSON 完全合法、
//! 型別也對，是**引用的資源不存在**，屬於語意問題 → 422。
//! 這讓呼叫端分得出「我送錯格式」與「我指到一個不存在的 Source」。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use core_config::SecretRef;
use core_model::Connector;
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{
    AUDIT_CONNECTOR_CREATE, AUDIT_CONNECTOR_UPDATE, AuditEvent, audit, check_if_match,
    double_option, etag_header, fingerprint_version, rejected_metadata, required_patch,
    storage_error, store, validate_optional_text, validate_text,
};
use crate::state::AppState;

const RESOURCE: &str = "connector";
const MAX_NAME: usize = 200;
const MAX_TYPE: usize = 64;
const MAX_VERSION: usize = 64;
const MAX_STATUS: usize = 32;
const MAX_SCHEDULE: usize = 128;
const MAX_SECRET_REF: usize = 512;
/// 建立時的預設版本字串。SPEC §6 要求 `version` 有值但沒定義格式。
const DEFAULT_VERSION: &str = "0.1.0";
/// 建立時的預設狀態。與 `core-api::import` 自動配置的 connector 一致。
const DEFAULT_STATUS: &str = "idle";

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// `true` 只列啟用的、`false` 只列停用的。**省略代表全部**（不是只列啟用的）：
    /// 「為什麼我的 connector 不見了」通常就是因為它被停用了，預設藏起來只會更難查。
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConnectorBody {
    #[serde(default)]
    pub id: Option<Uuid>,
    pub source_id: Uuid,
    pub name: String,
    #[serde(rename = "type")]
    pub connector_type: String,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "empty_object")]
    pub configuration: Value,
    #[serde(default)]
    pub credential_reference: Option<String>,
    #[serde(default)]
    pub schedule: Option<String>,
    #[serde(default = "empty_object")]
    pub rate_limit: Value,
    #[serde(default = "empty_object")]
    pub timeout: Value,
    #[serde(default)]
    pub proxy_reference: Option<String>,
    #[serde(default = "empty_object")]
    pub checkpoint: Value,
    #[serde(default)]
    pub status: Option<String>,
}

/// PATCH body。`source_id` 刻意不可改：把 connector 搬到另一個 Source
/// 會讓它既有的 RawEvidence 指向一個「現在已經不屬於它」的來源，
/// 之後查「這批證據怎麼來的」會得到互相矛盾的答案。要換就新建一個。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PatchConnectorBody {
    #[serde(default, deserialize_with = "double_option")]
    pub name: Option<Option<String>>,
    #[serde(rename = "type", default, deserialize_with = "double_option")]
    pub connector_type: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub version: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub enabled: Option<Option<bool>>,
    #[serde(default, deserialize_with = "double_option")]
    pub configuration: Option<Option<Value>>,
    #[serde(default, deserialize_with = "double_option")]
    pub credential_reference: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub schedule: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub rate_limit: Option<Option<Value>>,
    #[serde(default, deserialize_with = "double_option")]
    pub timeout: Option<Option<Value>>,
    #[serde(default, deserialize_with = "double_option")]
    pub proxy_reference: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub checkpoint: Option<Option<Value>>,
    #[serde(default, deserialize_with = "double_option")]
    pub status: Option<Option<String>>,
}

fn default_true() -> bool {
    true
}

fn empty_object() -> Value {
    json!({})
}

/// `GET /api/v1/connectors`。viewer 以上。
pub async fn list_connectors(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Connector>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_connectors_by_enabled(query.enabled, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |c| c.id)))
}

/// `GET /api/v1/connectors/{id}`。回應帶 `ETag`（狀態指紋，見模組 `resources` 說明）。
pub async fn get_connector(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<([(axum::http::HeaderName, String); 1], Json<Connector>), ApiError> {
    principal.role.require(Permission::Read)?;
    let connector = load(&state, id).await?;
    let version = fingerprint_version(&connector)?;
    Ok((etag_header(&version), Json(connector)))
}

/// `POST /api/v1/connectors`。operator 以上。
pub async fn create_connector(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Json(body): Json<CreateConnectorBody>,
) -> Result<
    (
        StatusCode,
        [(axum::http::HeaderName, String); 1],
        Json<Connector>,
    ),
    ApiError,
> {
    principal.role.require(Permission::Write)?;
    let source_id = body.source_id;
    let requested_id = body.id;
    match create_inner(&state, body).await {
        Ok(connector) => {
            let version = fingerprint_version(&connector)?;
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CONNECTOR_CREATE,
                    resource_type: RESOURCE,
                    resource_id: Some(connector.id.to_string()),
                    outcome: "success",
                    metadata: json!({
                        "source_id": connector.source_id,
                        "name": connector.name,
                        "type": connector.connector_type,
                        "enabled": connector.enabled,
                    }),
                },
            )
            .await;
            Ok((StatusCode::CREATED, etag_header(&version), Json(connector)))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
        action: AUDIT_CONNECTOR_CREATE,
        resource_type: RESOURCE,
        resource_id: requested_id.map(|id| id.to_string()),
        outcome: "rejected",
        metadata: // 不記 credential_reference——就算它是合法的 SecretRef，
                // 也沒有必要在稽核裡多留一份「密鑰放在哪」的指引。
                rejected_metadata(&err, json!({ "source_id": source_id })),
    },
            )
            .await;
            Err(err)
        }
    }
}

async fn create_inner(state: &AppState, body: CreateConnectorBody) -> Result<Connector, ApiError> {
    let store = store(state)?;

    // Source 必須先存在。FK 會擋，但那時錯誤訊息是資料庫的約束名，指不到「你填錯 source_id」。
    if store
        .get_source(body.source_id)
        .await
        .map_err(storage_error)?
        .is_none()
    {
        return Err(ApiError::unprocessable(format!(
            "找不到 Source `{}`。Connector 必須掛在既有的 Source 底下；\
             請先 POST /api/v1/sources 建立，或用 GET /api/v1/sources 找到正確的 id",
            body.source_id
        )));
    }

    let id = match body.id {
        None => Uuid::now_v7(),
        Some(id) => {
            if store
                .get_connector(id)
                .await
                .map_err(storage_error)?
                .is_some()
            {
                return Err(ApiError::conflict(format!(
                    "Connector `{id}` 已經存在。POST 是建立，不會覆寫既有資料；\
                     要改請用 PATCH /api/v1/connectors/{id}，或省略 id 讓伺服器產生新的"
                )));
            }
            id
        }
    };

    let connector = Connector {
        id,
        source_id: body.source_id,
        name: validate_text("name", &body.name, MAX_NAME)?,
        connector_type: validate_text("type", &body.connector_type, MAX_TYPE)?,
        version: match body.version {
            Some(v) => validate_text("version", &v, MAX_VERSION)?,
            None => DEFAULT_VERSION.to_string(),
        },
        enabled: body.enabled,
        configuration: validate_object("configuration", body.configuration)?,
        credential_reference: validate_secret_ref(
            "credential_reference",
            body.credential_reference,
        )?,
        schedule: validate_optional_text("schedule", body.schedule, MAX_SCHEDULE)?,
        rate_limit: validate_object("rate_limit", body.rate_limit)?,
        timeout: validate_object("timeout", body.timeout)?,
        proxy_reference: validate_secret_ref("proxy_reference", body.proxy_reference)?,
        checkpoint: validate_object("checkpoint", body.checkpoint)?,
        last_run: None,
        last_success: None,
        status: match body.status {
            Some(s) => validate_text("status", &s, MAX_STATUS)?,
            None => DEFAULT_STATUS.to_string(),
        },
        error_count: 0,
    };
    store
        .put_connector(&connector)
        .await
        .map_err(storage_error)?;
    Ok(connector)
}

/// `PATCH /api/v1/connectors/{id}`。operator 以上，必須帶 `If-Match`。
pub async fn patch_connector(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(body): Json<PatchConnectorBody>,
) -> Result<([(axum::http::HeaderName, String); 1], Json<Connector>), ApiError> {
    principal.role.require(Permission::Write)?;
    match patch_inner(&state, id, &headers, body).await {
        Ok((connector, changed)) => {
            let version = fingerprint_version(&connector)?;
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CONNECTOR_UPDATE,
                    resource_type: RESOURCE,
                    resource_id: Some(id.to_string()),
                    outcome: "success",
                    metadata: json!({ "fields": changed }),
                },
            )
            .await;
            Ok((etag_header(&version), Json(connector)))
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_CONNECTOR_UPDATE,
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

async fn patch_inner(
    state: &AppState,
    id: Uuid,
    headers: &HeaderMap,
    body: PatchConnectorBody,
) -> Result<(Connector, Vec<&'static str>), ApiError> {
    let store = store(state)?;
    let mut connector = load(state, id).await?;
    check_if_match(headers, &fingerprint_version(&connector)?)?;

    let mut changed = Vec::new();
    if let Some(name) = required_patch("name", body.name)? {
        connector.name = validate_text("name", &name, MAX_NAME)?;
        changed.push("name");
    }
    if let Some(value) = required_patch("type", body.connector_type)? {
        connector.connector_type = validate_text("type", &value, MAX_TYPE)?;
        changed.push("type");
    }
    if let Some(value) = required_patch("version", body.version)? {
        connector.version = validate_text("version", &value, MAX_VERSION)?;
        changed.push("version");
    }
    if let Some(value) = required_patch("enabled", body.enabled)? {
        connector.enabled = value;
        changed.push("enabled");
    }
    if let Some(value) = required_patch("configuration", body.configuration)? {
        connector.configuration = validate_object("configuration", value)?;
        changed.push("configuration");
    }
    if let Some(value) = required_patch("rate_limit", body.rate_limit)? {
        connector.rate_limit = validate_object("rate_limit", value)?;
        changed.push("rate_limit");
    }
    if let Some(value) = required_patch("timeout", body.timeout)? {
        connector.timeout = validate_object("timeout", value)?;
        changed.push("timeout");
    }
    if let Some(value) = required_patch("checkpoint", body.checkpoint)? {
        connector.checkpoint = validate_object("checkpoint", value)?;
        changed.push("checkpoint");
    }
    if let Some(value) = required_patch("status", body.status)? {
        connector.status = validate_text("status", &value, MAX_STATUS)?;
        changed.push("status");
    }
    if let Some(value) = body.credential_reference {
        connector.credential_reference = validate_secret_ref("credential_reference", value)?;
        changed.push("credential_reference");
    }
    if let Some(value) = body.proxy_reference {
        connector.proxy_reference = validate_secret_ref("proxy_reference", value)?;
        changed.push("proxy_reference");
    }
    if let Some(value) = body.schedule {
        connector.schedule = validate_optional_text("schedule", value, MAX_SCHEDULE)?;
        changed.push("schedule");
    }

    if changed.is_empty() {
        return Err(ApiError::bad_request(
            "PATCH body 沒有任何可更新的欄位。請至少給一個欄位；\
             `source_id` 不可更改（要換來源請新建一個 Connector）",
        ));
    }

    store
        .put_connector(&connector)
        .await
        .map_err(storage_error)?;
    Ok((connector, changed))
}

async fn load(state: &AppState, id: Uuid) -> Result<Connector, ApiError> {
    store(state)?
        .get_connector(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Connector `{id}`。請用 GET /api/v1/connectors 確認 id"
            ))
        })
}

/// 憑證欄位只收 SecretRef（`env:` / `file:` / `store:`）。
///
/// 空字串代表清除。任何看起來像明文的值都回 400，而且**不把值回顯出來**——
/// 錯誤訊息裡重複一次密碼等於再寫進一次 log。
fn validate_secret_ref(field: &str, value: Option<String>) -> Result<Option<String>, ApiError> {
    let Some(raw) = value else { return Ok(None) };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.chars().count() > MAX_SECRET_REF {
        return Err(ApiError::bad_request(format!(
            "`{field}` 超過 {MAX_SECRET_REF} 個字元。這個欄位只放密鑰的**位置**，不是密鑰本身"
        )));
    }
    match SecretRef::parse(trimmed) {
        Ok(parsed) => Ok(Some(parsed.as_str().to_string())),
        Err(_) => Err(ApiError::bad_request(format!(
            "`{field}` 必須是 SecretRef，不能是明文密碼或連線字串（SPEC §6：禁止儲存明文 password/token）。\
             請改成 `env:VAR_NAME`、`file:/path/to/secret` 或 `store:backend/path#key`"
        ))),
    }
}

fn validate_object(field: &str, value: Value) -> Result<Value, ApiError> {
    if value.is_object() {
        return Ok(value);
    }
    Err(ApiError::bad_request(format!(
        "`{field}` 必須是 JSON 物件（例如 {{}}）"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plaintext_credentials_are_rejected() {
        for value in [
            "hunter2",
            "postgres://user:pw@127.0.0.1/db",
            "Bearer abcdef",
        ] {
            let err = validate_secret_ref("credential_reference", Some(value.into())).unwrap_err();
            assert_eq!(err.status, StatusCode::BAD_REQUEST, "{value} 應被拒");
            assert!(
                !err.message.contains(value),
                "錯誤訊息不可回顯疑似密鑰的值：{}",
                err.message
            );
        }
    }

    #[test]
    fn secret_refs_are_accepted_and_normalized() {
        let got = validate_secret_ref("credential_reference", Some(" env:RSS_TOKEN ".into()))
            .unwrap()
            .unwrap();
        assert_eq!(got, "env:RSS_TOKEN");
        assert!(
            validate_secret_ref("proxy_reference", Some("file:/run/secrets/proxy".into()))
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn empty_string_clears_the_reference() {
        assert!(
            validate_secret_ref("credential_reference", Some("   ".into()))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn source_id_is_not_patchable() {
        assert!(
            serde_json::from_str::<PatchConnectorBody>(r#"{"source_id":"x"}"#).is_err(),
            "搬動 connector 的來源會讓既有 RawEvidence 的出處變成假的"
        );
    }

    #[test]
    fn type_is_serialized_as_type_not_connector_type() {
        let body: PatchConnectorBody = serde_json::from_str(r#"{"type":"rss"}"#).unwrap();
        assert_eq!(body.connector_type, Some(Some("rss".into())));
    }
}
