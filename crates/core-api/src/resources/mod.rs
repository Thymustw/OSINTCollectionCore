//! SPEC §19 的資源類 REST endpoint（sources／connectors／collections／objects／
//! entities／relationships／events／raw）。
//!
//! # 共用規則
//!
//! * **讀**（GET）需要 `Permission::Read`（viewer 以上），**寫**（POST／PATCH）
//!   需要 `Permission::Write`（operator 以上）。路由層有 middleware，handler 裡
//!   再擋一次——被搬到別的 router 時不會靜默失去保護。
//! * 所有 list 都是 cursor pagination（SPEC §19「所有 list API 以 cursor
//!   pagination 為主要策略」），過濾條件一律**在 SQL 裡做**，不在程式端濾
//!   （理由見 `storage_core::RelationalStore::list_documents_filtered`）。
//! * 所有寫入動作都寫稽核，**成功與失敗都寫**：只記成功的話，「誰一直試圖改一個
//!   不存在的 connector」這種訊號完全看不到。
//!
//! # PATCH 的併發保護
//!
//! PATCH 一律要求 `If-Match`。少了它，兩個人同時改同一個 Source 時後寫的會
//! 靜默蓋掉先寫的（lost update），而且兩邊都看到 200。
//!
//! * 沒帶 `If-Match` → **428 Precondition Required**
//! * 帶了但版本不符 → **412 Precondition Failed**
//!
//! ETag 對呼叫端是**不透明字串**：用 GET（或前一次 PATCH）回應的 `ETag` header
//! 原樣送回來即可。實作上兩種來源：
//!
//! | 資源 | ETag 內容 | 為什麼 |
//! |---|---|---|
//! | Source | `updated_at` 的 RFC3339 | SPEC §5 有 `updated_at` |
//! | Connector | `sha256:<前 16 位 hex>` 狀態指紋 | **SPEC §6 沒有 `updated_at`**，資料表也沒有這一欄 |
//!
//! 不為了 ETag 去加一個 SPEC 沒定義的 `connectors.updated_at`：那要動 migration、
//! 兩個 adapter 的 mapping 與所有 `Connector { … }` 建構點（25 處）。
//! 狀態指紋在語意上更嚴格（任何欄位變了就不符），對呼叫端的用法完全一樣。
//!
//! # PATCH 的欄位語意
//!
//! RFC 7396 JSON merge patch 的子集：**body 裡出現的欄位才會被改**，
//! 對可為空的欄位傳 `null` 代表清除。沒出現的欄位維持原值。
//! 不可為空的欄位（例如 `name`）傳 `null` 會被當成錯誤回 400，
//! 不會被當成「沒提供」而靜默忽略。

pub mod collections;
pub mod connectors;
pub mod discovery;
pub mod entities;
pub mod events;
pub mod graph;
pub mod merge;
pub mod objects;
pub mod raw;
pub mod relationships;
pub mod similar;
pub mod sources;
pub mod stix;
pub mod timeline;

use axum::http::{HeaderMap, StatusCode, header};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use core_security::{AuditEntry, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::state::{AppState, SharedObjects, SharedStore};

/// 明細回應裡「關聯清單」最多列幾筆（duplicate group、evidence、collection 成員…）。
///
/// 與 storage adapter 的 `clamp_limit` 上限相同。明細 API 不做無界查詢：
/// 一個 collection 掛五萬份文件時，`GET /collections/{id}` 不該回一份五萬筆的 JSON。
/// 回應裡會帶 `*_truncated` 旗標，讓呼叫端看得出「還有更多」而不是誤以為就這些。
pub(crate) const LINK_PAGE: u32 = 100;

/// 稽核 action。字串會進 `audit_log.action`，改動等於改稽核查詢條件——
/// `docs/developer/security.md` 的動作清單要一起改。
pub const AUDIT_SOURCE_CREATE: &str = "source.create";
pub const AUDIT_SOURCE_UPDATE: &str = "source.update";
pub const AUDIT_CONNECTOR_CREATE: &str = "connector.create";
pub const AUDIT_CONNECTOR_UPDATE: &str = "connector.update";
pub const AUDIT_COLLECTION_CREATE: &str = "collection.create";
pub const AUDIT_OBJECT_CREATE: &str = "object.create";
pub use discovery::{AUDIT_CANDIDATE_APPROVE, AUDIT_CANDIDATE_REJECT, AUDIT_SEED_CREATE};
pub use graph::AUDIT_GRAPH_REBUILD;
pub use merge::{
    AUDIT_ENTITY_MERGE, AUDIT_ENTITY_RESOLVE, AUDIT_ENTITY_RESOLVE_GRAPH_CONTEXT, AUDIT_MERGE_UNDO,
};
pub use stix::{AUDIT_STIX_EXPORT, AUDIT_STIX_IMPORT};

/// canonical store handle。沒接上 Postgres 時回 503。
pub(crate) fn store(state: &AppState) -> Result<&SharedStore, ApiError> {
    state.store.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "資源 API 未接上 canonical store。請設定 DATABASE_URL 並重啟 osint-api",
        )
    })
}

/// 物件儲存 handle。沒接上 MinIO 時回 503。
pub(crate) fn objects(state: &AppState) -> Result<&SharedObjects, ApiError> {
    state.objects.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "物件儲存未接上。請設定 S3_ENDPOINT／S3_BUCKET 並重啟 osint-api；\
             只要 metadata 的話請去掉 ?body=true",
        )
    })
}

/// 一次稽核寫入的內容。
///
/// 包成 struct 而不是一長串參數：`(action, resource_type, resource_id, outcome)`
/// 全都是字串，順序寫錯不會有任何編譯錯誤，只會讓稽核表裡的欄位悄悄錯位。
pub(crate) struct AuditEvent<'a> {
    pub action: &'a str,
    pub resource_type: &'a str,
    pub resource_id: Option<String>,
    pub outcome: &'a str,
    pub metadata: Value,
}

/// 寫一列稽核。**IP 一定要傳**（`ClientIp`）——少了它 `audit_log.ip` 會全是 NULL，
/// 而且完全不會報錯，只有在查「那些改動是從哪裡來的」時才會發現沒有資料。
pub(crate) async fn audit(
    state: &AppState,
    principal: &Principal,
    ip: &ClientIp,
    event: AuditEvent<'_>,
) {
    let entry = AuditEntry::new(
        principal.subject.clone(),
        event.action,
        event.resource_type,
        event.resource_id,
        event.outcome,
    )
    .with_ip(ip.0.clone())
    .with_metadata(event.metadata);
    let action = event.action;
    if let Err(err) = state.audit.append(entry).await {
        tracing::error!(error = %err, %action, "寫入資源稽核紀錄失敗");
    }
}

/// 把失敗的 `ApiError` 轉成稽核 metadata。
pub(crate) fn rejected_metadata(err: &ApiError, extra: Value) -> Value {
    let mut metadata = extra;
    if let Some(map) = metadata.as_object_mut() {
        map.insert("status_code".into(), json!(err.status.as_u16()));
        map.insert("error".into(), json!(err.error));
    }
    metadata
}

/// `ETag` 回應 header。
pub(crate) fn etag_header(value: &str) -> [(header::HeaderName, String); 1] {
    [(header::ETAG, format!("\"{value}\""))]
}

/// Source 的版本字串：`updated_at` 的 RFC3339（微秒精度）。
///
/// 固定用微秒而不是 chrono 預設的「有幾位印幾位」：PostgreSQL 的 `TIMESTAMPTZ`
/// 就是微秒精度，固定格式才能保證 GET 回的字串與下一次 PATCH 比對時一致。
pub(crate) fn timestamp_version(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// 狀態指紋版本：對序列化後的資源做 sha256，取前 16 位 hex。
///
/// 給沒有 `updated_at` 的資源（Connector）用。**不是安全機制**，是併發偵測：
/// 任何欄位變動都會讓指紋改變，因此「我讀到的那一版」比對得出來。
pub(crate) fn fingerprint_version<T: Serialize>(value: &T) -> Result<String, ApiError> {
    let canonical = serde_json::to_vec(value).map_err(|err| {
        ApiError::internal(format!(
            "計算資源版本失敗：{err}。請重試；持續發生請看記錄檔"
        ))
    })?;
    let digest = Sha256::digest(&canonical);
    Ok(format!("sha256:{}", hex::encode(&digest[..8])))
}

/// 檢查 `If-Match`。沒帶回 428、不符回 412。
///
/// 接受 `*`（RFC 7232：「只要資源存在就好」）、`W/"…"` 與逗號分隔的多個候選。
pub(crate) fn check_if_match(headers: &HeaderMap, current: &str) -> Result<(), ApiError> {
    let raw = headers.get(header::IF_MATCH).ok_or_else(|| {
        ApiError::new(
            StatusCode::PRECONDITION_REQUIRED,
            "precondition_required",
            format!(
                "PATCH 必須帶 If-Match，否則兩個人同時修改時後寫的會靜默蓋掉先寫的。\
                 請先 GET 這個資源，把回應的 ETag 原樣放進 If-Match（本次的值是 \"{current}\"）"
            ),
        )
    })?;
    let raw = raw.to_str().map_err(|_| {
        ApiError::bad_request("If-Match 不是合法的 header 值。請原樣送回 GET 回應的 ETag")
    })?;

    for candidate in raw.split(',') {
        let candidate = candidate.trim();
        if candidate == "*" {
            return Ok(());
        }
        let candidate = candidate.strip_prefix("W/").unwrap_or(candidate);
        let candidate = candidate.trim().trim_matches('"');
        if version_matches(candidate, current) {
            return Ok(());
        }
    }

    Err(ApiError::new(
        StatusCode::PRECONDITION_FAILED,
        "precondition_failed",
        format!(
            "If-Match 與目前的版本不符：這個資源在你讀取之後已經被改過。\
             請重新 GET 一次、把改動套用到新版本上，再用新的 ETag \"{current}\" 重試"
        ),
    ))
}

/// 兩個版本字串是否指同一版。
///
/// 先做字串比對；兩邊都是 RFC3339 時改比「時間點」——同一個時刻可以寫成
/// `…Z` 或 `…+00:00`，字串不同但版本相同，純字串比對會冒出假的 412。
fn version_matches(candidate: &str, current: &str) -> bool {
    if candidate == current {
        return true;
    }
    match (
        DateTime::parse_from_rfc3339(candidate),
        DateTime::parse_from_rfc3339(current),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// merge patch 的三態欄位：未出現／`null`／有值。
///
/// `Option<Option<T>>` 預設**分不出**「沒給」與「給了 null」（兩者都是 `None`）。
/// 少了這個 deserializer，`{"description": null}` 會被當成沒提供而靜默忽略——
/// 使用者以為清掉了，資料其實沒變。
pub(crate) fn double_option<'de, T, D>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(de).map(Some)
}

/// 不可為空的欄位：出現且非 null 才回 `Some`；出現但為 null 回 400。
pub(crate) fn required_patch<T>(
    field: &str,
    value: Option<Option<T>>,
) -> Result<Option<T>, ApiError> {
    match value {
        None => Ok(None),
        Some(Some(v)) => Ok(Some(v)),
        Some(None) => Err(ApiError::bad_request(format!(
            "`{field}` 不可為 null。要保持原值請整個省略這個欄位；要改值請直接給新值"
        ))),
    }
}

/// 一般文字欄位的共用檢查：非空、長度、不含控制字元。
///
/// 控制字元會被原樣寫進稽核與 log。
pub(crate) fn validate_text(field: &str, value: &str, max: usize) -> Result<String, ApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request(format!(
            "`{field}` 不可為空白。請填一個看得出用途的名稱"
        )));
    }
    if trimmed.chars().count() > max {
        return Err(ApiError::bad_request(format!(
            "`{field}` 超過 {max} 個字元。請縮短後重試"
        )));
    }
    if trimmed.chars().any(char::is_control) {
        return Err(ApiError::bad_request(format!(
            "`{field}` 不可含控制字元。請用一般文字"
        )));
    }
    Ok(trimmed.to_string())
}

/// 可為空的文字欄位。`None` 直接放行；有值時套 [`validate_text`]，
/// 但允許空字串代表「清成 null」。
pub(crate) fn validate_optional_text(
    field: &str,
    value: Option<String>,
    max: usize,
) -> Result<Option<String>, ApiError> {
    match value {
        None => Ok(None),
        Some(raw) if raw.trim().is_empty() => Ok(None),
        Some(raw) => validate_text(field, &raw, max).map(Some),
    }
}

/// 儲存層錯誤只留 log，對外給通用訊息——`StorageError` 可能夾帶表名或連線細節。
///
/// 例外：`NotFound`／`Conflict`／`ConstraintViolation` 是呼叫端該知道的語意，
/// 走 `ApiError: From<StorageError>` 的既有對映，不經過這裡。
pub(crate) fn storage_error(err: storage_core::StorageError) -> ApiError {
    use storage_core::StorageError as SE;
    match err {
        SE::NotFound { .. } | SE::Conflict { .. } | SE::ConstraintViolation { .. } => err.into(),
        other => {
            tracing::error!(error = %other, "資源 API 讀寫 canonical store 失敗");
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "unavailable",
                "canonical store 目前無法讀寫。請確認 Postgres 在跑後重試",
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(if_match: Option<&str>) -> HeaderMap {
        let mut map = HeaderMap::new();
        if let Some(value) = if_match {
            map.insert(header::IF_MATCH, HeaderValue::from_str(value).unwrap());
        }
        map
    }

    #[test]
    fn missing_if_match_is_428_not_412() {
        // 428 與 412 的下一步不同：前者「請帶 If-Match」，後者「請重新讀取」。
        let err = check_if_match(&headers(None), "v1").unwrap_err();
        assert_eq!(err.status, StatusCode::PRECONDITION_REQUIRED);
    }

    #[test]
    fn quoted_weak_and_star_all_match() {
        assert!(check_if_match(&headers(Some("\"v1\"")), "v1").is_ok());
        assert!(check_if_match(&headers(Some("W/\"v1\"")), "v1").is_ok());
        assert!(check_if_match(&headers(Some("*")), "v1").is_ok());
        assert!(check_if_match(&headers(Some("\"v0\", \"v1\"")), "v1").is_ok());
    }

    #[test]
    fn mismatch_is_412() {
        let err = check_if_match(&headers(Some("\"v0\"")), "v1").unwrap_err();
        assert_eq!(err.status, StatusCode::PRECONDITION_FAILED);
    }

    #[test]
    fn equivalent_rfc3339_spellings_are_the_same_version() {
        // 同一個時刻的兩種寫法不該產生假的 412。
        assert!(
            check_if_match(
                &headers(Some("\"2026-09-12T01:02:03+00:00\"")),
                "2026-09-12T01:02:03Z"
            )
            .is_ok()
        );
    }

    #[test]
    fn required_patch_rejects_explicit_null() {
        // null 被當成「沒提供」是靜默失效：使用者以為改了，其實沒有。
        let err = required_patch::<String>("name", Some(None)).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(required_patch::<String>("name", None).unwrap().is_none());
        assert_eq!(
            required_patch("name", Some(Some("a".to_string())))
                .unwrap()
                .as_deref(),
            Some("a")
        );
    }

    #[test]
    fn fingerprint_changes_with_content() {
        let a = fingerprint_version(&json!({"name": "a"})).unwrap();
        let b = fingerprint_version(&json!({"name": "b"})).unwrap();
        assert_ne!(a, b);
        assert!(a.starts_with("sha256:"));
    }

    #[test]
    fn text_validation_rejects_control_characters() {
        assert!(validate_text("name", "ok", 10).is_ok());
        assert!(validate_text("name", "   ", 10).is_err());
        assert!(validate_text("name", "a\nb", 10).is_err());
        assert!(validate_text("name", &"x".repeat(11), 10).is_err());
    }
}
