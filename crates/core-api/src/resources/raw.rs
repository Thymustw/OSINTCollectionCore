//! `GET /api/v1/raw/{id}`（SPEC §8／§19）。
//!
//! 預設只回 metadata。`?body=true` 才去物件儲存拉原始位元組——RawEvidence 的 body
//! 可能是幾 MB 的 PDF，把它塞進每一次查詢的回應等於讓「看一下這筆是什麼」
//! 變成一次大檔傳輸。
//!
//! # body 的兩種編碼
//!
//! * 合法 UTF-8 → `content_encoding: "utf8"`，`content` 就是原文
//! * 其他（PDF／圖片／壓縮檔）→ `content_encoding: "base64"`
//!
//! **不做「截斷後回前 N 個字元」**：截斷過的內容拿去算 sha256 對不上，
//! 會讓人以為證據被竄改。超過上限就回 413 並說明怎麼取完整內容。
//!
//! 上限沿用 `[import].max_upload_bytes`：那是「一筆證據可以多大」的既有設定，
//! 收得進來就應該讀得出去，兩邊用不同的數字只會製造「存得進去卻讀不出來」的洞。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use base64::Engine;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use core_model::RawEvidence;
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::resources::{objects, storage_error, store};
use crate::state::AppState;

#[derive(Debug, Clone, Deserialize)]
pub struct ShowQuery {
    /// 預設 false：只回 metadata。
    #[serde(default)]
    pub body: bool,
}

/// `?body=true` 時附上的內容。
#[derive(Debug, Clone, Serialize)]
pub struct RawBody {
    pub bytes: usize,
    /// `utf8` 或 `base64`。
    pub content_encoding: &'static str,
    pub content: String,
    /// 物件儲存裡的 key（`raw_evidence.storage_path`）。
    pub storage_path: String,
}

/// `GET /api/v1/raw/{id}`。viewer 以上。
pub async fn get_raw(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<ShowQuery>,
) -> Result<Json<Value>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let evidence: RawEvidence = store
        .get_raw_evidence(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 RawEvidence `{id}`。請用 osint-cli raw list 或 GET /api/v1/objects 反查"
            ))
        })?;

    let mut value = serde_json::to_value(&evidence).map_err(|err| {
        ApiError::internal(format!(
            "序列化 RawEvidence 失敗：{err}。請看 osint-api 記錄檔"
        ))
    })?;

    if query.body {
        let body = fetch_body(&state, &evidence).await?;
        value["body"] = serde_json::to_value(&body)
            .map_err(|err| ApiError::internal(format!("序列化 RawEvidence body 失敗：{err}")))?;
    }

    Ok(Json(value))
}

async fn fetch_body(state: &AppState, evidence: &RawEvidence) -> Result<RawBody, ApiError> {
    let objects = objects(state)?;
    let key = object_key(&evidence.storage_path, &state.object_bucket);
    let limit = usize::try_from(state.import_config.max_upload_bytes).unwrap_or(usize::MAX);

    // 先看 metadata 記的長度：超過上限就不必把整包拉進記憶體才發現。
    // `content_length` 可能是 NULL（舊資料），那時只能拉下來再檢查。
    if let Some(declared) = evidence
        .content_length
        .and_then(|n| usize::try_from(n).ok())
    {
        if declared > limit {
            return Err(too_large(declared, limit, &key));
        }
    }

    let bytes = objects
        .get(&key)
        .await
        .map_err(|err| {
            tracing::error!(error = %err, storage_path = %evidence.storage_path, "讀取 RawEvidence body 失敗");
            // 503 而不是 502，理由同 `import::persist` 的註解：後端沒有回應，
            // 不是回了壞東西，而且「後端不可用」全 API 只用一個狀態碼。
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
                "物件儲存讀取失敗。metadata 仍可取得（去掉 ?body=true），\
                 請確認 MinIO 在跑後重試",
            )
        })?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "RawEvidence `{}` 的 metadata 存在，但物件儲存裡找不到 key `{key}`。\
                 這代表資料庫與物件儲存不一致（bucket 被清過，或寫入時只成功了一半），\
                 請檢查 MinIO bucket 內容",
                evidence.id
            ))
        })?;

    if bytes.len() > limit {
        return Err(too_large(bytes.len(), limit, &key));
    }

    Ok(match String::from_utf8(bytes) {
        Ok(text) => RawBody {
            bytes: text.len(),
            content_encoding: "utf8",
            content: text,
            storage_path: evidence.storage_path.clone(),
        },
        Err(err) => {
            let bytes = err.into_bytes();
            RawBody {
                bytes: bytes.len(),
                content_encoding: "base64",
                content: base64::engine::general_purpose::STANDARD.encode(&bytes),
                storage_path: evidence.storage_path.clone(),
            }
        }
    })
}

fn too_large(actual: usize, limit: usize, key: &str) -> ApiError {
    ApiError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        format!(
            "這筆 RawEvidence 的內容是 {actual} bytes，超過 API 回傳上限 {limit} bytes\
             （config `[import].max_upload_bytes`）。\
             metadata 仍可取得（去掉 ?body=true）；要拿完整內容請直接從物件儲存讀 key `{key}`，\
             或請管理者調高該上限"
        ),
    )
}

/// `raw_evidence.storage_path` 平常存的就是物件 key（`raw/{source}/{y}/{m}/{d}/{id}`）。
/// 舊資料或測試 fixture 可能寫成 `s3://{bucket}/{key}`，這裡一併處理——
/// 否則會拿整串去查而得到「找不到」，那個錯誤完全不指向真因。
///
/// 與 `osint-cli raw show` 的 `object_key` 是同一套規則。
fn object_key(storage_path: &str, bucket: &str) -> String {
    let trimmed = storage_path
        .strip_prefix("s3://")
        .unwrap_or(storage_path)
        .to_string();
    if bucket.is_empty() {
        return trimmed;
    }
    trimmed
        .strip_prefix(&format!("{bucket}/"))
        .unwrap_or(&trimmed)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_key_is_unchanged() {
        assert_eq!(
            object_key("raw/abc/2026/09/12/def", "raw-evidence"),
            "raw/abc/2026/09/12/def"
        );
    }

    #[test]
    fn s3_url_is_stripped() {
        assert_eq!(
            object_key("s3://raw-evidence/raw/a/b", "raw-evidence"),
            "raw/a/b"
        );
    }

    #[test]
    fn empty_bucket_leaves_the_path_alone() {
        // bucket 未設定時不要亂剝前綴，否則會查一個不存在的 key。
        assert_eq!(
            object_key("s3://raw-evidence/raw/a", ""),
            "raw-evidence/raw/a"
        );
    }

    #[test]
    fn body_defaults_to_false() {
        let query: ShowQuery = serde_json::from_str("{}").unwrap();
        assert!(!query.body, "預設就拉 body 會讓每次查詢都變成大檔傳輸");
    }
}
