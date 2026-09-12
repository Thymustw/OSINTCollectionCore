//! `POST /api/v1/import`：把外部推進來的資料收成 RawEvidence。
//!
//! # 為什麼是新 endpoint，不是 SPEC §19 的 `POST /objects`
//!
//! 兩者語意完全不同，混在一起會讓 provenance 失真：
//!
//! - `POST /objects`（本階段未實作）＝ 直接建立 canonical Document。呼叫者就是內容的
//!   作者，沒有 RawEvidence 可以回溯。
//! - `POST /api/v1/import` ＝ 把**原始位元組**推進 pipeline。它產生的是 RawEvidence
//!   （immutable，body 存 MinIO），然後發 `raw.collected` 讓 normalizer 去拆 Document。
//!
//! 也就是說 import 是 pull 路徑（collector）的 push 版對應物：進入點不同，
//! 但落地格式、事件、正規化與 provenance 全部走同一條線。API handler 只負責
//! 「收下來變成 RawEvidence」，**不在這裡做正規化**。
//!
//! # 上傳格式
//!
//! `multipart/form-data`，兩個欄位：
//!
//! - `request`：JSON 文字，描述 source_id／kind／欄位對映等（見 `ImportRequest`）。
//! - `file`：實際檔案內容，串流讀取，累積超過上限立刻回 413。
//!
//! 選 multipart 而不是 JSON body 的理由：JSON body 只能塞 base64，體積膨脹 33%，
//! 而且**必須整包讀進記憶體才能解碼**，沒辦法在超過上限的當下就中止。

use axum::Json;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use chrono::Utc;
use connector_sdk::NewRawEvidence;
use core_events::EventTopic;
use core_model::{Connector, DocumentType, Source, SourceType};
use core_security::{AuditEntry, Permission, Principal};
use import_format::{FieldMapping, ImportKind, ImportLimits, ImportSpec};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::error::ApiError;
use crate::state::{AppState, ImportState};

/// 稽核 action。
pub const AUDIT_ACTION: &str = "import.upload";
/// 寫進 `RawEvidence.collector_version`。
const IMPORTER_VERSION: &str = concat!("core-api-import/", env!("CARGO_PKG_VERSION"));
/// `request` 這個文字欄位的上限。純 metadata，不需要大。
const MAX_REQUEST_FIELD_BYTES: usize = 64 * 1024;
/// 固定命名空間：同一個 Source ＋ 同一種匯入永遠對到同一個 connector（UUIDv5 是決定性的），
/// 不會每上傳一次就長出一列 connector。
const IMPORT_CONNECTOR_NAMESPACE: Uuid = Uuid::from_u128(0x0191_f3d2_5c0a_7b3e_9c21_6f6f_7e1d_4a55);

/// `request` 欄位的內容。
///
/// `deny_unknown_fields`：mapping 或欄位名打錯字必須當場失敗。靜默忽略會讓使用者拿到
/// 「匯入成功但每筆都空白」，那種錯誤要到查資料時才會發現。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImportRequest {
    /// 掛在哪個 Source 底下。必填，且 `source_type` 必須與 `kind` 相符。
    pub source_id: Uuid,
    /// 明確指定資料種類。不從副檔名或 `Content-Type` 推測。
    pub kind: ImportKind,
    /// 指定既有 connector；未填就自動配置該 Source 的匯入 connector。
    #[serde(default)]
    pub connector_id: Option<Uuid>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// 這份資料的來源位址（給人看的出處）。未填時合成 `import://…`。
    #[serde(default)]
    pub source_url: Option<String>,
    #[serde(default)]
    pub external_id: Option<String>,
    /// 只有 `kind=manual` 會用：使用者明確宣告的 MIME。未填則用內容 sniff。
    #[serde(default)]
    pub content_type: Option<String>,
    /// 產出的 Document 型別，預設 `report`。
    #[serde(default)]
    pub object_type: Option<DocumentType>,
    #[serde(default)]
    pub mapping: FieldMapping,
}

/// multipart 解出來的原料。
struct Upload {
    request: ImportRequest,
    filename: Option<String>,
    bytes: Vec<u8>,
}

/// `POST /api/v1/import` handler。
pub async fn import_upload(
    State(state): State<AppState>,
    principal: Principal,
    multipart: Multipart,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    // 路由層已經有 require_write，這裡再擋一次：handler 被搬到別的 router 時不會失去保護。
    principal.role.require(Permission::Write)?;

    match handle(&state, &principal, multipart).await {
        Ok(response) => {
            audit(
                &state,
                &principal,
                Some(response.source_id.to_string()),
                "success",
                json!({
                    "raw_evidence_id": response.raw_evidence_id,
                    "connector_id": response.connector_id,
                    "kind": response.kind.as_str(),
                    "content_type": response.content_type,
                    "bytes": response.bytes,
                    "sha256": response.sha256,
                    "record_count": response.record_count,
                    "published": response.published,
                }),
            )
            .await;
            Ok((StatusCode::CREATED, Json(response.into_json())))
        }
        Err(err) => {
            // 失敗時 source_id 不一定解析得出來（request 欄位就可能是壞的），
            // 所以 resource_id 留 None——而不是塞一個假的 "unknown" 字串進索引欄位。
            audit(
                &state,
                &principal,
                None,
                "rejected",
                json!({ "status": err.status.as_u16(), "error": err.error }),
            )
            .await;
            Err(err)
        }
    }
}

struct ImportResponse {
    raw_evidence_id: Uuid,
    source_id: Uuid,
    connector_id: Uuid,
    kind: ImportKind,
    content_type: String,
    bytes: usize,
    sha256: String,
    record_count: Option<usize>,
    skipped_empty: Option<usize>,
    published: bool,
    message: String,
}

impl ImportResponse {
    fn into_json(self) -> Value {
        json!({
            "raw_evidence_id": self.raw_evidence_id,
            "source_id": self.source_id,
            "connector_id": self.connector_id,
            "kind": self.kind.as_str(),
            "content_type": self.content_type,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "record_count": self.record_count,
            "skipped_empty": self.skipped_empty,
            "published": self.published,
            "message": self.message,
        })
    }
}

async fn handle(
    state: &AppState,
    principal: &Principal,
    multipart: Multipart,
) -> Result<ImportResponse, ApiError> {
    // 先讀 body 再檢查後端是否就緒：這樣「檔案太大」一定回 413，不會被 503 蓋掉。
    let limits = parse_limits(state);
    let upload = read_multipart(multipart, state.import_config.max_upload_bytes).await?;
    let import = state.import.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "匯入功能未接上 Postgres／MinIO。請設定 DATABASE_URL 與 S3_ENDPOINT 後重啟 osint-api",
        )
    })?;

    let request = upload.request;
    let source = load_source(import, request.source_id).await?;
    check_source_matches_kind(&source, request.kind)?;
    let connector = resolve_connector(import, &source, &request).await?;

    let spec = ImportSpec {
        kind: request.kind,
        mapping: request.mapping.clone(),
        limits,
        object_type: request.object_type.unwrap_or(DocumentType::Report),
    };

    // 上傳當下就解析一次。目的不是產生 Document（那是 normalizer 的事），
    // 而是讓格式與上限問題在 HTTP 回應裡當場講清楚。
    let parsed = match import_format::parse(&upload.bytes, &spec) {
        Some(Ok(outcome)) => Some(outcome),
        Some(Err(err)) => {
            return Err(ApiError::unprocessable(err.to_string()));
        }
        None => None,
    };

    let content_type =
        resolve_content_type(request.kind, request.content_type.as_deref(), &upload.bytes)?;
    let filename = upload.filename.as_deref().map(sanitize_filename);
    let source_url = resolve_source_url(&request, request.kind, filename.as_deref())?;

    let metadata = json!({
        "connector_type": request.kind.connector_type(),
        "import": spec,
        "upload": {
            "actor": principal.subject,
            "filename": filename,
            "title": request.title,
            "description": request.description,
            "record_count": parsed.as_ref().map(|p| p.records.len()),
            "skipped_empty": parsed.as_ref().map(|p| p.skipped_empty),
        },
    });

    let bytes = upload.bytes.len();
    let evidence = NewRawEvidence {
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: request.external_id.clone(),
        source_url,
        retrieved_at: Utc::now(),
        content_type: Some(content_type.clone()),
        mime_type: Some(content_type.clone()),
        // 不是 HTTP 抓取，沒有上游狀態碼可記。填假的 200 會讓之後查證據的人誤以為有抓過。
        http_status: None,
        http_headers: json!({}),
        metadata,
        collector_version: IMPORTER_VERSION.to_string(),
        body: upload.bytes,
    };

    let stored = import.sink.persist(evidence).await.map_err(|err| {
        // 底層訊息可能含 bucket／路徑，只寫進 log，不回給呼叫端。
        tracing::error!(error = %err, "匯入落地失敗");
        ApiError::new(
            StatusCode::BAD_GATEWAY,
            "storage_unavailable",
            "證據落地失敗（物件儲存或資料庫寫入沒有成功）。上傳未被接受，請稍後重試；\
             持續失敗請看 osint-api 記錄檔",
        )
    })?;

    let published = publish(import, &stored).await;
    let message = if published {
        "已收下並發出 raw.collected，normalizer 會接手正規化".to_string()
    } else {
        "證據已落地，但 raw.collected 發送失敗：這筆不會被自動正規化。\
         請確認 Redpanda 在跑，並用 raw_evidence_id 手動重送"
            .to_string()
    };

    Ok(ImportResponse {
        raw_evidence_id: stored.id,
        source_id: stored.source_id,
        connector_id: stored.connector_id,
        kind: request.kind,
        content_type,
        bytes,
        sha256: stored.sha256.clone(),
        record_count: parsed.as_ref().map(|p| p.records.len()),
        skipped_empty: parsed.as_ref().map(|p| p.skipped_empty),
        published,
        message,
    })
}

fn parse_limits(state: &AppState) -> ImportLimits {
    let cfg = &state.import_config;
    ImportLimits {
        max_records: cfg.max_records as usize,
        max_record_bytes: cfg.max_record_bytes as usize,
        max_field_bytes: cfg.max_field_bytes as usize,
        max_depth: cfg.max_depth as usize,
        max_columns: cfg.max_columns as usize,
    }
}

/// 串流讀 multipart。`file` 一超過上限就中止，不會先吃完整個檔再拒絕。
async fn read_multipart(
    mut multipart: Multipart,
    max_upload_bytes: u64,
) -> Result<Upload, ApiError> {
    let limit = usize::try_from(max_upload_bytes).unwrap_or(usize::MAX);
    let mut request_text: Option<String> = None;
    let mut filename = None;
    let mut bytes: Option<Vec<u8>> = None;

    while let Some(mut field) = multipart.next_field().await.map_err(multipart_error)? {
        match field.name().unwrap_or_default().to_string().as_str() {
            "request" => {
                let text = field.text().await.map_err(multipart_error)?;
                if text.len() > MAX_REQUEST_FIELD_BYTES {
                    return Err(ApiError::new(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "payload_too_large",
                        format!(
                            "request 欄位超過 {MAX_REQUEST_FIELD_BYTES} bytes。\
                             那裡只放 metadata 與欄位對映，檔案內容請放 file 欄位"
                        ),
                    ));
                }
                request_text = Some(text);
            }
            "file" => {
                filename = field.file_name().map(str::to_string);
                let mut buffer: Vec<u8> = Vec::new();
                while let Some(chunk) = field.chunk().await.map_err(multipart_error)? {
                    if buffer.len() + chunk.len() > limit {
                        return Err(ApiError::new(
                            StatusCode::PAYLOAD_TOO_LARGE,
                            "payload_too_large",
                            format!(
                                "上傳內容超過 {limit} bytes 上限。請拆成多個檔案分次上傳，\
                                 或請管理者調高 config [import].max_upload_bytes"
                            ),
                        ));
                    }
                    buffer.extend_from_slice(&chunk);
                }
                bytes = Some(buffer);
            }
            other => {
                return Err(ApiError::bad_request(format!(
                    "不認得 multipart 欄位 `{}`。只接受 request（JSON metadata）與 file（檔案內容）",
                    sanitize_field_name(other)
                )));
            }
        }
    }

    let request_text = request_text.ok_or_else(|| {
        ApiError::bad_request("缺少 request 欄位。請附上 JSON metadata，至少要有 source_id 與 kind")
    })?;
    let request: ImportRequest = serde_json::from_str(&request_text).map_err(|err| {
        ApiError::bad_request(format!(
            "request 欄位不是合法的匯入 JSON：{}。必要欄位是 source_id（UUID）與 kind（manual／json／csv）",
            err
        ))
    })?;
    let bytes = bytes.ok_or_else(|| {
        ApiError::bad_request("缺少 file 欄位。請用 multipart 附上要匯入的檔案內容")
    })?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request(
            "file 欄位是空的。請確認檔案真的有內容再上傳",
        ));
    }

    Ok(Upload {
        request,
        filename,
        bytes,
    })
}

/// multipart 本身的錯誤。超過 body 上限時 axum 回 413，其餘當 400。
fn multipart_error(err: axum::extract::multipart::MultipartError) -> ApiError {
    let status = err.status();
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        return ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            "上傳內容超過大小上限。請拆小再傳，或請管理者調高 config [import].max_upload_bytes",
        );
    }
    ApiError::bad_request(
        "multipart 內容解析失敗。請確認 Content-Type 是 multipart/form-data 且 boundary 正確",
    )
}

async fn load_source(import: &ImportState, source_id: Uuid) -> Result<Source, ApiError> {
    let source = import
        .store
        .get_source(source_id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Source `{source_id}`。請先用 POST /api/v1/sources 建立匯入用的 Source"
            ))
        })?;
    if !source.enabled {
        return Err(ApiError::conflict(format!(
            "Source `{source_id}` 已停用，不接受匯入。請先把它改回 enabled"
        )));
    }
    Ok(source)
}

/// Source 的種類必須與這次匯入相符。
///
/// 擋下來的原因：RSS 的 Source 底下混進 CSV 匯入，之後查「這個來源的資料怎麼來的」
/// 會得到互相矛盾的答案。要匯入就開一個匯入專用的 Source。
fn check_source_matches_kind(source: &Source, kind: ImportKind) -> Result<(), ApiError> {
    let expected = match kind {
        ImportKind::Manual => SourceType::ManualUpload,
        ImportKind::Json => SourceType::JsonImport,
        ImportKind::Csv => SourceType::CsvImport,
    };
    if source.source_type == expected {
        return Ok(());
    }
    Err(ApiError::bad_request(format!(
        "Source 的 source_type 與 kind=`{}` 不符（需要 `{}`）。\
         請改用對應的匯入 Source，或新建一個 source_type=`{}` 的 Source",
        kind.as_str(),
        kind.source_type(),
        kind.source_type()
    )))
}

/// 指定 connector 就用指定的；否則取用（必要時建立）該 Source 的匯入 connector。
async fn resolve_connector(
    import: &ImportState,
    source: &Source,
    request: &ImportRequest,
) -> Result<Connector, ApiError> {
    if let Some(id) = request.connector_id {
        let connector = import
            .store
            .get_connector(id)
            .await
            .map_err(storage_error)?
            .ok_or_else(|| ApiError::not_found(format!("找不到 Connector `{id}`")))?;
        if connector.source_id != source.id {
            return Err(ApiError::bad_request(format!(
                "Connector `{id}` 不屬於 Source `{}`。請改填該 Source 底下的 connector，或省略 connector_id",
                source.id
            )));
        }
        return Ok(connector);
    }

    let connector_type = request.kind.connector_type();
    let id = Uuid::new_v5(
        &IMPORT_CONNECTOR_NAMESPACE,
        format!("{}:{connector_type}", source.id).as_bytes(),
    );
    if let Some(existing) = import
        .store
        .get_connector(id)
        .await
        .map_err(storage_error)?
    {
        return Ok(existing);
    }
    let connector = Connector {
        id,
        source_id: source.id,
        name: format!("{}-{connector_type}", source.name),
        connector_type: connector_type.to_string(),
        version: IMPORTER_VERSION.to_string(),
        // push 路徑沒有排程。enabled=false 是刻意的：collector 只跑 enabled 的 connector，
        // 這樣它永遠不會試圖去「抓」一個根本沒有 URL 可抓的匯入來源。
        enabled: false,
        configuration: json!({ "managed_by": "core-api-import" }),
        credential_reference: None,
        schedule: None,
        rate_limit: json!({}),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    import
        .store
        .put_connector(&connector)
        .await
        .map_err(storage_error)?;
    Ok(connector)
}

/// 決定 `content_type`。
///
/// json／csv 由 `kind` 決定，manual 依「使用者明確宣告 → 內容 sniff」的順序。
/// 任何情況都不採信 multipart part 自己帶的 `Content-Type`，那是上傳者完全可控的字串。
fn resolve_content_type(
    kind: ImportKind,
    declared: Option<&str>,
    body: &[u8],
) -> Result<String, ApiError> {
    match kind {
        ImportKind::Json => Ok("application/json".into()),
        ImportKind::Csv => Ok("text/csv".into()),
        ImportKind::Manual => match declared {
            Some(value) => validate_media_type(value),
            None => Ok(sniff_content_type(body).to_string()),
        },
    }
}

/// 只接受 `type/subtype`（可帶參數）的保守字集，避免把換行或控制字元寫進 metadata。
fn validate_media_type(value: &str) -> Result<String, ApiError> {
    let trimmed = value.trim();
    let ok = !trimmed.is_empty()
        && trimmed.len() <= 127
        && trimmed.bytes().filter(|b| *b == b'/').count() == 1
        && trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"!#$&^_.+-/=; ".contains(&b));
    if !ok {
        return Err(ApiError::bad_request(
            "content_type 不是合法的 MIME。請填類似 `application/pdf` 的值，或整個省略改用內容判斷",
        ));
    }
    Ok(trimmed.to_ascii_lowercase())
}

/// 依內容開頭判斷型別。只認少數幾種常見 magic number，認不出來就是 octet-stream。
fn sniff_content_type(body: &[u8]) -> &'static str {
    const PNG: &[u8] = b"\x89PNG\r\n\x1a\n";
    if body.starts_with(b"%PDF-") {
        return "application/pdf";
    }
    if body.starts_with(PNG) {
        return "image/png";
    }
    if body.starts_with(b"\xff\xd8\xff") {
        return "image/jpeg";
    }
    if body.starts_with(b"GIF87a") || body.starts_with(b"GIF89a") {
        return "image/gif";
    }
    if body.starts_with(b"PK\x03\x04") {
        return "application/zip";
    }
    let head: String = String::from_utf8_lossy(&body[..body.len().min(512)])
        .trim_start()
        .to_ascii_lowercase();
    if head.starts_with("<!doctype html") || head.starts_with("<html") {
        return "text/html";
    }
    if head.starts_with("<?xml") || head.starts_with("<rss") || head.starts_with("<feed") {
        return "application/xml";
    }
    if head.starts_with('{') || head.starts_with('[') {
        return "application/json";
    }
    if std::str::from_utf8(body).is_ok() {
        return "text/plain";
    }
    "application/octet-stream"
}

/// `RawEvidence.source_url` 不可為空。沒有真實 URL 時合成一個看得懂的 `import://` URI。
fn resolve_source_url(
    request: &ImportRequest,
    kind: ImportKind,
    filename: Option<&str>,
) -> Result<String, ApiError> {
    if let Some(url) = request
        .source_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
    {
        if url.len() > 2048 || url.chars().any(char::is_control) {
            return Err(ApiError::bad_request(
                "source_url 太長或含控制字元。請填一般的 http(s) 位址，或整個省略",
            ));
        }
        return Ok(url.to_string());
    }
    Ok(format!(
        "import://{}/{}",
        kind.as_str(),
        filename.unwrap_or("upload")
    ))
}

/// 檔名是外部輸入。只留最後一段、去掉控制字元、截短——它會被寫進 metadata 與合成 URL。
fn sanitize_filename(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name);
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && *c != '"')
        .take(128)
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        "upload".into()
    } else {
        trimmed.to_string()
    }
}

fn sanitize_field_name(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
        .take(32)
        .collect()
}

/// 發 `raw.collected`。payload 形狀與 collector 完全一致，normalizer 不需要知道來源是 push 還是 pull。
async fn publish(import: &ImportState, stored: &core_model::RawEvidence) -> bool {
    let Some(producer) = &import.producer else {
        tracing::warn!(
            raw_evidence_id = %stored.id,
            "沒有 Redpanda producer，raw.collected 未發出"
        );
        return false;
    };
    let result = producer
        .publish(
            EventTopic::RawCollected,
            Some(&stored.source_id.to_string()),
            Some(stored.connector_id),
            json!({
                "raw_evidence_id": stored.id,
                "source_id": stored.source_id,
                "connector_id": stored.connector_id,
                "content_type": stored.content_type,
                "sha256": stored.sha256,
                "storage_path": stored.storage_path,
            }),
        )
        .await;
    match result {
        Ok(_envelope) => true,
        Err(err) => {
            tracing::error!(error = %err, raw_evidence_id = %stored.id, "publish raw.collected 失敗");
            false
        }
    }
}

/// 儲存層錯誤只留 log，對外給通用訊息——`StorageError` 可能夾帶表名或連線細節。
fn storage_error(err: storage_core::StorageError) -> ApiError {
    tracing::error!(error = %err, "匯入讀寫 canonical store 失敗");
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "unavailable",
        "canonical store 目前無法讀寫。請確認 Postgres 在跑後重試",
    )
}

async fn audit(
    state: &AppState,
    principal: &Principal,
    source_id: Option<String>,
    outcome: &str,
    metadata: Value,
) {
    let entry = AuditEntry::new(
        principal.subject.clone(),
        AUDIT_ACTION,
        "source",
        source_id,
        outcome,
    )
    .with_metadata(metadata);
    if let Err(err) = state.audit.append(entry).await {
        tracing::error!(error = %err, "寫入匯入稽核紀錄失敗");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniffs_common_binary_headers() {
        assert_eq!(sniff_content_type(b"%PDF-1.4 x"), "application/pdf");
        assert_eq!(sniff_content_type(b"PK\x03\x04zip"), "application/zip");
        assert_eq!(sniff_content_type(b"<!doctype html><html>"), "text/html");
        assert_eq!(sniff_content_type(b"{\"a\":1}"), "application/json");
        assert_eq!(sniff_content_type("純文字".as_bytes()), "text/plain");
        assert_eq!(
            sniff_content_type(&[0xff, 0xfe, 0x00, 0x9d]),
            "application/octet-stream"
        );
    }

    #[test]
    fn kind_decides_content_type_not_the_client() {
        // 宣稱 text/html 也不能改變 json 匯入的 content_type。
        let got = resolve_content_type(ImportKind::Json, Some("text/html"), b"[]").unwrap();
        assert_eq!(got, "application/json");
        let got = resolve_content_type(ImportKind::Csv, Some("text/html"), b"a\n1\n").unwrap();
        assert_eq!(got, "text/csv");
    }

    #[test]
    fn manual_prefers_declared_type_then_sniff() {
        let got = resolve_content_type(ImportKind::Manual, Some("application/pdf"), b"x").unwrap();
        assert_eq!(got, "application/pdf");
        let got = resolve_content_type(ImportKind::Manual, None, b"%PDF-1.4").unwrap();
        assert_eq!(got, "application/pdf");
    }

    #[test]
    fn header_injection_in_content_type_is_rejected() {
        let err = resolve_content_type(ImportKind::Manual, Some("text/html\r\nX-Evil: 1"), b"x")
            .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn filename_is_reduced_to_a_basename() {
        assert_eq!(sanitize_filename("../../etc/passwd"), "passwd");
        assert_eq!(sanitize_filename("C:\\tmp\\a.csv"), "a.csv");
        assert_eq!(sanitize_filename("  \n "), "upload");
        assert_eq!(sanitize_filename("報告.pdf"), "報告.pdf");
    }

    #[test]
    fn synthetic_source_url_when_not_given() {
        let request = ImportRequest {
            source_id: Uuid::nil(),
            kind: ImportKind::Csv,
            connector_id: None,
            title: None,
            description: None,
            source_url: None,
            external_id: None,
            content_type: None,
            object_type: None,
            mapping: FieldMapping::default(),
        };
        let url = resolve_source_url(&request, ImportKind::Csv, Some("a.csv")).unwrap();
        assert_eq!(url, "import://csv/a.csv");
    }

    #[test]
    fn connector_id_is_deterministic_per_source_and_kind() {
        let source = Uuid::from_u128(7);
        let first = Uuid::new_v5(
            &IMPORT_CONNECTOR_NAMESPACE,
            format!("{source}:json_import").as_bytes(),
        );
        let again = Uuid::new_v5(
            &IMPORT_CONNECTOR_NAMESPACE,
            format!("{source}:json_import").as_bytes(),
        );
        let other = Uuid::new_v5(
            &IMPORT_CONNECTOR_NAMESPACE,
            format!("{source}:csv_import").as_bytes(),
        );
        assert_eq!(first, again, "同一組輸入必須得到同一個 connector id");
        assert_ne!(first, other, "不同種類要分開掛");
    }
}
