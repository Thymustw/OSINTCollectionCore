//! STIX 2.1 匯入／匯出 HTTP 入口（SPEC_V0.2 §18-19）。
//!
//! `stix_import`／`stix_export` 這一步只做到「建立並派工 Job」。真正消費
//! `job.dispatched`、把 bundle 映成 Core Object（import）或把 Core Object
//! 組回 bundle（export）的是 Step 3／4 的 stix-worker——在那之前，Job 會一直
//! 停在 `queued`，這是預期的。
//!
//! # 為什麼 STIX import 不發 `raw.collected`
//!
//! 既有 `POST /api/v1/import` 存完 RawEvidence 會 publish `raw.collected`，
//! 讓 normalizer 把原始檔案拆成 Document。STIX bundle 已經是結構化的情報
//! 物件圖，不是待正規化的原始檔案；直接觸發 normalizer 會把整包 JSON
//! 當成一般文件去抽取，語意完全不對。這裡存 RawEvidence 只為了
//! provenance／稽核，存完之後建 `stix_import` Job，**不**發 `raw.collected`。
//!
//! # `GET /jobs/{id}/result` 讀取匯出結果
//!
//! worker 把匯出結果寫進物件儲存，key 慣例是 [`export_result_object_key`]
//! （`stix-exports/{job_id}.json`），並把同一個 key 寫進
//! `job.parameters.result_object_key`。這個 handler 依 `job.parameters` 裡的
//! key 真的去物件儲存讀擋回傳。handler 的行為：
//!
//! | 狀況 | 回應 |
//! |---|---|
//! | Job 不存在，或 `job_type` 不是 `stix_export` | 404 |
//! | 狀態不是 `completed` | 409（訊息帶目前狀態） |
//! | `completed` 但缺少 `result_object_key` | 500（worker 寫壞，或不該發生的狀態） |
//! | `completed` 有 key 但物件儲存讀不到內容，或內容不是合法 JSON | 500 |

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use chrono::Utc;
use connector_sdk::NewRawEvidence;
use core_model::{Connector, Job, JobStatus, Source, SourceType};
use core_security::{Permission, Principal};
use serde::Deserialize;
use serde_json::{Value, json};
use stix_adapter::{StixError, StixExportFilter, export_result_object_key};
use uuid::Uuid;

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::jobs::jobs_or_unavailable;
use crate::resources::{AuditEvent, audit, objects, rejected_metadata};
use crate::state::{AppState, ImportState};

/// 稽核 action。字串會進 `audit_log.action`；STIX 文件收尾時再寫進
/// `docs/developer/security.md` 的動作清單。
pub const AUDIT_STIX_IMPORT: &str = "stix.import";
pub const AUDIT_STIX_EXPORT: &str = "stix.export";

const RESOURCE_STIX: &str = "stix";
const JOB_TYPE_IMPORT: &str = "stix_import";
const JOB_TYPE_EXPORT: &str = "stix_export";
const IMPORTER_VERSION: &str = concat!("core-api-stix/", env!("CARGO_PKG_VERSION"));
const RESULT_OBJECT_KEY: &str = "result_object_key";

/// 固定命名空間：同一個 Source 的 STIX 匯入永遠對到同一個 connector
/// （UUIDv5 是決定性的），不會每上傳一次就長出一列。
const STIX_CONNECTOR_NAMESPACE: Uuid = Uuid::from_u128(0x0199_3c6a_7c3e_7a11_9c21_6f6f_7e1d_4b66);

/// `POST /api/v1/import/stix` 的 body。
///
/// `deny_unknown_fields`：欄位名打錯必須當場失敗。靜默忽略會讓使用者拿到
/// 「匯入成功但參數沒生效」，那種錯誤要到查 Job 時才會發現。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StixImportRequest {
    pub source_id: Uuid,
    pub bundle: Value,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StixExportRequest {
    #[serde(default)]
    pub filter: StixExportFilter,
}

/// `POST /api/v1/import/stix`。operator 以上。成功回 202 + Job。
///
/// Body 用 [`Bytes`] 而不是 `Json<T>`：要在 serde 之前量長度。超過
/// `[stix].max_bundle_bytes` 立刻 413，不會先把整包 JSON 解開才拒絕。
/// 路由層另外掛了稍寬的 [`axum::extract::DefaultBodyLimit`] 當硬性後盾
/// （見 `routes.rs`），讓這裡的檢查先觸發、回得出「是 max_bundle_bytes 擋的」。
pub async fn import_stix(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    body: Bytes,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    principal.role.require(Permission::Write)?;
    let result = import_stix_inner(&state, &principal, &body).await;
    audit_stix(
        &state,
        &principal,
        &ip,
        AUDIT_STIX_IMPORT,
        &result,
        json!({ "bytes": body.len() }),
    )
    .await;
    result.map(|job| (StatusCode::ACCEPTED, Json(job)))
}

async fn import_stix_inner(
    state: &AppState,
    principal: &Principal,
    body: &Bytes,
) -> Result<Job, ApiError> {
    let max_bytes = state.stix_config.max_bundle_bytes;
    let limit = usize::try_from(max_bytes).unwrap_or(usize::MAX);
    if body.len() > limit {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!(
                "STIX bundle 超過 {limit} bytes 上限。請拆成較小的 bundle 分次匯入，\
                 或請管理者調高 config [stix].max_bundle_bytes"
            ),
        ));
    }
    if body.is_empty() {
        return Err(ApiError::bad_request(
            "請求 body 是空的。請送 `{\"source_id\": \"<uuid>\", \"bundle\": {…}}`",
        ));
    }

    let request: StixImportRequest = serde_json::from_slice(body).map_err(|err| {
        ApiError::bad_request(format!(
            "不是合法的 STIX 匯入 JSON：{err}。必要欄位是 source_id（UUID）與 bundle（STIX 2.1 bundle 物件）"
        ))
    })?;

    stix_adapter::validate_bundle(&request.bundle, state.stix_config.max_objects)
        .map_err(stix_error)?;

    let object_count = request
        .bundle
        .get("objects")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);

    let import = import_or_unavailable(state)?;
    let jobs = jobs_or_unavailable(state)?;
    let source = load_source(import, request.source_id).await?;
    check_source_is_stix(&source)?;
    let connector = resolve_stix_connector(import, &source).await?;

    let bundle_id = request
        .bundle
        .get("id")
        .and_then(Value::as_str)
        .map(str::to_string);

    let evidence = NewRawEvidence {
        source_id: source.id,
        connector_id: connector.id,
        collection_id: None,
        external_id: bundle_id,
        source_url: format!("stix://import/{}", source.id),
        retrieved_at: Utc::now(),
        content_type: Some("application/json".into()),
        mime_type: Some("application/stix+json".into()),
        http_status: None,
        http_headers: json!({}),
        metadata: json!({
            "import_spec": "stix_bundle",
            "object_count": object_count,
            "actor": principal.subject,
        }),
        collector_version: IMPORTER_VERSION.to_string(),
        body: body.to_vec(),
    };

    let stored = import.sink.persist(evidence).await.map_err(|err| {
        tracing::error!(error = %err, "STIX 匯入落地失敗");
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "storage_unavailable",
            "證據落地失敗（物件儲存或資料庫寫入沒有成功）。上傳未被接受，請稍後重試；\
             持續失敗請看 osint-api 記錄檔",
        )
    })?;

    // 刻意不 publish `raw.collected`。見模組說明。
    let parameters = json!({
        "source_id": source.id,
        "raw_evidence_id": stored.id,
    });
    jobs.create_and_dispatch(JOB_TYPE_IMPORT, Some(stored.id), Some(parameters))
        .await
        .map_err(ApiError::from)
}

/// `POST /api/v1/export/stix`。operator 以上。成功回 202 + Job。
///
/// 這一步沒有 stix-worker，Job 建立後會停在 `queued`。這是預期的。
pub async fn export_stix(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
    Json(body): Json<StixExportRequest>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    principal.role.require(Permission::Write)?;
    let filter = body.filter;
    let result = async {
        let jobs = jobs_or_unavailable(&state)?;
        jobs.create_and_dispatch(JOB_TYPE_EXPORT, None, Some(json!({ "filter": filter })))
            .await
            .map_err(ApiError::from)
    }
    .await;
    audit_stix(
        &state,
        &principal,
        &ip,
        AUDIT_STIX_EXPORT,
        &result,
        json!({}),
    )
    .await;
    result.map(|job| (StatusCode::ACCEPTED, Json(job)))
}

/// `GET /api/v1/jobs/{id}/result`。viewer 以上。
///
/// worker 把結果寫進 `job.parameters.result_object_key` 指的物件儲存 key；
/// 這裡依 key 真的讀檔回傳 STIX bundle JSON。見模組說明。
pub async fn job_result(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<Value>, ApiError> {
    principal.role.require(Permission::Read)?;
    let jobs = jobs_or_unavailable(&state)?;
    let job = jobs.get(id).await?;
    if job.job_type != JOB_TYPE_EXPORT {
        return Err(ApiError::not_found(format!(
            "找不到 STIX 匯出結果 `{id}`。這個 endpoint 只服務 job_type=`stix_export` 的完成結果"
        )));
    }
    if job.status != JobStatus::Completed {
        return Err(ApiError::conflict(format!(
            "STIX 匯出 Job `{id}` 尚未完成（目前狀態是 {}）。完成後再查；\
             若一直停在 queued，代表 stix-worker 還沒接上（Step 3／4）",
            job_status_str(job.status)
        )));
    }
    let key = job
        .parameters
        .as_ref()
        .and_then(|p| p.get(RESULT_OBJECT_KEY))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty());
    let Some(key) = key else {
        // completed 但缺 key 代表 worker 寫壞了。回 500 而不是 404：呼叫端不該
        // 重試同一個已完成的 Job 期待它長出結果。
        return Err(ApiError::internal(format!(
            "STIX 匯出 Job `{id}` 已完成，但 parameters 缺少 `{RESULT_OBJECT_KEY}`。\
             這不該發生：stix-worker 寫完物件儲存後必須把 key（慣例 {}）寫回 Job。\
             請看 worker 記錄檔",
            export_result_object_key(id)
        )));
    };
    let objects = objects(&state)?;
    let bytes = objects
        .get(key)
        .await
        .map_err(crate::resources::storage_error)?
        .ok_or_else(|| {
            ApiError::internal(format!(
                "STIX 匯出 Job `{id}` 的 parameters 有 result_object_key=`{key}`，\
                 但物件儲存裡讀不到這個檔案。請查 stix-worker 記錄檔，這不該發生"
            ))
        })?;
    let bundle: Value = serde_json::from_slice(&bytes).map_err(|err| {
        ApiError::internal(format!(
            "STIX 匯出 Job `{id}` 的結果檔案不是合法 JSON：{err}。請查 stix-worker 記錄檔"
        ))
    })?;
    Ok(Json(bundle))
}

fn import_or_unavailable(state: &AppState) -> Result<&ImportState, ApiError> {
    state.import.as_deref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "STIX 匯入未接上 Postgres／MinIO。請設定 DATABASE_URL 與 S3_ENDPOINT 後重啟 osint-api",
        )
    })
}

async fn load_source(import: &ImportState, source_id: Uuid) -> Result<Source, ApiError> {
    let source = import
        .store
        .get_source(source_id)
        .await
        .map_err(crate::resources::storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 Source `{source_id}`。請先用 POST /api/v1/sources 建立 source_type=`stix_import` 的 Source"
            ))
        })?;
    if !source.enabled {
        return Err(ApiError::conflict(format!(
            "Source `{source_id}` 已停用，不接受匯入。請先把它改回 enabled"
        )));
    }
    Ok(source)
}

fn check_source_is_stix(source: &Source) -> Result<(), ApiError> {
    if source.source_type == SourceType::StixImport {
        return Ok(());
    }
    Err(ApiError::bad_request(format!(
        "Source 的 source_type 不是 `stix_import`（實際是 `{}`）。\
         請改用 STIX 匯入專用的 Source，或新建一個 source_type=`stix_import` 的 Source",
        encode_source_type(source.source_type)
    )))
}

fn encode_source_type(value: SourceType) -> &'static str {
    match value {
        SourceType::Rss => "rss",
        SourceType::Atom => "atom",
        SourceType::StaticWeb => "static_web",
        SourceType::RestApi => "rest_api",
        SourceType::ManualUpload => "manual_upload",
        SourceType::JsonImport => "json_import",
        SourceType::CsvImport => "csv_import",
        SourceType::StixImport => "stix_import",
    }
}

async fn resolve_stix_connector(
    import: &ImportState,
    source: &Source,
) -> Result<Connector, ApiError> {
    let connector_type = "stix_import";
    let id = Uuid::new_v5(
        &STIX_CONNECTOR_NAMESPACE,
        format!("{}:{connector_type}", source.id).as_bytes(),
    );
    if let Some(existing) = import
        .store
        .get_connector(id)
        .await
        .map_err(crate::resources::storage_error)?
    {
        return Ok(existing);
    }
    let connector = Connector {
        id,
        source_id: source.id,
        name: format!("{}-{connector_type}", source.name),
        connector_type: connector_type.to_string(),
        version: IMPORTER_VERSION.to_string(),
        // push 路徑沒有排程。enabled=false 是刻意的：collector 只跑 enabled 的
        // connector，這樣它永遠不會試圖去「抓」一個根本沒有 URL 可抓的匯入來源。
        enabled: false,
        configuration: json!({ "managed_by": "core-api-stix" }),
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
        .map_err(crate::resources::storage_error)?;
    Ok(connector)
}

fn stix_error(err: StixError) -> ApiError {
    match err {
        StixError::TooManyObjects { actual, limit } => ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            format!(
                "STIX bundle 物件數量 {actual} 超過上限 {limit}。請拆成較小的 bundle，\
                 或請管理者調高 config [stix].max_objects"
            ),
        ),
        other => ApiError::bad_request(other.to_string()),
    }
}

async fn audit_stix(
    state: &AppState,
    principal: &Principal,
    ip: &ClientIp,
    action: &'static str,
    result: &Result<Job, ApiError>,
    extra: Value,
) {
    match result {
        Ok(job) => {
            audit(
                state,
                principal,
                ip,
                AuditEvent {
                    action,
                    resource_type: RESOURCE_STIX,
                    resource_id: Some(job.id.to_string()),
                    outcome: "success",
                    metadata: json!({
                        "job_type": job.job_type,
                        "status": job.status,
                        "extra": extra,
                    }),
                },
            )
            .await;
        }
        Err(err) => {
            audit(
                state,
                principal,
                ip,
                AuditEvent {
                    action,
                    resource_type: RESOURCE_STIX,
                    resource_id: None,
                    outcome: "rejected",
                    metadata: rejected_metadata(err, extra),
                },
            )
            .await;
        }
    }
}

fn job_status_str(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Retrying => "retrying",
        JobStatus::Failed => "failed",
        JobStatus::Cancelled => "cancelled",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn export_result_key_is_deterministic() {
        let id = Uuid::from_u128(7);
        assert_eq!(
            export_result_object_key(id),
            format!("stix-exports/{id}.json")
        );
    }

    #[test]
    fn too_many_objects_is_413() {
        let err = stix_error(StixError::TooManyObjects {
            actual: 11,
            limit: 10,
        });
        assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(err.message.contains("max_objects"), "{}", err.message);
    }

    #[test]
    fn invalid_bundle_is_400() {
        let err = stix_error(StixError::InvalidBundle {
            message: "缺少 `type`".into(),
        });
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn connector_id_is_deterministic_per_source() {
        let source = Uuid::from_u128(7);
        let first = Uuid::new_v5(
            &STIX_CONNECTOR_NAMESPACE,
            format!("{source}:stix_import").as_bytes(),
        );
        let again = Uuid::new_v5(
            &STIX_CONNECTOR_NAMESPACE,
            format!("{source}:stix_import").as_bytes(),
        );
        assert_eq!(first, again);
    }
}
