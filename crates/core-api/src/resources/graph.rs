//! Graph API（SPEC_V0.2 §9）。
//!
//! 四條讀取路由走 [`storage_core::GraphStore`]；`POST /graph/rebuild` 只建立
//! `job_type=graph_rebuild` 的 Job，真正的重建由 graph-worker 消費
//! `job.dispatched` 之後執行。
//!
//! # `POST /graph/rebuild` 不會 drop
//!
//! Job model 沒有參數欄位，沒有安全的方式讓呼叫端傳「要不要 drop」。
//! drop 是破壞性操作，不該只憑一個字串 job_type 就觸發。全清重建請用 CLI：
//! `osint-graph-worker --rebuild --drop`。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use core_model::Job;
use core_security::{Permission, Principal};
use storage_core::{GraphEdge, GraphNode, GraphPath, GraphQuery, GraphTraversalOptions};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::jobs::jobs_or_unavailable;
use crate::resources::{AuditEvent, audit, rejected_metadata, storage_error};
use crate::state::{AppState, SharedGraphStore};

const RESOURCE_GRAPH: &str = "graph";

/// 稽核 action。字串會進 `audit_log.action`，改動等於改稽核查詢條件——
/// `docs/developer/security.md` 的動作清單要一起改。
pub const AUDIT_GRAPH_REBUILD: &str = "graph.rebuild";

/// GET 查詢字串 → [`GraphTraversalOptions`]。跟 storage_core 的欄位對齊，
/// 但攤平成 URL 友善的形狀（逗號分隔清單、分開的 `time_from`／`time_to`）。
#[derive(Debug, Clone, Deserialize)]
pub struct GraphTraversalQuery {
    pub max_hops: Option<u32>,
    /// 逗號分隔，例如 `mentions,associated_with`。
    pub relationship_types: Option<String>,
    pub entity_types: Option<String>,
    pub min_confidence: Option<f64>,
    pub time_from: Option<DateTime<Utc>>,
    pub time_to: Option<DateTime<Utc>>,
}

/// `GET /graph/path` 額外需要起點與終點。
///
/// **不要**對 [`GraphTraversalQuery`] 用 `#[serde(flatten)]`：`serde_urlencoded`
/// 在 flatten 之後會把 `max_hops=4` 當成字串而不是數字，呼叫端會拿到 400
/// （`invalid type: string "4", expected u32`）。欄位重複寫一次，換成 flatten
/// 就會讓文件裡的 `?max_hops=` 範例整條壞掉。
#[derive(Debug, Clone, Deserialize)]
pub struct GraphPathQuery {
    pub from: Uuid,
    pub to: Uuid,
    pub max_hops: Option<u32>,
    pub relationship_types: Option<String>,
    pub entity_types: Option<String>,
    pub min_confidence: Option<f64>,
    pub time_from: Option<DateTime<Utc>>,
    pub time_to: Option<DateTime<Utc>>,
}

impl GraphPathQuery {
    fn traversal(&self) -> GraphTraversalQuery {
        GraphTraversalQuery {
            max_hops: self.max_hops,
            relationship_types: self.relationship_types.clone(),
            entity_types: self.entity_types.clone(),
            min_confidence: self.min_confidence,
            time_from: self.time_from,
            time_to: self.time_to,
        }
    }
}

fn graph_store(state: &AppState) -> Result<&SharedGraphStore, ApiError> {
    state.graph.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "Graph API 未接上 Neo4j。請確認 [storage.graph] 的 bolt_uri 與 Neo4j 是否啟動後重啟 osint-api；\
             這不影響 POST /entities/{id}/resolve 的 Postgres 掃描方法",
        )
    })
}

fn split_csv(raw: Option<&str>) -> Option<Vec<String>> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }
    let parts: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if parts.is_empty() { None } else { Some(parts) }
}

fn traversal_options(query: &GraphTraversalQuery) -> Result<GraphTraversalOptions, ApiError> {
    let time_range = match (query.time_from, query.time_to) {
        (Some(from), Some(to)) => Some((from, to)),
        (None, None) => None,
        (Some(_), None) => {
            return Err(ApiError::bad_request(
                "time_from 與 time_to 必須成對提供。只給一端的話時間過濾不會生效，\
                 請補上缺少的那一端，或兩個都省略",
            ));
        }
        (None, Some(_)) => {
            return Err(ApiError::bad_request(
                "time_from 與 time_to 必須成對提供。只給一端的話時間過濾不會生效，\
                 請補上缺少的那一端，或兩個都省略",
            ));
        }
    };
    Ok(GraphTraversalOptions {
        max_hops: query.max_hops.unwrap_or(1),
        relationship_types: split_csv(query.relationship_types.as_deref()),
        entity_types: split_csv(query.entity_types.as_deref()),
        min_confidence: query.min_confidence,
        time_range,
    })
}

/// `GET /api/v1/graph/entities/{id}/neighbors`。viewer 以上。
///
/// 圖上沒有這個節點回空陣列，不是 404（跟 [`GraphStore::neighbors`] 一致）。
pub async fn neighbors(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<GraphTraversalQuery>,
) -> Result<Json<Vec<GraphNode>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let options = traversal_options(&query)?;
    let nodes = graph_store(&state)?
        .neighbors(&id, &options)
        .await
        .map_err(storage_error)?;
    Ok(Json(nodes))
}

/// `GET /api/v1/graph/entities/{id}/relationships`。viewer 以上。
pub async fn relationships(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<GraphTraversalQuery>,
) -> Result<Json<Vec<GraphEdge>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let options = traversal_options(&query)?;
    let edges = graph_store(&state)?
        .relationships(&id, &options)
        .await
        .map_err(storage_error)?;
    Ok(Json(edges))
}

/// `GET /api/v1/graph/path`。viewer 以上。
///
/// 找不到路徑回 `null`、永遠 200——「這兩點沒連上」是查詢結果，不是錯誤。
pub async fn path(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<GraphPathQuery>,
) -> Result<Json<Option<GraphPath>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let options = traversal_options(&query.traversal())?;
    let path = graph_store(&state)?
        .shortest_path(&query.from, &query.to, &options)
        .await
        .map_err(storage_error)?;
    Ok(Json(path))
}

/// `POST /api/v1/graph/query`。viewer 以上（唯讀，理由同 `POST /search`）。
///
/// `starts` 空陣列時 [`GraphStore::query`] 回 [`storage_core::StorageError::ConstraintViolation`]，
/// 走 [`storage_error`] 映射成 400。handler 不重複檢查。
pub async fn query(
    State(state): State<AppState>,
    principal: Principal,
    Json(body): Json<GraphQuery>,
) -> Result<Json<Vec<GraphPath>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let paths = graph_store(&state)?
        .query(&body)
        .await
        .map_err(storage_error)?;
    Ok(Json(paths))
}

/// `POST /api/v1/graph/rebuild`。operator 以上。
///
/// 建立並派工 `graph_rebuild` Job，201、狀態 `queued`。真正執行的是
/// graph-worker；**不會** drop 既有節點／邊。
pub async fn rebuild(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    principal.role.require(Permission::Write)?;
    let result = async {
        let jobs = jobs_or_unavailable(&state)?;
        jobs.create_and_dispatch("graph_rebuild", None)
            .await
            .map_err(ApiError::from)
    }
    .await;
    match &result {
        Ok(job) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_GRAPH_REBUILD,
                    resource_type: RESOURCE_GRAPH,
                    resource_id: Some(job.id.to_string()),
                    outcome: "success",
                    metadata: json!({ "job_type": job.job_type, "status": job.status }),
                },
            )
            .await;
        }
        Err(err) => {
            audit(
                &state,
                &principal,
                &ip,
                AuditEvent {
                    action: AUDIT_GRAPH_REBUILD,
                    resource_type: RESOURCE_GRAPH,
                    resource_id: None,
                    outcome: "rejected",
                    metadata: rejected_metadata(err, json!({})),
                },
            )
            .await;
        }
    }
    result.map(|job| (StatusCode::CREATED, Json(job)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn query(
        relationship_types: Option<&str>,
        entity_types: Option<&str>,
        time_from: Option<DateTime<Utc>>,
        time_to: Option<DateTime<Utc>>,
        max_hops: Option<u32>,
    ) -> GraphTraversalQuery {
        GraphTraversalQuery {
            max_hops,
            relationship_types: relationship_types.map(str::to_string),
            entity_types: entity_types.map(str::to_string),
            min_confidence: None,
            time_from,
            time_to,
        }
    }

    #[test]
    fn max_hops_defaults_to_one() {
        let options = traversal_options(&query(None, None, None, None, None)).unwrap();
        assert_eq!(options.max_hops, 1);
    }

    #[test]
    fn csv_splits_trim_and_drops_empty_segments() {
        let options = traversal_options(&query(
            Some(" mentions, associated_with , "),
            Some(""),
            None,
            None,
            Some(2),
        ))
        .unwrap();
        assert_eq!(
            options.relationship_types.as_deref(),
            Some(["mentions".to_string(), "associated_with".to_string()].as_slice())
        );
        assert!(options.entity_types.is_none());
        assert_eq!(options.max_hops, 2);
    }

    #[test]
    fn one_sided_time_range_is_400() {
        let ts = Utc.with_ymd_and_hms(2026, 9, 14, 0, 0, 0).unwrap();
        let err = traversal_options(&query(None, None, Some(ts), None, None)).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert!(err.message.contains("time_from"), "{}", err.message);
        let err = traversal_options(&query(None, None, None, Some(ts), None)).unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn both_time_bounds_become_a_range() {
        let from = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let to = Utc.with_ymd_and_hms(2026, 12, 31, 0, 0, 0).unwrap();
        let options = traversal_options(&query(None, None, Some(from), Some(to), None)).unwrap();
        assert_eq!(options.time_range, Some((from, to)));
    }

    /// `#[serde(flatten)]` 曾讓 `max_hops=4` 變成 400；這支從 URI 解，
    /// 不是從 struct 字面量，才能抓到那個 serde_urlencoded 的坑。
    #[test]
    fn path_query_parses_numeric_max_hops_from_query_string() {
        use axum::extract::Query;
        use axum::http::Uri;

        let from = Uuid::now_v7();
        let to = Uuid::now_v7();
        let uri: Uri =
            format!("/api/v1/graph/path?from={from}&to={to}&max_hops=4&min_confidence=0.5")
                .parse()
                .unwrap();
        let Query(parsed): Query<GraphPathQuery> = Query::try_from_uri(&uri).unwrap();
        assert_eq!(parsed.from, from);
        assert_eq!(parsed.to, to);
        assert_eq!(parsed.max_hops, Some(4));
        assert_eq!(parsed.min_confidence, Some(0.5));
        assert_eq!(traversal_options(&parsed.traversal()).unwrap().max_hops, 4);
    }
}
