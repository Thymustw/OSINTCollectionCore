//! SPEC §19 資源 endpoint 的**不需要資料庫**的那一半：認證、RBAC、
//! `POST /objects` 的 501、`/api/v1/ops/*`。
//!
//! 需要真實資料的行為（200 的回應形狀、404、If-Match、409／422、raw body）
//! 在 `tests/resources_api_e2e.rs`，那支連本機 Postgres／MinIO。
//!
//! 這支刻意**不接任何 store**：middleware 在 handler 之前就跑完，
//! 所以 401／403 的行為與接了資料庫時完全一樣。

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use common::{issue_test_jwt, test_app_parts, test_app_with_backends};
use core_api::{AUDIT_OBJECT_CREATE, ReadyCheck};
use core_observability::CheckResult;
use core_security::Role;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

/// SPEC §19 的全部唯讀路徑（`{id}` 已代入）。
fn read_paths() -> Vec<String> {
    let id = Uuid::now_v7();
    vec![
        "/api/v1/sources".into(),
        format!("/api/v1/sources/{id}"),
        "/api/v1/connectors".into(),
        format!("/api/v1/connectors/{id}"),
        "/api/v1/collections".into(),
        format!("/api/v1/collections/{id}"),
        "/api/v1/objects".into(),
        format!("/api/v1/objects/{id}"),
        "/api/v1/entities".into(),
        format!("/api/v1/entities/{id}"),
        format!("/api/v1/entities/{id}/resolution-candidates"),
        format!("/api/v1/entities/{id}/merge-history"),
        "/api/v1/relationships".into(),
        format!("/api/v1/relationships/{id}"),
        "/api/v1/events".into(),
        format!("/api/v1/events/{id}"),
        format!("/api/v1/raw/{id}"),
        "/api/v1/ops/health".into(),
        "/api/v1/ops/metrics".into(),
        format!("/api/v1/graph/entities/{id}/neighbors"),
        format!("/api/v1/graph/entities/{id}/relationships"),
        format!("/api/v1/graph/path?from={id}&to={id}&max_hops=2"),
    ]
}

/// 全部寫入路徑：(method, path, body)。
fn write_requests() -> Vec<(&'static str, String, Value)> {
    let id = Uuid::now_v7();
    vec![
        (
            "POST",
            "/api/v1/sources".into(),
            json!({"name": "s", "source_type": "rss"}),
        ),
        (
            "PATCH",
            format!("/api/v1/sources/{id}"),
            json!({"name": "s"}),
        ),
        (
            "POST",
            "/api/v1/connectors".into(),
            json!({"source_id": id, "name": "c", "type": "rss"}),
        ),
        (
            "PATCH",
            format!("/api/v1/connectors/{id}"),
            json!({"name": "c"}),
        ),
        ("POST", "/api/v1/collections".into(), json!({"name": "col"})),
        ("POST", "/api/v1/objects".into(), json!({})),
        ("POST", format!("/api/v1/jobs/{id}/retry"), json!({})),
        ("POST", format!("/api/v1/entities/{id}/resolve"), json!({})),
        (
            "POST",
            format!("/api/v1/entities/{id}/resolve/graph-context"),
            json!({}),
        ),
        (
            "POST",
            "/api/v1/entities/merge".into(),
            json!({
                "survivor_id": id,
                "merged_id": id,
                "reason": "rbac",
            }),
        ),
        (
            "POST",
            format!("/api/v1/merge-history/{id}/undo"),
            json!({}),
        ),
        ("POST", "/api/v1/graph/rebuild".into(), json!({})),
    ]
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, body)
}

fn get(uri: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

fn write(method: &str, uri: &str, token: Option<&str>, body: &Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

/// 帶上假的 TCP peer。少了它稽核的 `ip` 會是 NULL——那正是 Phase 6a 的缺口。
fn with_peer(mut request: Request<Body>) -> Request<Body> {
    request.extensions_mut().insert(ConnectInfo(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(198, 51, 100, 7)),
        54_321,
    )));
    request
}

#[tokio::test]
async fn every_endpoint_requires_authentication() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);

    for path in read_paths() {
        let (status, body) = send(&app, get(&path, None)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "GET {path} 應回 401：{body}"
        );
    }
    for (method, path, body) in write_requests() {
        let (status, got) = send(&app, write(method, &path, None, &body)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} 應回 401：{got}"
        );
    }
}

/// viewer 不可以寫。**包含 `POST /objects`**：那條路由雖然一律回 501，
/// 也必須先擋權限——否則它會變成一個不需要憑證就能探測伺服器支援什麼的入口。
#[tokio::test]
async fn viewer_cannot_write_anything() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, audit, _) = test_app_parts(jwt);

    let requests = write_requests();
    for (method, path, body) in &requests {
        let (status, got) = send(&app, write(method, path, Some(&viewer), body)).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{method} {path} 對 viewer 應回 403：{got}"
        );
    }

    let denied = audit
        .entries()
        .iter()
        .filter(|e| e.action == core_api::AUDIT_AUTHZ_DENIED)
        .count();
    assert_eq!(denied, requests.len(), "每一次 403 都要留稽核");
}

/// viewer 讀得到（這裡沒接 store，所以是 503 而不是 200——重點是**沒有** 403）。
#[tokio::test]
async fn viewer_may_read_and_gets_503_without_a_store() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);

    for path in read_paths() {
        // ops 的兩條不需要 store。
        if path.starts_with("/api/v1/ops/") {
            continue;
        }
        let (status, body) = send(&app, get(&path, Some(&viewer))).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "GET {path} 沒接 store 時應回 503（而不是 403／500）：{body}"
        );
        assert_eq!(body["error"], "unavailable");
        let message = body["message"].as_str().unwrap();
        if path.starts_with("/api/v1/graph/") {
            assert!(
                message.contains("Neo4j") || message.contains("bolt_uri"),
                "Graph 503 訊息要講 Neo4j／bolt_uri：{body}"
            );
        } else {
            assert!(
                message.contains("DATABASE_URL"),
                "503 訊息要講下一步怎麼做：{body}"
            );
        }
    }
}

/// `POST /graph/query` 是唯讀（viewer 即可），沒接 Neo4j 回 503 不是 403。
#[tokio::test]
async fn viewer_may_post_graph_query_and_gets_503_without_neo4j() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, body) = send(
        &app,
        write(
            "POST",
            "/api/v1/graph/query",
            Some(&viewer),
            &json!({
                "starts": [Uuid::now_v7()],
                "pattern": {"kind": "neighbors"},
                "options": {"max_hops": 1}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["error"], "unavailable");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("Neo4j") || message.contains("bolt_uri"),
        "Graph 503 訊息要講 Neo4j／bolt_uri：{body}"
    );
}

/// 未認證打 `POST /graph/query` 仍是 401。
#[tokio::test]
async fn graph_query_requires_authentication() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, body) = send(
        &app,
        write(
            "POST",
            "/api/v1/graph/query",
            None,
            &json!({
                "starts": [Uuid::now_v7()],
                "pattern": {"kind": "neighbors"},
                "options": {"max_hops": 1}
            }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
}

/// `POST /objects` 回 501，訊息要指向 `/import`，而且要留稽核（含 IP）。
#[tokio::test]
async fn post_objects_is_501_and_points_at_import() {
    let (jwt, operator) = issue_test_jwt(Role::Operator);
    let (app, audit, _) = test_app_parts(jwt);

    let (status, body) = send(
        &app,
        with_peer(write(
            "POST",
            "/api/v1/objects",
            Some(&operator),
            &json!({}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    assert_eq!(body["error"], "not_implemented");
    let message = body["message"].as_str().unwrap();
    assert!(message.contains("/api/v1/import"), "{message}");
    assert!(message.contains("ADR-006"), "{message}");

    let entry = audit
        .entries()
        .into_iter()
        .find(|e| e.action == AUDIT_OBJECT_CREATE)
        .expect("501 也要留稽核：有人一直想繞過 import 是要看得到的訊號");
    assert_eq!(entry.outcome, "rejected");
    assert_eq!(
        entry.ip.as_deref(),
        Some("198.51.100.7"),
        "稽核必須記下呼叫端 IP"
    );
}

/// 每一個寫入動作的稽核都要有 IP。Phase 6a 的缺口就是這個：
/// token handler 沒把 client address 傳進去，`audit_log.ip` 整欄是 NULL。
#[tokio::test]
async fn token_audit_records_the_client_ip() {
    let (jwt, admin) = issue_test_jwt(Role::Admin);
    let (app, audit, _) = test_app_parts(jwt);

    let (status, body) = send(
        &app,
        with_peer(write(
            "POST",
            "/api/v1/tokens",
            Some(&admin),
            &json!({"name": "ci", "role": "viewer"}),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let id = body["id"].as_str().unwrap().to_string();

    let (status, _) = send(
        &app,
        with_peer(
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/tokens/{id}"))
                .header("Authorization", format!("Bearer {admin}"))
                .body(Body::empty())
                .unwrap(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    for action in [core_api::AUDIT_TOKEN_ISSUE, core_api::AUDIT_TOKEN_REVOKE] {
        let entry = audit
            .entries()
            .into_iter()
            .find(|e| e.action == action)
            .unwrap_or_else(|| panic!("缺少 {action} 稽核"));
        assert_eq!(
            entry.ip.as_deref(),
            Some("198.51.100.7"),
            "{action} 的稽核沒有 IP：查「那些改動從哪來」時會查不到"
        );
    }
}

/// 一個永遠 down 的後端。
struct AlwaysDown(&'static str);

#[async_trait]
impl ReadyCheck for AlwaysDown {
    fn name(&self) -> &'static str {
        self.0
    }

    async fn check(&self) -> CheckResult {
        CheckResult::down(self.0, "連線被拒（測試用的假檢查）")
    }
}

struct AlwaysUp(&'static str);

#[async_trait]
impl ReadyCheck for AlwaysUp {
    fn name(&self) -> &'static str {
        self.0
    }

    async fn check(&self) -> CheckResult {
        CheckResult::ok(self.0, "ok")
    }
}

#[tokio::test]
async fn ops_health_is_200_when_every_backend_is_up() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_with_backends(
        jwt,
        vec![Arc::new(AlwaysUp("postgres")), Arc::new(AlwaysUp("redis"))],
        Vec::new(),
    );

    let (status, body) = send(&app, get("/api/v1/ops/health", Some(&viewer))).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["healthy"], true);
    assert_eq!(body["checks"].as_array().unwrap().len(), 2);
    assert!(body["unhealthy"].as_array().unwrap().is_empty());
}

/// 任一後端 down → 整體 503，而且回應要指出**是哪一個**。
#[tokio::test]
async fn ops_health_is_503_and_names_the_broken_backend() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_with_backends(
        jwt,
        vec![
            Arc::new(AlwaysUp("postgres")),
            Arc::new(AlwaysDown("redis")),
        ],
        Vec::new(),
    );

    let (status, body) = send(&app, get("/api/v1/ops/health", Some(&viewer))).await;
    // 503 而不是 200+healthy:false：監控預設看的是狀態碼。
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["healthy"], false);
    assert_eq!(body["unhealthy"], json!(["redis"]), "{body}");
    let redis = body["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "redis")
        .unwrap();
    assert_eq!(redis["healthy"], false);
    assert!(!redis["message"].as_str().unwrap().is_empty());
}

/// 沒接上的後端要與「接上了但壞掉」分開。全綠但其實只接了一個，是最危險的假綠燈。
#[tokio::test]
async fn ops_health_separates_not_configured_from_broken() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);

    let (status, body) = send(&app, get("/api/v1/ops/health", Some(&viewer))).await;
    assert_eq!(status, StatusCode::OK, "沒設定不等於壞掉：{body}");
    assert_eq!(body["healthy"], true);
    assert!(body["checks"].as_array().unwrap().is_empty());
    assert_eq!(
        body["not_configured"].as_array().unwrap().len(),
        6,
        "六個後端都沒接就要六個都列出來：{body}"
    );
}

#[tokio::test]
async fn ops_metrics_reports_real_process_usage() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);

    let (status, body) = send(&app, get("/api/v1/ops/metrics", Some(&viewer))).await;
    if cfg!(target_os = "linux") {
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["rss_bytes"].as_u64().unwrap() > 0,
            "RSS 不可能是 0——是 0 代表解析錯了而不是真的沒用記憶體：{body}"
        );
        assert!(body["threads"].as_u64().unwrap() >= 1);
        assert_eq!(body["clock_ticks_per_sec"], 100);
        assert!(body["cpu_user_seconds"].is_number());
    } else {
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{body}");
    }
}

/// ops 端點需要認證（後端拓樸與故障點本身就是情報）。
#[tokio::test]
async fn ops_endpoints_are_not_public() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    for path in ["/api/v1/ops/health", "/api/v1/ops/metrics"] {
        let (status, _) = send(&app, get(path, None)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} 不該公開");
    }
}
