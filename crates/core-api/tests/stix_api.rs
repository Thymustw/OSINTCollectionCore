//! `POST /api/v1/import/stix`／`POST /api/v1/export/stix`／`GET /jobs/{id}/result`
//! 不需要資料庫的那一半：認證、RBAC、bundle 驗證、body 大小上限。
//!
//! 真正落地 RawEvidence、寫 Job.parameters 在 `stix_api_e2e.rs`。

use axum::body::Body;
use axum::http::{Request, StatusCode};
mod common;

use common::{issue_test_jwt, test_app};
use core_security::Role;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;
use uuid::Uuid;

const UUID: &str = "2152fbe0-4471-4d43-8b64-0b907d186c23";

fn ok_bundle() -> Value {
    json!({
        "type": "bundle",
        "id": format!("bundle--{UUID}"),
        "objects": [
            {
                "type": "identity",
                "id": format!("identity--{UUID}"),
                "name": "Alice",
                "identity_class": "individual"
            }
        ]
    })
}

fn post_json(uri: &str, token: Option<&str>, body: &Value) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri(uri)
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

fn get(uri: &str, token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder.body(Body::empty()).unwrap()
}

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn import_stix_without_token_is_401() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/import/stix",
            None,
            &json!({"source_id": Uuid::now_v7(), "bundle": ok_bundle()}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body_json(response).await["error"], "unauthorized");
}

#[tokio::test]
async fn viewer_cannot_import_stix() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/import/stix",
            Some(&token),
            &json!({"source_id": Uuid::now_v7(), "bundle": ok_bundle()}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn operator_import_without_backend_gets_503_after_validation() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/import/stix",
            Some(&token),
            &json!({"source_id": Uuid::now_v7(), "bundle": ok_bundle()}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(response).await;
    assert_eq!(json["error"], "unavailable");
}

#[tokio::test]
async fn invalid_bundle_is_400_before_backend() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/import/stix",
            Some(&token),
            &json!({"source_id": Uuid::now_v7(), "bundle": {"type": "identity"}}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["error"], "bad_request");
    let message = json["message"].as_str().unwrap();
    assert!(message.contains("bundle"), "{message}");
}

#[tokio::test]
async fn malformed_json_body_is_400() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/import/stix")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from("{ this is not json"))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert_eq!(json["error"], "bad_request");
}

/// 惡意／格式錯誤的 STIX object id 在 HTTP 邊界一律 400，不是 500、也不是被悄悄接受。
///
/// `validate_bundle` → `StixId::parse` 會在 worker 跑之前就攔下這些形狀
/// （SQL injection 分號、路徑穿越、XSS、UUID 後面塞東西、內嵌 NUL）。
/// 這支 table-driven 測試證明這層防護在 HTTP 邊界真的生效，
/// 不只依賴「底層擋了所以應該沒事」。
#[tokio::test]
async fn malicious_object_ids_are_rejected_as_400_not_500() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let bad_ids = [
        "identity--'; DROP TABLE entities; --",
        "identity--../../../etc/passwd",
        "<script>alert(1)</script>--2152fbe0-4471-4d43-8b64-0b907d186c23",
        "identity--2152fbe0-4471-4d43-8b64-0b907d186c23; DROP TABLE entities;",
        "identity\u{0}--2152fbe0-4471-4d43-8b64-0b907d186c23",
    ];
    for bad_id in bad_ids {
        let body = json!({
            "source_id": Uuid::now_v7(),
            "bundle": {
                "type": "bundle",
                "id": format!("bundle--{UUID}"),
                "objects": [
                    { "type": "identity", "id": bad_id, "name": "x", "identity_class": "individual" }
                ]
            }
        });
        let response = app
            .clone()
            .oneshot(post_json("/api/v1/import/stix", Some(&token), &body))
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "id `{bad_id}` 應該是 400，不是 500 或被接受"
        );
        let json = body_json(response).await;
        assert_eq!(json["error"], "bad_request", "id `{bad_id}`: {json}");
    }
}

#[tokio::test]
async fn oversized_stix_body_is_413() {
    // test_app 的 max_bundle_bytes 是 4096。
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let mut body = json!({
        "source_id": Uuid::now_v7(),
        "bundle": ok_bundle(),
    });
    body["bundle"]["padding"] = json!("x".repeat(5_000));
    let raw = body.to_string();
    assert!(raw.len() > 4_096, "測試資料必須超過上限");
    let response = app
        .oneshot(post_json("/api/v1/import/stix", Some(&token), &body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let json = body_json(response).await;
    assert_eq!(json["error"], "payload_too_large");
    let message = json["message"].as_str().unwrap();
    assert!(
        message.contains("max_bundle_bytes") || message.contains("4096"),
        "要講清楚是哪個上限：{json}"
    );
}

#[tokio::test]
async fn too_many_objects_is_413() {
    // test_app 的 max_objects 是 2。三個合法物件仍遠低於 4096 bytes，
    // 所以會打到 validate_bundle 的 TooManyObjects，而不是 body 大小上限。
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let objects: Vec<Value> = (0..3)
        .map(|i| {
            json!({
                "type": "identity",
                "id": format!("identity--{UUID}"),
                "name": format!("n{i}"),
                "identity_class": "individual"
            })
        })
        .collect();
    let body = json!({
        "source_id": Uuid::now_v7(),
        "bundle": {
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": objects,
        }
    });
    let response = app
        .oneshot(post_json("/api/v1/import/stix", Some(&token), &body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let json = body_json(response).await;
    assert_eq!(json["error"], "payload_too_large");
    let message = json["message"].as_str().unwrap();
    assert!(
        message.contains("max_objects") || message.contains("上限"),
        "要講清楚是物件數量上限：{json}"
    );
}

#[tokio::test]
async fn export_stix_without_token_is_401() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/export/stix",
            None,
            &json!({"filter": {}}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn viewer_cannot_export_stix() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/export/stix",
            Some(&token),
            &json!({"filter": {}}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn operator_export_without_jobs_is_503() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let response = app
        .oneshot(post_json(
            "/api/v1/export/stix",
            Some(&token),
            &json!({"filter": {}}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn job_result_without_token_is_401() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let id = Uuid::now_v7();
    let response = app
        .oneshot(get(&format!("/api/v1/jobs/{id}/result"), None))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn viewer_may_read_job_result_and_gets_503_without_jobs() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let id = Uuid::now_v7();
    let response = app
        .oneshot(get(&format!("/api/v1/jobs/{id}/result"), Some(&token)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(response).await;
    assert_eq!(json["error"], "unavailable");
    assert!(
        json["message"].as_str().unwrap().contains("DATABASE_URL"),
        "{}",
        json
    );
}
