//! API skeleton：health 不需認證；/api 需要 JWT。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use core_api::{issue_test_jwt, test_app};
use core_security::Role;
use http_body_util::BodyExt;
use tower::ServiceExt;

#[tokio::test]
async fn health_is_public() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn metrics_is_public() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let text = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(text.contains("osint_collected_total"), "{text}");
}

#[tokio::test]
async fn jobs_without_auth_is_401() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/jobs")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["error"], "unauthorized");
}

#[tokio::test]
async fn whoami_with_jwt() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/whoami")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["role"], "operator");
    assert_eq!(json["subject"], "test-user");
}

#[tokio::test]
async fn viewer_cannot_create_job() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/jobs")
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"type":"collect"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    // 沒有 Postgres 時會先過 RBAC 再 503；viewer 應在 RBAC 被擋。
    assert_eq!(response.status(), StatusCode::FORBIDDEN,);
}

#[tokio::test]
async fn operator_create_without_store_is_503() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/jobs")
                .header("Authorization", format!("Bearer {token}"))
                .header("Content-Type", "application/json")
                .body(Body::from(r#"{"type":"collect"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}
