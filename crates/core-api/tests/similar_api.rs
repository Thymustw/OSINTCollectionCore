//! `GET /api/v1/objects/{id}/similar` 不需要下游的那一半：認證、503。
//!
//! 404／409 需要 Postgres，在 `resources_api_e2e.rs`。
//! 真打 OpenSearch k-NN 的路徑在 `hybrid_search_api_e2e.rs`，標了 `#[ignore]`。

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{issue_test_jwt, test_app_parts};
use core_security::Role;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

async fn get_similar(app: &axum::Router, id: Uuid, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(format!("/api/v1/objects/{id}/similar"));
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    let request = builder.body(Body::empty()).unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let json = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, json)
}

#[tokio::test]
async fn similar_requires_authentication() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, body) = get_similar(&app, Uuid::now_v7(), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
}

#[tokio::test]
async fn viewer_gets_503_without_store_not_403() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, body) = get_similar(&app, Uuid::now_v7(), Some(&viewer)).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "沒接 canonical store 應 503 不是 403：{body}"
    );
    assert_eq!(body["error"], "unavailable");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("DATABASE_URL"),
        "先擋 store 時訊息要講 DATABASE_URL：{body}"
    );
}
