//! `POST /api/v1/search/hybrid` 不需要下游的那一半：認證、空 query、503。
//!
//! 真打 OpenSearch + ml-commons 的路徑在 `hybrid_search_api_e2e.rs`，標了 `#[ignore]`。

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{issue_test_jwt, test_app_parts};
use core_security::Role;
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

async fn post_hybrid(app: &axum::Router, body: Value, token: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/search/hybrid")
        .header("Content-Type", "application/json");
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    let request = builder.body(Body::from(body.to_string())).unwrap();
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
async fn hybrid_search_requires_authentication() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, body) = post_hybrid(&app, json!({"query": "ransomware"}), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"], "unauthorized");
}

#[tokio::test]
async fn viewer_gets_503_naming_both_missing_backends() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, body) = post_hybrid(&app, json!({"query": "ransomware"}), Some(&viewer)).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "viewer 是唯讀角色，沒接後端應 503 不是 403：{body}"
    );
    assert_eq!(body["error"], "unavailable");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.contains("兩個都沒接") || message.contains("search"),
        "503 訊息要講是哪一個沒接上：{body}"
    );
}

#[tokio::test]
async fn empty_query_is_400_even_without_backend() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    for query in ["", "   ", "\n\t"] {
        let (status, body) = post_hybrid(&app, json!({"query": query}), Some(&viewer)).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "空 query 應 400 不是 503：{body}"
        );
        assert_eq!(body["error"], "bad_request");
        assert!(
            body["message"]
                .as_str()
                .is_some_and(|m| m.contains("query 不可為空")),
            "訊息要講下一步：{body}"
        );
    }
}

#[tokio::test]
async fn unknown_field_is_rejected() {
    let (jwt, viewer) = issue_test_jwt(Role::Viewer);
    let (app, _, _) = test_app_parts(jwt);
    let (status, _) =
        post_hybrid(&app, json!({"query": "x", "langauge": "en"}), Some(&viewer)).await;
    assert_eq!(
        status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "未知欄位必須被拒絕，否則使用者以為自己指定了語言其實沒有"
    );
}
