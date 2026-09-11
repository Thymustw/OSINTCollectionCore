//! `POST /api/v1/import` 的 HTTP 層行為：認證、RBAC、大小上限、稽核、輸入驗證。
//!
//! 這支不接 Postgres／MinIO（`test_app` 的 import state 是 None），所以只驗到
//! 「請求有沒有被正確擋下／收下」。真正落地與正規化在 `import_e2e.rs`。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use core_api::{IMPORT_AUDIT_ACTION, issue_test_jwt, test_app, test_app_parts};
use core_security::Role;
use http_body_util::BodyExt;
use tower::ServiceExt;

const BOUNDARY: &str = "X-OSINT-TEST-BOUNDARY";

/// 組一份 multipart body。
fn multipart_body(request_json: &str, filename: &str, file_bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"request\"\r\n");
    body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
    body.extend_from_slice(request_json.as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(b"Content-Type: application/octet-stream\r\n\r\n");
    body.extend_from_slice(file_bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

fn import_request(token: Option<&str>, body: Vec<u8>) -> Request<Body> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/import")
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        );
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    builder.body(Body::from(body)).unwrap()
}

fn json_request_field() -> String {
    format!(
        r#"{{"source_id":"{}","kind":"json"}}"#,
        uuid::Uuid::from_u128(1)
    )
}

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
}

#[tokio::test]
async fn import_without_token_is_401() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let body = multipart_body(&json_request_field(), "a.json", br#"[{"title":"a"}]"#);
    let response = app.oneshot(import_request(None, body)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body_json(response).await["error"], "unauthorized");
}

#[tokio::test]
async fn viewer_cannot_import() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let app = test_app(jwt);
    let body = multipart_body(&json_request_field(), "a.json", br#"[{"title":"a"}]"#);
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::FORBIDDEN,
        "viewer 必須在 RBAC 就被擋下，不能碰到 body"
    );
}

#[tokio::test]
async fn operator_without_backend_gets_503_not_500() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let body = multipart_body(&json_request_field(), "a.json", br#"[{"title":"a"}]"#);
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let json = body_json(response).await;
    assert_eq!(json["error"], "unavailable");
    assert!(
        json["message"].as_str().unwrap().contains("DATABASE_URL"),
        "訊息要指出下一步：{json}"
    );
}

#[tokio::test]
async fn oversized_upload_is_413() {
    // test_app 的 max_upload_bytes 是 4096。
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let big = vec![b'x'; 16 * 1024];
    let body = multipart_body(&json_request_field(), "big.bin", &big);
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let json = body_json(response).await;
    assert_eq!(json["error"], "payload_too_large");
    assert!(
        json["message"].as_str().unwrap().contains("4096"),
        "要講清楚是哪個上限：{json}"
    );
}

#[tokio::test]
async fn oversized_upload_is_rejected_before_the_whole_file_is_buffered() {
    // 送一份遠大於上限的 body，但 Content-Length 正確。串流檢查必須在累積到上限的
    // 當下就中止；若是「整份吃完再判斷」，下面這筆 64 MiB 會先被完整讀進記憶體。
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let huge = vec![b'x'; 64 * 1024 * 1024];
    let body = multipart_body(&json_request_field(), "huge.bin", &huge);
    let started = std::time::Instant::now();
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    let status = response.status();
    let json = body_json(response).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "實際回應：{json}");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(5),
        "應在讀到上限時立刻中止"
    );
}

#[tokio::test]
async fn unknown_multipart_field_is_rejected() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"payload\"\r\n\r\n");
    body.extend_from_slice(b"oops\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert!(
        json["message"].as_str().unwrap().contains("payload"),
        "{json}"
    );
}

#[tokio::test]
async fn missing_file_field_is_400() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"request\"\r\n\r\n");
    body.extend_from_slice(json_request_field().as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let json = body_json(response).await;
    assert!(json["message"].as_str().unwrap().contains("file"), "{json}");
}

#[tokio::test]
async fn typo_in_mapping_key_fails_fast() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let app = test_app(jwt);
    let request = format!(
        r#"{{"source_id":"{}","kind":"csv","mapping":{{"titel":"headline"}}}}"#,
        uuid::Uuid::from_u128(1)
    );
    let body = multipart_body(&request, "a.csv", b"headline\nx\n");
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "mapping 打錯字要當場失敗，不能靜默忽略"
    );
}

#[tokio::test]
async fn audit_entry_is_written_even_when_rejected() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let (app, audit) = test_app_parts(jwt);
    let body = multipart_body(&json_request_field(), "a.json", br#"[{"title":"a"}]"#);
    let response = app
        .oneshot(import_request(Some(&token), body))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let entries = audit.entries();
    assert_eq!(entries.len(), 1, "每次上傳都要留一筆稽核：{entries:?}");
    assert_eq!(entries[0].action, IMPORT_AUDIT_ACTION);
    assert_eq!(entries[0].actor, "test-user");
    assert_eq!(entries[0].outcome, "rejected");
    assert_eq!(entries[0].metadata["status"], 503);
}
