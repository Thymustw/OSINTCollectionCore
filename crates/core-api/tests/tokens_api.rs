//! `/api/v1/tokens`：admin-only、明文只回一次、撤銷後立刻失效。
//!
//! 這支不接 Postgres（token store 是 `MemoryApiTokenStore`），驗的是 HTTP 層
//! 與 RBAC 行為。Postgres adapter 的落地行為由 storage-postgres 的 conformance 驗。

mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{issue_test_jwt, test_app_parts};
use core_api::{AUDIT_TOKEN_ISSUE, AUDIT_TOKEN_REVOKE};
use core_security::{ApiTokenStore, JwtService, MemoryApiTokenStore, Role};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt;

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

fn post_tokens(token: &str, body: Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/api/v1/tokens")
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn get(uri: &str, token: &str) -> Request<Body> {
    Request::builder()
        .uri(uri)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn admin_app() -> (
    Router,
    String,
    core_security::MemoryAuditLog,
    Arc<MemoryApiTokenStore>,
) {
    let (jwt, admin) = issue_test_jwt(Role::Admin);
    let (app, audit, tokens) = test_app_parts(jwt);
    (app, admin, audit, tokens)
}

/// 完整生命週期：發行 → 用它呼叫 API → 撤銷 → 再呼叫得到 401。
#[tokio::test]
async fn issue_use_revoke_lifecycle() {
    let (app, admin, audit, store) = admin_app();

    let (status, body) = send(
        &app,
        post_tokens(&admin, json!({"name": "ci-indexer", "role": "operator"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    let plaintext = body["token"]
        .as_str()
        .expect("明文只在這裡出現")
        .to_string();
    let id = body["id"].as_str().unwrap().to_string();
    assert!(plaintext.starts_with("osint_"), "{plaintext}");
    assert_eq!(body["role"], "operator");
    assert_eq!(
        body["expires_at"],
        Value::Null,
        "沒填 expires_in_days 就是不過期"
    );

    // 用這把 token 呼叫 API 要成功，而且身分是 token 的角色而不是發行者的。
    let (status, body) = send(&app, get("/api/v1/whoami", &plaintext)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["role"], "operator");
    assert_eq!(body["subject"], "token:ci-indexer");
    assert_eq!(body["auth_method"], "ApiToken");

    // verify 時要更新 last_used_at。
    let record = store
        .get(id.parse().unwrap())
        .await
        .unwrap()
        .expect("token 應該在 store 裡");
    assert!(
        record.last_used_at.is_some(),
        "verify 成功後必須更新 last_used_at，否則沒人看得出哪把 token 還在用"
    );
    // store 裡存的必須是 hash，不是明文。
    assert!(
        record.secret_hash.starts_with("$argon2"),
        "{}",
        record.secret_hash
    );
    assert!(
        !record
            .secret_hash
            .contains(plaintext.split('.').nth(1).unwrap())
    );

    // GET 不含明文也不含 hash。
    let (status, body) = send(&app, get("/api/v1/tokens", &admin)).await;
    assert_eq!(status, StatusCode::OK);
    let listed = &body["items"][0];
    assert_eq!(listed["id"], id.as_str());
    assert_eq!(listed["active"], true);
    assert!(listed.get("token").is_none(), "list 不可回明文：{listed}");
    assert!(
        listed.get("secret_hash").is_none(),
        "list 不可回 hash：{listed}"
    );
    assert_eq!(listed["created_by"], "test-user");

    // 撤銷。
    let (status, _) = send(
        &app,
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/tokens/{id}"))
            .header("Authorization", format!("Bearer {admin}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // 撤銷後同一把 token 立刻不能用。
    let (status, body) = send(&app, get("/api/v1/whoami", &plaintext)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_eq!(body["error"], "unauthorized");

    // 稽核：發行與撤銷各一列，resource 指到同一個 token id。
    let actions: Vec<(String, String, Option<String>)> = audit
        .entries()
        .iter()
        .map(|e| (e.action.clone(), e.outcome.clone(), e.resource_id.clone()))
        .collect();
    assert!(
        actions.contains(&(
            AUDIT_TOKEN_ISSUE.to_string(),
            "success".to_string(),
            Some(id.clone())
        )),
        "發行要留稽核：{actions:?}"
    );
    assert!(
        actions.contains(&(
            AUDIT_TOKEN_REVOKE.to_string(),
            "success".to_string(),
            Some(id.clone())
        )),
        "撤銷要留稽核：{actions:?}"
    );
    // 稽核本身不可以夾帶明文。
    assert!(
        !format!("{:?}", audit.entries()).contains(&plaintext),
        "稽核紀錄裡不可以出現 token 明文"
    );
}

/// viewer 與 operator 都不能碰 token 端點——admin 那一欄第一次真的有守護對象。
#[tokio::test]
async fn only_admin_may_manage_tokens() {
    for role in [Role::Viewer, Role::Operator] {
        let (jwt, token) = issue_test_jwt(role);
        let (app, audit, _) = test_app_parts(jwt);

        let (status, body) = send(
            &app,
            post_tokens(&token, json!({"name": "x", "role": "viewer"})),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{role:?} 不該發得出 token：{body}"
        );

        let (status, _) = send(&app, get("/api/v1/tokens", &token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{role:?} 不該列得出 token");

        let (status, _) = send(
            &app,
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/tokens/{}", uuid::Uuid::now_v7()))
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{role:?} 不該撤得掉 token");

        // 三次 403 都要留痕。
        let denied = audit
            .entries()
            .iter()
            .filter(|e| e.action == core_api::AUDIT_AUTHZ_DENIED)
            .count();
        assert_eq!(denied, 3, "每一次 403 都要寫稽核（{role:?}）");
    }
}

/// 過期的 token 不能用，而且錯誤訊息要與「已撤銷」分得開。
#[tokio::test]
async fn expired_token_is_rejected() {
    let (jwt, _admin) = issue_test_jwt(Role::Admin);
    let (app, _audit, store) = test_app_parts(jwt);

    // 直接寫一筆已過期的進 store（走 API 的話最短也要等一天）。
    let issued = core_security::issue_api_token(
        "already-expired",
        Role::Operator,
        Some("test-user".into()),
        Some(chrono::Utc::now() - chrono::Duration::seconds(1)),
    )
    .unwrap();
    store.insert(&issued.record).await.unwrap();

    let (status, body) = send(&app, get("/api/v1/whoami", &issued.plaintext)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        body["message"].as_str().unwrap().contains("過期"),
        "訊息要講明是過期而不是撤銷，使用者的下一步不同：{body}"
    );
}

/// 不存在的 token id 要回 401，**不是** 404——404 會變成 token id 的枚舉 oracle。
#[tokio::test]
async fn unknown_token_id_is_401_not_404() {
    let (jwt, _) = issue_test_jwt(Role::Admin);
    let (app, _audit, _) = test_app_parts(jwt);
    let fake = format!("osint_{}.deadbeef", uuid::Uuid::now_v7());
    let (status, _) = send(&app, get("/api/v1/whoami", &fake)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

/// 輸入驗證：名稱與到期天數的邊界。
#[tokio::test]
async fn issue_input_is_validated() {
    let (app, admin, _audit, _) = admin_app();

    for (body, why) in [
        (json!({"name": "", "role": "viewer"}), "空名稱"),
        (
            json!({"name": "x".repeat(65), "role": "viewer"}),
            "名稱過長",
        ),
        (
            json!({"name": "ok", "role": "viewer", "expires_in_days": 0}),
            "0 天",
        ),
        (
            json!({"name": "ok", "role": "viewer", "expires_in_days": 3650}),
            "超過上限",
        ),
    ] {
        let (status, got) = send(&app, post_tokens(&admin, body)).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{why} 應被拒：{got}");
    }

    // 合法的到期天數要真的算出 expires_at。
    let (status, body) = send(
        &app,
        post_tokens(
            &admin,
            json!({"name": "ok", "role": "viewer", "expires_in_days": 30}),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");
    assert!(body["expires_at"].is_string(), "{body}");
}

/// 撤銷不存在的 id 回 404（這條路徑已經通過 admin 認證，不是枚舉 oracle）。
#[tokio::test]
async fn revoke_unknown_id_is_404() {
    let (app, admin, _audit, _) = admin_app();
    let (status, _) = send(
        &app,
        Request::builder()
            .method("DELETE")
            .uri(format!("/api/v1/tokens/{}", uuid::Uuid::now_v7()))
            .header("Authorization", format!("Bearer {admin}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// 沒有認證就打 token 端點要回 401（不是 403）。
#[tokio::test]
async fn tokens_without_auth_is_401() {
    let (app, _, _, _) = admin_app();
    let (status, body) = send(
        &app,
        Request::builder()
            .uri("/api/v1/tokens")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
}

/// 這支測試存在的理由：確認 `JwtService` 沒有被 token 路徑繞過。
#[tokio::test]
async fn jwt_from_another_secret_is_rejected() {
    let (app, _, _, _) = admin_app();
    let other = JwtService::new(&[b'x'; 32], "osint-core", chrono::Duration::hours(1)).unwrap();
    let forged = other.issue("attacker", Role::Admin).unwrap();
    let (status, _) = send(&app, get("/api/v1/tokens", &forged)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
