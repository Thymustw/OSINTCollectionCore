//! 稽核覆蓋率：401、403、Job 的三個寫入動作都必須留痕。
//!
//! Phase 6a 之前只有 `POST /api/v1/import` 會寫稽核。這支測試的作用是
//! **讓「忘了寫稽核」變成編譯不過以外的失敗**——新增寫入端點時如果漏了 audit，
//! 這裡會抓到。

mod common;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{issue_test_jwt, test_app_parts};
use core_api::{AUDIT_AUTH_FAILED, AUDIT_AUTHZ_DENIED, AUDIT_JOB_CREATE};
use core_security::{AuditEntry, AuditLog, MemoryAuditLog, Role};
use tower::ServiceExt;

async fn status_of(app: &Router, request: Request<Body>) -> StatusCode {
    app.clone().oneshot(request).await.unwrap().status()
}

fn entries_with(audit: &MemoryAuditLog, action: &str) -> Vec<AuditEntry> {
    audit
        .entries()
        .into_iter()
        .filter(|e| e.action == action)
        .collect()
}

/// 401 要留痕，而且**不可以**把出示的憑證寫進稽核。
#[tokio::test]
async fn failed_authentication_is_audited_without_leaking_the_credential() {
    let (jwt, _) = issue_test_jwt(Role::Viewer);
    let (app, audit, _) = test_app_parts(jwt);

    // 三種失敗：沒有 header、不是 Bearer、JWT 壞掉。
    let secret_looking = "Bearer this-is-not-a-valid-jwt-but-looks-secret";
    for request in [
        Request::builder()
            .uri("/api/v1/jobs")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/api/v1/jobs")
            .header("Authorization", "Basic dXNlcjpwYXNz")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .uri("/api/v1/jobs")
            .header("Authorization", secret_looking)
            .body(Body::empty())
            .unwrap(),
    ] {
        assert_eq!(status_of(&app, request).await, StatusCode::UNAUTHORIZED);
    }

    let failures = entries_with(&audit, AUDIT_AUTH_FAILED);
    assert_eq!(failures.len(), 3, "每一次 401 都要寫一列：{failures:?}");
    for entry in &failures {
        assert_eq!(entry.actor, "anonymous");
        assert_eq!(entry.outcome, "denied");
        assert_eq!(entry.resource_type, "auth");
        assert_eq!(entry.metadata["status"], 401);
        assert_eq!(entry.metadata["path"], "/api/v1/jobs");
    }
    // 稽核表不該存任何可以拿去重放的東西。
    let dumped = format!("{failures:?}");
    assert!(
        !dumped.contains("this-is-not-a-valid-jwt"),
        "稽核紀錄不可以夾帶出示的憑證：{dumped}"
    );
    assert!(!dumped.contains("dXNlcjpwYXNz"), "同上：{dumped}");
}

/// 403 要留痕，並記下角色與所需權限。
#[tokio::test]
async fn rbac_denial_is_audited() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let (app, audit, _) = test_app_parts(jwt);

    let status = status_of(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/jobs")
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"type":"collect"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    let denials = entries_with(&audit, AUDIT_AUTHZ_DENIED);
    assert_eq!(denials.len(), 1, "{denials:?}");
    assert_eq!(denials[0].actor, "test-user");
    assert_eq!(denials[0].metadata["role"], "viewer");
    assert_eq!(denials[0].metadata["required_permission"], "write");
    assert_eq!(denials[0].metadata["path"], "/api/v1/jobs");
}

/// Job 建立失敗（這裡是沒接 Postgres 的 503）一樣要留痕。
///
/// 只記成功的話，「一直有人在戳一個接不上的後端」這件事在稽核裡看不到。
#[tokio::test]
async fn job_create_is_audited_even_when_backend_is_down() {
    let (jwt, token) = issue_test_jwt(Role::Operator);
    let (app, audit, _) = test_app_parts(jwt);

    let status = status_of(
        &app,
        Request::builder()
            .method("POST")
            .uri("/api/v1/jobs")
            .header("Authorization", format!("Bearer {token}"))
            .header("Content-Type", "application/json")
            .body(Body::from(r#"{"type":"collect"}"#))
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let created = entries_with(&audit, AUDIT_JOB_CREATE);
    assert_eq!(created.len(), 1, "{created:?}");
    assert_eq!(created[0].actor, "test-user");
    assert_eq!(created[0].outcome, "rejected");
    assert_eq!(created[0].resource_type, "job");
    assert_eq!(created[0].resource_id, None, "還沒建出 job 就沒有 id 可記");
    assert_eq!(created[0].metadata["status_code"], 503);
    assert_eq!(created[0].metadata["type"], "collect");
}

/// 讀取端點不寫稽核——否則每次輪詢 `GET /api/v1/jobs` 都長一列，
/// 稽核表會被正常流量灌爆，真正要看的寫入動作反而被埋掉。
#[tokio::test]
async fn read_endpoints_do_not_write_audit() {
    let (jwt, token) = issue_test_jwt(Role::Viewer);
    let (app, audit, _) = test_app_parts(jwt);

    let status = status_of(
        &app,
        Request::builder()
            .uri("/api/v1/whoami")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        audit.entries().is_empty(),
        "成功的唯讀請求不該寫稽核：{:?}",
        audit.entries()
    );
}

/// 記憶體稽核的查詢方法（`list` / `list_by_resource`）與 trait 契約一致。
///
/// 這是 `AuditLog` 新增查詢方法後、Postgres adapter 之外的第二個實作，
/// 兩邊語意必須一樣——只能寫不能讀的稽核等於沒有稽核。
#[tokio::test]
async fn memory_audit_query_methods_match_the_contract() {
    let log = MemoryAuditLog::new();
    for i in 0..3u32 {
        log.append(AuditEntry::new(
            "alice",
            "job.create",
            "job",
            Some(format!("j{i}")),
            "success",
        ))
        .await
        .unwrap();
    }
    log.append(AuditEntry::new(
        "bob",
        "token.revoke",
        "api_token",
        Some("t0".into()),
        "success",
    ))
    .await
    .unwrap();

    let all = log.list(None, 100).await.unwrap();
    assert_eq!(all.len(), 4);
    assert!(all.windows(2).all(|w| w[0].id > w[1].id), "由新到舊");

    let one = log.list_by_resource("job", "j1").await.unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].resource(), "job/j1");

    assert!(
        log.list_by_resource("api_token", "j1")
            .await
            .unwrap()
            .is_empty(),
        "resource_type 不同不可以命中"
    );
}
