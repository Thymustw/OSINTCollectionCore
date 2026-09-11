//! SSRF Guard + GuardedFetcher 對本機假 HTTP server 的安全測試。
//! 不連真實外網，不打本機 8080／9200／9000。

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::routing::get;
use chrono::{Duration as ChronoDuration, Utc};
use connector_sdk::{
    DomainRateLimiter, GuardedFetcher, HostResolver, MapResolver, SourcePolicy, SsrfGuard,
};
use core_model::NetworkRule;
use core_security::MemoryAuditLog;
use tokio::net::TcpListener;
use uuid::Uuid;

const RSS: &str =
    r#"<?xml version="1.0"?><rss version="2.0"><channel><title>t</title></channel></rss>"#;

async fn serve(app: Router) -> (String, u16) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let addr = listener.local_addr().expect("local addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    (format!("http://127.0.0.1:{}", addr.port()), addr.port())
}

fn allow_loopback_rule(source_id: uuid::Uuid) -> NetworkRule {
    let now = Utc::now();
    NetworkRule {
        id: Uuid::now_v7(),
        source_id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "本機假 HTTP server 測試".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    }
}

fn fetcher(
    rules: Vec<NetworkRule>,
    resolver: Arc<dyn HostResolver>,
    audit: Arc<MemoryAuditLog>,
) -> GuardedFetcher {
    let source_id = rules
        .first()
        .map(|r| r.source_id)
        .unwrap_or_else(Uuid::now_v7);
    let guard = SsrfGuard::new(source_id, SourcePolicy::default(), rules, resolver, audit);
    GuardedFetcher::new(
        guard,
        DomainRateLimiter::new(SourcePolicy::default().rate_limit),
    )
}

fn system_or_map(host: &str, ip: IpAddr) -> Arc<MapResolver> {
    let mut map = HashMap::new();
    map.insert(host.to_string(), vec![ip]);
    Arc::new(MapResolver::new(map))
}

#[tokio::test]
async fn default_deny_loopback_without_rule() {
    let app = Router::new().route("/rss", get(|| async { RSS }));
    let (base, _) = serve(app).await;
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(vec![], Arc::new(MapResolver::default()), audit);
    let err = fetcher.get(&format!("{base}/rss"), &[]).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("NetworkRule") || msg.contains("loopback") || msg.contains("私網"),
        "{msg}"
    );
}

#[tokio::test]
async fn allow_loopback_with_matching_rule_and_audit() {
    let app = Router::new().route("/rss", get(|| async { RSS }));
    let (base, _) = serve(app).await;
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(
        vec![allow_loopback_rule(source_id)],
        Arc::new(MapResolver::default()),
        audit.clone(),
    );
    let got = fetcher
        .get(&format!("{base}/rss"), &[])
        .await
        .expect("allowlisted GET");
    assert_eq!(got.status, 200);
    assert_eq!(got.body, RSS.as_bytes());
    let entries = audit.entries();
    assert!(
        entries
            .iter()
            .any(|e| e.action == "connector.ssrf.allowlist"),
        "白名單放行必須寫稽核，實際 {:?}",
        entries
            .iter()
            .map(|e| e.action.as_str())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn expired_rule_treated_as_absent() {
    let app = Router::new().route("/rss", get(|| async { RSS }));
    let (base, _) = serve(app).await;
    let now = Utc::now();
    let source_id = Uuid::now_v7();
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "已過期".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: Some(now - ChronoDuration::hours(1)),
        created_at: now,
        updated_at: now,
    };
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(vec![rule], Arc::new(MapResolver::default()), audit);
    let err = fetcher.get(&format!("{base}/rss"), &[]).await.unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::SoftDenied { .. }),
        "{err}"
    );
}

#[tokio::test]
async fn metadata_hard_denied_even_with_rule() {
    let now = Utc::now();
    let source_id = Uuid::now_v7();
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id,
        cidr_or_host: "169.254.169.254".into(),
        ports: None,
        reason: "試圖放行 IMDS".into(),
        approved_by: "admin@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(vec![rule], Arc::new(MapResolver::default()), audit);
    let err = fetcher
        .get("http://169.254.169.254/latest/meta-data", &[])
        .await
        .unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::HardDenied { .. }),
        "{err}"
    );
}

#[tokio::test]
async fn google_metadata_hostname_hard_denied() {
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(vec![], Arc::new(MapResolver::default()), audit);
    let err = fetcher
        .get("http://metadata.google.internal/computeMetadata/v1/", &[])
        .await
        .unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::HardDenied { .. }),
        "{err}"
    );
}

#[tokio::test]
async fn redirect_to_metadata_denied() {
    let app = Router::new().route(
        "/bounce",
        get(|| async {
            Response::builder()
                .status(StatusCode::FOUND)
                .header("Location", "http://169.254.169.254/latest/meta-data")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let (base, _) = serve(app).await;
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(
        vec![allow_loopback_rule(source_id)],
        Arc::new(MapResolver::default()),
        audit,
    );
    let err = fetcher
        .get(&format!("{base}/bounce"), &[])
        .await
        .unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::HardDenied { .. }),
        "redirect 到 IMDS 應硬拒絕，實際 {err}"
    );
}

#[tokio::test]
async fn redirect_to_rfc1918_denied_without_rule_for_target() {
    let app = Router::new().route(
        "/bounce",
        get(|| async {
            Response::builder()
                .status(StatusCode::FOUND)
                .header("Location", "http://10.1.2.3/internal")
                .body(Body::empty())
                .unwrap()
        }),
    );
    let (base, _) = serve(app).await;
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(
        vec![allow_loopback_rule(source_id)],
        Arc::new(MapResolver::default()),
        audit,
    );
    let err = fetcher
        .get(&format!("{base}/bounce"), &[])
        .await
        .unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::SoftDenied { .. }),
        "redirect 到 RFC1918 應軟拒絕，實際 {err}"
    );
}

#[tokio::test]
async fn dns_rebinding_resolves_once_and_pins_ip() {
    let app = Router::new().route("/rss", get(|| async { RSS }));
    let (_base, port) = serve(app).await;
    let resolver = system_or_map("feed.test", "127.0.0.1".parse().unwrap());
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(
        vec![allow_loopback_rule(source_id)],
        resolver.clone(),
        audit,
    );
    let url = format!("http://feed.test:{port}/rss");
    let got = fetcher.get(&url, &[]).await.expect("pinned GET");
    assert_eq!(got.body, RSS.as_bytes());
    assert_eq!(
        resolver.resolve_count("feed.test"),
        1,
        "DNS rebinding 防護必須只 resolve 一次，不可在 connect 時再查"
    );
}

#[tokio::test]
async fn oversized_content_length_rejected() {
    // 用「Content-Length 造假但 body 對不上」測 hyper 伺服器端自己就會擋下來
    // （不合法的 HTTP response，連不上，不是我們的程式碼在判斷）。改成
    // 送一個真的超過 max_response_bytes（16）的 body、header 誠實對應大小，
    // 這樣測到的才是 connector-sdk 自己的 guard 邏輯，不是 hyper 的協定檢查。
    let app = Router::new().route(
        "/big",
        get(|| async {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(vec![b'x'; 64]))
                .unwrap()
        }),
    );
    let (base, _) = serve(app).await;
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let policy = SourcePolicy {
        max_response_bytes: 16,
        ..SourcePolicy::default()
    };
    let guard = SsrfGuard::new(
        source_id,
        policy.clone(),
        vec![allow_loopback_rule(source_id)],
        Arc::new(MapResolver::default()),
        audit,
    );
    let fetcher = GuardedFetcher::new(guard, DomainRateLimiter::new(policy.rate_limit));
    let err = fetcher.get(&format!("{base}/big"), &[]).await.unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::ResponseTooLarge { .. }),
        "{err}"
    );
}

#[tokio::test]
async fn not_modified_returns_empty_body() {
    let app = Router::new().route(
        "/rss",
        get(|headers: HeaderMap| async move {
            if headers.get("if-none-match").and_then(|v| v.to_str().ok()) == Some("\"abc\"") {
                StatusCode::NOT_MODIFIED
            } else {
                StatusCode::OK
            }
        }),
    );
    let (base, _) = serve(app).await;
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let fetcher = fetcher(
        vec![allow_loopback_rule(source_id)],
        Arc::new(MapResolver::default()),
        audit,
    );
    let got = fetcher
        .get(&format!("{base}/rss"), &[("If-None-Match", "\"abc\"")])
        .await
        .expect("304");
    assert!(got.is_not_modified());
    assert!(got.body.is_empty());
}

#[tokio::test]
async fn timeout_is_surfaced() {
    let app = Router::new().route(
        "/slow",
        get(|| async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            RSS
        }),
    );
    let (base, _) = serve(app).await;
    let source_id = Uuid::now_v7();
    let audit = Arc::new(MemoryAuditLog::new());
    let policy = SourcePolicy {
        request_timeout: Duration::from_millis(50),
        connect_timeout: Duration::from_millis(50),
        ..SourcePolicy::default()
    };
    let guard = SsrfGuard::new(
        source_id,
        policy.clone(),
        vec![allow_loopback_rule(source_id)],
        Arc::new(MapResolver::default()),
        audit,
    );
    let fetcher = GuardedFetcher::new(guard, DomainRateLimiter::new(policy.rate_limit));
    let err = fetcher.get(&format!("{base}/slow"), &[]).await.unwrap_err();
    assert!(
        matches!(err, connector_sdk::ConnectorError::Timeout { .. }),
        "{err}"
    );
}
