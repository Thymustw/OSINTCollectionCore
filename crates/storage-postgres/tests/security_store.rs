//! `PostgresAuditLog` / `PostgresApiTokenStore` 的落地行為（migration 0006）。
//!
//! 記憶體版的同名測試在 core-security 與 core-api 裡。這支驗的是**只有真的接上
//! Postgres 才看得到**的部分：欄位對應（`details` ↔ `metadata`）、
//! JSON 與時間的 round-trip、cursor 契約、以及重複撤銷不覆寫時間。

use chrono::{Duration, Utc};
use core_security::{
    ApiTokenStore, AuditEntry, AuditLog, Role, issue_api_token, parse_presented_token,
    verify_secret,
};
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use storage_postgres::{PostgresApiTokenStore, PostgresAuditLog, PostgresCanonicalStore};
use uuid::Uuid;

async fn connect() -> PostgresCanonicalStore {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "只連本機 Postgres，實際 DSN host 不像本機：{}",
        storage_core::StorageError::sanitize(&dsn)
    );
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    store
}

#[tokio::test]
async fn audit_round_trip_and_queries() {
    let store = connect().await;
    let log = PostgresAuditLog::new(&store);

    // resource_id 帶 run-specific UUID：這顆 Postgres 是共用的，
    // 寫死字串會讓兩次執行互相干擾（不會報錯，只會讓斷言偶發失敗）。
    let resource_id = Uuid::now_v7().to_string();
    let entry = AuditEntry::new(
        "conformance-actor",
        "job.transition",
        "job",
        Some(resource_id.clone()),
        "success",
    )
    .with_ip(Some("203.0.113.7".into()))
    .with_metadata(json!({"requested_status": "running", "nested": {"a": 1}}));
    let id = entry.id;
    log.append(entry.clone()).await.expect("append");

    // 依資源反查。
    let rows = log
        .list_by_resource("job", &resource_id)
        .await
        .expect("list_by_resource");
    assert_eq!(rows.len(), 1, "剛寫入的稽核查不到：{rows:?}");
    let got = &rows[0];
    assert_eq!(got.id, id);
    assert_eq!(got.actor, "conformance-actor");
    assert_eq!(got.action, "job.transition");
    assert_eq!(got.resource_type, "job");
    assert_eq!(got.resource_id.as_deref(), Some(resource_id.as_str()));
    assert_eq!(got.outcome, "success");
    assert_eq!(got.ip.as_deref(), Some("203.0.113.7"));
    // `details` 欄位 ↔ `metadata` 欄位的對應必須雙向成立，含巢狀結構。
    assert_eq!(got.metadata["requested_status"], "running");
    assert_eq!(got.metadata["nested"]["a"], 1);
    // 時間 round-trip 到毫秒。
    assert!(
        (got.timestamp - entry.timestamp).num_milliseconds().abs() <= 1,
        "timestamp round-trip 失真：{} vs {}",
        got.timestamp,
        entry.timestamp
    );

    // resource_type 必須參與過濾，不能只比 resource_id。
    assert!(
        log.list_by_resource("api_token", &resource_id)
            .await
            .expect("list_by_resource")
            .is_empty(),
        "resource_type 不同不可以命中"
    );

    // cursor 契約：`after = id + 1` 之下第一筆必定是自己（共用表也不受影響）。
    let page = log.list(Some(next_uuid(id)), 10).await.expect("list");
    assert_eq!(
        page.first().map(|e| e.id),
        Some(id),
        "cursor 分頁第一筆不對"
    );
    assert!(
        page.windows(2).all(|w| w[0].id > w[1].id),
        "list 必須依 id 嚴格遞減"
    );
    // 嚴格小於：用自己的 id 當 cursor 就不該再看到自己。
    assert!(
        log.list(Some(id), 10)
            .await
            .expect("list")
            .iter()
            .all(|e| e.id != id)
    );
    // limit 必須被夾住，0 不可以變成無界查詢。
    assert_eq!(log.list(None, 0).await.expect("list").len(), 1);
    assert!(log.list(None, 9_999).await.expect("list").len() <= 100);
}

/// `AuditEntry::new` 的 metadata 預設是 JSON null，欄位卻是 NOT NULL。
/// adapter 要把它折成 `{}`，而不是讓寫入失敗。
#[tokio::test]
async fn audit_null_metadata_becomes_empty_object() {
    let store = connect().await;
    let log = PostgresAuditLog::new(&store);
    let resource_id = Uuid::now_v7().to_string();
    log.append(AuditEntry::new(
        "conformance-actor",
        "auth.failed",
        "auth-probe",
        Some(resource_id.clone()),
        "denied",
    ))
    .await
    .expect("append");

    let rows = log
        .list_by_resource("auth-probe", &resource_id)
        .await
        .expect("list_by_resource");
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].metadata.is_object() && rows[0].metadata.as_object().unwrap().is_empty(),
        "預設 metadata 應落成 {{}}，實際 {:?}",
        rows[0].metadata
    );
}

#[tokio::test]
async fn token_store_round_trip() {
    let store = connect().await;
    let tokens = PostgresApiTokenStore::new(&store);

    let expires_at = Utc::now() + Duration::days(7);
    let issued = issue_api_token(
        format!("conformance-{}", Uuid::now_v7()),
        Role::Operator,
        Some("conformance-admin".into()),
        Some(expires_at),
    )
    .expect("issue");
    tokens.insert(&issued.record).await.expect("insert");

    let got = tokens
        .get(issued.record.id)
        .await
        .expect("get")
        .expect("剛寫入的 token 讀不到");
    assert_eq!(got.name, issued.record.name);
    assert_eq!(got.role, Role::Operator);
    assert_eq!(got.created_by.as_deref(), Some("conformance-admin"));
    assert_eq!(got.last_used_at, None);
    assert_eq!(got.revoked_at, None);
    assert!(
        (got.expires_at.unwrap() - expires_at)
            .num_milliseconds()
            .abs()
            <= 1,
        "expires_at round-trip 失真"
    );

    // 存的是 hash，而且真的驗得過。
    let presented = parse_presented_token(&issued.plaintext).expect("parse");
    assert!(verify_secret(&presented.secret, &got.secret_hash).expect("verify"));
    assert!(!got.secret_hash.contains(&presented.secret), "不可存明文");

    // 同一個 id 再 insert 一次必須失敗——upsert 會靜默換掉一把已發行的 token。
    assert!(
        tokens.insert(&issued.record).await.is_err(),
        "重複 insert 同一個 token id 應該失敗"
    );

    // touch_last_used。
    let used_at = Utc::now();
    tokens
        .touch_last_used(issued.record.id, used_at)
        .await
        .expect("touch");
    let got = tokens.get(issued.record.id).await.expect("get").unwrap();
    let last_used = got.last_used_at.expect("last_used_at 應該有值");
    assert!((last_used - used_at).num_milliseconds().abs() <= 1);
    assert!(got.ensure_usable(Utc::now()).is_ok(), "尚未撤銷也未過期");

    // 撤銷。
    let first_revoke = Utc::now();
    assert!(
        tokens
            .revoke(issued.record.id, first_revoke)
            .await
            .expect("revoke")
    );
    let got = tokens.get(issued.record.id).await.expect("get").unwrap();
    let recorded = got.revoked_at.expect("revoked_at 應該有值");
    assert!(got.ensure_usable(Utc::now()).is_err());

    // 重複撤銷：仍回 true（id 存在），但**不覆寫**第一次的時間。
    let second_revoke = first_revoke + Duration::hours(1);
    assert!(
        tokens
            .revoke(issued.record.id, second_revoke)
            .await
            .expect("revoke")
    );
    let got = tokens.get(issued.record.id).await.expect("get").unwrap();
    assert!(
        (got.revoked_at.unwrap() - recorded)
            .num_milliseconds()
            .abs()
            <= 1,
        "重複撤銷不可以改掉第一次撤銷的時間"
    );

    // 不存在的 id 回 false，不是 error。
    assert!(
        !tokens
            .revoke(Uuid::now_v7(), Utc::now())
            .await
            .expect("revoke")
    );
    assert!(tokens.get(Uuid::now_v7()).await.expect("get").is_none());

    // list 含已撤銷的，且依 id 遞減、有上限。
    let listed = tokens.list().await.expect("list");
    assert!(listed.len() <= 100, "list 必須有上限");
    assert!(
        listed.windows(2).all(|w| w[0].id > w[1].id),
        "list 依 id 遞減"
    );
}

/// 與 storage-core conformance 的 `next_uuid` 同一套語意（big-endian +1）。
fn next_uuid(id: Uuid) -> Uuid {
    let mut bytes = id.into_bytes();
    for byte in bytes.iter_mut().rev() {
        if *byte == 0xff {
            *byte = 0;
        } else {
            *byte += 1;
            break;
        }
    }
    Uuid::from_bytes(bytes)
}
