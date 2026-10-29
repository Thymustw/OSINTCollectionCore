//! 守門：共用開發 Postgres 不可留下「啟用中、有排程、名稱像測試」的舊 connector。
//!
//! collector 的 `tick()` 只掃 `enabled = TRUE`，再依 schedule 判斷到期。測試若
//! panic 跳過收尾、或根本沒停用，下一次開 collector 就會對殘留排程收集。
//!
//! 與其他測試平行時，可能剛好看到進行中（尚未 drop guard）的列。只把
//! **UUID v7 時間戳早於 5 分鐘**的列算違規——進行中的 e2e 通常幾秒到一兩分鐘。
//! 代價：剛留下的殘留要等下一輪（或 5 分鐘後）才會被這支抓到。

use std::panic::{AssertUnwindSafe, catch_unwind};

use chrono::{DateTime, Duration, Utc};
use core_model::{Connector, Source, SourceType};
use serde_json::json;
use sqlx::Row;
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use storage_postgres::{DisableConnectorsOnDrop, PostgresCanonicalStore};
use uuid::Uuid;

/// 進行中的測試可能還沒跑完；比這個更老才算沒收尾。
const STALE_AFTER: Duration = Duration::minutes(5);

/// 測試實際使用的 connector 名稱前綴／樣式。新增 fixture 命名時兩邊一起改。
/// sqlx 0.9 的 `query` 只收 `'static` SQL，所以整段寫成常數，不用 `format!`。
const LIST_STALE_SQL: &str = r#"
        SELECT id, name, schedule
        FROM connectors
        WHERE enabled = TRUE
          AND schedule IS NOT NULL
          AND btrim(schedule) <> ''
          AND (
            name LIKE 'e2e-%'
            OR name LIKE '%-e2e-%'
            OR name LIKE 'e2e %'
            OR name LIKE 'conformance-%'
            OR name LIKE 'acceptance-%'
            OR name LIKE 'reclaim-%'
            OR name LIKE 'run-state-%'
            OR name LIKE 'shutdown-%'
            OR name LIKE 'stage3-%'
            OR name LIKE 'indexer-%'
            OR name LIKE 'search-e2e-%'
            OR name LIKE 'stix-e2e-%'
            OR name LIKE 'dedup-%'
            OR name LIKE 'entity-e2e-%'
            OR name LIKE 'src-by-entity-%'
            OR name LIKE 'ev-col-conn-%'
            OR name LIKE 'link-batch-conn-%'
          )
"#;

#[derive(Debug)]
struct StaleConnector {
    id: Uuid,
    name: String,
    schedule: String,
}

async fn connect() -> (String, PostgresCanonicalStore) {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "守門測試只連本機 Postgres"
    );
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    (dsn, store)
}

fn uuid_v7_created_at(id: Uuid) -> Option<DateTime<Utc>> {
    let ts = id.get_timestamp()?;
    let (secs, nanos) = ts.to_unix();
    DateTime::<Utc>::from_timestamp(secs as i64, nanos)
}

fn is_stale(id: Uuid, now: DateTime<Utc>) -> bool {
    match uuid_v7_created_at(id) {
        Some(created) => now - created >= STALE_AFTER,
        // 匯入路徑用 UUID v5，沒有時間戳。無法證明是進行中的測試，當成舊列。
        None => true,
    }
}

async fn list_stale_enabled_scheduled_test_connectors(
    store: &PostgresCanonicalStore,
) -> Vec<StaleConnector> {
    let rows = sqlx::query(LIST_STALE_SQL)
        .fetch_all(store.pool())
        .await
        .expect("查 enabled+有排程的測試 connector");
    let now = Utc::now();
    rows.into_iter()
        .filter_map(|row| {
            let id: Uuid = row.get("id");
            if !is_stale(id, now) {
                return None;
            }
            Some(StaleConnector {
                id,
                name: row.get("name"),
                schedule: row.get("schedule"),
            })
        })
        .collect()
}

fn gatekeeper_failure_message(stale: &[StaleConnector]) -> String {
    let listed = stale
        .iter()
        .map(|row| {
            format!(
                "  - {} (id={}，schedule={:?})",
                row.name, row.id, row.schedule
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "開發 Postgres 留下啟用中且有排程的測試 connector（UUID v7 時間戳已超過 {} 分鐘，\
         不是進行中的測試）：\n{listed}\n\
         這代表建立它們的測試沒有收尾（斷言 panic 跳過函式結尾的停用，或根本沒停用）。\
         請用名稱前綴在 crates/**/tests 與 crates/storage-core/src/conformance.rs 搜尋是哪一支；\
         需要 enabled=true 的測試必須用 storage_postgres::DisableConnectorsOnDrop，\
         其餘 fixture 預設 enabled=false。守門測試：no_stale_enabled_scheduled_test_connectors。",
        STALE_AFTER.num_minutes()
    )
}

#[tokio::test]
async fn no_stale_enabled_scheduled_test_connectors() {
    let (dsn, store) = connect().await;
    let stale = list_stale_enabled_scheduled_test_connectors(&store).await;
    assert!(stale.is_empty(), "{}", gatekeeper_failure_message(&stale));

    // 反向驗證放同一支，避免與上面的空斷言平行執行時誤把 probe 當成殘留。
    let (source_id, connector_id) = insert_old_violation(&store).await;
    let _guard = DisableConnectorsOnDrop::from_dsn(&dsn).tracking(connector_id);
    let flagged = list_stale_enabled_scheduled_test_connectors(&store).await;
    let found = flagged.iter().any(|row| row.id == connector_id);
    let message = gatekeeper_failure_message(&flagged);
    let _ = store.delete_connector(connector_id).await;
    let _ = store.delete_source(source_id).await;
    assert!(
        found,
        "守門應該抓到故意留下的舊測試 connector {connector_id}。實際訊息：{message}"
    );
    assert!(
        message.contains("e2e-connector-gatekeeper-probe"),
        "失敗訊息要列出違規名稱，方便對回哪支測試。實際：{message}"
    );
    assert!(
        message.contains("DisableConnectorsOnDrop"),
        "失敗訊息要指出下一步用 guard。實際：{message}"
    );
}

/// panic 展開時 Drop 仍會把 connector 停用。這是 guard 存在的理由。
#[tokio::test]
async fn disable_connectors_on_drop_runs_after_panic() {
    let (dsn, store) = connect().await;
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("e2e-guard-source-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: None,
        base_url: None,
        description: None,
        language: None,
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    store.put_source(&source).await.expect("source");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("e2e-connector-guard-{}", Uuid::now_v7()),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({}),
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({}),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    store.put_connector(&connector).await.expect("connector");

    let panicked = catch_unwind(AssertUnwindSafe(|| {
        let _guard = DisableConnectorsOnDrop::from_dsn(&dsn).tracking(connector.id);
        panic!("故意 panic，驗證 DisableConnectorsOnDrop 仍會停用");
    }));
    assert!(panicked.is_err(), "閉包應該 panic");

    let got = store
        .get_connector(connector.id)
        .await
        .expect("get")
        .expect("connector 還在");
    assert!(
        !got.enabled,
        "Drop 必須在 panic 之後把 enabled 設成 false，實際仍是 true。\
         若這支失敗，guard 在 panic 路徑不可靠，不能當成收尾手段"
    );

    let _ = store.delete_connector(connector.id).await;
    let _ = store.delete_source(source.id).await;
}

async fn insert_old_violation(store: &PostgresCanonicalStore) -> (Uuid, Uuid) {
    let created = Utc::now() - Duration::minutes(10);
    let ts = uuid::Timestamp::from_unix_time(
        created.timestamp() as u64,
        created.timestamp_subsec_nanos(),
        0,
        0,
    );
    let source_id = Uuid::new_v7(ts);
    let connector_id = Uuid::new_v7(ts);
    let source = Source {
        id: source_id,
        name: format!("e2e-gatekeeper-source-{}", source_id),
        source_type: SourceType::Rss,
        platform: None,
        base_url: None,
        description: Some("守門測試故意留下的舊列".into()),
        language: None,
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: created,
        updated_at: created,
        last_seen: None,
    };
    store.put_source(&source).await.expect("source");
    let connector = Connector {
        id: connector_id,
        source_id,
        name: "e2e-connector-gatekeeper-probe".into(),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({}),
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({}),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: "idle".into(),
        error_count: 0,
    };
    store.put_connector(&connector).await.expect("connector");
    (source_id, connector_id)
}
