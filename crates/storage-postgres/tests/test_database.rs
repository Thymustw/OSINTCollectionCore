//! `TestDatabase`（一次性測試 DB，KNOWN_GAPS §26）本身的契約：建得起來、是空的且 migration 完整、
//! drop（含 panic）後真的刪掉、多個同時建不互撞。

use std::time::Instant;

use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::{TEST_DB_PREFIX, TestDatabase};

async fn database_exists(name: &str) -> bool {
    // 另開一個一次性 DB 來查 pg_database：不連開發 DB 本身。
    let probe = TestDatabase::create().await;
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(name)
        .fetch_one(probe.store().pool())
        .await
        .expect("查 pg_database")
}

#[tokio::test]
async fn fresh_database_is_migrated_and_empty_then_dropped() {
    load_workspace_dotenv();
    let started = Instant::now();
    let db = TestDatabase::create().await;
    eprintln!("建立一次性 DB 花 {} ms", started.elapsed().as_millis());
    assert!(db.name().starts_with(TEST_DB_PREFIX));
    let pool = db.store().pool();

    let migrations: i64 = sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations WHERE success")
        .fetch_one(pool)
        .await
        .expect("讀 _sqlx_migrations");
    assert!(migrations > 0, "模板沒有跑 migration");
    let sources: i64 = sqlx::query_scalar("SELECT count(*) FROM sources")
        .fetch_one(pool)
        .await
        .expect("讀 sources");
    assert_eq!(
        sources, 0,
        "一次性 DB 應該是空的；不是空的代表連到了開發 DB"
    );

    let name = db.name().to_string();
    drop(db);
    assert!(!database_exists(&name).await, "`{name}` drop 後仍存在");
}

#[tokio::test]
async fn database_is_dropped_even_when_the_test_panics() {
    load_workspace_dotenv();
    let name = tokio::spawn(async {
        let db = TestDatabase::create().await;
        let name = db.name().to_string();
        // 把名字送出去後 panic：展開時 db 的 Drop 仍要刪掉 DB。
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _db = db;
            panic!("故意 panic");
        }));
        assert!(result.is_err());
        name
    })
    .await
    .expect("task");
    assert!(!database_exists(&name).await, "panic 後 `{name}` 沒被刪");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_databases_can_be_created_concurrently() {
    load_workspace_dotenv();
    let started = Instant::now();
    let handles: Vec<_> = (0..12)
        .map(|_| tokio::spawn(async { TestDatabase::create().await }))
        .collect();
    let mut dbs = Vec::new();
    for h in handles {
        dbs.push(h.await.expect("task"));
    }
    eprintln!(
        "同時建 12 個一次性 DB 花 {} ms",
        started.elapsed().as_millis()
    );
    let mut names: Vec<_> = dbs.iter().map(|d| d.name().to_string()).collect();
    names.sort();
    names.dedup();
    assert_eq!(names.len(), 12, "名稱撞了");
    drop(dbs);
    for name in names {
        assert!(!database_exists(&name).await, "`{name}` 沒被刪");
    }
}
