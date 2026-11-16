//! 測試用：每個測試一個一次性 PostgreSQL database（KNOWN_GAPS §26，方案 B）。
//!
//! 以前整合／e2e 測試直接寫 `DATABASE_URL` 指的開發 DB，跑完不刪；`osint-indexer --rebuild`
//! 再把那些列投影進正式 index（實測 94 個 `*-e2e-*` source、156 份文件）。現在：
//!
//! 1. 第一次用到時，建一個已跑完 migration 的**模板** `osint_test_tpl_<migration 雜湊>`；
//! 2. 每個測試 `CREATE DATABASE osint_test_<uuidv7> TEMPLATE …`；
//! 3. [`TestDatabase`] drop（含 panic 展開）時 `DROP DATABASE … WITH (FORCE)`；
//! 4. SIGKILL／逾時砍行程時 Drop 不會跑，所以每次 [`TestDatabase::create`] 先清掃逾時的舊 DB。
//!
//! `DATABASE_URL` 只拿來當**管理連線**的來源（連到同一台伺服器的 `postgres` 維護 DB）；
//! 測試本身拿 [`TestDatabase::dsn`]。不用 `std::env::set_var` 改全域變數：有些測試在斷言
//! 「缺 DATABASE_URL 時的錯誤訊息」，而且 edition 2024 把它列為 unsafe。
//!
//! 不用 `#[sqlx::test]`：它只在測試**成功**時刪 DB，並在 `DATABASE_URL` 指的 DB 建 `_sqlx_test`
//! schema（sqlx-postgres 0.9.0 `testing/mod.rs`）——那本身就是污染開發 DB。
//! 不用 schema 隔離：漏設 `search_path` 的連線會**靜默**寫回開發 DB 的 public。

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{ConnectOptions, Connection, PgConnection};
use storage_core::StorageError;
use uuid::Uuid;

use crate::store::{MIGRATOR, PostgresCanonicalStore};

/// 一次性 DB 的名稱前綴。清掃只動這個前綴，不會碰開發 DB。
pub const TEST_DB_PREFIX: &str = "osint_test_";
const TEMPLATE_PREFIX: &str = "osint_test_tpl_";
/// 比這更舊、而且沒有連線的 `osint_test_*` 視為被砍掉的行程留下的，清掉。
const STALE_AFTER: Duration = Duration::from_secs(30 * 60);
/// 建模板用的 advisory lock key（任意常數；鎖在 `postgres` 維護 DB 上，所以所有行程互斥）。
const TEMPLATE_LOCK_KEY: i64 = 0x6f73_696e_745f_7470; // "osint_tp"

/// 一個測試專用的 database。drop 時刪掉（`OSINT_TEST_KEEP_DB=1` 時保留，方便除錯）。
#[must_use = "不綁成變數會立刻 drop，DB 剛建好就被刪"]
pub struct TestDatabase {
    name: String,
    dsn: String,
    admin: PgConnectOptions,
    store: PostgresCanonicalStore,
}

impl TestDatabase {
    /// 依 `DATABASE_URL` 指的伺服器建一個新 DB，回傳已連好的 store。
    ///
    /// # Panics
    /// 缺 `DATABASE_URL`、主機不是本機、或建 DB 失敗時 panic——測試環境設定錯，
    /// 繼續跑只會得到看似正常的錯誤結果。
    pub async fn create() -> Self {
        Self::try_create().await.unwrap_or_else(|err| {
            panic!("建立一次性測試 DB 失敗：{err}");
        })
    }

    pub async fn try_create() -> Result<Self, StorageError> {
        let url = std::env::var("DATABASE_URL").map_err(|_| {
            config(
                "TestDatabase 需要環境變數 `DATABASE_URL`（只用來取得伺服器位址與帳號）。\
             請在 repo 根目錄放 `.env`（可從 `.env.example` 複製）",
            )
        })?;
        let base: PgConnectOptions = url
            .parse()
            .map_err(|err| config(format!("DATABASE_URL 無法解析：{err}")))?;
        let host = base.get_host().to_string();
        if !matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1") && !host.starts_with('/') {
            return Err(config(format!(
                "TestDatabase 只對本機 Postgres 建／刪 database，DATABASE_URL 的主機是 `{host}`"
            )));
        }
        let admin = base.clone().database("postgres");

        let template = ensure_template(&admin).await?;
        let name = format!("{TEST_DB_PREFIX}{}", Uuid::now_v7().simple());
        let mut conn = admin_connect(&admin).await?;
        sweep_stale(&mut conn).await;
        ddl(
            &mut conn,
            &[&name, &template],
            format!(r#"CREATE DATABASE "{name}" TEMPLATE "{template}""#),
        )
        .await?;
        let _ = conn.close().await;

        let opts = base.database(&name);
        let dsn = opts.to_url_lossy().to_string();
        let store = PostgresCanonicalStore::connect(&dsn, 5).await?;
        Ok(Self {
            name,
            dsn,
            admin,
            store,
        })
    }

    /// 給需要 DSN 字串的 guard（例如 [`crate::DisableConnectorsOnDrop`]）或另開 pool 的測試。
    #[must_use]
    pub fn dsn(&self) -> &str {
        &self.dsn
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn store(&self) -> &PostgresCanonicalStore {
        &self.store
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if std::env::var("OSINT_TEST_KEEP_DB").is_ok_and(|v| v == "1") {
            eprintln!("TestDatabase：OSINT_TEST_KEEP_DB=1，保留 `{}`", self.name);
            return;
        }
        let name = self.name.clone();
        let admin = self.admin.clone();
        // 理由同 DisableConnectorsOnDrop：Drop 是同步的，在新 OS thread 開 current-thread runtime，
        // 不重用測試的 runtime／pool。測試 pool 還開著也沒關係——WITH (FORCE) 會踢掉那些連線（PG 13+）。
        let joined = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|err| format!("建立 tokio runtime 失敗：{err}"))?;
            runtime.block_on(async {
                let mut conn = admin_connect(&admin).await.map_err(|e| e.to_string())?;
                ddl(
                    &mut conn,
                    &[&name],
                    format!(r#"DROP DATABASE IF EXISTS "{name}" WITH (FORCE)"#),
                )
                .await
                .map_err(|e| e.to_string())?;
                let _ = conn.close().await;
                Ok::<(), String>(())
            })
        })
        .join();
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(message)) => eprintln!(
                "TestDatabase：{message}。下次建立測試 DB 時會清掃逾時 {} 分鐘的 `{TEST_DB_PREFIX}*`",
                STALE_AFTER.as_secs() / 60
            ),
            Err(_) => eprintln!(
                "TestDatabase：刪 DB 的執行緒 panic，`{}` 可能留下",
                self.name
            ),
        }
    }
}

/// 模板名稱綁 migration 內容：任何 migration 改了（版本或 checksum），就是新模板。
fn template_name() -> String {
    let mut hasher = DefaultHasher::new();
    for m in MIGRATOR.iter() {
        m.version.hash(&mut hasher);
        m.checksum.hash(&mut hasher);
    }
    format!("{TEMPLATE_PREFIX}{:016x}", hasher.finish())
}

async fn ensure_template(admin: &PgConnectOptions) -> Result<String, StorageError> {
    let name = template_name();
    let mut conn = admin_connect(admin).await?;
    if database_exists(&mut conn, &name).await? {
        let _ = conn.close().await;
        return Ok(name);
    }
    // 多個測試行程（nextest 每個測試一個行程）可能同時第一次建模板。鎖在 `postgres` 維護 DB 上：
    // advisory lock 以 database 為範圍，連到不同 DB 的鎖互相擋不住。
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(TEMPLATE_LOCK_KEY)
        .execute(&mut conn)
        .await
        .map_err(|err| db_err("pg_advisory_lock", err))?;
    let result = build_template_locked(admin, &mut conn, &name).await;
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(TEMPLATE_LOCK_KEY)
        .execute(&mut conn)
        .await;
    let _ = conn.close().await;
    result.map(|()| name)
}

async fn build_template_locked(
    admin: &PgConnectOptions,
    conn: &mut PgConnection,
    name: &str,
) -> Result<(), StorageError> {
    if database_exists(conn, name).await? {
        return Ok(()); // 等鎖期間別的行程建好了
    }
    // 先用暫時名稱建、跑完 migration，再改名：中途失敗不會留下一個「看起來存在但不完整」的模板。
    let building = format!("{name}_building");
    ddl(
        conn,
        &[&building],
        format!(r#"DROP DATABASE IF EXISTS "{building}" WITH (FORCE)"#),
    )
    .await?;
    ddl(
        conn,
        &[&building],
        format!(r#"CREATE DATABASE "{building}""#),
    )
    .await?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect_with(admin.clone().database(&building))
        .await
        .map_err(|err| db_err("連模板", err))?;
    let migrated = MIGRATOR.run(&pool).await;
    pool.close().await;
    migrated.map_err(|err| StorageError::MigrationRequired {
        message: format!("測試模板 migration 失敗：{err}"),
    })?;
    // PG 要求模板沒有任何連線；關掉連線權，避免有人誤連造成 `being accessed by other users`。
    ddl(
        conn,
        &[&building, name],
        format!(r#"ALTER DATABASE "{building}" RENAME TO "{name}""#),
    )
    .await?;
    ddl(
        conn,
        &[name],
        format!(r#"ALTER DATABASE "{name}" WITH ALLOW_CONNECTIONS false"#),
    )
    .await
}

/// 清掉被砍掉的行程留下的 `osint_test_*`（不含模板）：名稱裡的 uuidv7 時間早於 [`STALE_AFTER`]、
/// 而且目前沒有連線。失敗只記錄、不擋測試。
async fn sweep_stale(conn: &mut PgConnection) {
    let rows: Vec<(String, i64)> = match sqlx::query_as(
        "SELECT d.datname, (SELECT count(*) FROM pg_stat_activity a WHERE a.datname = d.datname) \
         FROM pg_database d WHERE d.datname LIKE 'osint\\_test\\_%' AND d.datname NOT LIKE 'osint\\_test\\_tpl\\_%'",
    )
    .fetch_all(&mut *conn)
    .await
    {
        Ok(rows) => rows,
        Err(err) => {
            eprintln!("TestDatabase：清掃舊測試 DB 時查詢失敗：{err}");
            return;
        }
    };
    let cutoff = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .saturating_sub(STALE_AFTER);
    for (name, connections) in rows {
        if connections > 0 {
            continue;
        }
        let Some(created) = created_at(&name) else {
            continue;
        };
        if created < cutoff
            && let Err(err) = ddl(
                &mut *conn,
                &[&name],
                format!(r#"DROP DATABASE IF EXISTS "{name}""#),
            )
            .await
        {
            eprintln!("TestDatabase：清掃 `{name}` 失敗：{err}");
        }
    }
}

/// `osint_test_<uuidv7 simple>` → 建立時間。不是這個格式就回 None（不碰它）。
fn created_at(name: &str) -> Option<Duration> {
    let id = Uuid::parse_str(name.strip_prefix(TEST_DB_PREFIX)?).ok()?;
    let (secs, nanos) = id.get_timestamp()?.to_unix();
    Some(Duration::new(secs, nanos))
}

/// 這個模組的 DDL（CREATE／DROP／ALTER DATABASE）都經過這裡。database 名稱不能用 bind 參數，只能組字串；
/// 所以先驗證每個識別字只含 `[a-z0-9_]` 且以 `osint_test_` 開頭——名稱全由本模組產生
/// （前綴＋uuid hex／migration 雜湊），或從 `pg_database` 以同一前綴篩出來。
async fn ddl(conn: &mut PgConnection, idents: &[&str], sql: String) -> Result<(), StorageError> {
    for ident in idents {
        let ok = ident.starts_with(TEST_DB_PREFIX)
            && ident
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !ok {
            return Err(config(format!(
                "拒絕對 `{ident}` 下 DDL：不是本模組產生的測試 DB 名稱"
            )));
        }
    }
    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
        .execute(conn)
        .await
        .map(|_| ())
        .map_err(|err| db_err(&sql, err))
}

async fn database_exists(conn: &mut PgConnection, name: &str) -> Result<bool, StorageError> {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)")
        .bind(name)
        .fetch_one(conn)
        .await
        .map_err(|err| db_err("查 pg_database", err))
}

async fn admin_connect(admin: &PgConnectOptions) -> Result<PgConnection, StorageError> {
    admin
        .connect()
        .await
        .map_err(|err| db_err("連管理 DB `postgres`", err))
}

fn config(message: impl Into<String>) -> StorageError {
    StorageError::Configuration {
        message: message.into(),
    }
}

fn db_err(what: &str, err: sqlx::Error) -> StorageError {
    config(format!("{what} 失敗：{err}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn created_at_reads_uuidv7_and_ignores_other_names() {
        let id = Uuid::now_v7();
        let name = format!("{TEST_DB_PREFIX}{}", id.simple());
        assert!(created_at(&name).is_some());
        assert!(created_at("osint_core").is_none());
        assert!(created_at(&format!("{TEMPLATE_PREFIX}abcdef")).is_none());
    }

    #[test]
    fn template_name_is_stable_and_prefixed() {
        assert_eq!(template_name(), template_name());
        assert!(template_name().starts_with(TEMPLATE_PREFIX));
    }
}
