//! 測試用：`Drop` 時把指定 connector 設成 `enabled=false`。
//!
//! 共用開發 Postgres 上，`enabled=true` 且 `schedule` 非空的 connector 會被
//! collector 的 `tick()` 排程。測試常在函式結尾才停用——斷言 panic 時那段
//! 收尾不會跑。`Drop` 在 panic 展開時仍會執行。
//!
//! SQLite conformance 用一次性檔案，不需要這個 guard。

use std::sync::Mutex;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// 持有 DSN 與 connector id；drop 時同步 `UPDATE connectors SET enabled = FALSE`。
///
/// Drop 是同步的、sqlx 是 async：在**新的 OS thread** 上開一個 current-thread
/// tokio runtime 執行 UPDATE 並 `join`。不重用測試當下的 runtime，避免
/// 「從 async 上下文再 `block_on`」死鎖。連線也是新開的，不借用測試的 `PgPool`
/// （跨 runtime 共用 pool 不保證安全）。
///
/// # 何時使用
///
/// 只有測試真的需要 `enabled=true` 時才打開 connector，並立刻綁這個 guard：
///
/// - 經由 `tick()`／`list_enabled_connectors` 觸發
/// - 在測 enabled 過濾、PATCH enabled 等行為本身
///
/// 其餘 fixture 預設 `enabled=false`。`CollectorRunner::run_connector` 不看
/// `enabled`，走手動觸發的 e2e 不必打開。
#[must_use = "不綁成變數的話會立刻 drop，connector 還來不及測就被停用"]
pub struct DisableConnectorsOnDrop {
    dsn: String,
    ids: Mutex<Vec<Uuid>>,
}

impl DisableConnectorsOnDrop {
    /// 從 Postgres DSN 建立空的 guard，稍後 [`track`](Self::track)。
    pub fn from_dsn(dsn: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            ids: Mutex::new(Vec::new()),
        }
    }

    /// 讀 `DATABASE_URL`。缺值時 panic，指出要設哪個變數。
    pub fn from_database_url() -> Self {
        let dsn = std::env::var("DATABASE_URL").unwrap_or_else(|_| {
            panic!(
                "DisableConnectorsOnDrop 需要環境變數 `DATABASE_URL`。\
                 請在 repo 根目錄放 `.env`（可從 `.env.example` 複製）"
            )
        });
        Self::from_dsn(dsn)
    }

    /// 登記一個測試結束（含 panic）時要停用的 connector。
    pub fn track(&self, id: Uuid) {
        self.ids
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .push(id);
    }

    /// 建立時就登記一筆，方便 `let _guard = DisableConnectorsOnDrop::from_dsn(dsn).tracking(id)`。
    pub fn tracking(self, id: Uuid) -> Self {
        self.track(id);
        self
    }
}

impl Drop for DisableConnectorsOnDrop {
    fn drop(&mut self) {
        let ids = match self.ids.get_mut() {
            Ok(ids) => std::mem::take(ids),
            Err(err) => std::mem::take(err.into_inner()),
        };
        if ids.is_empty() {
            return;
        }
        let dsn = self.dsn.clone();
        let joined = std::thread::spawn(move || disable_connectors_blocking(&dsn, &ids)).join();
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                eprintln!(
                    "DisableConnectorsOnDrop: 停用 connector 失敗：{message}。\
                     請手動 `UPDATE connectors SET enabled = FALSE WHERE id IN (...)`，\
                     否則 collector 一開就會排程這些殘留"
                );
            }
            Err(_) => {
                eprintln!(
                    "DisableConnectorsOnDrop: 停用 connector 的執行緒 panic。\
                     請檢查開發 Postgres 裡是否留下 enabled=true 的測試 connector"
                );
            }
        }
    }
}

fn disable_connectors_blocking(dsn: &str, ids: &[Uuid]) -> Result<(), String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("建立 tokio runtime 失敗：{err}"))?;
    runtime.block_on(async {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(dsn)
            .await
            .map_err(|err| format!("連 Postgres 失敗：{err}"))?;
        for id in ids {
            sqlx::query("UPDATE connectors SET enabled = FALSE WHERE id = $1")
                .bind(id)
                .execute(&pool)
                .await
                .map_err(|err| format!("UPDATE connector `{id}` 失敗：{err}"))?;
        }
        pool.close().await;
        Ok(())
    })
}
