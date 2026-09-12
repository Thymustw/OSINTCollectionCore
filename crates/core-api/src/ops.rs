//! `/api/v1/ops/*`：Operations Center 的基礎面板（SPEC §31）。
//!
//! * `GET /ops/health`：五個後端（PostgreSQL／物件儲存／Redis／OpenSearch／Redpanda）
//!   的聚合健康。任一 down → 整體 **503**，回應裡指出是哪一個。
//! * `GET /ops/metrics`：本行程的資源用量（RSS、CPU 時間、執行緒數）。
//!
//! # 為什麼與 `/ready`、`/metrics` 分開
//!
//! * `/ready` 回答的是「這個行程現在能不能收流量」，給 orchestrator 用，**公開無認證**，
//!   而且刻意只檢查 API 自己非有不可的依賴。
//! * `/ops/health` 回答的是「整套系統哪一塊壞了」，給運維的人用，**需要 viewer 以上**。
//!   它會去敲 Redis 與 Redpanda——那些是 API 自己不需要、但 pipeline 需要的後端。
//!   把它們塞進 `/ready` 會讓「Redis 掛了」變成「API 不接受流量」，
//!   而 API 其實還能好好地回答查詢。
//! * `/metrics` 是 Prometheus 的聚合計數器（公開，理由見 `routes::metrics`）。
//!   `/ops/metrics` 是**這個行程的資源用量**，來源是 `/proc/self/*`，不是計數器。
//!
//! # 為什麼不加 `sysinfo` crate
//!
//! 需要的東西只有 RSS 與 CPU 時間，`/proc/self/status` 與 `/proc/self/stat`
//! 兩個檔就有。為了兩個數字引進一個會掃描全系統行程的相依（以及它的相依樹與
//! 供應鏈稽核成本）不划算。代價是**只在 Linux 有效**，其他平台回 501。

use std::sync::Arc;

use async_trait::async_trait;
use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use core_observability::{CheckResult, ReadyStatus};
use serde::Serialize;
use storage_core::HealthProvider;

use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::ready::ReadyCheck;
use crate::state::AppState;

/// 把任何 `HealthProvider`（storage adapter）包成一個具名的 ops 檢查。
///
/// 這樣 `/ops/health` 不需要知道後面是 Postgres、MinIO 還是 Redis——
/// 加一個新後端只要在 `main.rs` 多包一個，不必改這個檔（CLAUDE.md §13）。
pub struct BackendCheck {
    name: &'static str,
    provider: Arc<dyn HealthProvider>,
}

impl BackendCheck {
    #[must_use]
    pub fn new(name: &'static str, provider: Arc<dyn HealthProvider>) -> Self {
        Self { name, provider }
    }
}

#[async_trait]
impl ReadyCheck for BackendCheck {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn check(&self) -> CheckResult {
        match self.provider.health().await {
            Ok(health) if health.healthy => CheckResult::ok(self.name, health.message),
            Ok(health) => CheckResult::down(self.name, health.message),
            // 連不上也是一種結果，不是 500：運維要的就是「哪一個連不上」。
            Err(err) => CheckResult::down(self.name, err.to_string()),
        }
    }
}

/// Redpanda 的 ops 檢查。
///
/// Broker 沒有 `HealthProvider`（它不是 storage adapter），所以單獨包一個：
/// 拿 cluster metadata 拿得到就算活著。逾時刻意設得短——health 端點自己被卡住
/// 一分鐘的話，運維看到的會是「這個頁面壞了」而不是「Redpanda 壞了」。
pub struct BrokerCheck {
    producer: Arc<core_events::EventProducer>,
    timeout: std::time::Duration,
}

impl BrokerCheck {
    #[must_use]
    pub fn new(producer: Arc<core_events::EventProducer>) -> Self {
        Self {
            producer,
            timeout: std::time::Duration::from_secs(3),
        }
    }
}

#[async_trait]
impl ReadyCheck for BrokerCheck {
    fn name(&self) -> &'static str {
        "redpanda"
    }

    async fn check(&self) -> CheckResult {
        match self.producer.cluster_metadata(self.timeout).await {
            Ok((brokers, topics)) => CheckResult::ok(
                "redpanda",
                format!("metadata 可取得：{brokers} 個 broker、{topics} 個 topic"),
            ),
            Err(err) => CheckResult::down(
                "redpanda",
                format!("{err}。請確認 Redpanda 在跑，以及 [broker].brokers 設定正確"),
            ),
        }
    }
}

/// `GET /ops/health` 的回應。
#[derive(Debug, Clone, Serialize)]
pub struct OpsHealth {
    pub healthy: bool,
    pub checks: Vec<CheckResult>,
    /// 不健康的後端名稱。**不要叫呼叫端自己去 filter `checks`**——
    /// 這一欄就是告警規則會盯的欄位。
    pub unhealthy: Vec<String>,
    /// 沒有設定（沒接上）的後端。與「接上了但壞掉」是兩回事：
    /// 前者要去看設定檔，後者要去看那個服務。
    pub not_configured: Vec<&'static str>,
}

/// `GET /api/v1/ops/health`。viewer 以上。任一後端 down → 503。
pub async fn health(
    State(state): State<AppState>,
    principal: Principal,
) -> Result<(StatusCode, Json<OpsHealth>), ApiError> {
    principal.role.require(Permission::Read)?;

    let ReadyStatus { ready, checks } = state.backends.status().await;
    let unhealthy: Vec<String> = checks
        .iter()
        .filter(|c| !c.healthy)
        .map(|c| c.name.clone())
        .collect();
    let not_configured = state.backends_missing.clone();

    let body = OpsHealth {
        healthy: ready,
        checks,
        unhealthy,
        not_configured,
    };
    // 503 而不是 200+healthy:false：監控系統預設看的是狀態碼，
    // 只在 body 裡說「壞了」等於沒有人會發現。
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    Ok((status, Json(body)))
}

/// `GET /ops/metrics` 的回應。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ProcessMetrics {
    /// 常駐記憶體（`VmRSS`）。
    pub rss_bytes: u64,
    /// 歷史最高常駐記憶體（`VmHWM`）。看 OOM 風險用歷史峰值，不是當下值。
    pub peak_rss_bytes: u64,
    /// 虛擬記憶體大小（`VmSize`）。
    pub virtual_bytes: u64,
    pub threads: u64,
    /// 使用者態／核心態 CPU 時間（秒）。
    pub cpu_user_seconds: f64,
    pub cpu_system_seconds: f64,
    /// 換算 CPU 時間用的每秒 tick 數。見 [`CLOCK_TICKS_PER_SEC`] 的說明。
    pub clock_ticks_per_sec: u64,
}

/// `/proc/self/stat` 的 CPU 時間單位。
///
/// 正確做法是 `sysconf(_SC_CLK_TCK)`，但那需要 `libc`。Linux 上這個值
/// **在所有主流架構都是 100**（核心的 `USER_HZ` 常數），而且它是 ABI 的一部分，
/// 不會為了相容性而改變。把假設寫成常數並回在 API 裡，比悄悄除以一個魔術數字好：
/// 呼叫端若發現數字不對，至少看得到我們是用什麼換算的。
const CLOCK_TICKS_PER_SEC: u64 = 100;

/// `GET /api/v1/ops/metrics`。viewer 以上。非 Linux 回 501。
pub async fn metrics(
    State(_state): State<AppState>,
    principal: Principal,
) -> Result<Json<ProcessMetrics>, ApiError> {
    principal.role.require(Permission::Read)?;
    collect_process_metrics().map(Json)
}

#[cfg(target_os = "linux")]
fn collect_process_metrics() -> Result<ProcessMetrics, ApiError> {
    let status = std::fs::read_to_string("/proc/self/status").map_err(proc_error)?;
    let stat = std::fs::read_to_string("/proc/self/stat").map_err(proc_error)?;
    parse_process_metrics(&status, &stat)
}

#[cfg(not(target_os = "linux"))]
fn collect_process_metrics() -> Result<ProcessMetrics, ApiError> {
    Err(ApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "not_implemented",
        "行程資源指標目前只在 Linux 提供（資料來源是 /proc/self/status 與 /proc/self/stat）。\
         其他平台請用作業系統自己的監控，或看 GET /metrics 的聚合計數器",
    ))
}

#[cfg(target_os = "linux")]
fn proc_error(err: std::io::Error) -> ApiError {
    ApiError::internal(format!(
        "讀取 /proc/self 失敗：{err}。容器裡 /proc 被遮蔽或掛載受限時會這樣，\
         請確認沒有用 hidepid 或唯讀的 /proc 掛載"
    ))
}

/// 解析 `/proc/self/status` 與 `/proc/self/stat`。
///
/// 獨立成純函式是為了能用固定的樣本測——直接讀 `/proc` 的測試只能斷言
/// 「大於 0」，解析錯一個欄位也看不出來。
fn parse_process_metrics(status: &str, stat: &str) -> Result<ProcessMetrics, ApiError> {
    let rss_bytes = status_kb(status, "VmRSS").unwrap_or(0);
    let peak_rss_bytes = status_kb(status, "VmHWM").unwrap_or(rss_bytes);
    let virtual_bytes = status_kb(status, "VmSize").unwrap_or(0);
    let threads = status_field(status, "Threads")
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(1);

    // `/proc/self/stat` 的第二欄是 comm，**可能含空白與括號**（行程名是可控的）。
    // 從最後一個 ')' 之後開始切，才不會把欄位位置算偏。
    let rest = stat
        .rsplit_once(')')
        .map(|(_, rest)| rest)
        .ok_or_else(|| ApiError::internal("/proc/self/stat 格式不如預期（找不到 comm 的結尾）"))?;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // rest[0] 是 state（第 3 欄），所以 utime（第 14 欄）在 index 11、stime 在 12。
    let utime: u64 = fields.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
    let stime: u64 = fields.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);

    Ok(ProcessMetrics {
        rss_bytes,
        peak_rss_bytes,
        virtual_bytes,
        threads,
        cpu_user_seconds: utime as f64 / CLOCK_TICKS_PER_SEC as f64,
        cpu_system_seconds: stime as f64 / CLOCK_TICKS_PER_SEC as f64,
        clock_ticks_per_sec: CLOCK_TICKS_PER_SEC,
    })
}

fn status_field<'a>(status: &'a str, key: &str) -> Option<&'a str> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix(':'))
}

/// `VmRSS:\t   12345 kB` → bytes。
fn status_kb(status: &str, key: &str) -> Option<u64> {
    let value = status_field(status, key)?;
    let kb: u64 = value.split_whitespace().next()?.parse().ok()?;
    Some(kb * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS: &str = "Name:\tosint-api\nThreads:\t9\nVmSize:\t 2097152 kB\nVmHWM:\t   65536 kB\nVmRSS:\t   32768 kB\n";
    // 欄位取自真實的 /proc/self/stat，utime=123、stime=45。
    const STAT: &str = "1234 (osint-api) S 1 1234 1234 0 -1 4194304 1000 0 0 0 123 45 0 0 20 0 9 0 100 2147483648 8192 18446744073709551615";

    #[test]
    fn parses_memory_and_cpu() {
        let m = parse_process_metrics(STATUS, STAT).unwrap();
        assert_eq!(m.rss_bytes, 32768 * 1024);
        assert_eq!(m.peak_rss_bytes, 65536 * 1024);
        assert_eq!(m.virtual_bytes, 2097152 * 1024);
        assert_eq!(m.threads, 9);
        assert!((m.cpu_user_seconds - 1.23).abs() < 1e-9);
        assert!((m.cpu_system_seconds - 0.45).abs() < 1e-9);
    }

    #[test]
    fn comm_with_spaces_and_parens_does_not_shift_the_fields() {
        // 行程名是可控的：`(my (weird) name)` 會讓「用空白切第 14 欄」整個算錯，
        // 而且結果只是一個看起來很合理的錯誤數字。
        let stat =
            "1234 (my (weird) name) S 1 1234 1234 0 -1 4194304 1000 0 0 0 123 45 0 0 20 0 9 0 100";
        let m = parse_process_metrics(STATUS, stat).unwrap();
        assert!((m.cpu_user_seconds - 1.23).abs() < 1e-9);
    }

    #[test]
    fn missing_fields_degrade_instead_of_failing() {
        // /proc 在某些容器設定下欄位會少。少一個欄位不該讓整個 endpoint 回 500。
        let m = parse_process_metrics("Name:\tx\n", STAT).unwrap();
        assert_eq!(m.rss_bytes, 0);
        assert_eq!(m.threads, 1);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn real_proc_reports_a_non_zero_rss() {
        let m = collect_process_metrics().expect("讀 /proc/self");
        assert!(m.rss_bytes > 0, "實際 RSS 不可能是 0：{m:?}");
        assert!(m.threads >= 1);
    }
}
