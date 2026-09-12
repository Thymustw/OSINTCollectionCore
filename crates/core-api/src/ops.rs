//! `/api/v1/ops/*`：Operations Center 的基礎面板（SPEC §31）。
//!
//! * `GET /ops/health`：五個後端（PostgreSQL／物件儲存／Redis／OpenSearch／Redpanda）
//!   的聚合健康。任一 down → 整體 **503**，回應裡指出是哪一個。
//! * `GET /ops/metrics`：本行程的資源用量（RSS、CPU 時間、執行緒數）。
//! * `GET /ops/connectors`：每個 connector 的採集健康（SPEC §31「connector health」）。
//! * `GET /ops/queues`：四個 consumer group 的 lag（SPEC §31「queue summary」）。
//! * `GET /ops/dlq`：失敗的 Job 清單（SPEC §31「basic DLQ view」）。
//!   **V0.1 沒有 DLQ topic**，回應裡的 `dlq_topic: null` 就是這件事的宣告。
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
use serde::{Deserialize, Serialize};
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

// --------------------------------------------------------------- connector health

/// 沒有指定 `stale_after_secs` 時，超過這段時間沒有成功就算「停滯」。
///
/// 24 小時是刻意保守的值：V0.1 的 schedule 最長可以到「每天一次」
/// （`0 3 * * *` 之類），閾值比最長排程週期短就會讓正常的每日來源一直亮紅燈，
/// 而一個一直亮紅燈的面板等於沒有面板。要抓更短週期的 connector 請傳
/// `?stale_after_secs=`。
const DEFAULT_STALE_AFTER_SECS: i64 = 86_400;

/// `GET /ops/connectors` 一次最多掃描幾筆 connector。
///
/// 這個視圖**不分頁**，因為 `unhealthy=true` 必須對整個 fleet 判斷：
/// 先取一頁再在程式端過濾，會讓「沒有不健康的 connector」與
/// 「最新一頁裡沒有不健康的 connector」變成同一個答案（同 `list_jobs_by_status`
/// 的註解）。connector 是運維手動建立的，數量級是幾十，不是幾百萬，
/// 所以整批掃是可行的——但仍然有硬上限，而且掃滿時會在回應裡說 `truncated: true`，
/// 不會假裝自己看完了全部。
const MAX_CONNECTOR_SCAN: usize = 1_000;
/// 內部分頁每頁大小（`list_connectors` 的 limit 上限是 100）。
const CONNECTOR_PAGE: u32 = 100;

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectorsQuery {
    /// `true` 只列不健康的。省略或 `false` 都是列全部。
    pub unhealthy: Option<bool>,
    /// 判定「太久沒成功」的秒數。預設 [`DEFAULT_STALE_AFTER_SECS`]。
    pub stale_after_secs: Option<i64>,
    /// 回傳筆數上限（過濾之後）。預設 100。
    pub limit: Option<usize>,
}

/// 單一 connector 的採集健康。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ConnectorHealth {
    pub id: uuid::Uuid,
    pub source_id: uuid::Uuid,
    pub name: String,
    #[serde(rename = "type")]
    pub connector_type: String,
    pub enabled: bool,
    /// connector 自己記的狀態字串（`idle`／`running`／`error`…）。
    pub status: String,
    pub error_count: i32,
    pub last_run: Option<chrono::DateTime<chrono::Utc>>,
    pub last_success: Option<chrono::DateTime<chrono::Utc>>,
    /// 距離上次成功幾秒。從來沒成功過時是 `null`。
    pub since_last_success_secs: Option<i64>,
    pub healthy: bool,
    /// 不健康的原因。`healthy = true` 時是 `null`。
    ///
    /// **這一欄是這個視圖存在的理由**：只回一個 `healthy: false` 等於要運維
    /// 自己去猜是「錯誤累積」還是「根本沒在跑」，那兩件事的下一步完全不同。
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConnectorsView {
    pub items: Vec<ConnectorHealth>,
    /// 掃描到的 connector 總數（過濾前）。
    pub scanned: usize,
    /// 其中不健康的數量（過濾前）。
    pub unhealthy_count: usize,
    /// 掃描撞到 [`MAX_CONNECTOR_SCAN`]，後面還有沒看到的 connector。
    pub truncated: bool,
    pub stale_after_secs: i64,
}

/// `GET /api/v1/ops/connectors`。viewer 以上。
pub async fn connectors(
    State(state): State<AppState>,
    principal: Principal,
    axum::extract::Query(query): axum::extract::Query<ConnectorsQuery>,
) -> Result<Json<ConnectorsView>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = crate::resources::store(&state)?;

    let stale_after_secs = match query.stale_after_secs {
        Some(secs) if secs <= 0 => {
            return Err(ApiError::bad_request(
                "stale_after_secs 必須大於 0。要看全部 connector 請不要帶 unhealthy=true",
            ));
        }
        Some(secs) => secs,
        None => DEFAULT_STALE_AFTER_SECS,
    };
    let limit = query.limit.unwrap_or(100).clamp(1, MAX_CONNECTOR_SCAN);
    let only_unhealthy = query.unhealthy.unwrap_or(false);
    let now = chrono::Utc::now();

    let mut after: Option<uuid::Uuid> = None;
    let mut scanned = 0_usize;
    let mut unhealthy_count = 0_usize;
    let mut items: Vec<ConnectorHealth> = Vec::new();
    let mut truncated = false;
    loop {
        let page = store
            .list_connectors(after, CONNECTOR_PAGE)
            .await
            .map_err(ApiError::from)?;
        if page.is_empty() {
            break;
        }
        after = page.last().map(|c| c.id);
        let page_len = page.len();
        for connector in page {
            scanned += 1;
            let health = connector_health(&connector, now, stale_after_secs);
            if !health.healthy {
                unhealthy_count += 1;
            }
            if (!only_unhealthy || !health.healthy) && items.len() < limit {
                items.push(health);
            }
        }
        if page_len < CONNECTOR_PAGE as usize {
            break;
        }
        if scanned >= MAX_CONNECTOR_SCAN {
            truncated = true;
            break;
        }
    }

    Ok(Json(ConnectorsView {
        items,
        scanned,
        unhealthy_count,
        truncated,
        stale_after_secs,
    }))
}

/// 判定一個 connector 健不健康。純函式，才測得到各種組合。
///
/// 判定順序刻意是「錯誤 → 從來沒成功 → 太久沒成功」，
/// 因為 `reason` 只回一條，要回最能指出下一步的那一條。
fn connector_health(
    connector: &core_model::Connector,
    now: chrono::DateTime<chrono::Utc>,
    stale_after_secs: i64,
) -> ConnectorHealth {
    let since_last_success_secs = connector
        .last_success
        .map(|at| (now - at).num_seconds().max(0));

    // 停用的 connector 不算不健康：那是人刻意關掉的，不是故障。
    // 但仍然列出來（而且 `enabled: false` 看得見）——「為什麼沒有資料」
    // 最常見的答案就是它被關掉了。
    let reason = if !connector.enabled {
        None
    } else if connector.error_count > 0 {
        Some(format!(
            "連續錯誤 {} 次（connector.status = `{}`）。請看 osint-collector 的記錄檔，\
             或用 GET /api/v1/connectors/{} 查設定",
            connector.error_count, connector.status, connector.id
        ))
    } else if connector.last_run.is_some() && connector.last_success.is_none() {
        Some("跑過但從來沒有成功過。多半是 URL、憑證或 SSRF allow-list 設定問題".into())
    } else {
        match since_last_success_secs {
            Some(secs) if secs > stale_after_secs => Some(format!(
                "已經 {secs} 秒沒有成功採集（門檻 {stale_after_secs} 秒）。\
                 請確認排程有在跑，以及來源站台是否改版"
            )),
            // 從來沒跑過（last_run 也是 None）不算故障：新建立的 connector
            // 在第一次排程到之前本來就是這個樣子。
            _ => None,
        }
    };

    ConnectorHealth {
        id: connector.id,
        source_id: connector.source_id,
        name: connector.name.clone(),
        connector_type: connector.connector_type.clone(),
        enabled: connector.enabled,
        status: connector.status.clone(),
        error_count: connector.error_count,
        last_run: connector.last_run,
        last_success: connector.last_success,
        since_last_success_secs,
        healthy: reason.is_none(),
        reason,
    }
}

// --------------------------------------------------------------- queue summary

/// 一個 consumer group 綁到哪個 topic。`main.rs` 依設定建這份清單。
#[derive(Debug, Clone)]
pub struct QueueBinding {
    /// 服務名（給人看的），例如 `normalizer`。
    pub service: &'static str,
    pub group: String,
    pub topic: &'static str,
}

/// `GET /ops/queues` 用的探針 + 綁定清單。
///
/// 沒接上 Redpanda 時 `AppState.queues` 是 `None`，那條路由回 503，其他路由照常。
#[derive(Debug, Clone)]
pub struct QueueInspector {
    pub probe: core_events::GroupLagProbe,
    pub bindings: Vec<QueueBinding>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueEntry {
    pub service: &'static str,
    pub group: String,
    pub topic: &'static str,
    /// 查不到時是 `null`，同時 `error` 會說明原因。**不要用 0 代替**：
    /// 「沒有落後」與「量不到」在面板上長得一樣的話，broker 掛掉會顯示成一切正常。
    pub lag: Option<u64>,
    pub topic_exists: Option<bool>,
    pub partitions: Vec<core_events::PartitionLag>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueSummary {
    pub brokers: String,
    /// 所有量得到的 group 的 lag 總和。有 group 量不到時 `complete` 會是 false。
    pub total_lag: u64,
    /// 是否每一個 group 都量到了。
    pub complete: bool,
    pub queues: Vec<QueueEntry>,
}

/// `GET /api/v1/ops/queues`。viewer 以上。
///
/// 單一 group 查詢失敗**不會**讓整個端點失敗：其他三個仍然回得出來，
/// 失敗的那個帶 `error`。一個 group 的問題不該讓運維連別的 group 都看不到。
pub async fn queues(
    State(state): State<AppState>,
    principal: Principal,
) -> Result<Json<QueueSummary>, ApiError> {
    principal.role.require(Permission::Read)?;
    let inspector = state.queues.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "佇列檢視未接上 Redpanda。請設定 [broker].brokers（本機 dev 是 127.0.0.1:9092）\
             並重啟 osint-api",
        )
    })?;

    let mut queues = Vec::with_capacity(inspector.bindings.len());
    let mut total_lag = 0_u64;
    let mut complete = true;
    for binding in &inspector.bindings {
        match inspector.probe.group_lag(&binding.group, binding.topic) {
            Ok(lag) => {
                total_lag = total_lag.saturating_add(lag.total_lag);
                queues.push(QueueEntry {
                    service: binding.service,
                    group: binding.group.clone(),
                    topic: binding.topic,
                    lag: Some(lag.total_lag),
                    topic_exists: Some(lag.topic_exists),
                    partitions: lag.partitions,
                    error: None,
                });
            }
            Err(err) => {
                complete = false;
                tracing::warn!(
                    group = %binding.group,
                    topic = binding.topic,
                    error = %err,
                    "查 consumer group lag 失敗"
                );
                queues.push(QueueEntry {
                    service: binding.service,
                    group: binding.group.clone(),
                    topic: binding.topic,
                    lag: None,
                    topic_exists: None,
                    partitions: Vec::new(),
                    error: Some(err.to_string()),
                });
            }
        }
    }

    Ok(Json(QueueSummary {
        brokers: inspector.probe.brokers().to_string(),
        total_lag,
        complete,
        queues,
    }))
}

// --------------------------------------------------------------- DLQ view

/// `GET /ops/dlq` 一次最多回幾筆失敗 Job。
const MAX_DLQ_JOBS: u32 = 100;

#[derive(Debug, Clone, Deserialize)]
pub struct DlqQuery {
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DlqView {
    /// V0.1 **沒有** DLQ topic，所以永遠是 `null`。
    ///
    /// 這不是遺漏而是明講的限制：消費失敗的事件目前會被記 error log 並照樣提交
    /// offset（見三個 consumer 的 main.rs），不會被搬到另一個 topic。
    /// 因此「永久失敗的事件」在 V0.1 沒有任何地方查得到——
    /// 這個欄位是 `null` 就是在說這件事，而不是在說「目前沒有壞掉的東西」。
    pub dlq_topic: Option<String>,
    pub note: String,
    /// `failed` 狀態的 Job。這是 V0.1 唯一真的落地的失敗紀錄。
    pub failed_jobs: Vec<core_model::Job>,
    pub failed_job_count: usize,
    /// 回傳筆數是否被 `limit` 截斷。
    pub truncated: bool,
}

/// `GET /api/v1/ops/dlq`。viewer 以上。
pub async fn dlq(
    State(state): State<AppState>,
    principal: Principal,
    axum::extract::Query(query): axum::extract::Query<DlqQuery>,
) -> Result<Json<DlqView>, ApiError> {
    principal.role.require(Permission::Read)?;
    let jobs = state.jobs.as_ref().ok_or_else(|| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            "DLQ 檢視未接上 Postgres（失敗 Job 存在 canonical store）。\
             請設定 DATABASE_URL 並重啟 osint-api",
        )
    })?;
    let limit = query.limit.unwrap_or(MAX_DLQ_JOBS).clamp(1, MAX_DLQ_JOBS);
    let failed_jobs = jobs
        .list_by_status(core_model::JobStatus::Failed, None, limit)
        .await
        .map_err(ApiError::from)?;
    let truncated = failed_jobs.len() as u32 >= limit;

    Ok(Json(DlqView {
        dlq_topic: None,
        note: "V0.1 沒有 DLQ topic：消費失敗的事件只會留在記錄檔，不會被搬到另一個 topic，\
               也沒有落地成可查詢的紀錄。這裡列的是 `failed` 狀態的 Job，\
               可以用 POST /api/v1/jobs/{id}/retry 重試（operator 以上）。\
               事件層級的重送在 V0.1 只能靠 raw_evidence_id 手動觸發"
            .into(),
        failed_job_count: failed_jobs.len(),
        failed_jobs,
        truncated,
    }))
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
