//! `osint-discovery-worker`：訂閱 `job.dispatched`，執行 `discovery_run` job。
//!
//! V0.3 Phase 3 Step A——這是骨架。`job_type=discovery_run` 目前只會被
//! **誠實地標記 Failed**（訊息說明「Step B 尚未實作」），不假裝有做事。
//! 實際 Discovery method（Entity Expansion／Graph Expansion）在 Step B
//! 接上，那時這裡的 `handle_discovery_run` 會換成真正呼叫執行邏輯。

use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventEnvelope, EventTopic};
use core_jobs::JobService;
use core_model::JobStatus;
use core_observability::{MetricsRegistry, init_tracing};
use discovery_worker::{JOB_TYPE_DISCOVERY_RUN, serve_health};
use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

/// 一次 poll 等多久。沒有批次，逾時只是為了能回應 SIGINT 與量 lag。
const IDLE_POLL: Duration = Duration::from_secs(30);
/// 每幾則事件量一次 consumer lag。
const LAG_PROBE_EVERY: u64 = 10;
/// 量 lag 的逾時。量不到就當作沒有 lag，不要讓它擋住消費。
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-discovery-worker 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cfg = AppConfig::load().map_err(|err| err.to_string())?;

    let dsn = cfg
        .storage
        .canonical
        .dsn_secret_ref
        .resolve()
        .map_err(|err| err.to_string())?;
    let store = PostgresCanonicalStore::connect(&dsn, cfg.storage.canonical.pool_max)
        .await
        .map_err(|err| err.to_string())?;
    store.migrate().await.map_err(|err| err.to_string())?;

    let metrics = MetricsRegistry::new();

    let bind = cfg.discovery_worker.bind.clone();
    let health_metrics = metrics.clone();
    let health_store = store.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store).await {
            tracing::error!(error = %err, "discovery-worker health 結束");
        }
    });

    let jobs = JobService::new(store.clone(), None);

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.discovery_worker.consumer_group,
        &[EventTopic::JobDispatched.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.discovery_worker.consumer_group,
        "discovery-worker 開始消費 job.dispatched"
    );

    consume_loop(&jobs, &consumer, &metrics).await;
    Ok(())
}

/// 消費迴圈。骨架階段只認得 `job_type=discovery_run`，一律轉 Failed；其他
/// job_type（別的 worker 訂閱同一個 topic 各挑各的）安靜略過並 commit。
async fn consume_loop(
    jobs: &JobService<PostgresCanonicalStore>,
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
) {
    let mut processed = 0_u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，discovery-worker 結束");
                break;
            }
            result = consumer.next_envelope(IDLE_POLL) => match result {
                Ok(envelope) => {
                    handle_envelope(jobs, &envelope).await;
                    processed += 1;
                    if processed % LAG_PROBE_EVERY == 0 {
                        update_queue_depth(consumer, metrics);
                    }
                    if let Err(err) = consumer.commit_last() {
                        tracing::warn!(error = %err, "commit offset 失敗");
                    }
                }
                Err(core_events::EventError::ConsumeTimeout { .. }) => {
                    update_queue_depth(consumer, metrics);
                }
                Err(err) => {
                    tracing::error!(error = %err, "消費事件失敗");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    }
}

/// 處理單一 `job.dispatched` envelope。
///
/// 骨架階段解析方式比照 `crates/stix-worker/src/service.rs` 的
/// `process_dispatched_job_inner`（約 line 185-216）：從 `payload` 的
/// `job_type`／`job_id` 字串欄位解析，缺合法 `job_id` 就 post 一則 error log
/// 後 return——**不 panic、不標錯 state**，這則事件照樣 commit，因為重送也
/// 解不出來。唯一差異：`job_type != JOB_TYPE_DISCOVERY_RUN` 時這份文件要
/// 我們「安靜略過」而不是以 `Ignored` outcome 記 debug——但兩者都是「不處理、
/// 照樣 commit」，行為等價。
async fn handle_envelope(jobs: &JobService<PostgresCanonicalStore>, envelope: &EventEnvelope) {
    let payload = &envelope.payload;
    let job_type = payload
        .get("job_type")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if job_type != JOB_TYPE_DISCOVERY_RUN {
        // 別的 worker 同 topic 各挑各的；這裡不認得就安靜略過，不是錯誤。
        tracing::debug!(
            job_type,
            "job.dispatched 的 job_type 不是 discovery_run，discovery-worker 忽略"
        );
        return;
    }

    let Some(job_id) = payload
        .get("job_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
    else {
        tracing::error!(
            payload = %payload,
            "discovery_run job.dispatched 缺少合法 job_id。重送也不會讓它出現在 Postgres，提交 offset"
        );
        return;
    };

    handle_discovery_run(jobs, job_id).await;
}

/// Step A 骨架：`discovery_run` job 一律標記 Failed，訊息說明尚未實作。
/// **不要偷懶回 `Completed`**——那會讓呼叫端以為 Discovery 真的跑過了。
async fn handle_discovery_run(jobs: &JobService<PostgresCanonicalStore>, job_id: Uuid) {
    // 先 Running 再 Failed：can_transition 不允許 Queued → Failed 一步到位
    // （crates/core-jobs/src/transition.rs），必須先 Running。
    if let Err(err) = jobs.transition(job_id, JobStatus::Running, None).await {
        tracing::error!(error = %err, %job_id, "啟動 discovery_run job 時標記 Running 失敗");
        return;
    }
    if let Err(err) = jobs
        .transition(
            job_id,
            JobStatus::Failed,
            Some(
                "discovery-worker Phase 3 Step A 骨架階段，Discovery method 執行邏輯尚未實作\
                 （Step B 才會補上 Entity/Graph Expansion）。這不是真正的執行失敗，\
                 是這個版本還沒做這件事"
                    .into(),
            ),
        )
        .await
    {
        tracing::error!(error = %err, %job_id, "標記 discovery_run job Failed 也失敗");
    }
}

fn update_queue_depth(consumer: &EventConsumer, metrics: &MetricsRegistry) {
    match consumer.consumer_lag(LAG_TIMEOUT) {
        Ok(lag) => metrics.set_queue_depth(lag),
        Err(err) => {
            tracing::warn!(error = %err, "量不到 consumer lag，本輪不更新 osint_queue_depth")
        }
    }
}
