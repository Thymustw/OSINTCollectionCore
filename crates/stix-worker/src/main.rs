//! `osint-stix-worker`：訂閱 `job.dispatched`，執行 `stix_import` 與 `stix_export`。
//!
//! ```text
//! osint-stix-worker          常駐消費（唯一模式）
//! osint-stix-worker --help   印用法後結束
//! ```
//!
//! 除了 `stix_import`／`stix_export` 之外的 job_type 一律忽略並 commit。
//! 沒有 `--rebuild`：匯入不是可重建的投影。

use std::sync::Arc;
use std::time::Duration;

use ai_gateway::OpenAiCompatibleLlmProvider;
use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_jobs::JobService;
use core_observability::{MetricsRegistry, init_tracing};
use resolver::AutoApprovalConfig;
use stix_worker::service::{StixWorker, StixWorkerOptions};
use stix_worker::{JobDispatchOutcome, serve_health};
use storage_core::conformance::load_workspace_dotenv;
use storage_core::mock::MockEmbeddingProvider;
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;

type ProdWorker = StixWorker<
    PostgresCanonicalStore,
    MockEmbeddingProvider,
    OpenAiCompatibleLlmProvider,
    S3ObjectStore,
>;

/// 一次 poll 等多久。沒有批次，逾時只是為了能回應 SIGINT 與量 lag。
const IDLE_POLL: Duration = Duration::from_secs(30);
/// 每幾則事件量一次 consumer lag。
const LAG_PROBE_EVERY: u64 = 10;
/// 量 lag 的逾時。量不到就當作沒有 lag，不要讓它擋住匯入。
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-stix-worker 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedArgs {
    Run,
    Help,
}

fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<ParsedArgs, String> {
    let mut help = false;
    for arg in args {
        match arg.as_str() {
            "-h" | "--help" => help = true,
            other => {
                return Err(format!("不認得的參數 `{other}`。\n{HELP}"));
            }
        }
    }
    if help {
        Ok(ParsedArgs::Help)
    } else {
        Ok(ParsedArgs::Run)
    }
}

const HELP: &str = "用法：
  osint-stix-worker        訂閱 job.dispatched，執行 stix_import 與 stix_export
  osint-stix-worker --help 印這段說明後結束
設定來源：config/default.toml → OSINT_CONFIG_FILE → OSINT__* 環境變數。";

async fn run() -> Result<(), String> {
    match parse_args(std::env::args().skip(1))? {
        ParsedArgs::Help => {
            println!("{HELP}");
            return Ok(());
        }
        ParsedArgs::Run => {}
    }

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

    let objects = connect_objects(&cfg)?;
    objects
        .ensure_bucket()
        .await
        .map_err(|err| err.to_string())?;

    let producer = EventProducer::connect(&cfg.broker.brokers, "stix-worker")
        .map_err(|err| err.to_string())?;
    let producer = Arc::new(producer);
    let metrics = MetricsRegistry::new();

    let auto_section = &cfg.auto_approval;
    let effectively_enabled = auto_section.enabled && auto_section.thresholds_are_sane();
    if auto_section.enabled && !auto_section.thresholds_are_sane() {
        tracing::error!(
            auto_confirm_score = auto_section.auto_confirm_score,
            llm_review_score = auto_section.llm_review_score,
            "auto_approval.enabled=true 但門檻不自洽（auto_confirm_score 應該 \
             >= llm_review_score），stix-worker 已強制停用自動核准"
        );
    }
    let llm = OpenAiCompatibleLlmProvider::new(&ai_gateway::OpenAiCompatibleLlmProviderConfig {
        enabled: effectively_enabled && auto_section.llm.enabled,
        base_url: auto_section.llm.base_url.clone(),
        timeout: Duration::from_secs(auto_section.llm.timeout_secs),
        max_concurrent: auto_section.llm.max_concurrent,
        max_retries: 0,
        rate_limit_per_second: None,
    });
    let auto_config = AutoApprovalConfig {
        enabled: effectively_enabled,
        auto_confirm_score: auto_section.auto_confirm_score,
        llm_review_score: auto_section.llm_review_score,
        llm_model: auto_section.llm.model.clone(),
        llm_model_version: auto_section.llm.model_version.clone(),
        llm_temperature: auto_section.llm.temperature,
        llm_max_tokens: auto_section.llm.max_tokens,
    };

    let service = StixWorker::new(
        store.clone(),
        objects.clone(),
        MockEmbeddingProvider::unsupported(),
        llm,
        Some(producer),
        metrics.clone(),
        StixWorkerOptions {
            max_objects_per_tx: cfg.stix_worker.max_objects_per_tx,
            max_export_objects: cfg.stix.max_objects,
            max_auto_merges_per_resolve: auto_section.max_auto_merges_per_resolve,
            auto_approval: auto_config,
        },
    );

    let bind = cfg.stix_worker.bind.clone();
    let health_metrics = metrics.clone();
    let health_store = store.clone();
    let health_objects = objects.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store, health_objects).await {
            tracing::error!(error = %err, "stix-worker health 結束");
        }
    });

    let jobs = JobService::new(store.clone(), None);
    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.stix_worker.consumer_group,
        &[EventTopic::JobDispatched.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.stix_worker.consumer_group,
        bind = %cfg.stix_worker.bind,
        "stix-worker 開始消費 job.dispatched"
    );

    consume_loop(&service, &jobs, &consumer, &metrics).await;
    Ok(())
}

fn connect_objects(cfg: &AppConfig) -> Result<S3ObjectStore, String> {
    let access = cfg
        .storage
        .object
        .access_key_ref
        .resolve()
        .map_err(|err| err.to_string())?;
    let secret = cfg
        .storage
        .object
        .secret_key_ref
        .resolve()
        .map_err(|err| err.to_string())?;
    S3ObjectStore::connect(
        &cfg.storage.object.endpoint,
        &cfg.storage.object.bucket,
        &access,
        &secret,
    )
    .map_err(|err| err.to_string())
}

/// 消費迴圈。逐則處理，沒有批次。
///
/// `job.dispatched`：**執行完（不管成敗）就 commit**。這裡的「失敗」是 job
/// 本身跑失敗，不是消費事件失敗；重送只會讓同一個 job_id 再跑一次
/// `Running → Completed/Failed`，`can_transition` 會擋下不合法的轉移。
async fn consume_loop(
    service: &ProdWorker,
    jobs: &JobService<PostgresCanonicalStore>,
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
) {
    let mut processed = 0_u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，stix-worker 結束");
                break;
            }
            result = consumer.next_envelope(IDLE_POLL) => {
                match result {
                    Ok(envelope) => {
                        match envelope.event_type.as_str() {
                            "job.dispatched" => {
                                handle_job_dispatched(service, jobs, consumer, &envelope).await;
                            }
                            other => {
                                tracing::warn!(
                                    event_type = other,
                                    "stix-worker 訂了不認識的 topic，跳過但仍 commit"
                                );
                                if let Err(err) = consumer.commit_last() {
                                    tracing::warn!(error = %err, "commit offset 失敗");
                                }
                            }
                        }
                        processed += 1;
                        if processed % LAG_PROBE_EVERY == 0 {
                            update_queue_depth(consumer, metrics);
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
}

async fn handle_job_dispatched(
    service: &ProdWorker,
    jobs: &JobService<PostgresCanonicalStore>,
    consumer: &EventConsumer,
    envelope: &core_events::EventEnvelope,
) {
    let outcome = service
        .process_dispatched_job(jobs, &envelope.payload)
        .await;
    log_job_outcome(&envelope.id.to_string(), &outcome);
    if let Err(err) = consumer.commit_last() {
        tracing::warn!(error = %err, "commit offset 失敗");
    }
}

fn log_job_outcome(event_id: &str, outcome: &JobDispatchOutcome) {
    match outcome {
        JobDispatchOutcome::Ignored { job_type } => tracing::debug!(
            job_type,
            event_id,
            "job.dispatched 不是 stix_import，已忽略"
        ),
        JobDispatchOutcome::Completed { job_id } => tracing::info!(
            %job_id,
            event_id,
            "stix_import job 完成"
        ),
        JobDispatchOutcome::Failed { job_id } => tracing::error!(
            %job_id,
            event_id,
            "stix_import job 失敗，已標記 Failed"
        ),
        JobDispatchOutcome::TransitionFailed { job_id } => tracing::error!(
            ?job_id,
            event_id,
            "stix_import job 狀態轉移失敗，仍提交 offset"
        ),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Result<ParsedArgs, String> {
        parse_args(list.iter().map(ToString::to_string))
    }

    #[test]
    fn default_is_consume_mode() {
        assert_eq!(args(&[]).unwrap(), ParsedArgs::Run);
    }

    #[test]
    fn help_is_a_success_path() {
        for flag in ["-h", "--help"] {
            assert_eq!(args(&[flag]).unwrap(), ParsedArgs::Help, "{flag}");
        }
    }

    #[test]
    fn unknown_flag_shows_usage() {
        let err = args(&["--rebuild"]).unwrap_err();
        assert!(err.contains("不認得的參數"), "{err}");
        assert!(err.contains("osint-stix-worker"), "{err}");
    }
}
