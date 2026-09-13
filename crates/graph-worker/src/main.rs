//! `osint-graph-worker`：訂閱 `relationship.changed` 與 `job.dispatched`。
//!
//! 兩種模式：
//!
//! ```text
//! osint-graph-worker                     常駐消費（預設）
//! osint-graph-worker --rebuild           從 PostgreSQL 補齊圖投影後結束
//! osint-graph-worker --rebuild --drop    先清空 :Entity 再從零重建後結束
//! ```
//!
//! 常駐模式只執行 `job_type=graph_rebuild` 的 job；其他 type 忽略並 commit。
//! API 觸發的 rebuild **永遠** `drop_graph=false`（`--drop` 只留給 CLI）。
//!
//! 沒有用 clap：這支 binary 只有兩個旗標，為它多一個相依不划算
//! （`osint-cli` 才是有子命令的那一支）。

use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventTopic};
use core_jobs::JobService;
use core_observability::{MetricsRegistry, init_tracing};
use graph_worker::service::{GraphWorker, RebuildOptions};
use graph_worker::{JobDispatchOutcome, ProcessOutcome, serve_health};
use storage_core::conformance::load_workspace_dotenv;
use storage_neo4j::Neo4jStore;
use storage_postgres::PostgresCanonicalStore;

/// 一次 poll 等多久。沒有批次，逾時只是為了能回應 SIGINT 與量 lag。
const IDLE_POLL: Duration = Duration::from_secs(30);
/// 每幾則事件量一次 consumer lag。
const LAG_PROBE_EVERY: u64 = 10;
/// 量 lag 的逾時。量不到就當作沒有 lag，不要讓它擋住投影。
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-graph-worker 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Args {
    rebuild: bool,
    drop_graph: bool,
}

/// 參數解析結果。`Help` 是**成功**路徑——回 exit 1 並記一行 ERROR log
/// 會讓「我只是想看用法」在 CI 裡變成一次失敗。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedArgs {
    Run(Args),
    Help,
}

fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<ParsedArgs, String> {
    let mut parsed = Args {
        rebuild: false,
        drop_graph: false,
    };
    for arg in args {
        match arg.as_str() {
            "--rebuild" => parsed.rebuild = true,
            "--drop" | "--drop-graph" => parsed.drop_graph = true,
            "-h" | "--help" => {
                return Ok(ParsedArgs::Help);
            }
            other => {
                return Err(format!("不認得的參數 `{other}`。\n{HELP}"));
            }
        }
    }
    if parsed.drop_graph && !parsed.rebuild {
        return Err(
            "--drop 必須搭配 --rebuild 使用。單獨清空圖會讓圖查詢直接空掉，\
             而且沒有任何東西會把它補回來。\n"
                .to_string()
                + HELP,
        );
    }
    Ok(ParsedArgs::Run(parsed))
}

const HELP: &str = "用法：
  osint-graph-worker                  訂閱 relationship.changed 常駐投影
  osint-graph-worker --rebuild        從 PostgreSQL 補齊圖（既有邊會被覆寫）後結束
  osint-graph-worker --rebuild --drop 先清空 :Entity 再從零重建後結束
                                      （Entity 已從 Postgres 刪除、圖上殘留幽靈節點時必須用這個）
設定來源：config/default.toml → OSINT_CONFIG_FILE → OSINT__* 環境變數。";

async fn run() -> Result<(), String> {
    let args = match parse_args(std::env::args().skip(1))? {
        ParsedArgs::Help => {
            println!("{HELP}");
            return Ok(());
        }
        ParsedArgs::Run(args) => args,
    };
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

    let graph = connect_graph(&cfg).await?;
    let metrics = MetricsRegistry::new();

    let service = GraphWorker::new(
        store.clone(),
        graph.clone(),
        metrics.clone(),
        cfg.graph_worker.projection.clone(),
    );

    if args.rebuild {
        let report = service
            .rebuild(RebuildOptions {
                drop_graph: args.drop_graph,
                page_size: cfg.graph_worker.page_size.max(1),
            })
            .await
            .map_err(|err| err.to_string())?;
        println!(
            "rebuild 完成：掃過 {} 筆 Relationship、寫入 {} 筆 Entity→Entity 邊、\
             跳過 {} 筆非 Entity 端點、失敗 {} 筆；投影 `{}`。",
            report.scanned,
            report.applied,
            report.skipped_non_entity,
            report.failed,
            cfg.graph_worker.projection,
        );
        if report.failed > 0 {
            return Err(format!(
                "有 {} 筆邊寫入 Neo4j 失敗，它們不會出現在圖上。\
                 請看上面的 error log 判斷原因；修好後再跑 --rebuild",
                report.failed
            ));
        }
        return Ok(());
    }

    let bind = cfg.graph_worker.bind.clone();
    let health_metrics = metrics.clone();
    let health_store = store.clone();
    let health_graph = graph.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store, health_graph).await {
            tracing::error!(error = %err, "graph-worker health 結束");
        }
    });

    let jobs = JobService::new(store.clone(), None);

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.graph_worker.consumer_group,
        &[
            EventTopic::RelationshipChanged.as_str(),
            EventTopic::JobDispatched.as_str(),
        ],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.graph_worker.consumer_group,
        projection = %cfg.graph_worker.projection,
        "graph-worker 開始消費 relationship.changed 與 job.dispatched"
    );

    consume_loop(
        &service,
        &jobs,
        &consumer,
        &metrics,
        cfg.graph_worker.page_size.max(1),
    )
    .await;
    Ok(())
}

/// 連 Neo4j。呼叫方式與 `crates/core-api/src/main.rs` 的 `connect_graph` 相同，
/// 但是**獨立的一份連線**——不要想辦法共用 core-api 那份。
async fn connect_graph(cfg: &AppConfig) -> Result<Neo4jStore, String> {
    let password = cfg
        .storage
        .graph
        .password_secret_ref
        .resolve()
        .map_err(|e| e.to_string())?;
    Neo4jStore::connect(
        &cfg.storage.graph.bolt_uri,
        &cfg.storage.graph.username,
        &password,
        cfg.storage.graph.pool_max,
    )
    .await
    .map_err(|e| e.to_string())
}

/// 消費迴圈。逐則處理，沒有批次。同一個 consumer group 訂兩個 topic，
/// 靠 `EventEnvelope.event_type` 分流（那個欄位就是 topic 字串）。
///
/// # offset 提交規則因事件而異
///
/// `relationship.changed`：真的失敗（Neo4j 連不上、非預期 `StorageError`）
/// **不 commit**，讓 broker 重送。先提交再寫的話，寫入失敗或行程被殺時那則邊
/// 會永遠不進圖，而且沒有任何跡象。`SkippedNonEntity`／`SkippedRace` 是預期
/// 行為，重送結果一樣，所以提交。
///
/// `job.dispatched`：**執行完（不管成敗）就 commit**。這裡的「失敗」是 job
/// 本身跑失敗，不是消費事件失敗；重送只會讓同一個 job_id 再跑一次
/// `Running → Completed/Failed`，`can_transition` 會擋下不合法的轉移並回錯，
/// 這則事件變成處理失敗、offset 又不 commit……迴圈。already-failed 的 job
/// 重送也跑不起來。
async fn consume_loop(
    service: &GraphWorker<PostgresCanonicalStore, Neo4jStore>,
    jobs: &JobService<PostgresCanonicalStore>,
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
    page_size: u32,
) {
    let mut processed = 0_u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                // 不在這裡 commit：上一則成功時已經 commit 過；上一則失敗時
                // 必須留給 broker 重送。關機時再 commit 一次會把失敗那則吃掉。
                tracing::info!("收到 SIGINT，graph-worker 結束");
                break;
            }
            result = consumer.next_envelope(IDLE_POLL) => {
                match result {
                    Ok(envelope) => {
                        match envelope.event_type.as_str() {
                            "relationship.changed" => {
                                handle_relationship_changed(service, consumer, metrics, &envelope).await;
                            }
                            "job.dispatched" => {
                                handle_job_dispatched(service, jobs, consumer, page_size, &envelope).await;
                            }
                            other => {
                                tracing::warn!(
                                    event_type = other,
                                    "graph-worker 訂了不認識的 topic，跳過但仍 commit"
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

async fn handle_relationship_changed(
    service: &GraphWorker<PostgresCanonicalStore, Neo4jStore>,
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
    envelope: &core_events::EventEnvelope,
) {
    match service.process_change(&envelope.payload).await {
        Ok(outcome) => {
            if let ProcessOutcome::Applied {
                relationship_id,
                last_seen,
            } = &outcome
            {
                service
                    .record_checkpoint(Some(*last_seen), Some(*relationship_id))
                    .await;
            }
            log_outcome(&envelope.id.to_string(), &outcome);
            if let Err(err) = consumer.commit_last() {
                tracing::warn!(error = %err, "commit offset 失敗");
            }
        }
        Err(err) => {
            metrics.inc("osint_graph_worker_errors_total", 1);
            tracing::error!(
                error = %err,
                event_id = %envelope.id,
                "處理 relationship.changed 失敗，不提交 offset，這則事件會被重送"
            );
        }
    }
}

async fn handle_job_dispatched(
    service: &GraphWorker<PostgresCanonicalStore, Neo4jStore>,
    jobs: &JobService<PostgresCanonicalStore>,
    consumer: &EventConsumer,
    page_size: u32,
    envelope: &core_events::EventEnvelope,
) {
    let outcome = service
        .process_dispatched_job(jobs, &envelope.payload, page_size)
        .await;
    log_job_outcome(&envelope.id.to_string(), &outcome);
    // 執行完（不管成敗）就 commit。理由見 consume_loop 的文件註解。
    if let Err(err) = consumer.commit_last() {
        tracing::warn!(error = %err, "commit offset 失敗");
    }
}

fn log_job_outcome(event_id: &str, outcome: &JobDispatchOutcome) {
    match outcome {
        JobDispatchOutcome::Ignored { job_type } => tracing::debug!(
            job_type,
            event_id,
            "job.dispatched 不是 graph_rebuild，已忽略"
        ),
        JobDispatchOutcome::Completed { job_id } => tracing::info!(
            %job_id,
            event_id,
            "graph_rebuild job 完成"
        ),
        JobDispatchOutcome::Failed { job_id } => tracing::error!(
            %job_id,
            event_id,
            "graph_rebuild job 失敗，已標記 Failed"
        ),
        JobDispatchOutcome::TransitionFailed { job_id } => tracing::error!(
            ?job_id,
            event_id,
            "graph_rebuild job 狀態轉移失敗，仍提交 offset"
        ),
    }
}

fn log_outcome(event_id: &str, outcome: &ProcessOutcome) {
    match outcome {
        ProcessOutcome::Applied {
            relationship_id, ..
        } => tracing::info!(
            %relationship_id,
            event_id,
            "圖邊已 upsert"
        ),
        ProcessOutcome::SkippedNonEntity { relationship_id } => tracing::debug!(
            %relationship_id,
            event_id,
            "這條邊有一端不是 Entity，不投影進圖"
        ),
        ProcessOutcome::SkippedRace { relationship_id } => tracing::info!(
            %relationship_id,
            event_id,
            "upserted 事件對應的 Relationship 已不在 Postgres（race），已當刪除處理"
        ),
        ProcessOutcome::Deleted { relationship_id } => tracing::info!(
            %relationship_id,
            event_id,
            "圖邊已刪"
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
        assert_eq!(
            args(&[]).unwrap(),
            ParsedArgs::Run(Args {
                rebuild: false,
                drop_graph: false
            })
        );
    }

    #[test]
    fn rebuild_and_drop_parse() {
        assert_eq!(
            args(&["--rebuild", "--drop"]).unwrap(),
            ParsedArgs::Run(Args {
                rebuild: true,
                drop_graph: true
            })
        );
    }

    #[test]
    fn help_is_a_success_path() {
        // `--help` 回 Err 的話，main 會以 exit 1 結束並記一行 ERROR log——
        // 「我只是想看用法」在 CI 裡會變成一次失敗。
        for flag in ["-h", "--help"] {
            assert_eq!(args(&[flag]).unwrap(), ParsedArgs::Help, "{flag}");
        }
    }

    #[test]
    fn drop_without_rebuild_is_rejected() {
        // 單獨 --drop 會讓圖直接空掉且沒有東西補回來。
        let err = args(&["--drop"]).unwrap_err();
        assert!(err.contains("--rebuild"), "{err}");
    }

    #[test]
    fn unknown_flag_shows_usage() {
        let err = args(&["--reindex"]).unwrap_err();
        assert!(err.contains("不認得的參數"), "{err}");
        assert!(err.contains("osint-graph-worker --rebuild"), "{err}");
    }
}
