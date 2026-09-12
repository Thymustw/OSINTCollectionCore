//! `osint-indexer`：訂閱 `entity.extracted`，把 Document + entities 寫進 OpenSearch。
//!
//! 兩種模式：
//!
//! ```text
//! osint-indexer                     常駐消費（預設）
//! osint-indexer --rebuild           從 PostgreSQL 補齊 index 後結束
//! osint-indexer --rebuild --drop    先刪掉 index 再從零重建後結束
//! ```
//!
//! 沒有用 clap：這支 binary 只有兩個旗標，為它多一個相依不划算
//! （`osint-cli` 才是有子命令的那一支）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_observability::{MetricsRegistry, init_tracing};
use indexer::batch::{BatchController, FlushReason, backpressure};
use indexer::service::RebuildOptions;
use indexer::{IndexBounds, Indexer, PrepareOutcome, serve_health};
use storage_core::SearchDocument;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, verify_not_opencti_search,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

/// 批次空的時候一次 poll 等多久。
const IDLE_POLL: Duration = Duration::from_secs(30);
/// 每幾次 flush 量一次 consumer lag。
///
/// `fetch_watermarks` 是一次網路往返，每則訊息都量會讓它變成主要成本。
const LAG_PROBE_EVERY: u32 = 10;
/// 量 lag 的逾時。量不到就當作沒有 backpressure，不要讓它擋住索引。
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-indexer 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Args {
    rebuild: bool,
    drop_index: bool,
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
        drop_index: false,
    };
    for arg in args {
        match arg.as_str() {
            "--rebuild" => parsed.rebuild = true,
            "--drop" | "--drop-index" => parsed.drop_index = true,
            "-h" | "--help" => {
                return Ok(ParsedArgs::Help);
            }
            other => {
                return Err(format!("不認得的參數 `{other}`。\n{HELP}"));
            }
        }
    }
    if parsed.drop_index && !parsed.rebuild {
        return Err(
            "--drop 必須搭配 --rebuild 使用。單獨刪掉 index 會讓搜尋直接空掉，\
             而且沒有任何東西會把它補回來。\n"
                .to_string()
                + HELP,
        );
    }
    Ok(ParsedArgs::Run(parsed))
}

const HELP: &str = "用法：
  osint-indexer                  訂閱 entity.extracted 常駐索引
  osint-indexer --rebuild        從 PostgreSQL 補齊 index（既有文件會被覆寫）後結束
  osint-indexer --rebuild --drop 先刪掉 index 再從零重建後結束
                                 （mapping 有破壞性變更時必須用這個）
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

    let search = connect_search(&cfg.storage.search.url).await?;

    let bounds = IndexBounds {
        batch_size: cfg.indexer.batch_size.max(1),
        batch_timeout: Duration::from_millis(cfg.indexer.batch_timeout_ms.max(1)),
        max_field_bytes: cfg.indexer.max_field_bytes.max(1),
        bulk_max_retries: cfg.indexer.bulk_max_retries,
        lag_threshold: cfg.indexer.lag_threshold.max(1),
        backpressure_sleep: Duration::from_millis(cfg.indexer.backpressure_sleep_ms),
    };
    let metrics = MetricsRegistry::new();

    // rebuild 不發事件也不需要 producer 連得上；常駐模式則需要。
    let producer = if args.rebuild {
        None
    } else {
        Some(Arc::new(
            EventProducer::connect(&cfg.broker.brokers, "indexer")
                .map_err(|err| err.to_string())?,
        ))
    };

    let service = Indexer::new(
        store.clone(),
        search.clone(),
        producer,
        metrics.clone(),
        cfg.indexer.index.clone(),
        bounds,
    );

    if args.rebuild {
        let report = service
            .rebuild(RebuildOptions {
                drop_index: args.drop_index,
                page_size: 100,
            })
            .await
            .map_err(|err| err.to_string())?;
        let count = service
            .indexed_count()
            .await
            .map_err(|err| err.to_string())?;
        println!(
            "rebuild 完成：掃過 {} 筆 canonical Document、跳過 {} 筆 duplicate、\
             寫入 {} 筆、失敗 {} 筆；index `{}` 現有 {} 筆文件。",
            report.scanned,
            report.skipped_duplicates,
            report.indexed,
            report.failed.len(),
            cfg.indexer.index,
            count
        );
        if !report.failed.is_empty() {
            return Err(format!(
                "有 {} 筆文件寫入 index 失敗（id：{:?}），它們不會出現在搜尋結果裡。\
                 請看上面的 error log 判斷原因；mapping 不符時要先改 \
                 crates/indexer/src/schema.rs 再用 --rebuild --drop 重來",
                report.failed.len(),
                report.failed
            ));
        }
        return Ok(());
    }

    service
        .ensure_index()
        .await
        .map_err(|err| err.to_string())?;

    let bind = cfg.indexer.bind.clone();
    let health_metrics = metrics.clone();
    let health_store = store.clone();
    let health_search = search.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store, health_search).await {
            tracing::error!(error = %err, "indexer health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.indexer.consumer_group,
        &[EventTopic::EntityExtracted.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.indexer.consumer_group,
        index = %cfg.indexer.index,
        batch_size = bounds.batch_size,
        batch_timeout_ms = bounds.batch_timeout.as_millis() as u64,
        lag_threshold = bounds.lag_threshold,
        "indexer 開始消費 entity.extracted"
    );

    consume_loop(&service, &consumer, &metrics, bounds).await;
    Ok(())
}

/// 連 OpenSearch 並確認它真的是 OpenSearch。
///
/// **這個檢查不是形式。** 本工作站的 9200 是 OpenCTI 的 Elasticsearch；
/// 少了它，設定寫錯一個埠號就會開始往別人的叢集寫資料，而且一路都不會報錯。
async fn connect_search(url: &str) -> Result<OpenSearchStore, String> {
    verify_not_opencti_search(url).map_err(|err| err.to_string())?;
    let store = OpenSearchStore::connect(url).map_err(|err| err.to_string())?;
    let info = store.cluster_info().await.map_err(|err| {
        format!("連不上 OpenSearch（{url}）：{err}。請先 `make compose-up` 並確認 OPENSEARCH_URL")
    })?;
    assert_opensearch_identity(&info).map_err(|err| err.to_string())?;
    tracing::info!(
        url,
        cluster = ?info.get("cluster_name"),
        version = ?info.pointer("/version/number"),
        "OpenSearch 身分驗證通過"
    );
    Ok(store)
}

/// 消費迴圈。批次累積 → flush → 提交 offset。
///
/// # offset 只在 flush 成功之後才提交
///
/// 先提交再送出的話，flush 失敗（或行程中途被殺）時那一批文件會**永遠不進 index**，
/// 而且不會有任何跡象——offset 已經往前走了，broker 不會再送一次。
/// 反過來（先送出再提交）最壞只是重送，而 `_id` 是 Document.id，重送是覆寫。
async fn consume_loop(
    service: &Indexer,
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
    bounds: IndexBounds,
) {
    let mut controller = BatchController::new(bounds);
    let mut batch: Vec<SearchDocument> = Vec::with_capacity(bounds.batch_size as usize);
    let mut ids: Vec<Uuid> = Vec::with_capacity(bounds.batch_size as usize);
    let mut flushes: u32 = 0;

    loop {
        let timeout = controller.poll_timeout(batch.len(), IDLE_POLL, Instant::now());
        let mut reason = None;

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!(pending = batch.len(), "收到 SIGINT，把手上的批次送完再結束");
                reason = Some(FlushReason::Shutdown);
            }
            result = consumer.next_envelope(timeout) => {
                match result {
                    Ok(envelope) => {
                        match handle_envelope(service, &envelope.payload).await {
                            Ok(Some(document)) => {
                                if let Ok(id) = Uuid::parse_str(&document.id) {
                                    ids.push(id);
                                }
                                batch.push(*document);
                                controller.record_push(Instant::now());
                            }
                            Ok(None) => {}
                            Err(err) => tracing::error!(
                                error = %err,
                                event_id = %envelope.id,
                                "準備索引文件失敗，跳過這則事件。\
                                 該文件不會進 index；重送事件或跑 `osint-indexer --rebuild` 可補回"
                            ),
                        }
                        reason = controller.should_flush(batch.len(), Instant::now());
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {
                        // 逾時本身就是「該把不滿一批的送出去了」的訊號。
                        reason = controller.should_flush(batch.len(), Instant::now())
                            .or(if batch.is_empty() { None } else { Some(FlushReason::Timeout) });
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "消費 entity.extracted 失敗");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }

        let shutting_down = reason == Some(FlushReason::Shutdown);
        if let Some(reason) = reason {
            if !batch.is_empty() {
                flush_once(service, consumer, &mut batch, &mut ids, reason).await;
                controller.reset();
                flushes = flushes.wrapping_add(1);
                apply_backpressure(consumer, metrics, bounds, flushes).await;
            } else if shutting_down {
                // 沒有待送的批次，但仍要把已消費過的 offset 提交掉。
                if let Err(err) = consumer.commit_last() {
                    tracing::warn!(error = %err, "關閉前 commit offset 失敗");
                }
            }
        }
        if shutting_down {
            break;
        }
    }
}

/// 一則事件 → 要進批次的文件（duplicate／找不到時回 `None`）。
async fn handle_envelope(
    service: &Indexer,
    payload: &serde_json::Value,
) -> Result<Option<Box<SearchDocument>>, indexer::IndexerError> {
    let document_id = Indexer::document_id_from_payload(payload)?;
    match service.prepare(document_id).await? {
        PrepareOutcome::Ready(document) => Ok(Some(document)),
        PrepareOutcome::RemovedDuplicate { .. } | PrepareOutcome::Missing { .. } => Ok(None),
    }
}

async fn flush_once(
    service: &Indexer,
    consumer: &EventConsumer,
    batch: &mut Vec<SearchDocument>,
    ids: &mut Vec<Uuid>,
    reason: FlushReason,
) {
    let submitted = batch.len();
    let documents = std::mem::take(batch);
    let document_ids = std::mem::take(ids);
    match service.flush(documents).await {
        Ok(report) => {
            tracing::info!(
                submitted,
                indexed = report.indexed,
                permanent_failures = report.permanent_failures.len(),
                retries = report.retries,
                ?reason,
                "bulk 索引完成"
            );
            if let Err(err) = service.publish_completed(&document_ids, &report).await {
                tracing::warn!(error = %err, "發 search.index.completed 失敗（文件已經寫進 index）");
            }
            // 永久性失敗已經在 service 裡記了 error log 與 metric。
            // 仍然提交 offset：重送同一則事件會得到同樣的結果，
            // 只會把 partition 卡死。要補救請跑 `osint-indexer --rebuild`。
            if let Err(err) = consumer.commit_last() {
                tracing::warn!(error = %err, "commit offset 失敗");
            }
        }
        Err(err) => {
            // **刻意不提交 offset**：這一批完全沒進 index，讓 broker 重送。
            tracing::error!(
                error = %err,
                submitted,
                "bulk 索引失敗，不提交 offset，這批事件會被重送"
            );
        }
    }
}

/// 量 lag、寫 gauge、必要時降速。
async fn apply_backpressure(
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
    bounds: IndexBounds,
    flushes: u32,
) {
    if flushes % LAG_PROBE_EVERY != 0 {
        return;
    }
    let lag = match consumer.consumer_lag(LAG_TIMEOUT) {
        Ok(lag) => lag,
        Err(err) => {
            // 量不到 lag 不該擋住索引。但要說出來——靜默跳過等於 backpressure
            // 悄悄失效，之後誰也不知道它其實沒在運作。
            tracing::warn!(error = %err, "量不到 consumer lag，本輪不做 backpressure 判斷");
            return;
        }
    };
    metrics.set_queue_depth(lag);
    let decision = backpressure(lag, bounds);
    if decision.active {
        metrics.inc("osint_indexer_backpressure_total", 1);
        tracing::warn!(
            lag,
            threshold = bounds.lag_threshold,
            sleep_ms = decision.sleep.as_millis() as u64,
            "consumer lag 超過門檻，批次之間降速。\
             瓶頸多半在 OpenSearch；要讓 lag 下降請降低上游採集速率——\
             V0.1 沒有自動降速，請手動調 [collector].tick_secs／global_inflight，\
             或先停用部分 connector"
        );
        tokio::time::sleep(decision.sleep).await;
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
                drop_index: false
            })
        );
    }

    #[test]
    fn rebuild_and_drop_parse() {
        assert_eq!(
            args(&["--rebuild", "--drop"]).unwrap(),
            ParsedArgs::Run(Args {
                rebuild: true,
                drop_index: true
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
        // 單獨 --drop 會讓搜尋直接空掉且沒有東西補回來。
        let err = args(&["--drop"]).unwrap_err();
        assert!(err.contains("--rebuild"), "{err}");
    }

    #[test]
    fn unknown_flag_shows_usage() {
        let err = args(&["--reindex"]).unwrap_err();
        assert!(err.contains("不認得的參數"), "{err}");
        assert!(err.contains("osint-indexer --rebuild"), "{err}");
    }
}
