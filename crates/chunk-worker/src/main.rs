//! `osint-chunk-worker`：訂閱 dedup.completed，把非重複文件切成段落（ADR-016 階段 5 M1）。
//!
//! ```text
//! osint-chunk-worker              常駐消費 dedup.completed
//! osint-chunk-worker --rebuild    從 PostgreSQL 重切全部非重複文件後結束（不發事件）
//! ```
//!
//! 沒有用 clap：只有一個旗標，理由同 osint-embedding-worker。

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use chunk_worker::{ChunkWorker, serve_health};
use chunker::{Chunker, ChunkerConfig, E5_TOKENIZER_SHA256, E5Tokenizer};
use core_config::{AppConfig, ChunkWorkerSection};
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_model::FailedEvent;
use core_observability::{MetricsRegistry, init_tracing};
use storage_core::RelationalStore;
use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::PostgresCanonicalStore;

/// 每處理幾則事件量一次 consumer lag（同 entity-worker）。
const LAG_PROBE_EVERY: u64 = 10;
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

const USAGE: &str = "用法：
  osint-chunk-worker            常駐消費 dedup.completed
  osint-chunk-worker --rebuild  從 PostgreSQL 重切全部非重複文件後結束（不發 document.chunked）";

#[derive(Debug, PartialEq, Eq)]
enum ParsedArgs {
    Run { rebuild: bool },
    Help,
}

fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<ParsedArgs, String> {
    let mut rebuild = false;
    for arg in args {
        match arg.as_str() {
            "--rebuild" => rebuild = true,
            "-h" | "--help" => return Ok(ParsedArgs::Help),
            other => return Err(format!("不認得的參數 `{other}`。\n{USAGE}")),
        }
    }
    Ok(ParsedArgs::Run { rebuild })
}

fn update_queue_depth(consumer: &EventConsumer, metrics: &MetricsRegistry) {
    match consumer.consumer_lag(LAG_TIMEOUT) {
        Ok(lag) => metrics.set_queue_depth(lag),
        Err(err) => {
            tracing::warn!(error = %err, "量不到 consumer lag，osint_queue_depth 這輪不更新")
        }
    }
}

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    let args = match parse_args(std::env::args().skip(1)) {
        Ok(ParsedArgs::Help) => {
            println!("{USAGE}");
            return;
        }
        Ok(ParsedArgs::Run { rebuild }) => rebuild,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    if let Err(err) = run(args).await {
        tracing::error!(error = %err, "osint-chunk-worker 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn build_chunker(section: &ChunkWorkerSection) -> Result<Chunker, String> {
    let tokenizer = E5Tokenizer::load(Path::new(&section.tokenizer_path), E5_TOKENIZER_SHA256)
        .map_err(|err| {
            format!(
                "{err}。本機請先跑 scripts/fetch-e5-tokenizer.sh，或用 \
                 OSINT__CHUNK_WORKER__TOKENIZER_PATH 指向檔案"
            )
        })?;
    let config = ChunkerConfig {
        target_tokens: section.target_tokens,
        overlap_tokens: section.overlap_tokens,
        max_model_tokens: section.max_model_tokens,
        max_header_tokens: section.max_header_tokens,
        max_source_bytes: section.max_source_bytes,
    };
    Chunker::new(Arc::new(tokenizer), config).map_err(|err| err.to_string())
}

async fn run(rebuild: bool) -> Result<(), String> {
    let cfg = AppConfig::load().map_err(|err| err.to_string())?;
    let section = cfg.chunk_worker.clone();
    // tokenizer 先載：檔案缺或 hash 不符時，連 DB 都不必連就該失敗。
    let chunker = Arc::new(build_chunker(&section)?);

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

    if rebuild {
        let service = ChunkWorker::new(store, chunker, None, metrics);
        let mut stop = core_observability::shutdown_watch();
        let started = std::time::Instant::now();
        let report = service
            .rebuild(section.rebuild_page_size, &mut stop)
            .await
            .map_err(|err| err.to_string())?;
        tracing::info!(
            ?report,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "rebuild 結束"
        );
        println!(
            "rebuild {}：掃過 {} 份，寫入 {} 份（{} 段），未變 {} 份，跳過重複 {} 份，失敗 {} 份，耗時 {:.1}s",
            if report.interrupted {
                "被中斷"
            } else {
                "完成"
            },
            report.scanned,
            report.chunked,
            report.chunks_written,
            report.unchanged,
            report.skipped_duplicate,
            report.failed,
            started.elapsed().as_secs_f64()
        );
        if report.failed > 0 {
            return Err(format!(
                "{} 份文件切段失敗，見上方 error log",
                report.failed
            ));
        }
        return Ok(());
    }

    let producer = EventProducer::connect(&cfg.broker.brokers, "chunk-worker")
        .map_err(|err| err.to_string())?;
    let service = ChunkWorker::new(
        store.clone(),
        chunker,
        Some(Arc::new(producer)),
        metrics.clone(),
    );

    let bind = section.bind.clone();
    let health_store = store.clone();
    let health_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store).await {
            tracing::error!(error = %err, "chunk-worker health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &section.consumer_group,
        &[EventTopic::DedupCompleted.as_str()],
    )
    .map_err(|err| err.to_string())?;
    tracing::info!(
        group = %section.consumer_group,
        target_tokens = section.target_tokens,
        overlap_tokens = section.overlap_tokens,
        "chunk-worker 開始消費 dedup.completed"
    );

    // 循序處理（同 entity-worker）：要更高吞吐是加 partition 與實例，不是對外部輸入無界 spawn
    // （CLAUDE.md §6）。切段本身走 spawn_blocking，不卡 executor。
    let mut processed = 0_u64;
    let mut stop = core_observability::shutdown_watch();
    loop {
        tokio::select! {
            _ = stop.changed() => {
                tracing::info!("收到停止訊號（SIGTERM／SIGINT），chunk-worker 結束");
                break;
            }
            result = consumer.next_envelope(Duration::from_secs(30)) => {
                match result {
                    Ok(envelope) => {
                        match service.handle_payload(&envelope.payload).await {
                            Ok(outcome) => tracing::debug!(
                                document_id = %outcome.document_id(),
                                event_id = %envelope.id,
                                ?outcome,
                                "切段處理完畢"
                            ),
                            Err(err) => {
                                // 毒訊息不卡 partition：記 failed_event（ADR-008）後照常提交 offset。
                                // 段落沒寫入，重送或 --rebuild 會再試。
                                tracing::error!(error = %err, event_id = %envelope.id, "切段失敗，仍提交 offset");
                                if let Some((topic, partition, offset)) = consumer.last_coordinates() {
                                    let failed = FailedEvent {
                                        id: uuid::Uuid::now_v7(),
                                        topic,
                                        partition,
                                        offset,
                                        consumer_group: section.consumer_group.clone(),
                                        failure_reason: err.to_string(),
                                        attempt_count: 1,
                                        envelope: serde_json::to_value(&envelope)
                                            .unwrap_or(serde_json::json!({})),
                                        first_seen: chrono::Utc::now(),
                                        last_seen: chrono::Utc::now(),
                                        replayed_at: None,
                                    };
                                    if let Err(store_err) = store.put_failed_event(&failed).await {
                                        tracing::warn!(error = %store_err, "寫入 failed_events 失敗，事件仍會照常提交 offset");
                                    }
                                }
                            }
                        }
                        if let Err(err) = consumer.commit_last() {
                            tracing::warn!(error = %err, "commit offset 失敗");
                        }
                        processed += 1;
                        if processed.is_multiple_of(LAG_PROBE_EVERY) {
                            update_queue_depth(&consumer, &metrics);
                        }
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {
                        update_queue_depth(&consumer, &metrics);
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "消費 dedup.completed 失敗");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(v: &[&str]) -> Result<ParsedArgs, String> {
        parse_args(v.iter().map(|s| (*s).to_string()))
    }

    #[test]
    fn parses_flags() {
        assert_eq!(args(&[]).unwrap(), ParsedArgs::Run { rebuild: false });
        assert_eq!(
            args(&["--rebuild"]).unwrap(),
            ParsedArgs::Run { rebuild: true }
        );
        assert_eq!(args(&["--help"]).unwrap(), ParsedArgs::Help);
        let err = args(&["--drop"]).unwrap_err();
        assert!(err.contains("--drop") && err.contains("--rebuild"), "{err}");
    }
}
