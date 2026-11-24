//! `osint-chunk-embedder`：訂閱 document.chunked，把段落向量寫進 OpenSearch `osint-chunks`。
//!
//! ```text
//! osint-chunk-embedder                  常駐消費 document.chunked
//! osint-chunk-embedder --rebuild        依 PostgreSQL 段落表重建全部向量後結束（覆寫既有）
//! osint-chunk-embedder --rebuild --drop 先刪掉 osint-chunks 再從零重建
//! ```

use std::sync::Arc;
use std::time::Duration;

use chunk_embedder::{
    ChunkEmbedder, ChunkEmbedderError, EmbedderConfig, InferenceClient, schema, serve_health,
};
use core_config::{AppConfig, ChunkEmbedderSection};
use core_events::{EventConsumer, EventTopic};
use core_model::FailedEvent;
use core_observability::{MetricsRegistry, init_tracing};
use storage_core::RelationalStore;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, verify_not_opencti_search,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;

const LAG_PROBE_EVERY: u64 = 10;
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

const USAGE: &str = "用法：
  osint-chunk-embedder                   常駐消費 document.chunked
  osint-chunk-embedder --rebuild         依 PostgreSQL 段落表重建全部向量後結束
  osint-chunk-embedder --rebuild --drop  先刪掉 osint-chunks 再從零重建";

#[derive(Debug, PartialEq, Eq)]
enum ParsedArgs {
    Run { rebuild: bool, drop: bool },
    Help,
}

fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<ParsedArgs, String> {
    let (mut rebuild, mut drop) = (false, false);
    for arg in args {
        match arg.as_str() {
            "--rebuild" => rebuild = true,
            "--drop" => drop = true,
            "-h" | "--help" => return Ok(ParsedArgs::Help),
            other => return Err(format!("不認得的參數 `{other}`。\n{USAGE}")),
        }
    }
    if drop && !rebuild {
        return Err(format!(
            "--drop 必須搭配 --rebuild。單獨刪掉 osint-chunks 會讓段落向量直接消失。\n{USAGE}"
        ));
    }
    Ok(ParsedArgs::Run { rebuild, drop })
}

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    let (rebuild, drop) = match parse_args(std::env::args().skip(1)) {
        Ok(ParsedArgs::Help) => {
            println!("{USAGE}");
            return;
        }
        Ok(ParsedArgs::Run { rebuild, drop }) => (rebuild, drop),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };
    if let Err(err) = run(rebuild, drop).await {
        tracing::error!(error = %err, "osint-chunk-embedder 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn embedder_config(s: &ChunkEmbedderSection) -> Result<EmbedderConfig, String> {
    if s.batch_size == 0 || s.batch_size > 32 {
        return Err(format!(
            "chunk_embedder.batch_size={} 必須在 1..=32（推論服務單次上限 32）",
            s.batch_size
        ));
    }
    // NaN 也要擋（NaN 與任何數比較都是 false，會讓慢批偵測永遠不觸發）。
    if s.slow_factor.is_nan() || s.slow_factor <= 1.0 {
        return Err(format!(
            "chunk_embedder.slow_factor={} 必須 > 1",
            s.slow_factor
        ));
    }
    Ok(EmbedderConfig {
        index: s.index.clone(),
        batch_size: s.batch_size,
        backoff_initial: Duration::from_millis(s.backoff_initial_ms.max(1)),
        backoff_max: Duration::from_millis(s.backoff_max_ms.max(s.backoff_initial_ms.max(1))),
        slow_factor: s.slow_factor,
        slow_pause: Duration::from_millis(s.slow_pause_ms),
    })
}

async fn run(rebuild: bool, drop: bool) -> Result<(), String> {
    indexer::script::init().map_err(|err| format!("簡繁轉換器初始化失敗：{err}"))?;
    let cfg = AppConfig::load().map_err(|err| err.to_string())?;
    let section = cfg.chunk_embedder.clone();
    let embedder_cfg = embedder_config(&section)?;

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

    let url = &cfg.storage.search.url;
    verify_not_opencti_search(url).map_err(|err| err.to_string())?;
    let search = OpenSearchStore::connect(url).map_err(|err| err.to_string())?;
    let info = search.cluster_info().await.map_err(|err| {
        format!("連不上 OpenSearch（{url}）：{err}。請先 `make compose-up` 並確認 OPENSEARCH_URL")
    })?;
    assert_opensearch_identity(&info).map_err(|err| err.to_string())?;

    if drop {
        let existed = search
            .delete_index(&section.index)
            .await
            .map_err(|err| err.to_string())?;
        tracing::warn!(index = %section.index, existed, "--drop：已刪除段落向量 index");
    }
    let created = search
        .ensure_index_with(
            &section.index,
            &schema::index_settings(),
            &schema::index_mappings(),
        )
        .await
        .map_err(|err| {
            format!(
                "建立／更新 {} 失敗：{err}。mapping 有破壞性變更時要 --rebuild --drop",
                section.index
            )
        })?;
    tracing::info!(index = %section.index, created, "段落向量 index 就緒");

    let inference = InferenceClient::new(
        &section.inference_url,
        Duration::from_secs(section.inference_timeout_secs),
    )?;
    let metrics = MetricsRegistry::new();
    let service = ChunkEmbedder::new(
        store.clone(),
        search.clone(),
        inference,
        metrics.clone(),
        embedder_cfg,
    );
    let stop = core_observability::shutdown_watch();

    if rebuild {
        let started = std::time::Instant::now();
        let report = service
            .rebuild(section.rebuild_page_size, &stop)
            .await
            .map_err(|err| err.to_string())?;
        println!(
            "rebuild {}：{} 份文件、{} 段寫入，失敗 {} 份，耗時 {:.1}s",
            if report.interrupted {
                "被中斷"
            } else {
                "完成"
            },
            report.documents,
            report.chunks,
            report.failed,
            started.elapsed().as_secs_f64()
        );
        if report.failed > 0 {
            return Err(format!(
                "{} 份文件向量化失敗，見上方 error log",
                report.failed
            ));
        }
        return Ok(());
    }

    let bind = section.bind.clone();
    let (h_store, h_search, h_metrics, h_inf) = (
        store.clone(),
        search.clone(),
        metrics.clone(),
        section.inference_url.clone(),
    );
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, h_metrics, h_store, h_search, &h_inf).await {
            tracing::error!(error = %err, "chunk-embedder health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &section.consumer_group,
        &[EventTopic::DocumentChunked.as_str()],
    )
    .map_err(|err| err.to_string())?;
    tracing::info!(group = %section.consumer_group, index = %section.index, "chunk-embedder 開始消費 document.chunked");

    let service = Arc::new(service);
    let mut processed = 0_u64;
    let mut stop_rx = stop.clone();
    loop {
        tokio::select! {
            _ = stop_rx.changed() => {
                tracing::info!("收到停止訊號（SIGTERM／SIGINT），chunk-embedder 結束");
                break;
            }
            result = consumer.next_envelope(Duration::from_secs(30)) => {
                match result {
                    Ok(envelope) => {
                        match service.handle_payload(&envelope.payload, &stop).await {
                            Ok(outcome) => tracing::debug!(document_id = %outcome.document_id(), ?outcome, "段落向量處理完畢"),
                            // 停止訊號打斷了退避：**不提交** offset，下次啟動重送這則。
                            Err(ChunkEmbedderError::Stopped) => {
                                tracing::info!(event_id = %envelope.id, "停止時這則未完成，不提交 offset");
                                break;
                            }
                            Err(err) => {
                                tracing::error!(error = %err, event_id = %envelope.id, "段落向量失敗，記 failed_event 後提交 offset");
                                if let Some((topic, partition, offset)) = consumer.last_coordinates() {
                                    let failed = FailedEvent {
                                        id: uuid::Uuid::now_v7(),
                                        topic,
                                        partition,
                                        offset,
                                        consumer_group: section.consumer_group.clone(),
                                        failure_reason: err.to_string(),
                                        attempt_count: 1,
                                        envelope: serde_json::to_value(&envelope).unwrap_or(serde_json::json!({})),
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
                            match consumer.consumer_lag(LAG_TIMEOUT) {
                                Ok(lag) => metrics.set_queue_depth(lag),
                                Err(err) => tracing::warn!(error = %err, "量不到 consumer lag"),
                            }
                        }
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {
                        if let Ok(lag) = consumer.consumer_lag(LAG_TIMEOUT) {
                            metrics.set_queue_depth(lag);
                        }
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "消費 document.chunked 失敗");
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
        assert_eq!(
            args(&[]).unwrap(),
            ParsedArgs::Run {
                rebuild: false,
                drop: false
            }
        );
        assert_eq!(
            args(&["--rebuild", "--drop"]).unwrap(),
            ParsedArgs::Run {
                rebuild: true,
                drop: true
            }
        );
        assert!(args(&["--drop"]).unwrap_err().contains("--rebuild"));
        assert!(args(&["--x"]).is_err());
    }

    #[test]
    fn rejects_bad_config() {
        let ok = ChunkEmbedderSection::default();
        assert!(embedder_config(&ok).is_ok());
        for bad in [
            ChunkEmbedderSection {
                batch_size: 0,
                ..ok.clone()
            },
            ChunkEmbedderSection {
                batch_size: 33,
                ..ok.clone()
            },
            ChunkEmbedderSection {
                slow_factor: 1.0,
                ..ok.clone()
            },
            ChunkEmbedderSection {
                slow_factor: f64::NAN,
                ..ok.clone()
            },
        ] {
            assert!(embedder_config(&bad).is_err(), "{bad:?}");
        }
    }
}
