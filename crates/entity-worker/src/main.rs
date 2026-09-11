//! `osint-entity-worker`：訂閱 dedup.completed，對 canonical Document 做 entity 抽取。

use std::sync::Arc;
use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_observability::{MetricsRegistry, init_tracing};
use entity_worker::{EntityWorker, ExtractionBounds, serve_health};
use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::PostgresCanonicalStore;

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-entity-worker 結束");
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

    // entity-worker 只讀 Document 的文字欄位（normalizer 已經把正文寫進 documents.body），
    // 不讀 RawEvidence 的 blob，所以不需要 MinIO。少一個相依就少一個啟動失敗點。
    let producer = EventProducer::connect(&cfg.broker.brokers, "entity-worker")
        .map_err(|err| err.to_string())?;
    let metrics = MetricsRegistry::new();
    let bounds = ExtractionBounds {
        max_extractions: cfg.entity_worker.max_extractions,
        max_scan_bytes: cfg.entity_worker.max_scan_bytes,
    };
    let service = EntityWorker::new(
        store.clone(),
        Some(Arc::new(producer)),
        metrics.clone(),
        bounds,
    );

    let bind = cfg.entity_worker.bind.clone();
    let health_store = store.clone();
    let health_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store).await {
            tracing::error!(error = %err, "entity-worker health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.entity_worker.consumer_group,
        &[EventTopic::DedupCompleted.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.entity_worker.consumer_group,
        max_extractions = bounds.max_extractions,
        max_scan_bytes = bounds.max_scan_bytes,
        "entity-worker 開始消費 dedup.completed"
    );
    // 循序處理，與 deduplicator 相同。要提高吞吐是增加 partition 與 consumer 實例，
    // 不是在這裡對同一則事件無界 spawn（CLAUDE.md §6）。
    // CPU-bound 的 regex 掃描已經在 service 內走 spawn_blocking，不會卡住這個迴圈的 I/O。
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，entity-worker 結束");
                break;
            }
            result = consumer.next_envelope(Duration::from_secs(30)) => {
                match result {
                    Ok(envelope) => {
                        match service.handle_payload(&envelope.payload).await {
                            Ok(outcome) => tracing::info!(
                                document_id = %outcome.document_id(),
                                event_id = %envelope.id,
                                "entity 抽取處理完畢"
                            ),
                            Err(err) => tracing::error!(
                                error = %err,
                                event_id = %envelope.id,
                                "entity 抽取失敗，仍提交 offset 以免卡住 partition。\
                                 該 Document 的 claim 沒有寫入，重送事件時會再試一次"
                            ),
                        }
                        if let Err(err) = consumer.commit_last() {
                            tracing::warn!(error = %err, "commit offset 失敗");
                        }
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {}
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
