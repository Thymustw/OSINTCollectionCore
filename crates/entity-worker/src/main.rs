//! `osint-entity-worker`：訂閱 dedup.completed，對 canonical Document 做 entity 抽取。

use std::sync::Arc;
use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_model::FailedEvent;
use core_observability::{MetricsRegistry, init_tracing};
use entity_worker::{EntityWorker, ExtractionBounds, serve_health};
use storage_core::RelationalStore;
use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::PostgresCanonicalStore;

/// 每處理幾則事件量一次 consumer lag。
///
/// `fetch_watermarks` 是一次網路往返，每則都量會讓它變成主要成本
/// （與 `osint-indexer` 的 `LAG_PROBE_EVERY` 同一個理由）。
const LAG_PROBE_EVERY: u64 = 10;
/// 量 lag 的逾時。量不到就跳過這一輪，不要擋住消費。
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

/// 量 consumer lag 並寫進 `osint_queue_depth`（SPEC §24 queue depth）。
///
/// 量不到時**保留上一個值並記 warn**，不要寫 0：把「量不到」顯示成「沒有積壓」
/// 會讓 broker 出問題時的儀表板看起來一切正常。
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
    // 已處理事件數，只用來決定何時量一次 lag。
    let mut processed = 0_u64;
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
                            Err(err) => {
                                // 這則事件本身處理失敗，不是消費失敗：記錄成
                                // failed_event（ADR-008）。寫入失敗只 warn，不影響
                                // 後續的 commit_last——一則毒訊息照舊不卡 partition。
                                tracing::error!(
                                    error = %err,
                                    event_id = %envelope.id,
                                    "entity 抽取失敗，仍提交 offset 以免卡住 partition。\
                                     該 Document 的 claim 沒有寫入，重送事件時會再試一次"
                                );
                                if let Some((topic, partition, offset)) =
                                    consumer.last_coordinates()
                                {
                                    let failed = FailedEvent {
                                        id: uuid::Uuid::now_v7(),
                                        topic,
                                        partition,
                                        offset,
                                        consumer_group: cfg.entity_worker.consumer_group.clone(),
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
                        if processed % LAG_PROBE_EVERY == 0 {
                            update_queue_depth(&consumer, &metrics);
                        }
                    }
                    // 閒置也量一次：不然「處理完最後一則之後積壓才長起來」
                    // 要等下一則事件進來才看得到。
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
