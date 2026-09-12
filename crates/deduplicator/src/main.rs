//! `osint-deduplicator`：訂閱 object.normalized，跑五階段去重。

use std::sync::Arc;
use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_observability::{MetricsRegistry, init_tracing};
use deduplicator::{DedupBounds, Deduplicator, serve_health};
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
        tracing::error!(error = %err, "osint-deduplicator 結束");
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

    // deduplicator 不讀 RawEvidence 的 blob（只讀 metadata 取 platform），
    // 所以不需要 MinIO。少一個相依就少一個啟動失敗點。
    let producer = EventProducer::connect(&cfg.broker.brokers, "deduplicator")
        .map_err(|err| err.to_string())?;
    let metrics = MetricsRegistry::new();
    let bounds = DedupBounds {
        candidate_limit: cfg.deduplicator.candidate_limit,
        simhash_scan_limit: cfg.deduplicator.simhash_scan_limit,
        simhash_max_distance: cfg.deduplicator.simhash_max_distance,
    };
    let service = Deduplicator::new(
        store.clone(),
        Some(Arc::new(producer)),
        metrics.clone(),
        bounds,
    );

    let bind = cfg.deduplicator.bind.clone();
    let health_store = store.clone();
    let health_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store).await {
            tracing::error!(error = %err, "deduplicator health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.deduplicator.consumer_group,
        &[EventTopic::ObjectNormalized.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.deduplicator.consumer_group,
        simhash_max_distance = bounds.simhash_max_distance,
        simhash_scan_limit = bounds.simhash_scan_limit,
        "deduplicator 開始消費 object.normalized"
    );
    // 單一 consumer group、循序處理：dedup 的正確性依賴「先到先成為 canonical」，
    // 同一份 Document 的處理不可以互相交錯。要提高吞吐是增加 partition 與 consumer 實例，
    // 不是在這裡對同一則事件無界 spawn。
    // 已處理事件數，只用來決定何時量一次 lag。
    let mut processed = 0_u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，deduplicator 結束");
                break;
            }
            result = consumer.next_envelope(Duration::from_secs(30)) => {
                match result {
                    Ok(envelope) => {
                        match service.handle_payload(&envelope.payload).await {
                            Ok(outcomes) => tracing::info!(
                                count = outcomes.len(),
                                event_id = %envelope.id,
                                "去重完成"
                            ),
                            Err(err) => tracing::error!(
                                error = %err,
                                event_id = %envelope.id,
                                "去重失敗，仍提交 offset 以免卡住 partition"
                            ),
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
                        tracing::error!(error = %err, "消費 object.normalized 失敗");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
    Ok(())
}
