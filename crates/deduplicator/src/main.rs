//! `osint-deduplicator`：訂閱 object.normalized，跑五階段去重。

use std::sync::Arc;
use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_observability::{MetricsRegistry, init_tracing};
use deduplicator::{DedupBounds, Deduplicator, serve_health};
use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::PostgresCanonicalStore;

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
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {}
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
