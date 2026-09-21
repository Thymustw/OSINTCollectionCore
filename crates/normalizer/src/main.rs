//! `osint-normalizer`：訂閱 raw.collected，寫 Document。

use std::sync::Arc;
use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_model::FailedEvent;
use core_observability::{MetricsRegistry, init_tracing};
use normalizer::{Normalizer, serve_health};
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;

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
        tracing::error!(error = %err, "osint-normalizer 結束");
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
    let _ = verify_not_opencti_s3(&cfg.storage.object.endpoint).map_err(|err| err.to_string())?;
    let objects = S3ObjectStore::connect(
        &cfg.storage.object.endpoint,
        &cfg.storage.object.bucket,
        &access,
        &secret,
    )
    .map_err(|err| err.to_string())?;
    objects
        .ensure_bucket()
        .await
        .map_err(|err| err.to_string())?;

    let producer =
        EventProducer::connect(&cfg.broker.brokers, "normalizer").map_err(|err| err.to_string())?;
    let metrics = MetricsRegistry::new();
    let service = Normalizer::new(
        store.clone(),
        objects,
        Some(Arc::new(producer)),
        metrics.clone(),
    );

    let bind = cfg.normalizer.bind.clone();
    let health_store = store.clone();
    let health_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store).await {
            tracing::error!(error = %err, "normalizer health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.normalizer.consumer_group,
        &[EventTopic::RawCollected.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.normalizer.consumer_group,
        "normalizer 開始消費 raw.collected"
    );
    // 已處理事件數，只用來決定何時量一次 lag。
    let mut processed = 0_u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，normalizer 結束");
                break;
            }
            result = consumer.next_envelope(Duration::from_secs(30)) => {
                match result {
                    Ok(envelope) => {
                        match service.handle_payload(&envelope.payload).await {
                            Ok(outcome) => tracing::info!(?outcome, event_id = %envelope.id, "正規化完成"),
                            Err(err) => {
                                tracing::error!(error = %err, event_id = %envelope.id, "正規化失敗，仍提交 offset 以免卡住 partition");
                                // ADR-008：把這則失敗的事件落地成可查詢的紀錄。
                                // 寫入失敗只 warn，**不影響後續流程**——`commit_last`
                                // 照樣要執行，否則一則毒訊息會卡死整個 partition。
                                if let Some((topic, partition, offset)) =
                                    consumer.last_coordinates()
                                {
                                    let failed = FailedEvent {
                                        id: uuid::Uuid::now_v7(),
                                        topic,
                                        partition,
                                        offset,
                                        consumer_group: cfg.normalizer.consumer_group.clone(),
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
                        tracing::error!(error = %err, "消費 raw.collected 失敗");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
    Ok(())
}
