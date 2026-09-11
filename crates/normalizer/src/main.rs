//! `osint-normalizer`：訂閱 raw.collected，寫 Document。

use std::sync::Arc;
use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_observability::{MetricsRegistry, init_tracing};
use normalizer::{Normalizer, serve_health};
use storage_core::conformance::{load_workspace_dotenv, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;

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
                            Err(err) => tracing::error!(error = %err, event_id = %envelope.id, "正規化失敗，仍提交 offset 以免卡住 partition"),
                        }
                        if let Err(err) = consumer.commit_last() {
                            tracing::warn!(error = %err, "commit offset 失敗");
                        }
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {}
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
