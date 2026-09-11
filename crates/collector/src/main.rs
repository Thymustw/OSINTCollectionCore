//! `osint-collector`：cron 排程 + 有界收集。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use collector::{CollectorRunner, RunBounds, serve_health};
use core_config::AppConfig;
use core_events::EventProducer;
use core_jobs::JobService;
use core_observability::{MetricsRegistry, init_tracing};
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
        tracing::error!(error = %err, "osint-collector 結束");
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
        EventProducer::connect(&cfg.broker.brokers, "collector").map_err(|err| err.to_string())?;
    let producer = Arc::new(producer);
    let jobs = Arc::new(JobService::new(store.clone(), Some(producer.clone())));
    let bounds = RunBounds::new(
        cfg.collector.global_inflight,
        cfg.collector.per_domain_inflight,
    );
    let metrics = MetricsRegistry::new();
    let runner = CollectorRunner::new(
        store.clone(),
        objects,
        producer,
        jobs,
        bounds,
        metrics.clone(),
    );

    let bind = cfg.collector.bind.clone();
    let health_store = store.clone();
    let health_metrics = metrics.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store).await {
            tracing::error!(error = %err, "collector health 結束");
        }
    });

    let tick = Duration::from_secs(cfg.collector.tick_secs.max(1));
    tracing::info!(tick_secs = tick.as_secs(), "collector 開始排程迴圈");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，collector 結束");
                break;
            }
            _ = tokio::time::sleep(tick) => {
                let _ = runner.tick(Utc::now()).await;
            }
        }
    }
    Ok(())
}
