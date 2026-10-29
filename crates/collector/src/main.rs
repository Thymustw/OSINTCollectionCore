//! `osint-collector`：cron 排程 + 有界收集。

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use collector::{BackpressureGate, BrokerLagSource, CollectorRunner, RunBounds, serve_health};
use connector_sdk::BackoffConfig;
use core_config::AppConfig;
use core_events::{EventProducer, EventTopic, GroupLagProbe};
use core_jobs::JobService;
use core_observability::{MetricsRegistry, init_tracing, shutdown_signal};
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio_util::sync::CancellationToken;

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
    let backpressure = build_backpressure_gate(&cfg, &metrics);
    let runner = CollectorRunner::new(
        store.clone(),
        objects,
        producer,
        jobs,
        bounds,
        metrics.clone(),
    )
    .with_backoff(
        BackoffConfig::from_secs(
            cfg.collector.backoff_base_secs,
            cfg.collector.backoff_max_secs,
        ),
        cfg.collector.max_retry_per_tick,
    )
    .with_shutdown_drain_secs(cfg.collector.shutdown_drain_secs)
    .with_backpressure(backpressure);

    let shutdown = CancellationToken::new();
    let signal_token = shutdown.clone();
    tokio::spawn(async move {
        let _cause = shutdown_signal().await;
        signal_token.cancel();
    });

    let bind = cfg.collector.bind.clone();
    let health_store = store.clone();
    let health_metrics = metrics.clone();
    let health_shutdown = shutdown.clone();
    let health_handle = tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store, health_shutdown).await {
            tracing::error!(error = %err, "collector health 結束");
        }
    });

    let (jobs_n, conn_n) = store
        .reclaim_orphan_collect_runs(Utc::now())
        .await
        .map_err(|err| err.to_string())?;
    if jobs_n + conn_n > 0 {
        tracing::warn!(
            jobs = jobs_n,
            connectors = conn_n,
            "啟動回收：上次非正常結束留下的 collect job／running connector 已收回"
        );
    }
    metrics.inc(
        "osint_collector_orphans_reclaimed_total",
        jobs_n.saturating_add(conn_n),
    );

    let tick = Duration::from_secs(cfg.collector.tick_secs.max(1));
    tracing::info!(
        tick_secs = tick.as_secs(),
        drain_secs = cfg.collector.shutdown_drain_secs,
        "collector 開始排程迴圈"
    );
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(tick) => {}
        }
        if shutdown.is_cancelled() {
            break;
        }
        let _ = runner.tick(Utc::now(), &shutdown).await;
    }
    let _ = health_handle.await;
    Ok(())
}

/// group 從 `[normalizer].consumer_group` 讀、topic 是 `raw.collected` 這個事實
/// （見 normalizer main.rs），不要在這裡寫死 group 字串。
fn build_backpressure_gate(cfg: &AppConfig, metrics: &MetricsRegistry) -> BackpressureGate {
    if !cfg.collector.backpressure.enabled {
        return BackpressureGate::disabled(metrics.clone());
    }
    let timeout = Duration::from_millis(cfg.collector.backpressure.probe_timeout_ms.max(1));
    match GroupLagProbe::new(&cfg.broker.brokers, timeout) {
        Ok(probe) => BackpressureGate::new(
            cfg.collector.backpressure.clone(),
            Some(Arc::new(BrokerLagSource::new(
                probe,
                cfg.normalizer.consumer_group.clone(),
                EventTopic::RawCollected.as_str(),
            ))),
            metrics.clone(),
        ),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "下游 lag 探針未建立；背壓閘門 fail-open（照常收集）。\
                 請確認 [broker].brokers，或把 [collector.backpressure].enabled 設 false"
            );
            BackpressureGate::new(cfg.collector.backpressure.clone(), None, metrics.clone())
        }
    }
}
