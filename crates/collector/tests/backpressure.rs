//! collector 下游 lag 背壓：pause 不改 last_run／error_count／status；
//! throttle 時實際併發為 1。Redpanda 端到端 lag 探針不在這裡測。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::routing::get;
use chrono::Utc;
use collector::{BackpressureGate, CollectOutcome, CollectorRunner, LagSource, RunBounds};
use core_config::CollectorBackpressureConfig;
use core_events::EventProducer;
use core_jobs::JobService;
use core_model::{Connector, JobStatus, NetworkRule, Source, SourceType};
use core_observability::MetricsRegistry;
use serde_json::json;
use storage_core::RelationalStore;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const RSS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Local Fixture</title>
    <link>http://127.0.0.1/feed</link>
    <description>backpressure e2e</description>
    <item>
      <title>CVE-2026-0001</title>
      <link>http://127.0.0.1/cve</link>
      <guid>CVE-2026-0001</guid>
      <description>fixture item</description>
    </item>
  </channel>
</rss>
"#;

struct Stack {
    pg: PostgresCanonicalStore,
    s3: S3ObjectStore,
    brokers: String,
}

struct FixedLag(u64);

#[async_trait]
impl LagSource for FixedLag {
    async fn current_lag(&self) -> Result<u64, String> {
        Ok(self.0)
    }
}

async fn connect_stack() -> Stack {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "e2e 只連本機 Postgres"
    );
    let endpoint = required_env("S3_ENDPOINT").expect("S3_ENDPOINT");
    let _ = verify_not_opencti_s3(&endpoint).expect("S3 埠隔離");
    let bucket = required_env("S3_BUCKET").unwrap_or_else(|_| "raw-evidence".into());
    let access = required_env("S3_ACCESS_KEY").expect("S3_ACCESS_KEY");
    let secret = required_env("S3_SECRET_KEY").expect("S3_SECRET_KEY");
    let brokers = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        brokers.contains("127.0.0.1") || brokers.contains("localhost"),
        "e2e 只連本機 Redpanda，實際 brokers={brokers}"
    );

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");
    let s3 = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("s3");
    s3.ensure_bucket().await.expect("bucket");
    Stack { pg, s3, brokers }
}

async fn serve_rss_delayed_on(host: &str, delay: Duration) -> String {
    let app = Router::new().route(
        "/rss.xml",
        get(move || async move {
            tokio::time::sleep(delay).await;
            RSS
        }),
    );
    let listener = TcpListener::bind(format!("{host}:0")).await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://{host}:{}/rss.xml", addr.port())
}

async fn seed_connector(
    pg: &PostgresCanonicalStore,
    feed_url: &str,
    error_count: i32,
    status: &str,
) -> (Source, NetworkRule, Connector) {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("bp-rss-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: None,
        base_url: Some(feed_url.to_string()),
        description: Some("backpressure fixture".into()),
        language: Some("en".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    pg.put_source(&source).await.expect("source");
    let host = url::Url::parse(feed_url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| "127.0.0.1".into());
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: host,
        ports: None,
        reason: "backpressure e2e 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("bp-rss-connector-{}", Uuid::now_v7()),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: false,
        configuration: json!({ "url": feed_url }),
        credential_reference: None,
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({ "per_second": 10.0, "burst": 10.0 }),
        timeout: json!({}),
        proxy_reference: None,
        checkpoint: json!({}),
        last_run: None,
        last_success: None,
        status: status.into(),
        error_count,
    };
    pg.put_connector(&connector).await.expect("connector");
    (source, rule, connector)
}

fn runner_with_lag(
    stack: &Stack,
    producer: Arc<EventProducer>,
    lag: u64,
    inflight: u32,
) -> CollectorRunner {
    let jobs = Arc::new(JobService::new(stack.pg.clone(), Some(producer.clone())));
    let metrics = MetricsRegistry::new();
    let gate = BackpressureGate::new(
        CollectorBackpressureConfig::default(),
        Some(Arc::new(FixedLag(lag))),
        metrics.clone(),
    );
    CollectorRunner::new(
        stack.pg.clone(),
        stack.s3.clone(),
        producer,
        jobs,
        RunBounds::new(inflight, inflight),
        metrics,
    )
    .with_backpressure(gate)
}

async fn cleanup(
    pg: &PostgresCanonicalStore,
    source: &Source,
    rule: &NetworkRule,
    connector: &Connector,
) {
    let _ = pg.delete_network_rule(rule.id).await;
    let _ = pg.delete_connector(connector.id).await;
    let _ = pg.delete_source(source.id).await;
}

#[tokio::test]
async fn pause_does_not_touch_last_run_error_count_or_status() {
    let stack = connect_stack().await;
    let feed_url = serve_rss_delayed_on("127.0.0.1", Duration::from_millis(50)).await;
    let error_count_before = 4;
    let (source, rule, connector) =
        seed_connector(&stack.pg, &feed_url, error_count_before, "error").await;
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "collector-bp-pause").expect("producer"));
    let collector = runner_with_lag(&stack, producer, 50_000, 4);
    let results = collector
        .tick_connectors(
            vec![connector.clone()],
            Utc::now(),
            &CancellationToken::new(),
        )
        .await;

    let reloaded = stack
        .pg
        .get_connector(connector.id)
        .await
        .expect("get")
        .expect("connector");
    let jobs = stack
        .pg
        .list_jobs_by_status_and_type(JobStatus::Queued, "collect", None, 100)
        .await
        .expect("pending");
    let running = stack
        .pg
        .list_jobs_by_status_and_type(JobStatus::Running, "collect", None, 100)
        .await
        .expect("running");
    let ours: Vec<_> = jobs
        .into_iter()
        .chain(running)
        .filter(|j| j.correlation_id == Some(connector.id))
        .collect();

    cleanup(&stack.pg, &source, &rule, &connector).await;

    assert!(
        results.iter().any(|(id, outcome)| *id == connector.id
            && matches!(outcome, CollectOutcome::BackpressureSkipped)),
        "pause 應回 BackpressureSkipped，實際 {results:?}"
    );
    assert_eq!(reloaded.last_run, None, "pause 不得改 last_run");
    assert_eq!(
        reloaded.error_count, error_count_before,
        "pause 不得改 error_count"
    );
    assert_eq!(reloaded.status, "error", "pause 不得改 status");
    assert!(
        ours.is_empty(),
        "pause 不該建 collect job，實際 {} 筆",
        ours.len()
    );
}

#[tokio::test]
async fn throttle_caps_actual_concurrency_at_one() {
    let stack = connect_stack().await;
    let _collect_lock = stack
        .pg
        .acquire_collect_state_test_lock()
        .await
        .expect("collect 狀態測試鎖");
    let feed_a = serve_rss_delayed_on("127.0.0.1", Duration::from_millis(400)).await;
    let feed_b = serve_rss_delayed_on("localhost", Duration::from_millis(400)).await;
    let (source_a, rule_a, connector_a) = seed_connector(&stack.pg, &feed_a, 0, "idle").await;
    let (source_b, rule_b, connector_b) = seed_connector(&stack.pg, &feed_b, 0, "idle").await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "collector-bp-throttle").expect("producer"),
    );
    let collector = runner_with_lag(&stack, producer, 5_000, 4);
    let ids = [connector_a.id, connector_b.id];
    let stop = CancellationToken::new();
    let poller_stop = stop.clone();
    let pg = stack.pg.clone();
    let poller = tokio::spawn(async move {
        let mut peak = 0usize;
        loop {
            tokio::select! {
                _ = poller_stop.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_millis(20)) => {
                    let jobs = pg
                        .list_jobs_by_status_and_type(JobStatus::Running, "collect", None, 100)
                        .await
                        .unwrap_or_default();
                    let n = jobs
                        .iter()
                        .filter(|j| j.correlation_id.is_some_and(|id| ids.contains(&id)))
                        .count();
                    peak = peak.max(n);
                }
            }
        }
        peak
    });

    let results = collector
        .tick_connectors(
            vec![connector_a.clone(), connector_b.clone()],
            Utc::now(),
            &CancellationToken::new(),
        )
        .await;
    stop.cancel();
    let peak = poller.await.expect("poller");

    let jobs_a = stack
        .pg
        .list_jobs_by_status_and_type(JobStatus::Completed, "collect", None, 100)
        .await
        .expect("completed");
    for job in jobs_a.iter().filter(|j| {
        j.correlation_id == Some(connector_a.id) || j.correlation_id == Some(connector_b.id)
    }) {
        let _ = stack.pg.delete_job(job.id).await;
    }
    let _ = stack.pg.delete_network_rule(rule_a.id).await;
    let _ = stack.pg.delete_network_rule(rule_b.id).await;
    // 成功收集會寫 raw_evidence，source／connector 清不掉。
    let _ = (source_a, source_b, connector_a, connector_b);

    let collected = results
        .iter()
        .filter(|(_, outcome)| matches!(outcome, CollectOutcome::Collected { .. }))
        .count();
    assert_eq!(
        collected, 2,
        "throttle 仍應把兩筆都收完，只是併發為 1，實際 {results:?}"
    );
    assert_eq!(peak, 1, "throttle 時實際併發應為 1，實際 peak={peak}");
}
