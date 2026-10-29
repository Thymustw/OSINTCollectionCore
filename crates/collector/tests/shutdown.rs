//! collector 優雅停止：取消後不再派工、drain 逾時 abort、drain 內自然完成、
//! `/ready` 取消後 503。不對測試行程送 SIGTERM。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use chrono::Utc;
use collector::{ABORT_JOB_ERROR, CollectorRunner, RunBounds, health_router};
use core_events::EventProducer;
use core_jobs::JobService;
use core_model::{Connector, JobStatus, NetworkRule, Source, SourceType};
use core_observability::MetricsRegistry;
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env, verify_not_opencti_s3};
use storage_core::{HealthProvider, RelationalStore, StorageError, StorageHealth};
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use uuid::Uuid;

const RSS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Local Fixture</title>
    <link>http://127.0.0.1/feed</link>
    <description>shutdown e2e</description>
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

async fn serve_rss_delayed(delay: Duration) -> String {
    let app = Router::new().route(
        "/rss.xml",
        get(move || async move {
            tokio::time::sleep(delay).await;
            RSS
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    format!("http://127.0.0.1:{}/rss.xml", addr.port())
}

async fn seed_connector(
    pg: &PostgresCanonicalStore,
    feed_url: &str,
    error_count: i32,
) -> (Source, NetworkRule, Connector) {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("shutdown-rss-{}", Uuid::now_v7()),
        source_type: SourceType::Rss,
        platform: None,
        base_url: Some(feed_url.to_string()),
        description: Some("shutdown fixture".into()),
        language: Some("en".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    pg.put_source(&source).await.expect("source");
    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "127.0.0.1".into(),
        ports: None,
        reason: "shutdown e2e 本機假 feed".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: now,
        updated_at: now,
    };
    pg.put_network_rule(&rule).await.expect("rule");
    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: format!("shutdown-rss-connector-{}", Uuid::now_v7()),
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
        status: "idle".into(),
        error_count,
    };
    pg.put_connector(&connector).await.expect("connector");
    (source, rule, connector)
}

fn runner(stack: &Stack, producer: Arc<EventProducer>, drain_secs: u64) -> CollectorRunner {
    let jobs = Arc::new(JobService::new(stack.pg.clone(), Some(producer.clone())));
    CollectorRunner::new(
        stack.pg.clone(),
        stack.s3.clone(),
        producer,
        jobs,
        RunBounds::new(2, 1),
        MetricsRegistry::new(),
    )
    .with_shutdown_drain_secs(drain_secs)
}

async fn wait_running_job(pg: &PostgresCanonicalStore, connector_id: Uuid) -> core_model::Job {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let jobs = pg
            .list_jobs_by_status_and_type(JobStatus::Running, "collect", None, 100)
            .await
            .expect("list running collect");
        if let Some(job) = jobs
            .into_iter()
            .find(|j| j.correlation_id == Some(connector_id))
        {
            return job;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "等不到 connector {connector_id} 的 running collect job。請確認 Redpanda／Postgres／本機假 feed 在跑；\
             若環境都正常，可能是別的測試行程呼叫了 reclaim_orphan_collect_runs 把它改成 failed\
             （該測試必須先拿 acquire_collect_state_test_lock）"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn cleanup_abort(
    pg: &PostgresCanonicalStore,
    source: &Source,
    rule: &NetworkRule,
    connector: &Connector,
    job_id: Option<Uuid>,
) {
    if let Some(id) = job_id {
        let _ = pg.delete_job(id).await;
    }
    let _ = pg.delete_network_rule(rule.id).await;
    let _ = pg.delete_connector(connector.id).await;
    let _ = pg.delete_source(source.id).await;
}

#[tokio::test]
async fn cancelled_tick_dispatches_nothing() {
    let stack = connect_stack().await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "collector-shutdown-none").expect("producer"),
    );
    let collector = runner(&stack, producer, 20);
    let token = CancellationToken::new();
    token.cancel();
    let results = collector.tick(Utc::now(), &token).await;
    assert!(
        results.is_empty(),
        "已取消的 tick 必須立刻回空、不掃 enabled connector 去派工，實際 {} 筆",
        results.len()
    );
}

#[tokio::test]
async fn drain_timeout_aborts_inflight_without_bumping_error_count() {
    let stack = connect_stack().await;
    // reclaim_orphan_collect_runs 會收回全表 running collect job；與 storage-postgres
    // conformance 跨行程互斥，見 PostgresCanonicalStore::acquire_collect_state_test_lock。
    let _collect_lock = stack
        .pg
        .acquire_collect_state_test_lock()
        .await
        .expect("collect 狀態測試鎖");
    let feed_url = serve_rss_delayed(Duration::from_secs(60)).await;
    let error_count_before = 2;
    let (source, rule, connector) = seed_connector(&stack.pg, &feed_url, error_count_before).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "collector-shutdown-abort").expect("producer"),
    );
    let collector = runner(&stack, producer, 1);
    let token = CancellationToken::new();
    let handle = {
        let collector = collector.clone();
        let connectors = vec![connector.clone()];
        let token = token.clone();
        tokio::spawn(async move {
            collector
                .tick_connectors(connectors, Utc::now(), &token)
                .await
        })
    };

    let job = wait_running_job(&stack.pg, connector.id).await;
    token.cancel();
    let _ = handle.await.expect("tick_connectors join");

    let finished = stack
        .pg
        .get_job(job.id)
        .await
        .expect("get job")
        .expect("job 還在");
    let reloaded = stack
        .pg
        .get_connector(connector.id)
        .await
        .expect("get connector")
        .expect("connector 還在");

    let job_ok = finished.status == JobStatus::Cancelled
        && finished.completed_at.is_some()
        && finished.error.as_deref() == Some(ABORT_JOB_ERROR);
    let connector_ok = reloaded.status == "idle" && reloaded.error_count == error_count_before;
    cleanup_abort(&stack.pg, &source, &rule, &connector, Some(job.id)).await;

    assert_eq!(
        finished.status,
        JobStatus::Cancelled,
        "drain 逾時應把 in-flight job 標成 cancelled，實際 {:?}",
        finished.status
    );
    assert!(
        finished.completed_at.is_some(),
        "Cancelled 必須設 completed_at，否則保留期限清不到"
    );
    assert_eq!(
        finished.error.as_deref(),
        Some(ABORT_JOB_ERROR),
        "abort 收尾必須寫入固定 error，實際 {:?}",
        finished.error
    );
    assert_eq!(reloaded.status, "idle", "connector 應回到 idle");
    assert_eq!(
        reloaded.error_count, error_count_before,
        "被中斷不是來源的錯，error_count 必須與收集前相同（收集前是 {error_count_before}）"
    );
    assert!(job_ok && connector_ok);
}

#[tokio::test]
async fn drain_waits_for_inflight_to_complete() {
    let stack = connect_stack().await;
    // reclaim_orphan_collect_runs 會收回全表 running collect job；與 storage-postgres
    // conformance 跨行程互斥，見 PostgresCanonicalStore::acquire_collect_state_test_lock。
    let _collect_lock = stack
        .pg
        .acquire_collect_state_test_lock()
        .await
        .expect("collect 狀態測試鎖");
    let feed_url = serve_rss_delayed(Duration::from_millis(300)).await;
    let (source, rule, connector) = seed_connector(&stack.pg, &feed_url, 0).await;
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "collector-shutdown-complete").expect("producer"),
    );
    let collector = runner(&stack, producer, 5);
    let token = CancellationToken::new();
    let handle = {
        let collector = collector.clone();
        let connectors = vec![connector.clone()];
        let token = token.clone();
        tokio::spawn(async move {
            collector
                .tick_connectors(connectors, Utc::now(), &token)
                .await
        })
    };

    let job = wait_running_job(&stack.pg, connector.id).await;
    token.cancel();
    let results = handle.await.expect("tick_connectors join");
    assert!(
        results.iter().any(|(id, outcome)| *id == connector.id
            && matches!(outcome, collector::CollectOutcome::Collected { .. })),
        "drain 期限內應讓 300ms 的收集自然完成，實際 {results:?}"
    );

    let finished = stack
        .pg
        .get_job(job.id)
        .await
        .expect("get job")
        .expect("job 還在");
    let reloaded = stack
        .pg
        .get_connector(connector.id)
        .await
        .expect("get connector")
        .expect("connector 還在");

    let _ = stack.pg.delete_job(job.id).await;
    let _ = stack.pg.delete_network_rule(rule.id).await;
    // 成功收集會寫 raw_evidence，對 sources／connectors 有 FK 且無 CASCADE，
    // 也沒有 delete_raw_evidence；source／connector 清不掉，jobs 仍可刪。
    let _ = (source, connector);

    assert_eq!(
        finished.status,
        JobStatus::Completed,
        "drain 內自然結束應為 completed，實際 {:?}",
        finished.status
    );
    assert!(finished.completed_at.is_some());
    assert_eq!(reloaded.status, "idle");
    assert_eq!(reloaded.error_count, 0);
}

#[derive(Clone)]
struct AlwaysOk;

#[async_trait]
impl HealthProvider for AlwaysOk {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        Ok(StorageHealth::ok("test", "ok"))
    }
}

#[tokio::test]
async fn ready_returns_503_after_shutdown_health_stays_200() {
    let shutdown = CancellationToken::new();
    let app = health_router(MetricsRegistry::new(), AlwaysOk, shutdown.clone());

    let ready_ok = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("ready request"),
        )
        .await
        .expect("ready oneshot");
    assert_eq!(ready_ok.status(), StatusCode::OK, "取消前 /ready 應為 200");

    shutdown.cancel();

    let ready_down = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("ready request"),
        )
        .await
        .expect("ready oneshot");
    assert_eq!(
        ready_down.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "取消後 /ready 必須 503，讓編排器停送流量"
    );

    let health_ok = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("health request"),
        )
        .await
        .expect("health oneshot");
    assert_eq!(
        health_ok.status(),
        StatusCode::OK,
        "drain 期間 /health 必須維持 200（liveness）；若也 503，編排器會 SIGKILL"
    );
}
