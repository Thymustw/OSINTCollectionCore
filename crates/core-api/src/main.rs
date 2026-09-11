//! `osint-api`：V0.1 API skeleton。

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use connector_sdk::StoreEvidenceSink;
use core_api::{AppState, AuthState, ErrorBody, ImportState, PostgresReady, ReadyProbe, router};
use core_config::AppConfig;
use core_events::EventProducer;
use core_jobs::JobService;
use core_observability::{MetricsRegistry, init_tracing};
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog};
use storage_core::conformance::load_workspace_dotenv;
use storage_postgres::PostgresCanonicalStore;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }

    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-api 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let cfg = AppConfig::load().map_err(|err| err.to_string())?;
    let jwt_secret = cfg
        .auth
        .jwt_secret_ref
        .resolve()
        .map_err(|err| err.to_string())?;
    let jwt = JwtService::new(
        jwt_secret.as_bytes(),
        cfg.auth.jwt_issuer.clone(),
        ChronoDuration::seconds(cfg.auth.jwt_ttl_secs as i64),
    )
    .map_err(|err| err.to_string())?;

    let producer = match EventProducer::connect(&cfg.broker.brokers, "core-api") {
        Ok(p) => Some(Arc::new(p)),
        Err(err) => {
            tracing::warn!(error = %err, "Redpanda 未連上；Job 派工不會發 event");
            None
        }
    };

    let (jobs, import, ready) = match connect_postgres(&cfg).await {
        Ok(store) => {
            let ready = ReadyProbe::new(vec![Arc::new(PostgresReady {
                store: store.clone(),
            })]);
            // 匯入另外需要物件儲存。MinIO 沒接上時只有 /api/v1/import 回 503，
            // 其他路由照常——沒有理由讓查詢功能陪著一起掛掉。
            let import = match connect_objects(&cfg).await {
                Ok(objects) => Some(Arc::new(ImportState {
                    store: store.clone(),
                    sink: Arc::new(StoreEvidenceSink::new(store.clone(), objects)),
                    producer: producer.clone(),
                })),
                Err(err) => {
                    tracing::warn!(error = %err, "MinIO 未連上；POST /api/v1/import 會回 503");
                    None
                }
            };
            let service = JobService::new(store, producer);
            (Some(Arc::new(service)), import, ready)
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Postgres 未連上；/jobs 與 /api/v1/import 會回 503，/health 仍可用"
            );
            (None, None, ReadyProbe::always_ready())
        }
    };

    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        jobs,
        import,
        ready,
        rate_limit_per_second: cfg.http.rate_limit_per_second,
        request_body_limit_bytes: cfg.http.request_body_limit_bytes,
        import_config: cfg.import.clone(),
        rate_limiter: core_api::RateLimiter::new(cfg.http.rate_limit_per_second),
    };

    let app = router(state).fallback(fallback);
    let addr: SocketAddr = cfg
        .http
        .bind
        .parse()
        .map_err(|err| format!("http.bind `{}` 不是合法位址：{err}", cfg.http.bind))?;
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|err| format!("綁定 {addr} 失敗：{err}。請改 config [http].bind 或釋放該埠"))?;
    tracing::info!(%addr, "osint-api 開始聽");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|err| format!("伺服器結束：{err}"))
}

async fn connect_postgres(cfg: &AppConfig) -> Result<PostgresCanonicalStore, String> {
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
    Ok(store)
}

async fn connect_objects(cfg: &AppConfig) -> Result<storage_s3::S3ObjectStore, String> {
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
    let store = storage_s3::S3ObjectStore::connect(
        &cfg.storage.object.endpoint,
        &cfg.storage.object.bucket,
        &access,
        &secret,
    )
    .map_err(|err| err.to_string())?;
    store.ensure_bucket().await.map_err(|err| err.to_string())?;
    Ok(store)
}

async fn fallback() -> (axum::http::StatusCode, axum::Json<ErrorBody>) {
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(ErrorBody {
            error: "not_found".into(),
            message:
                "沒有這個路徑。V0.1 提供 GET /health /ready /metrics、/api/v1/jobs 與 POST /api/v1/import"
                    .into(),
        }),
    )
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
