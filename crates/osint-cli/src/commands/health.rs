//! `osint-cli health`：對五個後端各做一次 health check。
//!
//! 五項**獨立且平行**跑，任何一項失敗都不會中斷其他項——「PostgreSQL 掛了」這種情況下，
//! 使用者最需要知道的正是其他四個還活著沒有。每一項都有逾時，避免一個卡住的服務
//! 讓整個指令看起來沒反應。

use std::time::Duration;

use core_events::EventProducer;
use serde::Serialize;
use serde_json::json;
use storage_core::conformance::verify_not_opencti_search;
use storage_core::{HealthProvider, StorageHealth};
use storage_opensearch::OpenSearchStore;
use storage_redis::RedisKeyValueStore;

use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, print_json, print_table, truncate};

/// 單一服務的 health check 逾時。連不上的服務通常是 TCP 沒人接，會很快失敗；
/// 這個上限是給「連得上但沒回應」的情況用的。
const CHECK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize)]
pub struct ServiceHealth {
    pub service: &'static str,
    pub endpoint: String,
    pub healthy: bool,
    pub message: String,
    pub details: serde_json::Value,
}

impl ServiceHealth {
    fn from_storage(service: &'static str, endpoint: String, health: StorageHealth) -> Self {
        Self {
            service,
            endpoint,
            healthy: health.healthy,
            message: health.message,
            details: health.details,
        }
    }

    fn down(service: &'static str, endpoint: String, message: String) -> Self {
        Self {
            service,
            endpoint,
            healthy: false,
            message,
            details: serde_json::Value::Null,
        }
    }
}

/// 回傳 `true` 代表五項全部健康。呼叫端據此決定 exit code。
pub async fn run(ctx: &Context) -> Result<bool, CliError> {
    let (postgres, object, redis, search, broker) = tokio::join!(
        check_postgres(ctx),
        check_object(ctx),
        check_redis(ctx),
        check_search(ctx),
        check_broker(ctx),
    );
    let results = vec![postgres, object, redis, search, broker];
    let all_healthy = results.iter().all(|r| r.healthy);

    if ctx.format == Format::Json {
        print_json(&results)?;
    } else {
        let rows = results
            .iter()
            .map(|r| {
                vec![
                    r.service.to_string(),
                    if r.healthy { "OK" } else { "DOWN" }.to_string(),
                    truncate(&r.endpoint, 40),
                    truncate(&r.message, 70),
                ]
            })
            .collect();
        print_table(&["服務", "狀態", "位址", "訊息"], rows, "");
        if !all_healthy {
            println!(
                "有服務不可用。下一步：`make compose-ps` 看容器狀態，`make compose-up` 重新啟動，並確認 `.env` 的位址與容器實際掛的埠一致。"
            );
        }
    }
    Ok(all_healthy)
}

/// 統一的逾時包裝：逾時算 DOWN，而不是讓指令無限等下去。
async fn with_timeout<F>(service: &'static str, endpoint: String, fut: F) -> ServiceHealth
where
    F: std::future::Future<Output = Result<StorageHealth, String>>,
{
    match tokio::time::timeout(CHECK_TIMEOUT, fut).await {
        Err(_) => ServiceHealth::down(
            service,
            endpoint,
            format!(
                "health check 超過 {} 秒沒有回應。服務可能在啟動中或負載過高。",
                CHECK_TIMEOUT.as_secs()
            ),
        ),
        Ok(Err(message)) => ServiceHealth::down(service, endpoint, message),
        Ok(Ok(health)) => ServiceHealth::from_storage(service, endpoint, health),
    }
}

async fn check_postgres(ctx: &Context) -> ServiceHealth {
    // DSN 含密碼，不能當成 endpoint 印出來。只顯示 SecretRef 的名字。
    let endpoint = ctx
        .cfg
        .storage
        .canonical
        .dsn_secret_ref
        .as_str()
        .to_string();
    with_timeout("PostgreSQL", endpoint, async {
        let store = ctx.store().await.map_err(|err| err.to_string())?;
        store.health().await.map_err(|err| err.to_string())
    })
    .await
}

async fn check_object(ctx: &Context) -> ServiceHealth {
    let endpoint = ctx.cfg.storage.object.endpoint.clone();
    with_timeout("MinIO/S3", endpoint, async {
        let objects = ctx.objects().map_err(|err| err.to_string())?;
        objects.health().await.map_err(|err| err.to_string())
    })
    .await
}

async fn check_redis(ctx: &Context) -> ServiceHealth {
    let endpoint = ctx.cfg.storage.cache.url_secret_ref.as_str().to_string();
    with_timeout("Redis", endpoint, async {
        let url = ctx
            .cfg
            .storage
            .cache
            .url_secret_ref
            .resolve()
            .map_err(|err| err.to_string())?;
        let store = RedisKeyValueStore::connect(&url).map_err(|err| err.to_string())?;
        store.health().await.map_err(|err| err.to_string())
    })
    .await
}

async fn check_search(ctx: &Context) -> ServiceHealth {
    let endpoint = ctx.cfg.storage.search.url.clone();
    let url = endpoint.clone();
    with_timeout("OpenSearch", endpoint, async move {
        verify_not_opencti_search(&url).map_err(|err| err.to_string())?;
        let store = OpenSearchStore::connect(&url).map_err(|err| err.to_string())?;
        store.health().await.map_err(|err| err.to_string())
    })
    .await
}

async fn check_broker(ctx: &Context) -> ServiceHealth {
    let endpoint = ctx.cfg.broker.brokers.clone();
    let brokers = endpoint.clone();
    with_timeout("Redpanda", endpoint, async move {
        let producer =
            EventProducer::connect(&brokers, "osint-cli").map_err(|err| err.to_string())?;
        let (broker_count, topic_count) = producer
            .cluster_metadata(CHECK_TIMEOUT)
            .await
            .map_err(|err| err.to_string())?;
        if broker_count == 0 {
            return Ok(StorageHealth::down(
                "redpanda",
                "metadata 查得到但 broker 數是 0".to_string(),
            ));
        }
        Ok(
            StorageHealth::ok("redpanda", format!("metadata OK，{broker_count} 個 broker"))
                .with_details(json!({
                    "brokers": broker_count,
                    "topics": topic_count,
                })),
        )
    })
    .await
}
