//! `osint-embedding-worker`：訂閱 `entity.extracted`，把 Document／Entity 向量寫進 OpenSearch。
//!
//! 兩種模式：
//!
//! ```text
//! osint-embedding-worker                     常駐消費（預設）
//! osint-embedding-worker --rebuild           從 PostgreSQL 補齊向量後結束
//! osint-embedding-worker --rebuild --drop    先刪掉 osint-entities 再從零重建後結束
//! ```
//!
//! `--drop` **只**刪 `osint-entities`。永不刪 `osint-documents`——那個 index
//! 是 indexer 的，本服務只疊加向量欄位；刪掉等於把搜尋投影整份清掉，
//! 而且本服務重建時不會把 title／body／entities 補回去。
//!
//! 沒有用 clap：這支 binary 只有兩個旗標，為它多一個相依不划算
//! （`osint-cli` 才是有子命令的那一支）。

use std::time::Duration;

use core_config::AppConfig;
use core_events::{EventConsumer, EventTopic};
use core_observability::{MetricsRegistry, init_tracing};
use embedding_worker::schema as entity_schema;
use embedding_worker::service::RebuildOptions;
use embedding_worker::{EmbeddingWorker, serve_health};
use std::sync::Arc;

use storage_core::HealthProvider;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, verify_not_opencti_search,
};
use storage_opensearch::{MlCommonsEmbeddingProvider, OpenSearchStore};
use storage_postgres::PostgresCanonicalStore;
use storage_redis::RedisKeyValueStore;

/// 一次 poll 等多久。沒有批次，逾時只是為了能回應 SIGINT 與量 lag。
const IDLE_POLL: Duration = Duration::from_secs(30);
/// 每幾則事件量一次 consumer lag。
const LAG_PROBE_EVERY: u64 = 10;
/// 量 lag 的逾時。量不到就當作沒有 lag，不要讓它擋住投影。
const LAG_TIMEOUT: Duration = Duration::from_secs(2);

#[tokio::main]
async fn main() {
    load_workspace_dotenv();
    if let Err(err) = init_tracing("info,rdkafka=warn,librdkafka=warn") {
        eprintln!("tracing 已初始化：{err}");
    }
    if let Err(err) = run().await {
        tracing::error!(error = %err, "osint-embedding-worker 結束");
        eprintln!("{err}");
        std::process::exit(1);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Args {
    rebuild: bool,
    drop_entities: bool,
}

/// 參數解析結果。`Help` 是**成功**路徑——回 exit 1 並記一行 ERROR log
/// 會讓「我只是想看用法」在 CI 裡變成一次失敗。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedArgs {
    Run(Args),
    Help,
}

fn parse_args<I: IntoIterator<Item = String>>(args: I) -> Result<ParsedArgs, String> {
    let mut parsed = Args {
        rebuild: false,
        drop_entities: false,
    };
    for arg in args {
        match arg.as_str() {
            "--rebuild" => parsed.rebuild = true,
            "--drop" | "--drop-index" => parsed.drop_entities = true,
            "-h" | "--help" => {
                return Ok(ParsedArgs::Help);
            }
            other => {
                return Err(format!("不認得的參數 `{other}`。\n{HELP}"));
            }
        }
    }
    if parsed.drop_entities && !parsed.rebuild {
        return Err(
            "--drop 必須搭配 --rebuild 使用。單獨刪掉 osint-entities 會讓 Entity 向量搜尋直接空掉，\
             而且沒有任何東西會把它補回來。\n"
                .to_string() + HELP,
        );
    }
    Ok(ParsedArgs::Run(parsed))
}

const HELP: &str = "用法：
  osint-embedding-worker                  訂閱 entity.extracted 常駐寫向量
  osint-embedding-worker --rebuild        從 PostgreSQL 補齊向量（既有文件覆寫）後結束
  osint-embedding-worker --rebuild --drop 先刪掉 osint-entities 再從零重建後結束
                                          （mapping 有破壞性變更時必須用這個）
                                          --drop 只刪 osint-entities，永不刪 osint-documents
設定來源：config/default.toml → OSINT_CONFIG_FILE → OSINT__* 環境變數。";

async fn run() -> Result<(), String> {
    let args = match parse_args(std::env::args().skip(1))? {
        ParsedArgs::Help => {
            println!("{HELP}");
            return Ok(());
        }
        ParsedArgs::Run(args) => args,
    };
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

    let search = connect_search(&cfg.storage.search.url).await?;
    let embeddings = MlCommonsEmbeddingProvider::connect(&cfg.storage.search.url)
        .await
        .map_err(|err| err.to_string())?;
    let metrics = MetricsRegistry::new();

    let documents_index = cfg.indexer.index.clone();
    let entities_index = cfg.embedding_worker.entities_index.clone();

    let mut service = EmbeddingWorker::new(
        store.clone(),
        embeddings,
        search.clone(),
        metrics.clone(),
        embedding_worker::EmbeddingBounds::new(
            documents_index.clone(),
            entities_index.clone(),
            cfg.embedding.batch_size,
            cfg.embedding.concurrent_inferences,
        ),
    );
    match connect_embedding_cache(&cfg).await {
        Ok(redis) => {
            tracing::info!("embedding Redis 快取已接上（Stage 5 寫入的向量可在這裡撿）");
            service = service.with_embedding_cache(Arc::new(redis));
        }
        Err(reason) => {
            tracing::warn!(
                %reason,
                "embedding Redis 快取這次沒接上，永遠 miss，改打 ml-commons。\
                 不影響正確性；修好 REDIS_URL 後重啟即可"
            );
        }
    }

    if args.rebuild {
        if args.drop_entities {
            if entities_index == documents_index {
                return Err(format!(
                    "[embedding_worker].entities_index 與 [indexer].index 都是 `{entities_index}`。\
                     --drop 只該刪 Entity 向量 index，兩個同名的話會連搜尋投影一起刪掉。\
                     請把 entities_index 改成獨立名稱（預設 osint-entities）"
                ));
            }
            tracing::warn!(
                index = %entities_index,
                "即將刪掉 Entity 向量 index。osint-documents 不會動"
            );
            let _ = search
                .delete_index(&entities_index)
                .await
                .map_err(|err| err.to_string())?;
        }
        search
            .ensure_index_with(
                &entities_index,
                &entity_schema::index_settings(),
                &entity_schema::index_mappings(),
            )
            .await
            .map_err(|err| err.to_string())?;
        let report = service
            .rebuild(RebuildOptions {
                page_size: cfg.embedding_worker.page_size.max(1),
            })
            .await
            .map_err(|err| err.to_string())?;
        println!(
            "rebuild 完成：掃過 {} 筆 Document（跳過 {} 筆 duplicate）、\
             掃過 {} 筆 Entity（跳過 {} 筆 merged）；\
             寫入 title {}／body {}／entity {}；失敗 {} 筆。\
             Document 向量疊加進 `{}`，Entity 向量寫進 `{}`。",
            report.documents_scanned,
            report.documents_skipped_duplicate,
            report.entities_scanned,
            report.entities_skipped_merged,
            report.title_applied,
            report.body_applied,
            report.entities_applied,
            report.failed,
            documents_index,
            entities_index,
        );
        if report.failed > 0 {
            return Err(format!(
                "有 {} 筆寫入失敗。請看上面的 error log 判斷原因；\
                 mapping 不符時要先改 crates/embedding-worker/src/schema.rs \
                 再用 --rebuild --drop 重來（只刪 osint-entities）",
                report.failed
            ));
        }
        return Ok(());
    }

    search
        .ensure_index_with(
            &entities_index,
            &entity_schema::index_settings(),
            &entity_schema::index_mappings(),
        )
        .await
        .map_err(|err| err.to_string())?;

    let bind = cfg.embedding_worker.bind.clone();
    let health_metrics = metrics.clone();
    let health_store = store.clone();
    let health_search = search.clone();
    tokio::spawn(async move {
        if let Err(err) = serve_health(&bind, health_metrics, health_store, health_search).await {
            tracing::error!(error = %err, "embedding-worker health 結束");
        }
    });

    let consumer = EventConsumer::connect(
        &cfg.broker.brokers,
        &cfg.embedding_worker.consumer_group,
        &[EventTopic::EntityExtracted.as_str()],
    )
    .map_err(|err| err.to_string())?;

    tracing::info!(
        group = %cfg.embedding_worker.consumer_group,
        documents_index = %documents_index,
        entities_index = %entities_index,
        batch_size = cfg.embedding.batch_size,
        concurrent_inferences = cfg.embedding.concurrent_inferences,
        "embedding-worker 開始消費 entity.extracted"
    );

    consume_loop(&service, &consumer, &metrics).await;
    Ok(())
}

/// 連 OpenSearch 並確認它真的是 OpenSearch。
///
/// **這個檢查不是形式。** 本工作站的 9200 是 OpenCTI 的 Elasticsearch；
/// 少了它，設定寫錯一個埠號就會開始往別人的叢集寫資料，而且一路都不會報錯。
async fn connect_search(url: &str) -> Result<OpenSearchStore, String> {
    verify_not_opencti_search(url).map_err(|err| err.to_string())?;
    let store = OpenSearchStore::connect(url).map_err(|err| err.to_string())?;
    let info = store.cluster_info().await.map_err(|err| {
        format!("連不上 OpenSearch（{url}）：{err}。請先 `make compose-up` 並確認 OPENSEARCH_URL")
    })?;
    assert_opensearch_identity(&info).map_err(|err| err.to_string())?;
    tracing::info!(
        url,
        cluster = ?info.get("cluster_name"),
        version = ?info.pointer("/version/number"),
        "OpenSearch 身分驗證通過"
    );
    Ok(store)
}

/// 連 Redis 當 Stage 5 向量暫存。連不上**不**讓服務啟動失敗。
async fn connect_embedding_cache(
    cfg: &core_config::AppConfig,
) -> Result<RedisKeyValueStore, String> {
    let url = cfg
        .storage
        .cache
        .url_secret_ref
        .resolve()
        .map_err(|err| format!("解析 [storage.cache].url_secret_ref：{err}"))?;
    let redis = RedisKeyValueStore::connect(&url).map_err(|err| format!("Redis client：{err}"))?;
    let health = redis
        .health()
        .await
        .map_err(|err| format!("Redis PING 失敗：{err}"))?;
    if !health.healthy {
        return Err(format!("Redis 不健康：{}", health.message));
    }
    Ok(redis)
}

/// 消費迴圈。逐則處理，沒有批次。
///
/// # offset 提交規則
///
/// 真的失敗（Postgres／OpenSearch 連不上、非預期 `StorageError`）**不 commit**，
/// 讓 broker 重送。先提交再寫的話，寫入失敗或行程被殺時那則向量會永遠不進
/// 投影，而且沒有任何跡象。
///
/// `update_fields` 重試耗盡仍 NotFound（indexer lag）**仍 commit**：卡住
/// partition 等 indexer 沒有意義——indexer 可能永遠寫不進去（mapping 不符）。
/// 漏寫靠 `--rebuild` 回填。這與 indexer 永久性 bulk 失敗同一慣例。
///
/// SIGINT **不**額外 commit：上一則成功時已經 commit 過；上一則失敗時必須
/// 留給 broker 重送。關機時再 commit 一次會把失敗那則吃掉。
async fn consume_loop(
    service: &EmbeddingWorker<PostgresCanonicalStore, MlCommonsEmbeddingProvider, OpenSearchStore>,
    consumer: &EventConsumer,
    metrics: &MetricsRegistry,
) {
    let mut processed = 0_u64;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("收到 SIGINT，embedding-worker 結束");
                break;
            }
            result = consumer.next_envelope(IDLE_POLL) => {
                match result {
                    Ok(envelope) => {
                        match service.process_extracted(&envelope.payload).await {
                            Ok(report) => {
                                tracing::info!(
                                    event_id = %envelope.id,
                                    document_id = ?report.document_id,
                                    title_applied = report.title_applied,
                                    body_applied = report.body_applied,
                                    entities_applied = report.entities_applied,
                                    update_fields_exhausted = report.update_fields_exhausted,
                                    "entity.extracted 已處理"
                                );
                                if let Err(err) = consumer.commit_last() {
                                    tracing::warn!(error = %err, "commit offset 失敗");
                                }
                            }
                            Err(err) => {
                                metrics.inc("osint_embedding_worker_errors_total", 1);
                                tracing::error!(
                                    error = %err,
                                    event_id = %envelope.id,
                                    "處理 entity.extracted 失敗，不提交 offset，這則事件會被重送"
                                );
                            }
                        }
                        processed += 1;
                        if processed % LAG_PROBE_EVERY == 0 {
                            update_queue_depth(consumer, metrics);
                        }
                    }
                    Err(core_events::EventError::ConsumeTimeout { .. }) => {
                        update_queue_depth(consumer, metrics);
                    }
                    Err(err) => {
                        tracing::error!(error = %err, "消費事件失敗");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    }
}

fn update_queue_depth(consumer: &EventConsumer, metrics: &MetricsRegistry) {
    match consumer.consumer_lag(LAG_TIMEOUT) {
        Ok(lag) => metrics.set_queue_depth(lag),
        Err(err) => {
            tracing::warn!(error = %err, "量不到 consumer lag，本輪不更新 osint_queue_depth")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(list: &[&str]) -> Result<ParsedArgs, String> {
        parse_args(list.iter().map(ToString::to_string))
    }

    #[test]
    fn default_is_consume_mode() {
        assert_eq!(
            args(&[]).unwrap(),
            ParsedArgs::Run(Args {
                rebuild: false,
                drop_entities: false
            })
        );
    }

    #[test]
    fn rebuild_and_drop_parse() {
        assert_eq!(
            args(&["--rebuild", "--drop"]).unwrap(),
            ParsedArgs::Run(Args {
                rebuild: true,
                drop_entities: true
            })
        );
    }

    #[test]
    fn help_is_a_success_path() {
        for flag in ["-h", "--help"] {
            assert_eq!(args(&[flag]).unwrap(), ParsedArgs::Help, "{flag}");
        }
    }

    #[test]
    fn drop_without_rebuild_is_rejected() {
        let err = args(&["--drop"]).unwrap_err();
        assert!(err.contains("--rebuild"), "{err}");
        assert!(err.contains("osint-entities"), "{err}");
    }

    #[test]
    fn unknown_flag_shows_usage() {
        let err = args(&["--reindex"]).unwrap_err();
        assert!(err.contains("不認得的參數"), "{err}");
        assert!(err.contains("osint-embedding-worker --rebuild"), "{err}");
    }
}
