//! `osint-api`：V0.1 API skeleton。

use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Duration as ChronoDuration;
use connector_sdk::StoreEvidenceSink;
use core_api::{
    AppState, AuthState, AutoApprovalState, BackendCheck, BrokerCheck, ErrorBody,
    GraphProjectionState, ImportState, PostgresReady, QueueBinding, QueueInspector, ReadyCheck,
    ReadyProbe, SemanticSearchState, SharedGraphStore, SharedObjects, SharedStore,
    SharedTokenStore, router, warn_unimplemented_hybrid_weights,
};
use core_config::AppConfig;
use core_events::EventProducer;
use core_jobs::JobService;
use core_observability::{MetricsRegistry, init_tracing};
use core_security::{AuditLog, JwtService, MemoryApiTokenStore, MemoryAuditLog};
use merge::MergeService;
use resolver::{GraphContextResolver, ResolverService};
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, verify_not_opencti_search,
};
use storage_core::mock::MockEmbeddingProvider;
use storage_postgres::{PostgresApiTokenStore, PostgresAuditLog, PostgresCanonicalStore};
use tokio::net::TcpListener;

/// 查一個 consumer group lag 的逾時。
///
/// 短是刻意的：`/ops/queues` 要查四個 group，每個都可能要打好幾次 broker。
/// 逾時設長的話 Redpanda 掛掉時運維看到的會是「這個頁面沒反應」
/// 而不是「Redpanda 連不上」，而且會撞到 router 的 30 秒請求逾時（回 408）。
const LAG_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

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

    let mut shared_store: Option<SharedStore> = None;
    let mut shared_objects: Option<SharedObjects> = None;
    // 具體型別的 Postgres store，給 GraphContextResolver 用。
    // `shared_store` 是型別擦除過的 `Arc<dyn RelationalStore>`，裝不進
    // `GraphContextResolver<PostgresCanonicalStore, _>`。
    let mut pg_store: Option<PostgresCanonicalStore> = None;
    // `/api/v1/ops/health` 要敲的後端。**用具體型別建**：
    // `Arc<dyn RelationalStore>` 沒辦法直接當成 `Arc<dyn HealthProvider>`
    // （trait object 之間不能互轉），而這裡手上正好有具體的 store。
    let mut health_checks: Vec<Arc<dyn ReadyCheck>> = Vec::new();
    let mut missing: Vec<&'static str> = Vec::new();
    // 預設是記憶體版。Postgres 接上後就換成落地版——**不要**把這裡當成常態：
    // 記憶體 audit 在重啟時整批消失，記憶體 token store 會讓發出去的 token 失效。
    // 沒換成功時下面會發一條 warn，那是唯一的訊號。
    let mut audit: Arc<dyn AuditLog> = Arc::new(MemoryAuditLog::new());
    let mut tokens: SharedTokenStore = Arc::new(MemoryApiTokenStore::new());

    let (jobs, merge, resolver, auto_approval, import, ready) = match connect_postgres(&cfg).await {
        Ok(store) => {
            let ready = ReadyProbe::new(vec![Arc::new(PostgresReady {
                store: store.clone(),
            })]);
            // 稽核與 token 落到 `audit_log` / `api_tokens`（migration 0006）。
            // 兩者共用 canonical store 的連線池，不另開池。
            audit = Arc::new(PostgresAuditLog::new(&store));
            tokens = Arc::new(PostgresApiTokenStore::new(&store));

            // 一份 Arc 給所有 handler 用（AppState.store 與 ImportState.store 是同一個）。
            let shared: SharedStore = Arc::new(store.clone());
            shared_store = Some(shared.clone());
            health_checks.push(Arc::new(BackendCheck::new(
                "postgres",
                Arc::new(store.clone()),
            )));

            // 匯入另外需要物件儲存。MinIO 沒接上時只有 /api/v1/import 回 503，
            // 其他路由照常——沒有理由讓查詢功能陪著一起掛掉。
            let import = match connect_objects(&cfg).await {
                Ok(objects) => {
                    shared_objects = Some(Arc::new(objects.clone()));
                    health_checks.push(Arc::new(BackendCheck::new(
                        "object_store",
                        Arc::new(objects.clone()),
                    )));
                    Some(Arc::new(ImportState {
                        store: shared,
                        // sink 用**具體型別**組裝：`StoreEvidenceSink<R, O>` 要求
                        // `R: RelationalStore`，而 `Arc<dyn RelationalStore>` 本身
                        // 沒有實作那個 trait（沒有 blanket impl）。這裡手上正好有
                        // 具體的 store，不需要為此加一層 wrapper。
                        sink: Arc::new(StoreEvidenceSink::new(store.clone(), objects)),
                        producer: producer.clone(),
                    }))
                }
                Err(err) => {
                    tracing::warn!(error = %err, "MinIO 未連上；POST /api/v1/import 會回 503");
                    missing.push("object_store");
                    None
                }
            };
            let jobs = Some(Arc::new(JobService::new(store.clone(), producer.clone())));
            let merge = Some(Arc::new(MergeService::new(store.clone(), producer.clone())));
            // MockEmbeddingProvider::unsupported() 讓 semantic_similarity 誠實回空，
            // 不是假裝已接上。graph_context 不在 ResolverService 上，見下方
            // 獨立的 graph_resolver。
            let resolver = Some(Arc::new(ResolverService::new(
                store.clone(),
                MockEmbeddingProvider::unsupported(),
            )));
            let auto_approval = Some(Arc::new(assemble_auto_approval(
                store.clone(),
                producer.clone(),
                &cfg.auto_approval,
            )));
            pg_store = Some(store);
            (jobs, merge, resolver, auto_approval, import, ready)
        }
        Err(err) => {
            tracing::warn!(
                error = %err,
                "Postgres 未連上；/jobs、/api/v1/import 與 /api/v1/tokens 會回 503，\
                 /health 仍可用。稽核與 API token 這次只存在記憶體裡，重啟即遺失"
            );
            missing.push("postgres");
            missing.push("object_store");
            (None, None, None, None, None, ReadyProbe::always_ready())
        }
    };

    // graph_resolver、graph、graph_projection 共用**同一次** `connect_graph`——不要連兩次。
    // `Neo4jStore` 是 Clone（連線池 handle），clone 很便宜。
    // 三者跟 resolver 都是獨立的可用性：Neo4j 沒接上只讓
    // POST /entities/{id}/resolve/graph-context、/graph/* 讀取路由與 /ops/graph 回 503，
    // resolve_entity 的另外幾個方法不受影響。
    // Postgres 沒接上時 graph_resolver 也是 None：沒有 canonical store 就沒辦法 persist candidate。
    // graph 讀取端只需要 Neo4j，理論上 Postgres 掛了仍可查圖；但目前 connect_graph
    // 只在有 pg_store 時呼叫一次，兩邊一起 None——不要為了這個分叉再開第二次連線。
    let (graph_resolver, graph, graph_projection) = if let Some(store) = pg_store {
        match connect_graph(&cfg).await {
            Ok(neo4j) => {
                // Bolt 健康檢查用同一個 clone，不要再開第二次連線。
                // 名字刻意是 `neo4j_bolt`：現有 HTTP 檢查已經叫 `neo4j`，兩筆獨立。
                // `Arc::new(neo4j.clone())` 從具體型別建 `Arc<dyn HealthProvider>`，
                // 不要寫 trait upcasting（workspace 鎖 1.85，upcasting 1.86 才穩定）。
                health_checks.push(Arc::new(BackendCheck::new(
                    "neo4j_bolt",
                    Arc::new(neo4j.clone()),
                )));
                let graph: SharedGraphStore = Arc::new(neo4j.clone());
                let graph_projection = Some(Arc::new(GraphProjectionState {
                    store: Arc::new(neo4j.clone())
                        as Arc<dyn storage_core::ProjectionStore + Send + Sync>,
                    projection: cfg.graph_worker.projection.clone(),
                }));
                let resolver = Arc::new(GraphContextResolver::new(store, neo4j));
                (Some(resolver), Some(graph), graph_projection)
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Neo4j 未連上；POST /api/v1/entities/{{id}}/resolve/graph-context \
                     與 GET／POST /api/v1/graph/* 讀取路由、GET /api/v1/ops/graph 會回 503"
                );
                missing.push("neo4j");
                missing.push("neo4j_bolt");
                (None, None, None)
            }
        }
    } else {
        // 沒有 Postgres 就不會連 Bolt。HTTP 探活下面仍可能獨立掛上，
        // 但 `neo4j_bolt` 必須列進 not_configured，否則它會從清單上消失。
        missing.push("neo4j_bolt");
        (None, None, None)
    };

    // 搜尋接不上時只有 POST /api/v1/search 回 503，其他路由照常——
    // 沒有理由讓 Job 查詢陪著搜尋投影一起掛掉。
    let search = match connect_search(&cfg).await {
        Ok(state) => {
            health_checks.push(Arc::new(BackendCheck::new(
                "opensearch",
                Arc::new(state.store.clone()),
            )));
            Some(Arc::new(state))
        }
        Err(err) => {
            tracing::warn!(error = %err, "OpenSearch 未連上；POST /api/v1/search 會回 503");
            missing.push("opensearch");
            None
        }
    };

    // 語意搜尋跟全文搜尋分開組裝：ml-commons 模型沒部署時，只有
    // POST /api/v1/search/semantic 回 503，不要連帶讓 BM25 搜尋掛掉。
    // 連線失敗不推進 `missing`：`/ops/health` 的後端名單沒有獨立的
    // ml-commons 項目，OpenSearch 本身的健康已經由上面的 `opensearch` 檢查覆蓋。
    let semantic_search = match connect_semantic_search(&cfg).await {
        Ok(state) => Some(Arc::new(state)),
        Err(err) => {
            tracing::warn!(
                error = %err,
                "語意搜尋未接上（OpenSearch 或 ml-commons）；POST /api/v1/search/semantic、\
                 GET /api/v1/objects/{{id}}/similar 與 POST /api/v1/search/hybrid 會回 503"
            );
            None
        }
    };

    // Redis 在 V0.1 沒有 API 路由用到它（它是 collector／worker 的節流與去重快取），
    // 但運維要看得到它活著——pipeline 會因為它掛掉而變慢卻不報錯。
    match connect_cache(&cfg) {
        Ok(cache) => health_checks.push(Arc::new(BackendCheck::new("redis", Arc::new(cache)))),
        Err(err) => {
            tracing::warn!(error = %err, "Redis 未連上；GET /api/v1/ops/health 會標示為未設定");
            missing.push("redis");
        }
    }

    match &producer {
        Some(producer) => health_checks.push(Arc::new(BrokerCheck::new(producer.clone()))),
        None => missing.push("redpanda"),
    }

    // Neo4j HTTP 探活（7474）。Bolt 探活是上面 `connect_graph` 成功時推進去的
    // `neo4j_bolt`（`BackendCheck` 跑 `RETURN 1`），兩個檢查各自獨立。
    //
    // 空字串 = 刻意未設定（例如還沒起 Neo4j 的環境），列進 not_configured
    // 而不是每次 health 都去連一個不存在的位址然後報 down。
    //
    // `connect_graph` 失敗時已經把 "neo4j" 推進 missing——不要再推一次，
    // 否則 `/ops/health` 的 `not_configured` 會出現兩個 neo4j。
    let graph_url = cfg.storage.graph.http_url.trim();
    if !missing.contains(&"neo4j") {
        if graph_url.is_empty() {
            tracing::info!(
                "[storage.graph].http_url 未設定；GET /api/v1/ops/health 會標示為未設定"
            );
            missing.push("neo4j");
        } else {
            match core_api::GraphCheck::new(graph_url) {
                Some(check) => health_checks.push(Arc::new(check)),
                None => {
                    tracing::warn!(
                        url = graph_url,
                        "Neo4j 探針建立失敗（HTTP client 無法建立）；\
                         GET /api/v1/ops/health 會標示為未設定"
                    );
                    missing.push("neo4j");
                }
            }
        }
    }

    // `GET /api/v1/ops/queues`。與 producer 分開建：producer 連不上時 lag 探針
    // 仍然值得建起來（它自己會回報查不到），但 broker 位址設定錯誤要在這裡就講清楚。
    let queues = match core_events::GroupLagProbe::new(&cfg.broker.brokers, LAG_PROBE_TIMEOUT) {
        Ok(probe) => Some(Arc::new(QueueInspector {
            probe,
            bindings: queue_bindings(&cfg),
        })),
        Err(err) => {
            tracing::warn!(error = %err, "consumer group lag 探針未建立；GET /api/v1/ops/queues 會回 503");
            None
        }
    };

    warn_unimplemented_hybrid_weights(&cfg.search_hybrid);

    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens,
        },
        audit,
        store: shared_store,
        objects: shared_objects,
        jobs,
        merge,
        resolver,
        auto_approval,
        graph_resolver,
        graph,
        graph_projection,
        import,
        search,
        semantic_search,
        hybrid_weights: cfg.search_hybrid.clone(),
        ready,
        // 沒接上的後端不會變成「壞掉」，而是列進 `backends_missing`——
        // 「沒設定」與「壞了」的下一步完全不同。
        backends: ReadyProbe::new(health_checks),
        queues,
        backends_missing: missing,
        rate_limit_per_second: cfg.http.rate_limit_per_second,
        request_body_limit_bytes: cfg.http.request_body_limit_bytes,
        import_config: cfg.import.clone(),
        stix_config: cfg.stix.clone(),
        object_bucket: cfg.storage.object.bucket.clone(),
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
    // `into_make_service_with_connect_info` 是為了讓 auth middleware 拿得到 TCP peer IP
    // 寫進稽核。少了它 `audit_log.ip` 會全部是 NULL，而且完全不會報錯——
    // 查「那些 401 是從哪裡來的」時才會發現沒有資料。
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .map_err(|err| format!("伺服器結束：{err}"))
}

/// `GET /api/v1/ops/queues` 要查哪幾個 (service, group, topic)。
///
/// **group 名稱一律從設定讀**，不要在這裡寫死字串：運維改了
/// `[normalizer].consumer_group` 卻沒改這裡的話，面板會去查一個不存在的 group
/// 並永遠回 lag 0——看起來完全正常的假綠燈。
///
/// topic 則是寫死的：那是每個服務訂閱哪個 topic 的事實（見各服務的 main.rs），
/// 不是設定項。
fn queue_bindings(cfg: &AppConfig) -> Vec<QueueBinding> {
    vec![
        QueueBinding {
            service: "normalizer",
            group: cfg.normalizer.consumer_group.clone(),
            topic: core_events::EventTopic::RawCollected.as_str(),
        },
        QueueBinding {
            service: "deduplicator",
            group: cfg.deduplicator.consumer_group.clone(),
            topic: core_events::EventTopic::ObjectNormalized.as_str(),
        },
        QueueBinding {
            service: "entity-worker",
            group: cfg.entity_worker.consumer_group.clone(),
            topic: core_events::EventTopic::DedupCompleted.as_str(),
        },
        QueueBinding {
            service: "indexer",
            group: cfg.indexer.consumer_group.clone(),
            topic: core_events::EventTopic::EntityExtracted.as_str(),
        },
    ]
}

/// 從 `[auto_approval]` 組 [`AutoApprovalState`]。
///
/// 門檻不自洽時強制停用（`core-config` 沒有 `tracing`，警告只能在這裡發）。
/// 不論 `enabled` 是不是 true 都會組出 evaluator：關閉路徑是
/// [`resolver::AutoApprovalEvaluator::evaluate_pair`] 立刻回 `Disabled`，
/// 組裝成本幾乎為零，handler 也不用再分「沒組」與「組了但關著」兩種 `None`。
fn assemble_auto_approval(
    store: PostgresCanonicalStore,
    producer: Option<Arc<EventProducer>>,
    section: &core_config::AutoApprovalSection,
) -> AutoApprovalState {
    let effectively_enabled = section.enabled && section.thresholds_are_sane();
    if section.enabled && !section.thresholds_are_sane() {
        tracing::error!(
            auto_confirm_score = section.auto_confirm_score,
            llm_review_score = section.llm_review_score,
            "auto_approval.enabled=true 但門檻不自洽（auto_confirm_score 應該 \
             >= llm_review_score），已強制停用自動核准，所有候選維持 Pending"
        );
    }
    if effectively_enabled {
        tracing::warn!(
            auto_confirm_score = section.auto_confirm_score,
            llm_review_score = section.llm_review_score,
            llm_enabled = section.llm.enabled,
            "AI 輔助自動核准已啟用（ADR-012）。門檻未經真實 OSINT 語料驗證，\
             見 docs/adr/ADR-012-ai-assisted-auto-approval.md。建議上線初期定期 \
             抽查 merge_history WHERE auto_approval_audit IS NOT NULL"
        );
    }
    let llm_provider = ai_gateway::OpenAiCompatibleLlmProvider::new(
        &ai_gateway::OpenAiCompatibleLlmProviderConfig {
            enabled: effectively_enabled && section.llm.enabled,
            base_url: section.llm.base_url.clone(),
            timeout: std::time::Duration::from_secs(section.llm.timeout_secs),
            max_concurrent: section.llm.max_concurrent,
            max_retries: 0,
            rate_limit_per_second: None,
        },
    );
    let evaluator_config = resolver::AutoApprovalConfig {
        enabled: effectively_enabled,
        auto_confirm_score: section.auto_confirm_score,
        llm_review_score: section.llm_review_score,
        llm_model: section.llm.model.clone(),
        llm_temperature: section.llm.temperature,
        llm_max_tokens: section.llm.max_tokens,
    };
    AutoApprovalState {
        evaluator: Arc::new(resolver::AutoApprovalEvaluator::new(
            store,
            producer,
            llm_provider,
            evaluator_config,
        )),
        max_auto_merges_per_resolve: section.max_auto_merges_per_resolve,
    }
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

async fn connect_graph(cfg: &AppConfig) -> Result<storage_neo4j::Neo4jStore, String> {
    let password = cfg
        .storage
        .graph
        .password_secret_ref
        .resolve()
        .map_err(|e| e.to_string())?;
    storage_neo4j::Neo4jStore::connect(
        &cfg.storage.graph.bolt_uri,
        &cfg.storage.graph.username,
        &password,
        cfg.storage.graph.pool_max,
    )
    .await
    .map_err(|e| e.to_string())
}

/// 連 OpenSearch 並驗證它真的是 OpenSearch。
///
/// **身分驗證不是形式。** 本工作站的 9200 是 OpenCTI 的 Elasticsearch；
/// 少了這一步，設定寫錯一個埠號就會去查別人的叢集，而且一路都不會報錯——
/// 使用者只會覺得「搜尋結果怪怪的」。
async fn connect_search(cfg: &AppConfig) -> Result<core_api::SearchState, String> {
    let url = &cfg.storage.search.url;
    verify_not_opencti_search(url).map_err(|err| err.to_string())?;
    let store = storage_opensearch::OpenSearchStore::connect(url).map_err(|err| err.to_string())?;
    let info = store
        .cluster_info()
        .await
        .map_err(|err| format!("連不上 OpenSearch（{url}）：{err}"))?;
    assert_opensearch_identity(&info).map_err(|err| err.to_string())?;
    Ok(core_api::SearchState {
        store,
        // 與 osint-indexer 讀同一個設定鍵。兩邊不一致的話搜尋會查到空 index，
        // 而且不會有任何錯誤訊息。
        index: cfg.indexer.index.clone(),
    })
}

/// 連 OpenSearch + ml-commons。任一失敗整份 state 是 `None`。
///
/// 與 [`connect_search`] 分開各自持有連線：比照 `graph`／`graph_resolver`／
/// `graph_projection` 各自獨立 wiring 的既有慣例。`index` 讀 `[indexer].index`
/// （Document index），不要看成 `[embedding_worker].entities_index`。
async fn connect_semantic_search(cfg: &AppConfig) -> Result<SemanticSearchState, String> {
    let url = &cfg.storage.search.url;
    verify_not_opencti_search(url).map_err(|err| err.to_string())?;
    let store = storage_opensearch::OpenSearchStore::connect(url).map_err(|err| err.to_string())?;
    let info = store
        .cluster_info()
        .await
        .map_err(|err| format!("連不上 OpenSearch（{url}）：{err}"))?;
    assert_opensearch_identity(&info).map_err(|err| err.to_string())?;
    let embeddings = storage_opensearch::MlCommonsEmbeddingProvider::connect(url)
        .await
        .map_err(|err| {
            format!(
                "連不上 OpenSearch ml-commons 或模型不是 DEPLOYED（{url}）：{err}。\
                 請跑 `bash scripts/opensearch-ml-setup.sh` 與 \
                 `bash scripts/opensearch-ml-setup-e5.sh` 後重啟 osint-api"
            )
        })?;
    Ok(SemanticSearchState {
        store,
        embeddings,
        index: cfg.indexer.index.clone(),
    })
}

/// 連 Redis。只給 `/api/v1/ops/health` 用，連不上不影響其他路由。
fn connect_cache(cfg: &AppConfig) -> Result<storage_redis::RedisKeyValueStore, String> {
    let url = cfg
        .storage
        .cache
        .url_secret_ref
        .resolve()
        .map_err(|err| err.to_string())?;
    storage_redis::RedisKeyValueStore::connect(&url).map_err(|err| err.to_string())
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
            message: "沒有這個路徑。V0.1 提供 GET /health /ready /metrics、/api/v1/jobs、\
                 /api/v1/tokens（admin）、/api/v1/ops/health、/api/v1/ops/metrics、\
                 /api/v1/ops/connectors、/api/v1/ops/queues、/api/v1/ops/dlq 與 /api/v1/ops/graph、\
                 sources／connectors／collections／objects／entities／relationships／events／raw、\
                 /api/v1/graph/*、POST /api/v1/import、POST /api/v1/search、POST /api/v1/search/semantic、\
                 POST /api/v1/search/hybrid 與 GET /api/v1/objects/{id}/similar"
                .into(),
        }),
    )
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
