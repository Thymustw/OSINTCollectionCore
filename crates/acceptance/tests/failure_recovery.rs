//! `TEST_STRATEGY.md` §6 failure/recovery：**後端真的停掉之後，系統的行為是什麼。**
//!
//! 涵蓋 §6 清單裡的四項（AI 三項屬於 V0.2 之後，這裡不做）：
//!
//! | 測試 | 停掉什麼 | 要證明的事 |
//! |---|---|---|
//! | [`broker_redelivery_is_idempotent`] | 不停服務（consumer 不 commit 就 drop） | 同 group 的下一個 consumer 會拿到同一則，重新處理不會多出 Document |
//! | [`db_temporary_failure_degrades_then_recovers`] | `postgres` | 讀取路由回 503 且不會 hang；PG 回來之後自己恢復 |
//! | [`search_unavailable_does_not_block_ingestion`] | `opensearch` | 匯入照常 201，只有搜尋回 503；索引重建後搜得到 |
//! | [`object_storage_unavailable_leaves_no_half_written_evidence`] | `minio` | 匯入回 503，而且**沒有留下沒有 body 的 metadata** |
//!
//! # 這些測試會停掉真的 Docker 服務
//!
//! 所以全部標 `#[ignore]`，不會在 `cargo test --workspace` 跟著跑——同時跑的其他
//! 測試（整個 workspace 幾乎每一支都要 Postgres）會被連帶弄壞，而那種失敗看起來
//! 會像是被測程式的 bug。要跑這一支請單獨跑，而且**不要平行**：
//!
//! ```bash
//! cargo test -p acceptance --test failure_recovery -- --ignored --test-threads=1
//! ```
//!
//! 跑的當下不要同時跑 workspace 測試。
//!
//! # 服務一定會被起回來：[`ServiceGuard`]
//!
//! 每個要停服務的測試都先建一個 [`ServiceGuard`]。它的 `Drop` 會
//! `docker compose start <svc>` 並**輪詢到 healthy 為止**，因此不管測試是正常結束、
//! 提早 `return`、還是中途 panic（斷言失敗），服務都會被恢復。
//! 沒有這個機制的話，一次斷言失敗就會讓整台開發機的 Postgres 停在關閉狀態，
//! 之後每一支測試都會失敗，而失敗訊息完全指不到真正的原因。
//!
//! `Drop` 裡**不會**在已經 panic 的情況下再 panic（那會讓行程 abort，
//! 原本的斷言訊息就看不到了）；恢復失敗時改成寫 stderr 並提示手動指令。
//!
//! 用的是 `stop`／`start`，**絕對不是 `down`**：`down` 會連同其他測試累積的資料
//! 一起清掉（`down -v` 更是直接刪 volume）。

mod common;

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use common::*;
use connector_sdk::StoreEvidenceSink;
use core_api::{AppState, AuthState, ImportState, RateLimiter, SearchState, ready_always, router};
use core_config::ImportSection;
use core_events::{EventConsumer, EventEnvelope, EventProducer, EventTopic};
use core_model::{Source, SourceType};
use core_observability::MetricsRegistry;
use core_security::{JwtService, MemoryApiTokenStore, MemoryAuditLog, Role};
use http_body_util::BodyExt;
use indexer::{IndexBounds, Indexer, PrepareOutcome};
use normalizer::NormalizeOutcome;
use serde_json::{Value, json};
use storage_core::RelationalStore;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use tower::ServiceExt;
use uuid::Uuid;

/// 新建的 consumer group 等 partition 指派的時間（同 `acceptance_f.rs`）。
const ASSIGN_WAIT: Duration = Duration::from_millis(1_500);
/// 單一 API 請求的耐心上限。後端掛掉時**必須**在這之內回一個錯誤狀態碼，
/// 不可以一直卡著——卡住的 API 對呼叫端來說比 503 難處理得多。
const API_PATIENCE: Duration = Duration::from_secs(10);
/// multipart boundary（同 `core-api/tests/import_e2e.rs`）。
const BOUNDARY: &str = "X-OSINT-FAILURE-RECOVERY";

// ---------------------------------------------------------------------------
// docker compose 控制
// ---------------------------------------------------------------------------

/// workspace 根目錄。`cargo test` 的工作目錄是 crate 目錄，compose 檔在根目錄下。
fn workspace_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("找不到 workspace 根目錄")
}

/// 跑一次 `docker compose -f docker-compose.yml -f docker-compose.dev.yml <args>`。
///
/// 兩個 `-f` 都要帶：dev override 決定了 OpenSearch／MinIO 掛在 19200／19000，
/// 只帶第一個檔會操作到另一組埠設定的服務定義。
fn compose(args: &[&str]) -> std::process::Output {
    let root = workspace_root();
    let mut command = Command::new("docker");
    command
        .current_dir(&root)
        .arg("compose")
        .args(["-f", "docker/docker-compose.yml"])
        .args(["-f", "docker/docker-compose.dev.yml"])
        .args(args);
    command.output().unwrap_or_else(|err| {
        panic!("執行 docker compose {args:?} 失敗：{err}。請確認 docker 可用")
    })
}

/// 這個服務目前的健康狀態字串（沒有 healthcheck 時回容器狀態）。
fn health_of(service: &str) -> String {
    let ids = compose(&["ps", "-q", service]);
    let id = String::from_utf8_lossy(&ids.stdout).trim().to_string();
    if id.is_empty() {
        return "(沒有容器)".into();
    }
    let output = Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
            &id,
        ])
        .output()
        .expect("docker inspect");
    String::from_utf8_lossy(&output.stdout).trim().to_string()
}

/// 停一個服務，並保證它會被起回來。
///
/// **不要用 `down`**：見本檔頂部的說明。
struct ServiceGuard {
    service: &'static str,
    restored: bool,
}

impl ServiceGuard {
    /// 停掉服務並等到它真的不再 healthy。
    fn stop(service: &'static str) -> Self {
        let output = compose(&["stop", service]);
        assert!(
            output.status.success(),
            "docker compose stop {service} 失敗：{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let status = health_of(service);
        assert_ne!(
            status, "healthy",
            "{service} 停了之後不該還是 healthy（實際 `{status}`）。\
             停不掉就別往下測，後面的斷言會驗到一個還活著的服務"
        );
        Self {
            service,
            restored: false,
        }
    }

    /// 起回服務並輪詢到 healthy。測試中途需要服務時自己呼叫；沒呼叫的話 `Drop` 會做。
    fn restore(&mut self) {
        if self.restored {
            return;
        }
        self.restored = true;
        if let Err(message) = restore_service(self.service) {
            panic!("{message}");
        }
    }
}

impl Drop for ServiceGuard {
    fn drop(&mut self) {
        if self.restored {
            return;
        }
        self.restored = true;
        if let Err(message) = restore_service(self.service) {
            // 已經在 panic 的路徑上再 panic 會讓行程直接 abort，原本的斷言訊息就沒了。
            // 恢復失敗改成寫 stderr + 明確的手動指令。
            if std::thread::panicking() {
                eprintln!("⚠️ {message}");
            } else {
                panic!("{message}");
            }
        }
    }
}

/// `docker compose start` + 輪詢到 healthy。錯誤訊息裡直接給手動恢復指令。
fn restore_service(service: &str) -> Result<(), String> {
    let output = compose(&["start", service]);
    if !output.status.success() {
        return Err(format!(
            "docker compose start {service} 失敗：{}。請手動執行：\
             docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml start {service}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    // healthcheck 的 interval 是秒級，start 之後不會立刻轉 healthy。
    let deadline = Instant::now() + Duration::from_secs(180);
    let mut last = String::new();
    while Instant::now() < deadline {
        last = health_of(service);
        if last == "healthy" || last == "running" {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    Err(format!(
        "{service} 起回來了但 180 秒內沒有變成 healthy（最後狀態 `{last}`）。\
         請手動檢查：docker compose -f docker/docker-compose.yml \
         -f docker/docker-compose.dev.yml ps {service}"
    ))
}

// ---------------------------------------------------------------------------
// 測試用 API
// ---------------------------------------------------------------------------

struct TestApi {
    app: Router,
    token: String,
}

/// 組一個接上 Postgres／MinIO（以及可選的 OpenSearch）的 API。
///
/// 這裡刻意用**與 `main.rs` 相同的 handle 來源**（同一個 pool、同一個 S3 client）：
/// 失效測試要驗的是「連線建立好之後後端才掛掉」這個真實情境，
/// 不是「啟動時就連不上所以 state 是 None」那條早就有測試的路徑。
fn build_api(stack: &Stack, producer: Arc<EventProducer>, search: Option<SearchState>) -> TestApi {
    let jwt = JwtService::new(&[b't'; 32], "osint-core", chrono::Duration::hours(1)).expect("jwt");
    let token = jwt
        .issue("failure-recovery", Role::Operator)
        .expect("issue");
    let sink = StoreEvidenceSink::new(stack.pg.clone(), stack.s3.clone());
    let state = AppState {
        metrics: MetricsRegistry::new(),
        auth: AuthState {
            jwt: Arc::new(jwt),
            tokens: Arc::new(MemoryApiTokenStore::new()),
        },
        audit: Arc::new(MemoryAuditLog::new()),
        store: Some(Arc::new(stack.pg.clone())),
        objects: Some(Arc::new(stack.s3.clone())),
        jobs: None,
        merge: None,
        resolver: None,
        graph_resolver: None,
        graph: None,
        graph_projection: None,
        import: Some(Arc::new(ImportState {
            store: Arc::new(stack.pg.clone()),
            sink: Arc::new(sink),
            producer: Some(producer),
        })),
        search: search.map(Arc::new),
        ready: ready_always(),
        // 這一支不驗 /ops/health 的聚合（那是 §31 的事），給空清單。
        backends: core_api::ReadyProbe::new(Vec::new()),
        queues: None,
        backends_missing: Vec::new(),
        rate_limit_per_second: 1_000,
        request_body_limit_bytes: 1_048_576,
        import_config: ImportSection::default(),
        object_bucket: stack.bucket.clone(),
        rate_limiter: RateLimiter::new(1_000),
    };
    TestApi {
        app: router(state),
        token,
    }
}

/// 送一個請求，並在 [`API_PATIENCE`] 內逾時。回 `(狀態碼, body, 耗時)`。
///
/// 逾時**直接 panic**：後端掛掉時 API 卡住不回應是一個獨立的 bug，
/// 與「回錯狀態碼」不同，必須分得出來。
async fn call(api: &TestApi, request: Request<Body>, what: &str) -> (StatusCode, Value, Duration) {
    let started = Instant::now();
    let response = tokio::time::timeout(API_PATIENCE, api.app.clone().oneshot(request))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{what} 在 {} 秒內沒有回應。後端不可用時 API 必須**快速失敗**，\
                 不可以把呼叫端一直卡住",
                API_PATIENCE.as_secs()
            )
        })
        .expect("call api");
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        started.elapsed(),
    )
}

async fn get_sources(api: &TestApi) -> (StatusCode, Value, Duration) {
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/sources?limit=1")
        .header("Authorization", format!("Bearer {}", api.token))
        .body(Body::empty())
        .unwrap();
    call(api, request, "GET /api/v1/sources").await
}

async fn post_search(api: &TestApi, body: Value) -> (StatusCode, Value, Duration) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/search")
        .header("Content-Type", "application/json")
        .header("Authorization", format!("Bearer {}", api.token))
        .body(Body::from(body.to_string()))
        .unwrap();
    call(api, request, "POST /api/v1/search").await
}

fn multipart_body(request_json: &str, filename: &str, file_bytes: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(b"Content-Disposition: form-data; name=\"request\"\r\n\r\n");
    body.extend_from_slice(request_json.as_bytes());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\n")
            .as_bytes(),
    );
    body.extend_from_slice(b"Content-Type: application/json\r\n\r\n");
    body.extend_from_slice(file_bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{BOUNDARY}--\r\n").as_bytes());
    body
}

async fn import_json(
    api: &TestApi,
    source_id: Uuid,
    payload: &str,
) -> (StatusCode, Value, Duration) {
    let request_json = format!(r#"{{"source_id":"{source_id}","kind":"json"}}"#);
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/import")
        .header(
            "Content-Type",
            format!("multipart/form-data; boundary={BOUNDARY}"),
        )
        .header("Authorization", format!("Bearer {}", api.token))
        .body(Body::from(multipart_body(
            &request_json,
            "payload.json",
            payload.as_bytes(),
        )))
        .unwrap();
    call(api, request, "POST /api/v1/import").await
}

/// 匯入專用 Source（不對外連線，不需要 NetworkRule）。
async fn seed_import_source(pg: &PostgresCanonicalStore) -> Source {
    let now = Utc::now();
    let source = Source {
        id: Uuid::now_v7(),
        name: format!("failrec-import-{}", Uuid::now_v7()),
        source_type: SourceType::JsonImport,
        platform: None,
        base_url: None,
        description: Some("failure/recovery fixture".into()),
        language: Some("en".into()),
        country: None,
        enabled: true,
        collection_policy: json!({}),
        created_at: now,
        updated_at: now,
        last_seen: None,
    };
    pg.put_source(&source).await.expect("source");
    source
}

/// 連 OpenSearch（埠隔離：本機 9200 是 OpenCTI 的 Elasticsearch）。
async fn connect_search() -> (OpenSearchStore, String) {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    verify_not_opencti_search(&url).expect("OpenSearch 埠隔離");
    let store = OpenSearchStore::connect(&url)
        .expect("opensearch")
        .with_refresh_on_write(true);
    assert_opensearch_identity(&store.cluster_info().await.expect("GET /"))
        .expect("必須是 OpenSearch，不是 OpenCTI 的 Elasticsearch");
    (store, url)
}

// ---------------------------------------------------------------------------
// 1. broker redelivery
// ---------------------------------------------------------------------------

/// consumer A 收到 `raw.collected` 但**沒有 commit** 就 drop
/// → 同一個 group 的 consumer B 拿到**同一則**（同 envelope id）
/// → B 處理（normalize）→ 再處理一次（等同 A 也處理過）仍然只有一組 Document。
///
/// # 為什麼 A 要對「不是目標的那幾則」commit
///
/// `raw.collected` 是共用 topic，裡面有前幾次測試留下的訊息，而新 group 的
/// `auto.offset.reset` 是 `earliest`。如果 A 什麼都不 commit，B 只是「從頭讀一遍」，
/// 那驗到的是 earliest 這個設定，不是 redelivery。
/// 讓 A 把目標之前的訊息全部 commit 掉，group 的位置就**正好停在目標那一則**，
/// 於是「B 拿到的第一則就是它」這個斷言才真的在驗未提交的訊息會被重送。
#[tokio::test]
#[ignore = "會操作 Redpanda 的 consumer group；請用 --ignored --test-threads=1 單獨跑"]
async fn broker_redelivery_is_idempotent() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();

    let platform = format!("failrec-rd-{}", run.simple());
    let guid = format!("failrec-rd-guid-{}", run.simple());
    let link = format!("http://127.0.0.1/failrec-rd/{}", run.simple());
    let feed_url = serve_body(rss_body(
        &guid,
        &link,
        &format!("Advisory {}", run.simple()),
        &article(run),
    ))
    .await;
    let source = seed_source(&stack.pg, Some(&platform), Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    // A 與 B 共用這個 group。run-specific，不會干擾真正在跑的服務。
    let group = format!("osint-failrec-redeliver-{}", run.simple());
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "failure-recovery").expect("producer"));

    let consumer_a =
        EventConsumer::connect(&stack.brokers, &group, &[EventTopic::RawCollected.as_str()])
            .expect("consumer A");
    tokio::time::sleep(ASSIGN_WAIT).await;

    let collector = collector_runner(&stack, producer.clone());
    let raw_evidence_id = collect_once(&collector, &connector).await;
    let want = raw_evidence_id.to_string();

    // ---- A 收到目標之後就「crash」：不 commit，直接 drop ----
    let received_by_a = read_until_target_without_committing(&consumer_a, &want).await;
    drop(consumer_a);

    // ---- B 用同一個 group 接上 ----
    let consumer_b =
        EventConsumer::connect(&stack.brokers, &group, &[EventTopic::RawCollected.as_str()])
            .expect("consumer B");
    tokio::time::sleep(ASSIGN_WAIT).await;
    let received_by_b = consumer_b
        .next_envelope(Duration::from_secs(30))
        .await
        .expect(
            "B 應該收得到被重送的那一則。收不到代表未提交的 offset 被當成已處理，\
             那樣 consumer crash 會**靜默遺失**事件",
        );
    assert_eq!(
        received_by_b.id, received_by_a.id,
        "B 拿到的必須是 A 沒有 commit 的那一則（同 envelope id）。\
         A 收到的：{received_by_a:?}，B 收到的：{received_by_b:?}"
    );
    assert_eq!(
        received_by_b.payload["raw_evidence_id"],
        json!(raw_evidence_id)
    );

    // ---- B 處理它 ----
    let metrics = MetricsRegistry::new();
    let normalizer = normalizer(&stack, metrics.clone());
    let first = normalizer
        .handle_payload(&received_by_b.payload)
        .await
        .expect("B 正規化");
    let NormalizeOutcome::Created { document_ids } = first else {
        panic!("這筆證據還沒有人處理過，預期 Created，得到 {first:?}");
    };
    assert_eq!(document_ids.len(), 1, "fixture 只有一則 item");
    consumer_b.commit_last().expect("B 處理完才 commit");

    // ---- 冪等：同一則再被處理一次（等同 A 當初也處理完了才 crash）----
    let again = normalizer
        .handle_payload(&received_by_b.payload)
        .await
        .expect("重送後再處理一次");
    match &again {
        NormalizeOutcome::AlreadyDone { document_ids: ids } => assert_eq!(
            ids, &document_ids,
            "重送時應該回到同一組 Document，而不是另一組"
        ),
        other => panic!(
            "已經有 normalized claim 了，重送應該是 AlreadyDone，實際 {other:?}。\
             這一條就是 redelivery 不會產生重複資料的根據"
        ),
    }

    let derived = documents_of_raw_evidence(&stack.pg, raw_evidence_id).await;
    assert_eq!(
        derived, document_ids,
        "重送處理兩次之後，這筆證據底下仍然只能有那一組 Document"
    );
    assert_eq!(
        count_raw_evidence(&stack.pg, source.id).await,
        1,
        "重送事件不會產生第二筆 RawEvidence"
    );
}

/// 讀到 `raw_evidence_id == want` 的那一則就停手，而且**不 commit 它**。
///
/// 途中其他訊息（前幾次測試留下的）照常 commit，讓 group 的位置推進到目標之前。
async fn read_until_target_without_committing(
    consumer: &EventConsumer,
    want: &str,
) -> EventEnvelope {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "等 raw.collected（raw_evidence_id={want}）逾時。請確認 Redpanda 在跑（埠 9092）"
        );
        let envelope = consumer
            .next_envelope(remaining)
            .await
            .expect("consume 失敗。請確認 osint-core-redpanda-1 在跑（埠 9092）");
        if envelope
            .payload
            .get("raw_evidence_id")
            .and_then(Value::as_str)
            == Some(want)
        {
            // 這一則刻意不 commit：模擬「處理到一半就 crash」。
            return envelope;
        }
        consumer
            .commit_last()
            .expect("commit 舊訊息失敗。group 位置沒推進的話，後面驗的就不是 redelivery");
    }
}

// ---------------------------------------------------------------------------
// 2. DB 暫時失效
// ---------------------------------------------------------------------------

/// `stop postgres` → 讀取路由回錯誤狀態碼且不 hang → `start postgres` → 自己恢復。
///
/// 另外驗 collector：PG 掛掉時 `run_connector` 回 `Err` 而不是 panic。
///
/// # 503 與那 5 秒是怎麼來的（實測，不是推論）
///
/// 停掉 PG 之後這個請求實測**耗時 5.00 秒、回 503**，body 是
/// `{"error":"unavailable","message":"canonical store 目前無法讀寫。請確認 Postgres 在跑後重試"}`。
///
/// * 5 秒＝`PostgresCanonicalStore::connect` 設的 `acquire_timeout`。sqlx 在這段
///   時間內反覆重試連線，逾時後回 `PoolTimedOut`（對應 `StorageError::Timeout`）。
/// * 之所以仍然是 503 而不是 `From<StorageError>` 表上的 504，是因為資源類 handler
///   走的是 `core_api::resources::storage_error`：那裡把 NotFound／Conflict／
///   ConstraintViolation 以外的儲存錯誤**全部**收斂成 503「後端不可用」。
///
/// 也就是說這 5 秒是設定值，不是「剛好這次比較慢」。它同時決定了
/// [`API_PATIENCE`]（10 秒）是否合理：acquire_timeout 若被調大到 10 秒以上，
/// 這個測試會失敗——那正是我們想知道的事。
#[tokio::test]
#[ignore = "會 stop/start postgres 容器；請用 --ignored --test-threads=1 單獨跑"]
async fn db_temporary_failure_degrades_then_recovers() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "failure-recovery-db").expect("producer"));

    // connector 要在 PG 還活著的時候先建好——待會要拿它來驗 collector 的行為。
    let platform = format!("failrec-db-{}", run.simple());
    let guid = format!("failrec-db-guid-{}", run.simple());
    let link = format!("http://127.0.0.1/failrec-db/{}", run.simple());
    let feed_url = serve_body(rss_body(
        &guid,
        &link,
        &format!("Advisory {}", run.simple()),
        &article(run),
    ))
    .await;
    let source = seed_source(&stack.pg, Some(&platform), Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    let api = build_api(&stack, producer.clone(), None);
    let collector = collector_runner(&stack, producer);

    // 先確認在正常狀態下是 200：不然後面的 503 可能只是因為別的原因。
    let (status, body, _) = get_sources(&api).await;
    assert_eq!(status, StatusCode::OK, "停掉之前就該是 200：{body}");

    let mut guard = ServiceGuard::stop("postgres");

    // ---- PG 掛掉：快速失敗，不是 500，也不是 hang ----
    let (status, body, elapsed) = get_sources(&api).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "PG 掛掉時 GET /sources 必須回 503。500 代表『API 自己壞了』，\
         會讓人往完全錯誤的方向查；504 則會讓呼叫端以為只是這次比較慢。實際 {status}：{body}"
    );
    assert!(
        elapsed < API_PATIENCE,
        "回應花了 {elapsed:?}，超過 {API_PATIENCE:?} 的耐心上限"
    );
    assert!(
        body["message"].as_str().is_some_and(|m| !m.is_empty()),
        "錯誤訊息不可為空，運維要看得出下一步做什麼：{body}"
    );

    // ---- collector：回 Err，不 panic ----
    let result = collector.run_connector(connector.clone()).await;
    assert!(
        result.is_err(),
        "PG 掛掉時採集不可能成功。回 Ok 代表失敗被吞掉了，實際 {result:?}"
    );

    // `tick()` 的簽名是 `Vec<(Uuid, CollectOutcome)>`，**沒有 Err 可以回**：
    // 列 connector 失敗時它只記 error log 並回空清單（見 `collector::runner::tick`）。
    // 這裡斷言的是「不 panic、而且不會假裝有跑過任何 connector」。
    // ⚠️ 這是一個已知的靜默降級：外部只看回傳值的話，「PG 掛了」與
    // 「沒有到期的 connector」長得一模一樣。
    let ticked = collector.tick(Utc::now()).await;
    assert!(
        ticked.is_empty(),
        "PG 掛掉時 tick 不該回報任何 connector 結果，實際 {ticked:?}"
    );

    // ---- PG 回來：不需要重啟 API，連線池自己重連 ----
    guard.restore();
    let (status, body, _) = get_sources(&api).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "PG 恢復之後同一個 API 行程要自己好起來（sqlx pool 會重新建立連線），實際 {body}"
    );

    // 恢復後資料還在：停掉的是服務，不是資料。
    let found = stack
        .pg
        .get_source(source.id)
        .await
        .expect("查 source")
        .expect("重啟之後 Source 必須還在");
    assert_eq!(found.id, source.id);
}

// ---------------------------------------------------------------------------
// 3. search unavailable
// ---------------------------------------------------------------------------

/// `stop opensearch` → 匯入仍然 **201**（ingestion 不依賴搜尋投影）→ 搜尋回 503
/// → `start opensearch` → 重新索引那份 Document → 搜得到。
///
/// 這一條就是「OpenSearch 是可重建的投影，不是 canonical」在執行期的樣子
/// （CLAUDE.md §5）：它掛掉只能讓「查得到」暫時失效，不可以讓資料收不進來。
#[tokio::test]
#[ignore = "會 stop/start opensearch 容器；請用 --ignored --test-threads=1 單獨跑"]
async fn search_unavailable_does_not_block_ingestion() {
    let stack = connect_stack().await;
    let (search_store, _url) = connect_search().await;
    let run = Uuid::now_v7();
    // run-specific index：不會動到別的測試或真正的 osint-documents。
    let index = format!("osint-documents-failrec-{}", run.simple());
    let marker = format!("failrecsearch{}", run.simple());

    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "failure-recovery-search").expect("producer"),
    );
    let source = seed_import_source(&stack.pg).await;
    let api = build_api(
        &stack,
        producer,
        Some(SearchState {
            store: search_store.clone(),
            index: index.clone(),
        }),
    );

    let payload = json!([{
        "title": format!("{marker} advisory"),
        "description": format!("{marker} 這份資料是在 OpenSearch 掛掉的時候收進來的"),
        "id": format!("{marker}-1"),
    }])
    .to_string();

    let mut guard = ServiceGuard::stop("opensearch");

    // ---- 搜尋掛了，ingestion 照常 ----
    let (status, body, _) = import_json(&api, source.id, &payload).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "OpenSearch 掛掉不可以擋住匯入——它是可重建的投影，不是 canonical：{body}"
    );
    let raw_evidence_id: Uuid = body["raw_evidence_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .expect("回應要帶 raw_evidence_id");

    // ---- 搜尋本身要誠實地回 503 ----
    let (status, body, elapsed) = post_search(&api, json!({ "query": marker })).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "OpenSearch 掛掉時搜尋必須回 503（不可以回 200 + 空結果，那等於謊稱『沒有資料』）：{body}"
    );
    assert!(
        elapsed < API_PATIENCE,
        "搜尋失敗花了 {elapsed:?}，超過 {API_PATIENCE:?}"
    );

    // ---- OpenSearch 回來：重建投影 ----
    guard.restore();

    let metrics = MetricsRegistry::new();
    let NormalizeOutcome::Created { document_ids } = normalizer(&stack, metrics.clone())
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize")
    else {
        panic!("匯入的 JSON 應該正規化成 Document");
    };
    let document_id = document_ids[0];

    let indexer = Indexer::new(
        stack.pg.clone(),
        search_store.clone(),
        None,
        metrics,
        index.clone(),
        IndexBounds::default(),
    );
    indexer.ensure_index().await.expect("ensure index");
    let PrepareOutcome::Ready(document) = indexer.prepare(document_id).await.expect("prepare")
    else {
        panic!("這份不是重複也不是不存在，應該 Ready");
    };
    let report = indexer.flush(vec![*document]).await.expect("flush");
    assert_eq!(
        report.indexed, 1,
        "OpenSearch 恢復之後必須索引得進去：{:?}",
        report.permanent_failures
    );

    // ---- 搜得到 ----
    let (status, body, _) = post_search(&api, json!({ "query": marker })).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "OpenSearch 回來之後搜尋要恢復：{body}"
    );
    assert_eq!(
        body["total"],
        json!(1),
        "重建投影之後，停機期間收進來的資料必須搜得到（它一直都在 Postgres 裡）：{body}"
    );
    assert_eq!(
        body["hits"][0]["document_id"],
        json!(document_id.to_string())
    );

    // 清掉這次 run 的 index，不要在 OpenSearch 留一堆測試 index。
    let _ = search_store.delete_index(&index).await;
}

// ---------------------------------------------------------------------------
// 4. object storage unavailable
// ---------------------------------------------------------------------------

/// `stop minio` → 匯入回 503，而且 **`raw_evidence` 一列都不會留**
/// → `start minio` → 再匯入 → 201。
///
/// # 這裡真正要防的東西
///
/// 不是「回了錯誤碼」，而是**沒有 body 的 metadata**。RawEvidence 的 metadata 在
/// Postgres、body 在 MinIO；如果先寫 metadata 再寫 body，MinIO 掛掉就會留下一列
/// 指向不存在物件的證據——之後 normalizer 讀不到內容，而 DB 看起來一切正常。
/// `StoreEvidenceSink::persist` 的順序（先 `objects.put` 再
/// `insert_raw_evidence`，insert 失敗才回頭刪物件）就是為了這件事，
/// 這個測試是它唯一的執行期證明。
#[tokio::test]
#[ignore = "會 stop/start minio 容器；請用 --ignored --test-threads=1 單獨跑"]
async fn object_storage_unavailable_leaves_no_half_written_evidence() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();
    let producer = Arc::new(
        EventProducer::connect(&stack.brokers, "failure-recovery-minio").expect("producer"),
    );
    let source = seed_import_source(&stack.pg).await;
    let api = build_api(&stack, producer, None);

    let payload = json!([{
        "title": format!("failrecminio{} advisory", run.simple()),
        "description": "MinIO 掛掉時不可以留下半套證據",
        "id": format!("failrecminio{}-1", run.simple()),
    }])
    .to_string();

    let mut guard = ServiceGuard::stop("minio");

    // ---- MinIO 掛掉：503 ----
    let (status, body, elapsed) = import_json(&api, source.id, &payload).await;
    assert_eq!(
        status,
        StatusCode::SERVICE_UNAVAILABLE,
        "物件儲存不可用時匯入要回 503（後端沒有回應，不是呼叫端送錯東西）：{body}"
    );
    assert!(
        elapsed < API_PATIENCE,
        "失敗花了 {elapsed:?}，超過 {API_PATIENCE:?}。S3 client 的重試不可以讓呼叫端無限期等待"
    );
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| !m.contains("raw/") && !m.contains(&stack.bucket)),
        "錯誤訊息不可洩漏 bucket 或物件路徑：{body}"
    );

    // ---- 關鍵斷言：一列 metadata 都沒有留下 ----
    assert_eq!(
        count_raw_evidence(&stack.pg, source.id).await,
        0,
        "body 沒有寫成功就不可以留下 RawEvidence metadata。留下來的話，\
         DB 上會有一筆指向不存在物件的證據，而且不會有任何錯誤訊息"
    );

    // ---- MinIO 回來：同一個 API 行程直接恢復 ----
    guard.restore();
    let (status, body, _) = import_json(&api, source.id, &payload).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "MinIO 恢復之後匯入要能成功（不需要重啟 osint-api）：{body}"
    );
    assert_eq!(
        count_raw_evidence(&stack.pg, source.id).await,
        1,
        "恢復之後應該只有這一次成功匯入留下的證據"
    );

    // body 真的在 MinIO，而且與上傳的位元組一致。
    let raw_evidence_id: Uuid = body["raw_evidence_id"]
        .as_str()
        .and_then(|s| s.parse().ok())
        .expect("回應要帶 raw_evidence_id");
    let meta = stack
        .pg
        .get_raw_evidence(raw_evidence_id)
        .await
        .expect("查 raw evidence")
        .expect("RawEvidence 應在 Postgres");
    let blob = storage_core::ObjectStore::get(&stack.s3, &meta.storage_path)
        .await
        .expect("讀物件")
        .expect("MinIO 應有 body");
    assert_eq!(blob, payload.as_bytes(), "存下來的內容必須與上傳的一致");
}
