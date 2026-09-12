//! 共享狀態。JobService 可選，沒有 Postgres 時 health 仍可活。

use std::sync::Arc;

use connector_sdk::EvidenceSink;
use core_config::ImportSection;
use core_events::EventProducer;
use core_jobs::JobService;
use core_observability::MetricsRegistry;
use core_security::{ApiTokenStore, AuditLog, JwtService};
use merge::MergeService;
use resolver::ResolverService;
use storage_core::mock::{MockEmbeddingProvider, MockGraphStore};
use storage_core::{ObjectStore, RelationalStore};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;

use crate::ready::ReadyProbe;

pub type SharedTokenStore = Arc<dyn ApiTokenStore>;
pub type SharedAudit = Arc<dyn AuditLog>;
pub type SharedJobService = Arc<JobService<PostgresCanonicalStore>>;
/// 理由同 [`SharedJobService`]：`MergeService<S: TransactionalStore>` 的泛型參數
/// 不能吃 `Arc<dyn RelationalStore>`（沒有 blanket impl）。直接綁生產 adapter。
pub type SharedMergeService = Arc<MergeService<PostgresCanonicalStore>>;
/// 目前用 [`MockEmbeddingProvider::unsupported`] 與空的 [`MockGraphStore`]：
/// `semantic_similarity`／`graph_context` 誠實回空，不是假裝已接上。
/// Phase 2 接 `storage-neo4j` 與 ml-commons adapter 後才換真的。
pub type SharedResolverService =
    Arc<ResolverService<PostgresCanonicalStore, MockEmbeddingProvider, MockGraphStore>>;
pub type SharedSearchState = Arc<SearchState>;

/// 泛用的 canonical store handle。
///
/// # 為什麼是 `dyn RelationalStore` 而不是 `PostgresCanonicalStore`
///
/// Phase 6b 要加的 REST handler（objects／sources／collections／relationships…）
/// 需要的全部是 `RelationalStore` 上的方法。用具體型別會讓每一個 handler 都
/// 硬編在 PostgreSQL 上，違反 CLAUDE.md §13
/// 「Domain Service → storage-core capability interface → concrete adapter」；
/// 換言之，之後要讓某個 App 走 SQLite projection 就得改二十幾個 handler 的簽名。
///
/// 代價是拿不到 `pool()`（那是 PG 專屬的）。需要 pool 的兩個地方
/// （`PostgresAuditLog`、`PostgresApiTokenStore`）在 `main.rs` 連線時就從具體型別
/// 建好再放進 `AppState`，不經過這個 handle。
///
/// ⚠️ `+ Send + Sync` 不能省。`RelationalStore` 沒有把它們寫成 supertrait，
/// 所以 `Arc<dyn RelationalStore>` 預設**不是** `Send + Sync`，
/// `AppState` 會因此無法當 axum 的 state——而編譯器報的是
/// 「`FromFn<…>: Service<…>` 不滿足」這種完全指不到真因的訊息。
pub type SharedStore = Arc<dyn RelationalStore + Send + Sync>;

/// 物件儲存（Raw Evidence blob）handle。理由同 [`SharedStore`]：
/// handler 只需要 `ObjectStore` 的四個方法，不需要知道後面是 MinIO 還是別的 S3。
pub type SharedObjects = Arc<dyn ObjectStore + Send + Sync>;

/// `POST /api/v1/search` 要用到的下游。沒接上時只有這條路由回 503。
///
/// `index` 必須與 `osint-indexer` 用的是同一個名字（兩邊都讀 `[indexer].index`）。
/// 不一致的話搜尋會查一個空的（或別人的）index，而且完全不會報錯——
/// 使用者看到的是「都沒有資料」。
#[derive(Clone)]
pub struct SearchState {
    pub store: OpenSearchStore,
    pub index: String,
}

/// 匯入路徑要用到的下游。沒接上時 `POST /api/v1/import` 回 503，其他路由不受影響。
///
/// `sink` 直接用 `connector-sdk` 的 `EvidenceSink`：push 與 pull 兩條路徑寫 RawEvidence
/// 的方式必須是同一套（同樣的 sha256、同樣的 storage_path、同樣的失敗回滾），
/// 不能各寫各的。
#[derive(Clone)]
pub struct ImportState {
    /// 與 [`AppState::store`] 是同一個 `Arc`（`main.rs` 只建一次）。
    /// 這裡只用到 `RelationalStore` 上的 `get_source`／`get_connector`／`put_connector`。
    pub store: SharedStore,
    pub sink: Arc<dyn EvidenceSink>,
    pub producer: Option<Arc<EventProducer>>,
}

/// 認證相關。
#[derive(Clone)]
pub struct AuthState {
    pub jwt: Arc<JwtService>,
    pub tokens: SharedTokenStore,
}

/// 整個 API 的狀態。
///
/// `store` 與 `objects` 是**通用** handle：Phase 6b 之後每個資源 handler 都從這裡拿，
/// 不要再各自持有一份連線。`import`／`jobs`／`search` 是有額外組裝需求的子狀態
/// （分別要 EvidenceSink、JobService、index 名稱），才維持獨立欄位。
#[derive(Clone)]
pub struct AppState {
    pub metrics: MetricsRegistry,
    pub auth: AuthState,
    pub audit: SharedAudit,
    /// canonical store。`None` 代表沒接上 Postgres，資源類 handler 應回 503。
    pub store: Option<SharedStore>,
    /// 物件儲存。`None` 代表沒接上 MinIO。
    pub objects: Option<SharedObjects>,
    pub jobs: Option<SharedJobService>,
    /// Entity merge。`None` 代表沒接上 Postgres，對應 handler 回 503。
    pub merge: Option<SharedMergeService>,
    /// Entity resolution。`None` 代表沒接上 Postgres。embedder／graph 目前是 mock
    /// （見 [`SharedResolverService`]），不要假設 `POST /entities/{id}/resolve`
    /// 會產出 `semantic_similarity` 或 `graph_context` 候選。
    pub resolver: Option<SharedResolverService>,
    pub import: Option<Arc<ImportState>>,
    pub search: Option<SharedSearchState>,
    pub ready: ReadyProbe,
    /// `GET /api/v1/ops/health` 要敲的後端。與 [`AppState::ready`] **刻意分開**：
    /// `/ready` 只檢查 API 自己非有不可的依賴（給 orchestrator 判斷要不要送流量），
    /// 這裡是整套 pipeline 的後端（含 Redis／Redpanda，API 自己不需要它們）。
    /// 混在一起會讓「Redis 掛了」變成「API 不接受流量」。
    pub backends: ReadyProbe,
    /// `GET /api/v1/ops/queues` 的 consumer group lag 探針。
    ///
    /// `None` = 沒接上 Redpanda，**只有那條路由**回 503。刻意與 [`AppState::backends`]
    /// 分開：health 檢查用的是 producer（發得出去嗎），這裡用的是不加入 group 的
    /// 唯讀探針（見 `core_events::GroupLagProbe` 的模組說明）。
    pub queues: Option<Arc<crate::ops::QueueInspector>>,
    /// 沒接上（因此不在 [`AppState::backends`] 裡）的後端名稱。
    ///
    /// 少了這一欄，一個「只接了 Postgres」的部署會回 `healthy: true`，
    /// 看起來跟六個後端全綠一模一樣——那是最危險的一種假綠燈。
    pub backends_missing: Vec<&'static str>,
    pub rate_limit_per_second: u32,
    pub request_body_limit_bytes: u32,
    pub import_config: ImportSection,
    /// 物件儲存的 bucket 名稱。`GET /raw/{id}?body=true` 用它把
    /// `s3://{bucket}/{key}` 形式的 `storage_path` 還原成物件 key。
    /// 沒接物件儲存時是空字串（那時 `?body=true` 本來就會回 503）。
    pub object_bucket: String,
    pub rate_limiter: crate::rate_limit::RateLimiter,
}
