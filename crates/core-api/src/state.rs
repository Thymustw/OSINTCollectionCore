//! 共享狀態。JobService 可選，沒有 Postgres 時 health 仍可活。

use std::sync::Arc;

use ai_gateway::OpenAiCompatibleLlmProvider;
use connector_sdk::EvidenceSink;
use core_config::{HybridSearchSection, ImportSection, StixSection};
use core_events::EventProducer;
use core_jobs::JobService;
use core_observability::MetricsRegistry;
use core_security::{ApiTokenStore, AuditLog, JwtService};
use merge::MergeService;
use resolver::{AutoApprovalEvaluator, GraphContextResolver, ResolverService};
use storage_core::mock::MockEmbeddingProvider;
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
/// 目前用 [`MockEmbeddingProvider::unsupported`]：`semantic_similarity` 誠實回空，
/// 不是假裝已接上。ml-commons adapter 接上後才換真的。
/// `graph_context` 不在這個 service 上，見 [`SharedGraphContextResolver`]。
pub type SharedResolverService =
    Arc<ResolverService<PostgresCanonicalStore, MockEmbeddingProvider>>;
pub type SharedGraphContextResolver =
    Arc<GraphContextResolver<PostgresCanonicalStore, storage_neo4j::Neo4jStore>>;

/// ADR-012 AI 輔助自動核准。`None`（見 [`AppState::auto_approval`]）代表沒接上
/// Postgres——跟 [`SharedResolverService`] 同一個可用性：沒有 canonical store
/// 就沒有候選可評估。
///
/// LLM 是否真的會被呼叫由 `evaluator` 內部的 [`OpenAiCompatibleLlmProvider`]
/// 決定（`enabled=false` 時該 provider 已經會直接短路回 `Unsupported`），這裡
/// 不重複一份判斷。`max_auto_merges_per_resolve` 是跨多對候選才有意義的上限，
/// [`AutoApprovalEvaluator::evaluate_pair`] 本身只管一對，所以放在這個包裝
/// struct，由呼叫端（`resources/merge.rs`）在迴圈裡自己數。
#[derive(Clone)]
pub struct AutoApprovalState {
    pub evaluator: Arc<AutoApprovalEvaluator<PostgresCanonicalStore, OpenAiCompatibleLlmProvider>>,
    pub max_auto_merges_per_resolve: u32,
}

pub type SharedAutoApprovalState = Arc<AutoApprovalState>;
pub type SharedSearchState = Arc<SearchState>;
pub type SharedSemanticSearchState = Arc<SemanticSearchState>;

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
/// Graph API 讀取端。理由同 [`SharedStore`]：handler 綁 [`storage_core::GraphStore`]
/// 這個 capability，不要綁死在 `Neo4jStore`。
pub type SharedGraphStore = Arc<dyn storage_core::GraphStore + Send + Sync>;

/// `GET /ops/graph` 用。跟 [`SearchState`] 同一個模式——把 store 跟
/// 它對應的投影名稱包在一起，不要讓 handler 自己去猜投影叫什麼。
///
/// **不要**跟 [`SharedGraphStore`] 合併：那個是圖讀取 API 已經在用的型別，
/// 這份只服務投影 lag／rebuild 狀態。
#[derive(Clone)]
pub struct GraphProjectionState {
    pub store: Arc<dyn storage_core::ProjectionStore + Send + Sync>,
    pub projection: String,
}

pub type SharedGraphProjection = Arc<GraphProjectionState>;

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

/// `POST /api/v1/search/semantic` 與 `GET /objects/{id}/similar` 要用到的下游。
/// 沒接上時這兩條與 `POST /search/hybrid` 回 503。
///
/// 跟 [`SearchState`] 刻意分開：這條路由多需要一個 `EmbeddingProvider`
/// （ml-commons），ml-commons 掛掉不該連帶讓 `POST /api/v1/search`
/// （純全文，不需要 embedding）也跟著回 503。
///
/// `index` 與 [`SearchState::index`] 讀同一個 `[indexer].index`（Document index），
/// **不是** `[embedding_worker].entities_index`。這條路由只查 `osint-documents`。
#[derive(Clone)]
pub struct SemanticSearchState {
    pub store: OpenSearchStore,
    pub embeddings: storage_opensearch::MlCommonsEmbeddingProvider,
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
/// 不要再各自持有一份連線。`import`／`jobs`／`search`／`semantic_search` 是有額外組裝需求的子狀態
/// （分別要 EvidenceSink、JobService、index 名稱、ml-commons EmbeddingProvider），才維持獨立欄位。
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
    /// Entity resolution。`None` 代表沒接上 Postgres。embedder 目前是 mock
    /// （見 [`SharedResolverService`]），不要假設 `POST /entities/{id}/resolve`
    /// 會產出 `semantic_similarity` 候選。`graph_context` 走獨立欄位
    /// [`AppState::graph_resolver`]。
    pub resolver: Option<SharedResolverService>,
    /// ADR-012 自動核准。`None` 代表沒接上 Postgres。見 [`AutoApprovalState`]。
    pub auto_approval: Option<SharedAutoApprovalState>,
    /// graph_context resolution。`None` 代表沒接上 Neo4j——**跟 [`AppState::resolver`]
    /// 完全獨立**，Neo4j 斷線只影響 `POST /entities/{id}/resolve/graph-context`
    /// 這一條路由，不影響 resolve_entity 的另外幾個方法。這正是拆成獨立 endpoint
    /// 的目的，見 docs/developer/resolver.md。
    pub graph_resolver: Option<SharedGraphContextResolver>,
    /// Graph API 讀取端。`None` 代表沒接上 Neo4j——這 4 條讀取路由
    /// （neighbors／relationships／path／query）回 503，`resolver`／`graph_resolver`
    /// 不受影響（三者是分開的可用性狀態，各自獨立組裝）。
    /// 與 [`AppState::graph_resolver`] 共用同一個 Neo4j 連線，不要連兩次。
    pub graph: Option<SharedGraphStore>,
    /// 圖投影 lag／rebuild 狀態。`None` 代表沒接上 Neo4j，
    /// **只有** `GET /api/v1/ops/graph` 回 503。
    /// 與 [`AppState::graph`] 分開：讀圖與讀投影狀態是兩件不同的事。
    pub graph_projection: Option<SharedGraphProjection>,
    pub import: Option<Arc<ImportState>>,
    pub search: Option<SharedSearchState>,
    /// 語意搜尋。`None` 代表沒接上 OpenSearch 或 ml-commons 模型未部署，
    /// `POST /api/v1/search/semantic` 與 `GET /objects/{id}/similar` 回 503；
    /// 全文搜尋不受影響。hybrid 另外還要 [`AppState::search`]。
    pub semantic_search: Option<SharedSemanticSearchState>,
    /// hybrid 的 RRF 權重。永遠有值（`HybridSearchSection` 有 `Default`），
    /// 不是 `Option`。V0.2 只有 `bm25_weight`／`vector_weight` 真的有算。
    pub hybrid_weights: HybridSearchSection,
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
    /// 看起來跟七個後端名稱全綠一模一樣——那是最危險的一種假綠燈。
    pub backends_missing: Vec<&'static str>,
    pub rate_limit_per_second: u32,
    pub request_body_limit_bytes: u32,
    pub import_config: ImportSection,
    /// STIX 匯入／匯出上限。永遠有值（`StixSection` 有 `Default`），不是 `Option`。
    pub stix_config: StixSection,
    /// 目前唯一真的會呼叫 AI 的路徑（ADR-012 自動核准）允許的最大並發推論數。
    /// 純設定值，永遠有值——跟 [`AppState::auto_approval`] 是否為 `None`
    /// 無關（那個 `None` 代表沒接上 Postgres，這裡只是讀 config）。
    /// `GET /ops/discovery` 用它回報「目前設定的並發上限」，**不是**即時
    /// in-flight 請求數（那個目前沒有任何地方在計數，見 handler 的
    /// doc comment）。
    pub auto_approval_max_concurrent: usize,
    /// 物件儲存的 bucket 名稱。`GET /raw/{id}?body=true` 用它把
    /// `s3://{bucket}/{key}` 形式的 `storage_path` 還原成物件 key。
    /// 沒接物件儲存時是空字串（那時 `?body=true` 本來就會回 503）。
    pub object_bucket: String,
    pub rate_limiter: crate::rate_limit::RateLimiter,
}
