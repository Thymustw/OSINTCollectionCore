//! 應用設定載入。
//!
//! 來源優先序（後蓋前）：
//! 1. `config/default.toml`
//! 2. `OSINT_CONFIG_FILE` 指向的 TOML（若有）
//! 3. 環境變數 `OSINT__...`（`__` 分隔巢狀鍵）

mod secret;

pub use secret::{SecretRef, SecretRefError};

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// 載入後的完整設定。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppConfig {
    pub app: AppSection,
    pub storage: StorageSection,
    pub broker: BrokerSection,
    #[serde(default)]
    pub http: HttpSection,
    #[serde(default)]
    pub auth: AuthSection,
    #[serde(default)]
    pub collector: CollectorSection,
    #[serde(default)]
    pub normalizer: NormalizerSection,
    #[serde(default)]
    pub deduplicator: DeduplicatorSection,
    #[serde(default)]
    pub entity_worker: EntityWorkerSection,
    #[serde(default)]
    pub indexer: IndexerSection,
    #[serde(default)]
    pub graph_worker: GraphWorkerSection,
    #[serde(default)]
    pub embedding_worker: EmbeddingWorkerSection,
    #[serde(default)]
    pub stix_worker: StixWorkerSection,
    #[serde(default)]
    pub import: ImportSection,
    #[serde(default)]
    pub embedding: EmbeddingSection,
    #[serde(default)]
    pub search_hybrid: HybridSearchSection,
    #[serde(default)]
    pub auto_approval: AutoApprovalSection,
    #[serde(default)]
    pub stix: StixSection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AppSection {
    pub environment: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StorageSection {
    pub canonical: CanonicalStorage,
    pub sqlite_local: SqliteStorage,
    pub search: SearchStorage,
    pub object: ObjectStorage,
    pub cache: CacheStorage,
    /// 圖投影（Neo4j）。`#[serde(default)]` 是為了讓既有的設定檔／測試
    /// 不加這一段也能載入——V0.1 時期的設定檔沒有 `[storage.graph]`。
    #[serde(default)]
    pub graph: GraphStorage,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalStorage {
    pub adapter: String,
    pub dsn_secret_ref: SecretRef,
    pub pool_max: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteStorage {
    pub adapter: String,
    pub path: PathBuf,
    #[serde(default)]
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchStorage {
    pub adapter: String,
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectStorage {
    pub adapter: String,
    pub endpoint: String,
    pub bucket: String,
    pub access_key_ref: SecretRef,
    pub secret_key_ref: SecretRef,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheStorage {
    pub adapter: String,
    pub url_secret_ref: SecretRef,
}

/// 圖投影（Neo4j）。
///
/// `http_url`（7474）給 `/ops/health` 的 HTTP 探活用；真正的圖寫入走
/// `bolt_uri`（7687），由 `storage-neo4j` 的 `Neo4jStore` 讀取。
/// 密碼只存 [`SecretRef`]，與 PostgreSQL／Redis 同一套，不要寫明文。
///
/// Bolt／帳密／`pool_max` 都有 `#[serde(default)]`：V0.1／Phase 0b 的設定檔只有
/// `adapter` + `http_url`，缺欄位時必須仍能載入，不能整份解析失敗。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStorage {
    pub adapter: String,
    /// Neo4j 的 HTTP 埠（7474）。空字串 = 未設定，`/ops/health` 會列進
    /// `not_configured` 而不是 `unhealthy`。
    pub http_url: String,
    /// Bolt 連線字串。`storage-neo4j` 的 `Neo4jStore` 用這個，不是 `http_url`。
    #[serde(default = "default_bolt_uri")]
    pub bolt_uri: String,
    #[serde(default = "default_neo4j_username")]
    pub username: String,
    /// 密碼走 SecretRef（例如 `env:NEO4J_PASSWORD`），與
    /// `[storage.canonical].dsn_secret_ref` 同一套格式。
    #[serde(default = "default_neo4j_password_secret_ref")]
    pub password_secret_ref: SecretRef,
    /// Bolt 連線池上限。傳給 `storage-neo4j` 的 `Neo4jStore::connect`。
    /// 0 會被 adapter 拒絕。
    #[serde(default = "default_graph_pool_max")]
    pub pool_max: usize,
}

fn default_bolt_uri() -> String {
    "bolt://127.0.0.1:7687".into()
}

fn default_neo4j_username() -> String {
    "neo4j".into()
}

fn default_neo4j_password_secret_ref() -> SecretRef {
    SecretRef::parse("env:NEO4J_PASSWORD").expect("literal SecretRef")
}

fn default_graph_pool_max() -> usize {
    5
}

impl Default for GraphStorage {
    fn default() -> Self {
        Self {
            adapter: "neo4j".into(),
            http_url: "http://127.0.0.1:7474".into(),
            bolt_uri: default_bolt_uri(),
            username: default_neo4j_username(),
            password_secret_ref: default_neo4j_password_secret_ref(),
            pool_max: default_graph_pool_max(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerSection {
    pub brokers: String,
}

/// HTTP 綁定位址與全域 rate limit。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HttpSection {
    pub bind: String,
    pub rate_limit_per_second: u32,
    pub request_body_limit_bytes: u32,
}

impl Default for HttpSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18080".into(),
            rate_limit_per_second: 20,
            request_body_limit_bytes: 1_048_576,
        }
    }
}

/// JWT／API token 設定。secret 只存 SecretRef。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthSection {
    pub jwt_secret_ref: SecretRef,
    pub jwt_issuer: String,
    pub jwt_ttl_secs: u64,
}

impl Default for AuthSection {
    fn default() -> Self {
        Self {
            jwt_secret_ref: SecretRef::parse("env:JWT_SECRET").expect("literal SecretRef"),
            jwt_issuer: "osint-core".into(),
            jwt_ttl_secs: 3600,
        }
    }
}

/// collector 排程與併發上限。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectorSection {
    pub bind: String,
    pub tick_secs: u64,
    pub global_inflight: u32,
    pub per_domain_inflight: u32,
}

impl Default for CollectorSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18081".into(),
            tick_secs: 5,
            global_inflight: 4,
            per_domain_inflight: 1,
        }
    }
}

/// normalizer consumer 設定。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NormalizerSection {
    pub bind: String,
    pub consumer_group: String,
}

impl Default for NormalizerSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18082".into(),
            consumer_group: "osint-normalizer".into(),
        }
    }
}

/// deduplicator consumer 與 SPEC §15 五階段的上限設定。
///
/// 三個上限都必須有值，沒有「不限」這個選項：Stage 4 沒有可走索引的等值條件，
/// 少了上限就是「每來一份 Document 全表掃一次」。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeduplicatorSection {
    pub bind: String,
    pub consumer_group: String,
    /// Stage 1～3 每個鍵最多取回幾筆候選。
    pub candidate_limit: u32,
    /// Stage 4 每次最多掃描幾筆有 fingerprint 的 Document。
    pub simhash_scan_limit: u32,
    /// Stage 4 的 Hamming 距離門檻（0..=64）。門檻選擇的理由見
    /// `crates/deduplicator/src/simhash.rs`。
    pub simhash_max_distance: u32,
}

impl Default for DeduplicatorSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18083".into(),
            consumer_group: "osint-deduplicator".into(),
            candidate_limit: 20,
            simhash_scan_limit: 500,
            simhash_max_distance: 3,
        }
    }
}

/// entity-worker consumer 與 SPEC §17 抽取的上限設定。
///
/// 兩個上限都必須有值，沒有「不限」這個選項：一篇塞滿 IOC 的傾印檔若不設上限，
/// 會讓單份 Document 產生上萬列 entity_extractions 並拖垮整個 consumer group。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EntityWorkerSection {
    pub bind: String,
    pub consumer_group: String,
    /// 單份 Document 最多留幾筆抽取命中。超過就截斷並記 warn。
    /// 預設值的取捨見 `crates/entity-worker/src/extract.rs` 的 `ExtractionBounds`。
    pub max_extractions: usize,
    /// 只掃描 `title + summary + body` 的前 N 個 byte。
    pub max_scan_bytes: usize,
}

impl Default for EntityWorkerSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18084".into(),
            consumer_group: "osint-entity-worker".into(),
            max_extractions: 500,
            max_scan_bytes: 256 * 1024,
        }
    }
}

/// indexer consumer 與 SPEC §18 搜尋投影的上限設定。
///
/// 每一項都必須有值，沒有「不限」這個選項：indexer 是高吞吐消費者
/// （CLAUDE.md §6），少了批次上限就是把整個 partition 的內容塞進一次 bulk 請求。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexerSection {
    pub bind: String,
    pub consumer_group: String,
    /// OpenSearch index 名稱。改名等於換一個空 index，要跑 `osint-indexer --rebuild`。
    pub index: String,
    /// 一次 bulk 最多送幾筆。預設值的取捨見 `crates/indexer/src/service.rs`。
    pub batch_size: u32,
    /// 累積不到 `batch_size` 時，最久等多久就強制送出（毫秒）。
    /// 設成 0 會讓低流量時最後幾筆永遠不進 index。
    pub batch_timeout_ms: u64,
    /// 單一文字欄位（title／summary／body）寫進 index 的 byte 上限。
    pub max_field_bytes: usize,
    /// 暫時性 bulk 失敗最多重試幾次。
    pub bulk_max_retries: u32,
    /// consumer lag 超過這個值就在批次之間插入延遲，並把 lag 寫進
    /// `osint_queue_depth` gauge。
    ///
    /// 注意：那個 gauge 在 V0.1 **沒有任何程式讀它**，collector 不會自動降速
    /// （跨服務 backpressure 是 V0.2）。這個門檻只影響 indexer 自己的節奏。
    pub lag_threshold: u64,
    /// 降速時的基礎延遲（毫秒）。實際延遲會依超出門檻的倍數放大，最多 8 倍。
    pub backpressure_sleep_ms: u64,
}

impl Default for IndexerSection {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:18085".into(),
            consumer_group: "osint-indexer".into(),
            index: "osint-documents".into(),
            batch_size: 200,
            batch_timeout_ms: 1_000,
            max_field_bytes: 256 * 1024,
            bulk_max_retries: 3,
            lag_threshold: 5_000,
            backpressure_sleep_ms: 200,
        }
    }
}

/// graph-worker consumer 與圖投影重建的上限設定。
///
/// `projection` 對 graph-worker 的角色等同 indexer 的 `index`：改名等於換一個
/// 空投影，要跑 rebuild。`page_size` 必須有值，沒有「不限」這個選項——
/// rebuild 掃 canonical store 時少了分頁就是一次把全部實體／關係載進記憶體。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphWorkerSection {
    pub bind: String,
    pub consumer_group: String,
    /// ProjectionStore 用的投影名稱，等同 indexer 的 `index` 欄位角色。
    pub projection: String,
    /// rebuild 時每頁掃描筆數。
    pub page_size: u32,
}

impl Default for GraphWorkerSection {
    fn default() -> Self {
        Self {
            // 18080–18085 已分別是 api／collector／normalizer／deduplicator／
            // entity-worker／indexer 的 health 埠。
            bind: "127.0.0.1:18086".into(),
            consumer_group: "osint-graph-worker".into(),
            projection: "osint-graph".into(),
            page_size: 100,
        }
    }
}

/// embedding-worker（`osint-embedding-worker`）的 health 與投影設定。
///
/// `batch_size`／`concurrent_inferences` **不**在這裡——沿用 [`EmbeddingSection`]。
/// 兩個各自維護一份只會製造「兩者不一致」的坑。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmbeddingWorkerSection {
    pub bind: String,
    pub consumer_group: String,
    /// Entity 向量投影的 index 名。Document 向量仍寫進 `[indexer].index`。
    pub entities_index: String,
    /// rebuild 時每頁掃描筆數。
    pub page_size: u32,
}

impl Default for EmbeddingWorkerSection {
    fn default() -> Self {
        Self {
            // 18080–18086 已分別是 api／collector／normalizer／deduplicator／
            // entity-worker／indexer／graph-worker 的 health 埠。
            bind: "127.0.0.1:18087".into(),
            consumer_group: "osint-embedding-worker".into(),
            entities_index: "osint-entities".into(),
            page_size: 100,
        }
    }
}

/// stix-worker（`osint-stix-worker`）的 health 與匯入交易上限。
///
/// 真正的 STIX 物件數量上限仍是 [`StixSection::max_objects`]（API 入口先擋）；
/// `max_objects_per_tx` 是 worker 端的第二道硬上限，超過就整批失敗、不拆交易。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StixWorkerSection {
    pub bind: String,
    pub consumer_group: String,
    /// 單次 `stix_import` 交易最多寫入幾筆已對應的 Entity。超過整批 Failed。
    pub max_objects_per_tx: usize,
}

impl Default for StixWorkerSection {
    fn default() -> Self {
        Self {
            // 18080–18087 已分別是 api／collector／normalizer／deduplicator／
            // entity-worker／indexer／graph-worker／embedding-worker 的 health 埠。
            bind: "127.0.0.1:18088".into(),
            consumer_group: "osint-stix-worker".into(),
            max_objects_per_tx: 10_000,
        }
    }
}

/// `POST /api/v1/import` 的上傳與解析上限。每一項都必須有值，沒有「不限」這個選項。
///
/// `max_upload_bytes` 與 `[http].request_body_limit_bytes` 是兩條獨立的界線：
/// 一般 API 的 JSON body 維持 1 MiB 即可，檔案上傳需要大得多，
/// 所以 import 路由自己掛一層較寬的上限，不去放寬其他路由。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportSection {
    /// 單次上傳的檔案 bytes 上限。超過回 413。
    pub max_upload_bytes: u64,
    /// 單次匯入最多幾筆紀錄。
    pub max_records: u32,
    /// 單筆紀錄（NDJSON 一行／JSON 陣列一元素／CSV 一列）bytes 上限。
    pub max_record_bytes: u32,
    /// 單一對映欄位 bytes 上限。
    pub max_field_bytes: u32,
    /// JSON 巢狀深度上限。
    pub max_depth: u32,
    /// CSV 欄位數上限。
    pub max_columns: u32,
}

impl Default for ImportSection {
    fn default() -> Self {
        Self {
            max_upload_bytes: 10 * 1024 * 1024,
            max_records: 10_000,
            max_record_bytes: 262_144,
            max_field_bytes: 65_536,
            max_depth: 32,
            max_columns: 512,
        }
    }
}

/// STIX 2.1 Adapter（SPEC_V0.2 §18-19）的匯入／匯出限制。
///
/// 跟 `[http].request_body_limit_bytes` 是兩條獨立界線：一般 API 的 JSON
/// body 維持 1 MiB，STIX bundle 另外給 50 MiB，不放寬其他路由。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StixSection {
    /// STIX bundle 上傳大小上限（bytes）。超過回 413。
    pub max_bundle_bytes: u64,
    /// bundle 裡 `objects` 陣列的數量上限。超過回 413。
    pub max_objects: usize,
}

impl Default for StixSection {
    fn default() -> Self {
        Self {
            max_bundle_bytes: 50 * 1024 * 1024,
            max_objects: 10_000,
        }
    }
}

/// Embedding 產生的參數（V0.2 Phase 3）。OpenSearch 連線位址**不**在這裡——
/// 直接沿用 `[storage.search].url`，ml-commons 跑在同一個 OpenSearch 叢集上，
/// 兩個各自維護一份 URL 只會製造「兩者不一致」的坑。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmbeddingSection {
    /// `embed_batch` 一次送幾筆給 ml-commons `_predict`。
    pub batch_size: usize,
    /// 同時進行中的推論請求數上限（bounded semaphore，CLAUDE.md §6）。
    pub concurrent_inferences: usize,
    /// Semantic dedup（deduplicator Stage 5）的 cosine 門檻。
    ///
    /// ⚠️ **這個值目前只有 deduplicator 在用**。resolver 的
    /// `semantic_similarity` 方法目前是獨立硬編碼的
    /// `SEMANTIC_SIMILARITY_THRESHOLD`（見 `crates/resolver/src/service.rs`），
    /// 沒有讀這個 config 欄位——兩者是兩個不同的數字，不要假設改這裡
    /// resolver 的行為會跟著變。這是已知的技術債，不在這次改動範圍內。
    ///
    /// # ⚠️ 這不是校準過的數字，現在會真的把文件標成 duplicate
    ///
    /// `0.90` 是 Phase 0 用一小組句子**推論**出來的暫定值，**從來沒有用真實
    /// OSINT 語料驗證過**。e5-small 的向量各向異性很強：完全不相關的文字
    /// baseline cosine 就有 ~0.83（見 `docs/developer/embedding.md` §5.1），
    /// 跟 0.90 只差 0.07。Stage 5 會用這個數字自動寫 `DuplicateGroup`、
    /// 把 `documents.duplicate_of` 指過去——誤判的代價不再是「紙上談兵」，
    /// 是下游 entity／search 直接跳過那份文件。
    ///
    /// 不要因為「設定檔裡寫了 0.90」就當成已驗證。要改這個值，先用真實
    /// 語料量 false-positive／false-negative，不要靠感覺微調。
    /// `osint-deduplicator` 啟用 Stage 5 時會再打一行 `tracing::warn!`。
    pub similarity_threshold: f64,
    /// Stage 5 把當場算出的向量寫進 Redis 的 TTL（秒）。
    ///
    /// 給 `object.normalized` → Stage 5 → `entity.extracted` →
    /// embedding-worker 這段路徑用：embedding-worker 先查這把 key，
    /// 命中就不必再打 ml-commons。預設 15 分鐘——短於 5 分鐘時正常
    /// consumer lag 就會讓快取過期，長於 1 小時會讓 Redis 堆不會再被
    /// 讀到的向量。實際使用時會夾在 300..=3600。
    ///
    /// `#[serde(default)]`：舊設定檔沒有這個鍵時仍能載入。
    #[serde(default = "default_dedup_cache_ttl_secs")]
    pub dedup_cache_ttl_secs: u64,
}

fn default_dedup_cache_ttl_secs() -> u64 {
    900
}

impl EmbeddingSection {
    /// Stage 5 Redis 快取 TTL。設定值會夾在 300..=3600 秒：
    /// 短於 5 分鐘，正常 consumer lag 就會讓快取過期；長於 1 小時會讓
    /// Redis 堆不會再被讀到的向量。
    #[must_use]
    pub fn dedup_cache_ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.dedup_cache_ttl_secs.clamp(300, 3600))
    }
}

impl Default for EmbeddingSection {
    fn default() -> Self {
        Self {
            batch_size: 32,
            concurrent_inferences: 4,
            similarity_threshold: 0.90,
            dedup_cache_ttl_secs: default_dedup_cache_ttl_secs(),
        }
    }
}

/// Hybrid search（`POST /search/hybrid`，SPEC §15）的排序權重。
///
/// SPEC 明講「權重放 config，不可 hardcode route handler」。V0.2 用 RRF
/// （Reciprocal Rank Fusion）合成各訊號的排名，不是加權和——RRF 不需要
/// 把 BM25 分數與 cosine 分數強行對齊到同一個範圍。權重是「這個訊號在
/// RRF 公式裡的比重」，不是原始分數的乘數。
///
/// `entity_match`／`recency`／`source_score`／`confidence` 預設 0.0。
/// V0.2 **沒有實作這四個訊號**（沒有對應的排名清單），不是「權重設 0
/// 但其實有算」。設非零時 `osint-api` 啟動會打 warning，排序不會變。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HybridSearchSection {
    pub bm25_weight: f64,
    pub vector_weight: f64,
    pub entity_match_weight: f64,
    pub recency_weight: f64,
    pub source_score_weight: f64,
    pub confidence_weight: f64,
}

impl Default for HybridSearchSection {
    fn default() -> Self {
        Self {
            bm25_weight: 1.0,
            vector_weight: 1.0,
            entity_match_weight: 0.0,
            recency_weight: 0.0,
            source_score_weight: 0.0,
            confidence_weight: 0.0,
        }
    }
}

/// AI 輔助自動核准（ADR-012）。**預設完全關閉**——`enabled = false` 時，
/// 這個 section 的其他欄位都不會被讀取，resolver 行為跟 ADR-012 之前完全一樣。
///
/// 這是對 `.claude/CLAUDE.md` §5「AI output ... cannot directly override
/// policy/identity decisions」的刻意例外，細節見 ADR-012。門檻數字**沒有用真實
/// OSINT 語料驗證過**，比照 [`EmbeddingSection::similarity_threshold`] 的警告寫法。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutoApprovalSection {
    /// 主開關。`false` = 所有 resolution candidate 維持 `Pending`，
    /// 與 ADR-012 之前的行為完全相同。
    #[serde(default)]
    pub enabled: bool,
    /// score >= 這個值的候選直接自動核准並執行 merge，不經 LLM。
    /// 設超過 1.0 等同「純分數路徑永不觸發」。
    ///
    /// ⚠️ 未經真實語料驗證。目前只有 `exact_identifier`（固定 0.95 分）能達到。
    #[serde(default = "default_auto_confirm_score")]
    pub auto_confirm_score: f64,
    /// `[llm_review_score, auto_confirm_score)` 區間的候選送 LLM 審查
    /// （前提是 `llm.enabled = true`；未啟用時這個區間的候選維持 Pending）。
    ///
    /// ⚠️ 未經真實語料驗證。
    #[serde(default = "default_llm_review_score")]
    pub llm_review_score: f64,
    /// 單次 `resolve_entity` 呼叫最多觸發幾筆自動 merge，防止連環效應。
    #[serde(default = "default_max_auto_merges_per_resolve")]
    pub max_auto_merges_per_resolve: u32,
    /// 自動核准後 merge 的 survivor 選擇策略。V0.2 只有 `"source"`
    /// （呼叫 `resolve_entity` 的目標 Entity 存活）。
    #[serde(default = "default_survivor_strategy")]
    pub survivor_strategy: String,
    #[serde(default)]
    pub llm: AutoApprovalLlmSection,
}

/// 本地 Qwen LLM（或任何 OpenAI 相容 endpoint）的連線設定。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AutoApprovalLlmSection {
    /// LLM 中間帶審查開關。`false` 時中間帶候選一律維持 Pending
    /// （即使 `auto_approval.enabled = true`）。
    #[serde(default)]
    pub enabled: bool,
    /// OpenAI 相容 endpoint base URL，例如 vLLM 的 `http://ai-inference:8000/v1`。
    #[serde(default = "default_llm_base_url")]
    pub base_url: String,
    /// 模型名稱（對應 `docs/architecture/LOCAL_AI.md` 的 alias `qwen-primary`）。
    #[serde(default = "default_llm_model")]
    pub model: String,
    /// 模型版本／revision（SPEC_V0.3 §4 `AiRun.model_version`）。目前沒有真的
    /// 在跑的推論服務可以查版本，預設 `"unknown"` 誠實反映這件事——不要編一個
    /// 假版本號。等真實 runtime（V0.3 Phase 5）接上才會有真實值可填。
    #[serde(default = "default_llm_model_version")]
    pub model_version: String,
    /// 單次推論逾時（秒）。
    #[serde(default = "default_llm_timeout_secs")]
    pub timeout_secs: u64,
    /// 逾時後重試次數。`0` = 不重試，直接退回 Pending
    /// （LLM 推論是非必要路徑，重試只是讓使用者多等，比照
    /// CLAUDE.md「暫時性錯誤可退避重試；永久性錯誤立刻拋出」的分類，
    /// 但這裡連暫時性錯誤預設都不重試，因為降級路徑本來就安全）。
    #[serde(default)]
    pub max_retries: u32,
    /// 同時進行的 LLM 推論上限（bounded semaphore，CLAUDE.md §6）。
    #[serde(default = "default_llm_max_concurrent")]
    pub max_concurrent: usize,
    #[serde(default = "default_llm_max_tokens")]
    pub max_tokens: u32,
    /// 0.0 = 確定性輸出，適合判斷任務而非生成任務。
    #[serde(default)]
    pub temperature: f64,
}

fn default_auto_confirm_score() -> f64 {
    0.95
}

fn default_llm_review_score() -> f64 {
    0.70
}

fn default_max_auto_merges_per_resolve() -> u32 {
    3
}

fn default_survivor_strategy() -> String {
    "source".to_string()
}

fn default_llm_base_url() -> String {
    "http://ai-inference:8000/v1".to_string()
}

fn default_llm_model() -> String {
    "qwen-primary".to_string()
}

fn default_llm_model_version() -> String {
    "unknown".to_string()
}

fn default_llm_timeout_secs() -> u64 {
    30
}

fn default_llm_max_concurrent() -> usize {
    2
}

fn default_llm_max_tokens() -> u32 {
    512
}

impl AutoApprovalSection {
    /// 門檻設定是否自洽（`auto_confirm_score >= llm_review_score`）。
    /// 不自洽時呼叫端應該視為設定錯誤、記警告並停用自動核准——但記警告
    /// 需要 `tracing`，這個 crate 不依賴 `tracing`，所以只回傳布林值，
    /// 由組裝端（`core-api` 或未來的 resolver 組裝點）決定怎麼處理。
    #[must_use]
    pub fn thresholds_are_sane(&self) -> bool {
        self.auto_confirm_score >= self.llm_review_score
    }
}

impl Default for AutoApprovalSection {
    fn default() -> Self {
        Self {
            enabled: false,
            auto_confirm_score: default_auto_confirm_score(),
            llm_review_score: default_llm_review_score(),
            max_auto_merges_per_resolve: default_max_auto_merges_per_resolve(),
            survivor_strategy: default_survivor_strategy(),
            llm: AutoApprovalLlmSection::default(),
        }
    }
}

impl Default for AutoApprovalLlmSection {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: default_llm_base_url(),
            model: default_llm_model(),
            model_version: default_llm_model_version(),
            timeout_secs: default_llm_timeout_secs(),
            max_retries: 0,
            max_concurrent: default_llm_max_concurrent(),
            max_tokens: default_llm_max_tokens(),
            temperature: 0.0,
        }
    }
}

/// 設定載入失敗。訊息會指出缺哪個檔／哪個鍵，以及建議怎麼修。
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("讀取設定失敗：{0}。請確認 config/default.toml 存在，或用 OSINT_CONFIG_FILE 指定檔案")]
    Load(#[from] config::ConfigError),
    #[error(
        "工作區根目錄找不到 config/default.toml（從 {cwd} 往上找也沒有）。請在 repo 根目錄執行，或設定 OSINT_CONFIG_FILE"
    )]
    DefaultConfigMissing { cwd: String },
}

impl AppConfig {
    /// 依預設搜尋路徑載入。
    pub fn load() -> Result<Self, ConfigError> {
        let extra = std::env::var_os("OSINT_CONFIG_FILE")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from);
        Self::load_from(None, extra.as_deref())
    }

    /// 測試或明確指定路徑時使用。
    pub fn load_from(
        default_file: Option<&Path>,
        extra_file: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let mut builder = config::Config::builder();

        let default_path = match default_file {
            Some(path) => path.to_path_buf(),
            None => find_default_config()?,
        };
        builder = builder.add_source(config::File::from(default_path).required(true));

        if let Some(path) = extra_file.filter(|p| !p.as_os_str().is_empty()) {
            builder = builder.add_source(config::File::from(path.to_path_buf()).required(true));
        }

        builder = builder.add_source(
            config::Environment::with_prefix("OSINT")
                .separator("__")
                .try_parsing(true),
        );

        let cfg = builder.build()?;
        Ok(cfg.try_deserialize()?)
    }
}

fn find_default_config() -> Result<PathBuf, ConfigError> {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut dir = cwd.clone();
    loop {
        let candidate = dir.join("config/default.toml");
        if candidate.is_file() {
            return Ok(candidate);
        }
        if !dir.pop() {
            return Err(ConfigError::DefaultConfigMissing {
                cwd: cwd.display().to_string(),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    /// `config` crate 會讀行程環境變數；測試必須序列化，否則會互相覆蓋。
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn workspace_default() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config/default.toml")
    }

    fn clear_osint_overrides() {
        unsafe {
            std::env::remove_var("OSINT__APP__ENVIRONMENT");
            std::env::remove_var("OSINT__STORAGE__CANONICAL__POOL_MAX");
            // GraphStorage 欄位都有 CI/docker-compose 會設的 OSINT__ 覆寫。
            // 2026-09-13 CI run 34724946970 實測：clear 清單漏了 HTTP_URL，
            // ci.yml 的 job env 設 OSINT__STORAGE__GRAPH__HTTP_URL=http://localhost:7474，
            // 滲進 load_default_toml，斷言 "127.0.0.1" 對不上而 panic。
            // 之後每加一個 GraphStorage 欄位都要在這裡同步 remove_var，不要重演。
            std::env::remove_var("OSINT__STORAGE__GRAPH__HTTP_URL");
            std::env::remove_var("OSINT__STORAGE__GRAPH__BOLT_URI");
            std::env::remove_var("OSINT__STORAGE__GRAPH__USERNAME");
            std::env::remove_var("OSINT__STORAGE__GRAPH__PASSWORD_SECRET_REF");
            std::env::remove_var("OSINT__STORAGE__GRAPH__POOL_MAX");
        }
    }

    #[test]
    fn load_default_toml() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        let cfg = AppConfig::load_from(Some(&workspace_default()), None).unwrap();
        assert_eq!(cfg.app.environment, "dev");
        assert_eq!(cfg.storage.canonical.adapter, "postgres");
        assert_eq!(
            cfg.storage.canonical.dsn_secret_ref.as_str(),
            "env:DATABASE_URL"
        );
        assert_eq!(cfg.storage.canonical.pool_max, 10);
        assert_eq!(cfg.storage.object.bucket, "raw-evidence");
        assert_eq!(cfg.broker.brokers, "127.0.0.1:9092");
        assert_eq!(cfg.http.bind, "127.0.0.1:18080");
        assert_eq!(cfg.auth.jwt_secret_ref.as_str(), "env:JWT_SECRET");
        assert_eq!(cfg.auth.jwt_issuer, "osint-core");
        assert_eq!(cfg.collector.bind, "127.0.0.1:18081");
        assert_eq!(cfg.collector.global_inflight, 4);
        assert_eq!(cfg.collector.per_domain_inflight, 1);
        assert_eq!(cfg.normalizer.bind, "127.0.0.1:18082");
        assert_eq!(cfg.normalizer.consumer_group, "osint-normalizer");
        assert_eq!(cfg.deduplicator.bind, "127.0.0.1:18083");
        assert_eq!(cfg.deduplicator.consumer_group, "osint-deduplicator");
        assert_eq!(cfg.deduplicator.candidate_limit, 20);
        assert_eq!(cfg.deduplicator.simhash_scan_limit, 500);
        assert_eq!(cfg.deduplicator.simhash_max_distance, 3);
        assert_eq!(cfg.entity_worker.bind, "127.0.0.1:18084");
        assert_eq!(cfg.entity_worker.consumer_group, "osint-entity-worker");
        assert_eq!(cfg.entity_worker.max_extractions, 500);
        assert_eq!(cfg.entity_worker.max_scan_bytes, 262_144);
        assert_eq!(cfg.indexer.bind, "127.0.0.1:18085");
        assert_eq!(cfg.indexer.consumer_group, "osint-indexer");
        assert_eq!(cfg.indexer.index, "osint-documents");
        assert_eq!(cfg.indexer.batch_size, 200);
        assert_eq!(cfg.indexer.batch_timeout_ms, 1_000);
        assert_eq!(cfg.indexer.max_field_bytes, 262_144);
        assert_eq!(cfg.indexer.bulk_max_retries, 3);
        assert_eq!(cfg.indexer.lag_threshold, 5_000);
        assert_eq!(cfg.indexer.backpressure_sleep_ms, 200);
        assert_eq!(cfg.storage.graph.adapter, "neo4j");
        assert_eq!(cfg.storage.graph.http_url, "http://127.0.0.1:7474");
        assert_eq!(cfg.storage.graph.bolt_uri, "bolt://127.0.0.1:7687");
        assert_eq!(cfg.storage.graph.username, "neo4j");
        assert_eq!(
            cfg.storage.graph.password_secret_ref.as_str(),
            "env:NEO4J_PASSWORD"
        );
        assert_eq!(cfg.storage.graph.pool_max, 5);
        assert_eq!(cfg.graph_worker.bind, "127.0.0.1:18086");
        assert_eq!(cfg.graph_worker.consumer_group, "osint-graph-worker");
        assert_eq!(cfg.graph_worker.projection, "osint-graph");
        assert_eq!(cfg.graph_worker.page_size, 100);
        assert_eq!(cfg.embedding_worker.bind, "127.0.0.1:18087");
        assert_eq!(
            cfg.embedding_worker.consumer_group,
            "osint-embedding-worker"
        );
        assert_eq!(cfg.embedding_worker.entities_index, "osint-entities");
        assert_eq!(cfg.embedding_worker.page_size, 100);
        assert_eq!(cfg.stix_worker.bind, "127.0.0.1:18088");
        assert_eq!(cfg.stix_worker.consumer_group, "osint-stix-worker");
        assert_eq!(cfg.stix_worker.max_objects_per_tx, 10_000);
        assert_eq!(cfg.import.max_upload_bytes, 10 * 1024 * 1024);
        assert_eq!(cfg.import.max_records, 10_000);
        assert_eq!(cfg.import.max_record_bytes, 262_144);
        assert_eq!(cfg.import.max_field_bytes, 65_536);
        assert_eq!(cfg.import.max_depth, 32);
        assert_eq!(cfg.import.max_columns, 512);
        assert_eq!(cfg.embedding.batch_size, 32);
        assert_eq!(cfg.embedding.concurrent_inferences, 4);
        assert_eq!(cfg.embedding.similarity_threshold, 0.90);
        assert_eq!(cfg.embedding.dedup_cache_ttl_secs, 900);
        assert_eq!(cfg.search_hybrid.bm25_weight, 1.0);
        assert_eq!(cfg.search_hybrid.vector_weight, 1.0);
        assert_eq!(cfg.search_hybrid.entity_match_weight, 0.0);
        assert_eq!(cfg.search_hybrid.recency_weight, 0.0);
        assert_eq!(cfg.search_hybrid.source_score_weight, 0.0);
        assert_eq!(cfg.search_hybrid.confidence_weight, 0.0);
        assert!(!cfg.auto_approval.enabled);
        assert_eq!(cfg.auto_approval.auto_confirm_score, 0.95);
        assert_eq!(cfg.auto_approval.llm_review_score, 0.70);
        assert_eq!(cfg.auto_approval.max_auto_merges_per_resolve, 3);
        assert_eq!(cfg.auto_approval.survivor_strategy, "source");
        assert!(!cfg.auto_approval.llm.enabled);
        assert_eq!(
            cfg.auto_approval.llm.base_url,
            "http://ai-inference:8000/v1"
        );
        assert_eq!(cfg.auto_approval.llm.model, "qwen-primary");
        assert_eq!(cfg.auto_approval.llm.model_version, "unknown");
        assert_eq!(cfg.auto_approval.llm.timeout_secs, 30);
        assert_eq!(cfg.auto_approval.llm.max_retries, 0);
        assert_eq!(cfg.auto_approval.llm.max_concurrent, 2);
        assert_eq!(cfg.auto_approval.llm.max_tokens, 512);
        assert_eq!(cfg.auto_approval.llm.temperature, 0.0);
        assert!(cfg.auto_approval.thresholds_are_sane());
        assert_eq!(cfg.stix.max_bundle_bytes, 50 * 1024 * 1024);
        assert_eq!(cfg.stix.max_objects, 10_000);
    }

    #[test]
    fn environment_overrides_nested_value() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        unsafe {
            std::env::set_var("OSINT__APP__ENVIRONMENT", "test-override");
            std::env::set_var("OSINT__STORAGE__CANONICAL__POOL_MAX", "3");
        }
        let cfg = AppConfig::load_from(Some(&workspace_default()), None).unwrap();
        assert_eq!(cfg.app.environment, "test-override");
        assert_eq!(cfg.storage.canonical.pool_max, 3);
        clear_osint_overrides();
    }

    #[test]
    fn extra_file_overrides_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        let dir = std::env::temp_dir();
        let path = dir.join("osint-core-config-override.toml");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "[app]\nenvironment = \"from-file\"").unwrap();
        drop(f);
        let cfg = AppConfig::load_from(Some(&workspace_default()), Some(&path)).unwrap();
        assert_eq!(cfg.app.environment, "from-file");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn empty_extra_file_is_ignored() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        let cfg = AppConfig::load_from(Some(&workspace_default()), Some(Path::new(""))).unwrap();
        assert_eq!(cfg.app.environment, "dev");
    }

    #[test]
    fn graph_worker_section_default_values() {
        let section = GraphWorkerSection::default();
        assert_eq!(section.bind, "127.0.0.1:18086");
        assert_eq!(section.consumer_group, "osint-graph-worker");
        assert_eq!(section.projection, "osint-graph");
        assert_eq!(section.page_size, 100);
    }

    #[test]
    fn embedding_worker_section_default_values() {
        let section = EmbeddingWorkerSection::default();
        assert_eq!(section.bind, "127.0.0.1:18087");
        assert_eq!(section.consumer_group, "osint-embedding-worker");
        assert_eq!(section.entities_index, "osint-entities");
        assert_eq!(section.page_size, 100);
    }

    #[test]
    fn stix_worker_section_default_values() {
        let section = StixWorkerSection::default();
        assert_eq!(section.bind, "127.0.0.1:18088");
        assert_eq!(section.consumer_group, "osint-stix-worker");
        assert_eq!(section.max_objects_per_tx, 10_000);
    }

    #[test]
    fn missing_stix_worker_section_does_not_fail_load() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        let cfg = AppConfig::load_from(Some(&workspace_default()), None).unwrap();
        let parsed: AppConfig = {
            let mut value = serde_json::to_value(&cfg).unwrap();
            value.as_object_mut().unwrap().remove("stix_worker");
            serde_json::from_value(value).unwrap()
        };
        assert_eq!(parsed.stix_worker, StixWorkerSection::default());
    }

    #[test]
    fn graph_storage_missing_bolt_fields_use_serde_defaults() {
        // Phase 0b 設定檔只有 adapter + http_url；新欄位必須靠 serde default
        // 補上，不能讓整份設定解析失敗。
        let graph: GraphStorage =
            serde_json::from_str(r#"{"adapter":"neo4j","http_url":"http://127.0.0.1:7474"}"#)
                .unwrap();
        assert_eq!(graph.bolt_uri, "bolt://127.0.0.1:7687");
        assert_eq!(graph.username, "neo4j");
        assert_eq!(graph.password_secret_ref.as_str(), "env:NEO4J_PASSWORD");
        assert_eq!(graph.pool_max, 5);
        assert_eq!(graph, GraphStorage::default());
    }

    #[test]
    fn graph_storage_deserializes_explicit_bolt_fields() {
        let graph: GraphStorage = serde_json::from_str(
            r#"{
                "adapter": "neo4j",
                "http_url": "http://neo4j:7474",
                "bolt_uri": "bolt://neo4j:7687",
                "username": "neo4j",
                "password_secret_ref": "env:NEO4J_PASSWORD"
            }"#,
        )
        .unwrap();
        assert_eq!(graph.http_url, "http://neo4j:7474");
        assert_eq!(graph.bolt_uri, "bolt://neo4j:7687");
        assert_eq!(graph.username, "neo4j");
        assert_eq!(graph.password_secret_ref.as_str(), "env:NEO4J_PASSWORD");
    }

    #[test]
    fn missing_graph_worker_section_does_not_fail_load() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        // 既有設定檔沒有 [graph_worker] 時，#[serde(default)] 必須讓載入成功。
        let cfg = AppConfig::load_from(Some(&workspace_default()), None).unwrap();
        let parsed: AppConfig = {
            // 用 JSON 模擬「整段 graph_worker 缺席」：先載入完整設定再拿掉該鍵。
            let mut value = serde_json::to_value(&cfg).unwrap();
            value.as_object_mut().unwrap().remove("graph_worker");
            serde_json::from_value(value).unwrap()
        };
        assert_eq!(parsed.graph_worker, GraphWorkerSection::default());
    }

    #[test]
    fn missing_embedding_and_hybrid_sections_do_not_fail_load() {
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        let cfg = AppConfig::load_from(Some(&workspace_default()), None).unwrap();
        let parsed: AppConfig = {
            let mut value = serde_json::to_value(&cfg).unwrap();
            let obj = value.as_object_mut().unwrap();
            obj.remove("embedding");
            obj.remove("search_hybrid");
            obj.remove("embedding_worker");
            serde_json::from_value(value).unwrap()
        };
        assert_eq!(parsed.embedding, EmbeddingSection::default());
        assert_eq!(parsed.search_hybrid, HybridSearchSection::default());
        assert_eq!(parsed.embedding_worker, EmbeddingWorkerSection::default());
    }

    #[test]
    fn missing_dedup_cache_ttl_secs_uses_serde_default() {
        // 舊設定檔沒有這個鍵時仍能載入，不可讓整份 [embedding] 解析失敗。
        let section: EmbeddingSection = serde_json::from_str(
            r#"{"batch_size":32,"concurrent_inferences":4,"similarity_threshold":0.9}"#,
        )
        .unwrap();
        assert_eq!(section.dedup_cache_ttl_secs, 900);
        assert_eq!(
            section.dedup_cache_ttl(),
            std::time::Duration::from_secs(900)
        );
    }

    #[test]
    fn missing_auto_approval_section_does_not_fail_load() {
        // 舊設定檔沒有 [auto_approval] 時，#[serde(default)] 必須讓載入成功，
        // 而且主開關維持關閉——否則升級就會意外打開自動核准。
        let _guard = ENV_LOCK.lock().unwrap();
        clear_osint_overrides();
        let cfg = AppConfig::load_from(Some(&workspace_default()), None).unwrap();
        let parsed: AppConfig = {
            let mut value = serde_json::to_value(&cfg).unwrap();
            value.as_object_mut().unwrap().remove("auto_approval");
            serde_json::from_value(value).unwrap()
        };
        assert_eq!(parsed.auto_approval, AutoApprovalSection::default());
        assert!(!parsed.auto_approval.enabled);
    }

    #[test]
    fn partial_auto_approval_section_uses_remaining_defaults() {
        let section: AutoApprovalSection = serde_json::from_str(r#"{"enabled":true}"#).unwrap();
        assert!(section.enabled);
        assert_eq!(section.auto_confirm_score, 0.95);
        assert_eq!(section.llm_review_score, 0.70);
        assert_eq!(section.max_auto_merges_per_resolve, 3);
        assert_eq!(section.survivor_strategy, "source");
        assert_eq!(section.llm, AutoApprovalLlmSection::default());
        assert!(!section.llm.enabled);
    }

    #[test]
    fn auto_approval_thresholds_are_sane_rejects_inverted_range() {
        assert!(AutoApprovalSection::default().thresholds_are_sane());
        let inverted = AutoApprovalSection {
            auto_confirm_score: 0.50,
            llm_review_score: 0.80,
            ..Default::default()
        };
        assert!(!inverted.thresholds_are_sane());
    }

    #[test]
    fn dedup_cache_ttl_clamps_to_five_minutes_and_one_hour() {
        let too_small = EmbeddingSection {
            dedup_cache_ttl_secs: 1,
            ..Default::default()
        };
        assert_eq!(
            too_small.dedup_cache_ttl(),
            std::time::Duration::from_secs(300)
        );
        let too_large = EmbeddingSection {
            dedup_cache_ttl_secs: 99_999,
            ..Default::default()
        };
        assert_eq!(
            too_large.dedup_cache_ttl(),
            std::time::Duration::from_secs(3600)
        );
    }
}
