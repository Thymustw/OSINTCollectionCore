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
    pub import: ImportSection,
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
/// 三個新欄位都有 `#[serde(default)]`：V0.1／Phase 0b 的設定檔只有
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

impl Default for GraphStorage {
    fn default() -> Self {
        Self {
            adapter: "neo4j".into(),
            http_url: "http://127.0.0.1:7474".into(),
            bolt_uri: default_bolt_uri(),
            username: default_neo4j_username(),
            password_secret_ref: default_neo4j_password_secret_ref(),
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
        assert_eq!(cfg.graph_worker.bind, "127.0.0.1:18086");
        assert_eq!(cfg.graph_worker.consumer_group, "osint-graph-worker");
        assert_eq!(cfg.graph_worker.projection, "osint-graph");
        assert_eq!(cfg.graph_worker.page_size, 100);
        assert_eq!(cfg.import.max_upload_bytes, 10 * 1024 * 1024);
        assert_eq!(cfg.import.max_records, 10_000);
        assert_eq!(cfg.import.max_record_bytes, 262_144);
        assert_eq!(cfg.import.max_field_bytes, 65_536);
        assert_eq!(cfg.import.max_depth, 32);
        assert_eq!(cfg.import.max_columns, 512);
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
    fn graph_storage_missing_bolt_fields_use_serde_defaults() {
        // Phase 0b 設定檔只有 adapter + http_url；新欄位必須靠 serde default
        // 補上，不能讓整份設定解析失敗。
        let graph: GraphStorage =
            serde_json::from_str(r#"{"adapter":"neo4j","http_url":"http://127.0.0.1:7474"}"#)
                .unwrap();
        assert_eq!(graph.bolt_uri, "bolt://127.0.0.1:7687");
        assert_eq!(graph.username, "neo4j");
        assert_eq!(graph.password_secret_ref.as_str(), "env:NEO4J_PASSWORD");
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
}
