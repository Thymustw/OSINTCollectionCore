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
}
