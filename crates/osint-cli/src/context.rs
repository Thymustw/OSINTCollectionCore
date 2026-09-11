//! 設定載入與後端連線。
//!
//! 設定來源與 osint-api／osint-collector／osint-normalizer 完全相同：
//! `core_config::AppConfig::load()`（config/default.toml → OSINT_CONFIG_FILE → `OSINT__*`），
//! 密鑰一律走 SecretRef。CLI 不自己讀 `DATABASE_URL` 之類的環境變數，
//! 否則同一台機器上 CLI 與服務會看到不同的設定。

use core_config::AppConfig;
use storage_core::conformance::verify_not_opencti_s3;
use storage_postgres::PostgresCanonicalStore;
use storage_s3::S3ObjectStore;

use crate::error::CliError;
use crate::output::Format;

pub struct Context {
    pub cfg: AppConfig,
    pub format: Format,
}

impl Context {
    pub fn load(format: Format) -> Result<Self, CliError> {
        let cfg = AppConfig::load().map_err(|err| CliError::Config {
            message: err.to_string(),
        })?;
        Ok(Self { cfg, format })
    }

    /// 連 canonical store。
    ///
    /// **刻意不跑 migration**：這是唯讀工具，不該在使用者只想看資料時偷偷改 schema。
    /// migration 由 `make migrate-postgres` 或服務啟動時負責。
    pub async fn store(&self) -> Result<PostgresCanonicalStore, CliError> {
        let dsn = self
            .cfg
            .storage
            .canonical
            .dsn_secret_ref
            .resolve()
            .map_err(|err| CliError::Config {
                message: err.to_string(),
            })?;
        PostgresCanonicalStore::connect(&dsn, self.cfg.storage.canonical.pool_max)
            .await
            .map_err(|err| CliError::PostgresUnavailable {
                message: err.to_string(),
            })
    }

    /// 連物件儲存。不呼叫 `ensure_bucket()`——建 bucket 是寫入動作。
    pub fn objects(&self) -> Result<S3ObjectStore, CliError> {
        let object = &self.cfg.storage.object;
        let access = object
            .access_key_ref
            .resolve()
            .map_err(|err| CliError::Config {
                message: err.to_string(),
            })?;
        let secret = object
            .secret_key_ref
            .resolve()
            .map_err(|err| CliError::Config {
                message: err.to_string(),
            })?;
        verify_not_opencti_s3(&object.endpoint).map_err(|err| CliError::Config {
            message: err.to_string(),
        })?;
        S3ObjectStore::connect(&object.endpoint, &object.bucket, &access, &secret).map_err(|err| {
            CliError::ObjectStoreUnavailable {
                message: err.to_string(),
            }
        })
    }
}
