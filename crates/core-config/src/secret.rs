//! 密鑰參照。設定檔與資料庫只存參照，不存明文 password/token。

use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// 密鑰的存放位置，而不是密鑰本身。
///
/// 支援的寫法：
/// - `env:VAR_NAME` — 從環境變數讀取
/// - `file:/absolute/or/relative/path` — 從檔案讀取（會 trim 結尾空白）
/// - `store:backend/path#key` — 外部 secret store（目前尚未實作解析後端）
///
/// 解析時會拒絕看起來像連線字串或裸密文的值，避免不小心把 secret 寫進設定檔。
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SecretRef {
    raw: String,
    kind: SecretKind,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SecretKind {
    Env(String),
    File(PathBuf),
    Store(String),
}

impl SecretRef {
    /// 解析參照字串。失敗時請改成 `env:` / `file:` / `store:` 其中一種。
    pub fn parse(value: &str) -> Result<Self, SecretRefError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(SecretRefError::Empty);
        }
        if looks_like_inline_secret(value) {
            return Err(SecretRefError::InlineSecret);
        }

        if let Some(name) = value.strip_prefix("env:") {
            let name = name.trim();
            if name.is_empty() {
                return Err(SecretRefError::EmptyEnvName);
            }
            if name.contains(char::is_whitespace) {
                return Err(SecretRefError::InvalidEnvName {
                    name: name.to_string(),
                });
            }
            return Ok(Self {
                raw: format!("env:{name}"),
                kind: SecretKind::Env(name.to_string()),
            });
        }

        if let Some(path) = value.strip_prefix("file:") {
            let path = path.trim();
            if path.is_empty() {
                return Err(SecretRefError::EmptyFilePath);
            }
            return Ok(Self {
                raw: format!("file:{path}"),
                kind: SecretKind::File(PathBuf::from(path)),
            });
        }

        if let Some(locator) = value.strip_prefix("store:") {
            let locator = locator.trim();
            if locator.is_empty() {
                return Err(SecretRefError::EmptyStoreLocator);
            }
            return Ok(Self {
                raw: format!("store:{locator}"),
                kind: SecretKind::Store(locator.to_string()),
            });
        }

        Err(SecretRefError::UnknownScheme {
            value: redact_for_error(value),
        })
    }

    /// 原始參照字串，可安全寫入 log（不含密文）。
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// 解析出明文。`store:` 在 V0.1 bootstrap 尚未接後端，會回傳明確錯誤。
    pub fn resolve(&self) -> Result<String, SecretRefError> {
        match &self.kind {
            SecretKind::Env(name) => {
                std::env::var(name).map_err(|_| SecretRefError::EnvNotSet { name: name.clone() })
            }
            SecretKind::File(path) => read_secret_file(path),
            SecretKind::Store(locator) => Err(SecretRefError::StoreUnresolved {
                locator: locator.clone(),
            }),
        }
    }
}

fn read_secret_file(path: &Path) -> Result<String, SecretRefError> {
    let contents = fs::read_to_string(path).map_err(|source| SecretRefError::FileRead {
        path: path.display().to_string(),
        source,
    })?;
    let trimmed = contents.trim_end_matches(['\n', '\r', ' ', '\t']);
    if trimmed.is_empty() {
        return Err(SecretRefError::EmptyFile {
            path: path.display().to_string(),
        });
    }
    Ok(trimmed.to_string())
}

fn looks_like_inline_secret(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    lower.contains("://")
        || lower.contains("password=")
        || lower.contains("token=")
        || (value.len() >= 24 && !value.contains(':') && !value.contains('/'))
}

fn redact_for_error(value: &str) -> String {
    if value.len() <= 12 {
        return "(已遮罩)".to_string();
    }
    format!("{}…(已遮罩)", &value[..4])
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecretRef").field("ref", &self.raw).finish()
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.raw)
    }
}

impl FromStr for SecretRef {
    type Err = SecretRefError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for SecretRef {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.raw)
    }
}

impl<'de> Deserialize<'de> for SecretRef {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// SecretRef 解析或取值失敗。訊息會說明下一步，不會回傳密文。
#[derive(Debug, thiserror::Error)]
pub enum SecretRefError {
    #[error("SecretRef 不可為空；請改用 env:VAR、file:/path 或 store:locator")]
    Empty,
    #[error("看起來像把密文或連線字串直接寫進設定。請改成 SecretRef，例如 env:DATABASE_URL")]
    InlineSecret,
    #[error("env: 後面缺少環境變數名稱，正確寫法例如 env:DATABASE_URL")]
    EmptyEnvName,
    #[error("環境變數名稱 `{name}` 含空白，請改成不含空白的名稱")]
    InvalidEnvName { name: String },
    #[error("file: 後面缺少路徑，正確寫法例如 file:/run/secrets/db_url")]
    EmptyFilePath,
    #[error("store: 後面缺少 locator")]
    EmptyStoreLocator,
    #[error("無法辨識 SecretRef `{value}`。請用 env:VAR、file:/path 或 store:locator，不要寫明文")]
    UnknownScheme { value: String },
    #[error("環境變數 `{name}` 未設定。請在環境或 .env 提供該變數，或改 SecretRef 指向實際來源")]
    EnvNotSet { name: String },
    #[error("讀取密鑰檔 `{path}` 失敗：{source}。請確認路徑與檔案權限")]
    FileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("密鑰檔 `{path}` 是空的。請寫入密鑰內容（可含結尾換行）")]
    EmptyFile { path: String },
    #[error(
        "SecretRef `store:{locator}` 尚未接上 secret backend。V0.1 bootstrap 請改用 env: 或 file:"
    )]
    StoreUnresolved { locator: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn parse_env_ref() {
        let r = SecretRef::parse("env:DATABASE_URL").unwrap();
        assert_eq!(r.as_str(), "env:DATABASE_URL");
        assert!(format!("{r:?}").contains("env:DATABASE_URL"));
    }

    #[test]
    fn reject_postgres_url() {
        let err =
            SecretRef::parse("postgres://osint:secret@127.0.0.1:5432/osint_core").unwrap_err();
        assert!(matches!(err, SecretRefError::InlineSecret));
    }

    #[test]
    fn resolve_env() {
        unsafe {
            std::env::set_var("OSINT_TEST_SECRET_REF", "from-env");
        }
        let value = SecretRef::parse("env:OSINT_TEST_SECRET_REF")
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(value, "from-env");
        unsafe {
            std::env::remove_var("OSINT_TEST_SECRET_REF");
        }
    }

    #[test]
    fn resolve_file_trims_newline() {
        let dir = std::env::temp_dir();
        let path = dir.join("osint-core-secret-ref-test");
        let mut f = fs::File::create(&path).unwrap();
        writeln!(f, "file-secret").unwrap();
        drop(f);
        let value = SecretRef::parse(&format!("file:{}", path.display()))
            .unwrap()
            .resolve()
            .unwrap();
        assert_eq!(value, "file-secret");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn serde_round_trip() {
        let original = SecretRef::parse("env:MINIO_ROOT_PASSWORD").unwrap();
        let json = serde_json::to_string(&original).unwrap();
        assert_eq!(json, "\"env:MINIO_ROOT_PASSWORD\"");
        let back: SecretRef = serde_json::from_str(&json).unwrap();
        assert_eq!(original, back);
    }

    #[test]
    fn store_ref_does_not_resolve_yet() {
        let err = SecretRef::parse("store:vault/osint#db")
            .unwrap()
            .resolve()
            .unwrap_err();
        assert!(matches!(err, SecretRefError::StoreUnresolved { .. }));
    }
}
