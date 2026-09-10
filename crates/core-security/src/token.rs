//! API token：明文只在發行當下回傳一次；儲存只留 argon2 hash。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, PasswordVerifier, phc::PasswordHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::RngCore;
use uuid::Uuid;

use crate::SecurityError;
use crate::rbac::Role;

const TOKEN_PREFIX: &str = "osint_";

/// 存在 store 裡的記錄。不含明文 secret。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiTokenRecord {
    pub id: Uuid,
    pub name: String,
    pub role: Role,
    pub secret_hash: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// 發行當下才看得到的明文。之後再也拿不到。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IssuedApiToken {
    pub record: ApiTokenRecord,
    /// 完整 token，格式 `osint_<id>.<secret>`。
    pub plaintext: String,
}

/// 解析後的出示 token。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresentedToken {
    pub id: Uuid,
    pub secret: String,
}

/// Token 儲存。由呼叫端接到 Postgres／記憶體。
#[async_trait]
pub trait ApiTokenStore: Send + Sync {
    async fn insert(&self, record: &ApiTokenRecord) -> Result<(), SecurityError>;
    async fn get(&self, id: Uuid) -> Result<Option<ApiTokenRecord>, SecurityError>;
    async fn list(&self) -> Result<Vec<ApiTokenRecord>, SecurityError>;
    async fn revoke(&self, id: Uuid, at: DateTime<Utc>) -> Result<bool, SecurityError>;
    async fn touch_last_used(&self, id: Uuid, at: DateTime<Utc>) -> Result<(), SecurityError>;
}

/// 行程內記憶體 store，給測試與尚未接 DB 的 API skeleton 用。
#[derive(Debug, Default, Clone)]
pub struct MemoryApiTokenStore {
    inner: Arc<Mutex<HashMap<Uuid, ApiTokenRecord>>>,
}

impl MemoryApiTokenStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl ApiTokenStore for MemoryApiTokenStore {
    async fn insert(&self, record: &ApiTokenRecord) -> Result<(), SecurityError> {
        let mut map = self.inner.lock().expect("token store lock");
        map.insert(record.id, record.clone());
        Ok(())
    }

    async fn get(&self, id: Uuid) -> Result<Option<ApiTokenRecord>, SecurityError> {
        let map = self.inner.lock().expect("token store lock");
        Ok(map.get(&id).cloned())
    }

    async fn list(&self) -> Result<Vec<ApiTokenRecord>, SecurityError> {
        let map = self.inner.lock().expect("token store lock");
        Ok(map.values().cloned().collect())
    }

    async fn revoke(&self, id: Uuid, at: DateTime<Utc>) -> Result<bool, SecurityError> {
        let mut map = self.inner.lock().expect("token store lock");
        if let Some(record) = map.get_mut(&id) {
            record.revoked_at = Some(at);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn touch_last_used(&self, id: Uuid, at: DateTime<Utc>) -> Result<(), SecurityError> {
        let mut map = self.inner.lock().expect("token store lock");
        if let Some(record) = map.get_mut(&id) {
            record.last_used_at = Some(at);
        }
        Ok(())
    }
}

/// 發行一把新 token。明文只在回傳值裡。
pub fn issue_api_token(
    name: impl Into<String>,
    role: Role,
) -> Result<IssuedApiToken, SecurityError> {
    let id = Uuid::now_v7();
    let mut secret_bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut secret_bytes);
    let secret = hex::encode(&secret_bytes);
    let plaintext = format!("{TOKEN_PREFIX}{id}.{secret}");
    let secret_hash = hash_secret(&secret)?;
    let record = ApiTokenRecord {
        id,
        name: name.into(),
        role,
        secret_hash,
        created_at: Utc::now(),
        last_used_at: None,
        revoked_at: None,
    };
    Ok(IssuedApiToken { record, plaintext })
}

pub fn parse_presented_token(raw: &str) -> Result<PresentedToken, SecurityError> {
    let rest = raw
        .strip_prefix(TOKEN_PREFIX)
        .ok_or(SecurityError::MalformedApiToken)?;
    let (id_part, secret) = rest
        .split_once('.')
        .ok_or(SecurityError::MalformedApiToken)?;
    if secret.is_empty() {
        return Err(SecurityError::MalformedApiToken);
    }
    let id = Uuid::parse_str(id_part).map_err(|_| SecurityError::MalformedApiToken)?;
    Ok(PresentedToken {
        id,
        secret: secret.to_string(),
    })
}

pub fn hash_secret(secret: &str) -> Result<String, SecurityError> {
    let argon2 = Argon2::default();
    argon2
        .hash_password(secret.as_bytes())
        .map(|h| h.to_string())
        .map_err(|err| SecurityError::TokenHash {
            message: err.to_string(),
        })
}

pub fn verify_secret(secret: &str, hash: &str) -> Result<bool, SecurityError> {
    let parsed = PasswordHash::new(hash).map_err(|err| SecurityError::TokenHash {
        message: format!("儲存的 token hash 不是合法 PHC 字串：{err}。請重新發行 token"),
    })?;
    match Argon2::default().verify_password(secret.as_bytes(), &parsed) {
        Ok(()) => Ok(true),
        Err(argon2::password_hash::Error::PasswordInvalid) => Ok(false),
        Err(err) => Err(SecurityError::TokenHash {
            message: err.to_string(),
        }),
    }
}

// hex 編碼不另加 crate；32 bytes 用簡單實作即可。
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            out.push(HEX[(b >> 4) as usize] as char);
            out.push(HEX[(b & 0x0f) as usize] as char);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn issue_parse_verify() {
        let issued = issue_api_token("ci", Role::Operator).unwrap();
        let presented = parse_presented_token(&issued.plaintext).unwrap();
        assert_eq!(presented.id, issued.record.id);
        assert!(verify_secret(&presented.secret, &issued.record.secret_hash).unwrap());
        assert!(!verify_secret("wrong", &issued.record.secret_hash).unwrap());
    }

    #[test]
    fn malformed() {
        assert!(parse_presented_token("not-a-token").is_err());
        assert!(parse_presented_token("osint_not-uuid.abc").is_err());
    }
}
