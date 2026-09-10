//! JWT 簽發／驗證（HS256，jsonwebtoken 11 + rust_crypto）。

use chrono::{Duration, Utc};
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::SecurityError;
use crate::rbac::Role;

const MIN_SECRET_LEN: usize = 32;

/// JWT claims。`role` 是 RBAC 角色。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claims {
    pub sub: String,
    pub role: Role,
    pub iss: String,
    pub iat: i64,
    pub exp: i64,
    pub jti: String,
}

/// HS256 JWT 服務。secret 至少 32 bytes。
#[derive(Clone)]
pub struct JwtService {
    encoding: EncodingKey,
    decoding: DecodingKey,
    issuer: String,
    ttl: Duration,
}

impl JwtService {
    pub fn new(
        secret: &[u8],
        issuer: impl Into<String>,
        ttl: Duration,
    ) -> Result<Self, SecurityError> {
        if secret.len() < MIN_SECRET_LEN {
            return Err(SecurityError::JwtSecretTooShort { len: secret.len() });
        }
        if secret.iter().any(|b| !b.is_ascii()) {
            return Err(SecurityError::JwtEncode {
                message: "JWT secret 含非 ASCII 字元。請改用隨機 bytes／base64，避免之後 HTTP header 編碼失敗".into(),
            });
        }
        Ok(Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            issuer: issuer.into(),
            ttl,
        })
    }

    pub fn issue(&self, subject: impl Into<String>, role: Role) -> Result<String, SecurityError> {
        let now = Utc::now();
        let claims = Claims {
            sub: subject.into(),
            role,
            iss: self.issuer.clone(),
            iat: now.timestamp(),
            exp: (now + self.ttl).timestamp(),
            jti: Uuid::now_v7().to_string(),
        };
        jsonwebtoken::encode(&Header::default(), &claims, &self.encoding).map_err(|err| {
            SecurityError::JwtEncode {
                message: err.to_string(),
            }
        })
    }

    pub fn verify(&self, token: &str) -> Result<Claims, SecurityError> {
        if token.bytes().any(|b| !b.is_ascii()) {
            return Err(SecurityError::JwtInvalid {
                message:
                    "token 含非 ASCII 字元，不是合法 JWT。請重新複製 token，不要夾進不可見字元"
                        .into(),
            });
        }
        let mut validation = Validation::new(jsonwebtoken::Algorithm::HS256);
        validation.set_issuer(&[&self.issuer]);
        jsonwebtoken::decode::<Claims>(token, &self.decoding, &validation)
            .map(|data| data.claims)
            .map_err(|err| SecurityError::JwtInvalid {
                message: err.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc() -> JwtService {
        JwtService::new(&[b'x'; 32], "osint-core", Duration::hours(1)).unwrap()
    }

    #[test]
    fn round_trip() {
        let token = svc().issue("alice", Role::Operator).unwrap();
        let claims = svc().verify(&token).unwrap();
        assert_eq!(claims.sub, "alice");
        assert_eq!(claims.role, Role::Operator);
        assert_eq!(claims.iss, "osint-core");
    }

    #[test]
    fn rejects_short_secret() {
        match JwtService::new(b"short", "osint-core", Duration::hours(1)) {
            Err(SecurityError::JwtSecretTooShort { len: 5 }) => {}
            Ok(_) => panic!("預期 JwtSecretTooShort，卻成功建立 JwtService"),
            Err(err) => panic!("預期 JwtSecretTooShort，得到 {err:?}"),
        }
    }

    #[test]
    fn rejects_tampered_token() {
        let token = svc().issue("alice", Role::Admin).unwrap();
        let bad = format!("{token}x");
        assert!(svc().verify(&bad).is_err());
    }
}
