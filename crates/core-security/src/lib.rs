//! 認證、授權、稽核介面、SSRF IP 分類。
//!
//! 不依賴任何 storage adapter。token 實際存哪裡由呼叫端決定。

mod audit;
mod error;
mod jwt;
mod rbac;
mod ssrf;
mod token;

pub use audit::{AuditEntry, AuditLog, MemoryAuditLog};
pub use error::SecurityError;
pub use jwt::{Claims, JwtService};
pub use rbac::{AuthMethod, Permission, Principal, Role};
pub use ssrf::{DenyTier, IpClass, classify_host, classify_ip};
pub use token::{
    ApiTokenRecord, ApiTokenStore, IssuedApiToken, MemoryApiTokenStore, PresentedToken,
    hash_secret, issue_api_token, parse_presented_token, verify_secret,
};
