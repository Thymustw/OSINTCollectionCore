//! V0.1 RBAC。三個角色，權限是嚴格超集。
//!
//! SPEC §23 只寫「RBAC base」，沒有權限矩陣。這裡採用：
//!
//! | 權限 | viewer | operator | admin |
//! |---|---|---|---|
//! | 讀（objects/jobs/health） | yes | yes | yes |
//! | 寫／重試 Job、改 Source | no | yes | yes |
//! | 管 token、改角色 | no | no | yes |
//!
//! 沒有「部分繼承可關閉」；operator 永遠包含 viewer，admin 永遠包含 operator。

use serde::{Deserialize, Serialize};

use crate::SecurityError;

/// 三個固定角色。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Operator => "operator",
            Self::Admin => "admin",
        }
    }

    #[must_use]
    pub fn from_str_strict(value: &str) -> Option<Self> {
        match value {
            "viewer" => Some(Self::Viewer),
            "operator" => Some(Self::Operator),
            "admin" => Some(Self::Admin),
            _ => None,
        }
    }

    #[must_use]
    pub fn includes(self, other: Role) -> bool {
        matches!(
            (self, other),
            (Self::Admin, _)
                | (Self::Operator, Self::Operator | Self::Viewer)
                | (Self::Viewer, Self::Viewer)
        )
    }

    #[must_use]
    pub fn allows(self, permission: Permission) -> bool {
        match permission {
            Permission::Read => true,
            Permission::Write => self.includes(Role::Operator),
            Permission::Admin => self.includes(Role::Admin),
        }
    }

    pub fn require(self, permission: Permission) -> Result<(), SecurityError> {
        if self.allows(permission) {
            Ok(())
        } else {
            Err(SecurityError::Forbidden {
                role: self.as_str().to_string(),
                permission: permission.as_str().to_string(),
            })
        }
    }
}

impl std::fmt::Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 粗粒度權限。V0.1 不做資源級 ACL。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

impl Permission {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Admin => "admin",
        }
    }
}

/// 通過認證後的主體。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    pub role: Role,
    pub auth_method: AuthMethod,
}

/// 認證方式，給 audit 用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuthMethod {
    Jwt,
    ApiToken,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matrix() {
        assert!(Role::Viewer.allows(Permission::Read));
        assert!(!Role::Viewer.allows(Permission::Write));
        assert!(!Role::Viewer.allows(Permission::Admin));
        assert!(Role::Operator.allows(Permission::Write));
        assert!(!Role::Operator.allows(Permission::Admin));
        assert!(Role::Admin.allows(Permission::Admin));
        assert!(Role::Admin.allows(Permission::Read));
    }

    #[test]
    fn require_explains_next_step() {
        let err = Role::Viewer.require(Permission::Write).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("operator"), "{msg}");
        assert!(msg.contains("write"), "{msg}");
    }
}
