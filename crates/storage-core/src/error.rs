//! 穩定的 storage 錯誤分類。後端原文可留在 `message`，但必須先遮罩密文。

use std::fmt::Write as _;

/// Storage adapter 的穩定錯誤分類（對齊 `STORAGE_ARCHITECTURE.md` §19）。
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("儲存後端 `{backend}` 目前不可用：{message}")]
    Unavailable {
        backend: &'static str,
        message: String,
    },
    #[error("儲存後端 `{backend}` 逾時：{message}。請稍後再試或把批次調小")]
    Timeout {
        backend: &'static str,
        message: String,
    },
    #[error("寫入衝突：{message}")]
    Conflict { message: String },
    #[error("資料約束不符：{message}")]
    ConstraintViolation { message: String },
    #[error("找不到資料：{message}")]
    NotFound { message: String },
    #[error("儲存後端拒絕存取：{message}")]
    PermissionDenied { message: String },
    #[error("儲存容量或配額不足：{message}")]
    CapacityExceeded { message: String },
    #[error("資料可能已損壞：{message}")]
    CorruptionSuspected { message: String },
    #[error("後端 `{backend}` 不支援 capability `{capability}`")]
    UnsupportedCapability {
        backend: &'static str,
        capability: &'static str,
    },
    #[error("需要先跑 migration：{message}")]
    MigrationRequired { message: String },
    #[error("儲存設定不正確：{message}")]
    Configuration { message: String },
    #[error("儲存後端 `{backend}` 發生未分類錯誤：{message}")]
    Unknown {
        backend: &'static str,
        message: String,
    },
}

impl StorageError {
    /// 把可能含 DSN／密碼的字串遮罩後放進錯誤訊息。
    #[must_use]
    pub fn sanitize(raw: &str) -> String {
        redact_secrets(raw)
    }
}

/// 遮罩 URL 裡的 user:password，以及常見密文參數。
pub fn redact_secrets(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for token in raw.split_whitespace() {
        if !out.is_empty() {
            out.push(' ');
        }
        let _ = write!(out, "{}", redact_token(token));
    }
    if out.is_empty() {
        redact_token(raw)
    } else {
        out
    }
}

fn redact_token(token: &str) -> String {
    let mut s = token.to_string();
    if let Some(scheme_end) = s.find("://") {
        let rest = &s[scheme_end + 3..];
        if let Some(at) = rest.find('@') {
            let creds = &rest[..at];
            if let Some(colon) = creds.find(':') {
                let user = &creds[..colon];
                let prefix = &s[..scheme_end + 3];
                let suffix = &rest[at..];
                s = format!("{prefix}{user}:***{suffix}");
            }
        }
    }
    for key in ["password=", "PASSWORD=", "token=", "secret=", "Secret="] {
        if let Some(idx) = s.find(key) {
            let start = idx + key.len();
            let end = s[start..]
                .find(['&', ' ', ';', '"', '\''])
                .map(|n| start + n)
                .unwrap_or(s.len());
            s.replace_range(start..end, "***");
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_postgres_password() {
        let raw = "error postgres://osint:s3cret@127.0.0.1:5432/osint_core timeout";
        let out = redact_secrets(raw);
        assert!(out.contains("osint:***@127.0.0.1:5432"), "{out}");
        assert!(!out.contains("s3cret"), "{out}");
    }

    #[test]
    fn redacts_password_query() {
        let out = redact_secrets("failed password=super-secret&x=1");
        assert_eq!(out, "failed password=***&x=1");
    }
}
