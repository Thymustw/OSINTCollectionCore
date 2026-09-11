//! Source 層網路／內容政策（CONNECTOR_SECURITY §2）。

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// 一個 Source 的抓取邊界。
///
/// `domain_allowlist` 為空表示不額外限制公開網域（仍受 SSRF 兩層 deny 約束）。
/// denylist 優先於 allowlist。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourcePolicy {
    pub domain_allowlist: Vec<String>,
    pub domain_denylist: Vec<String>,
    pub max_redirects: u32,
    pub max_response_bytes: u64,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub rate_limit: RateLimitConfig,
}

impl Default for SourcePolicy {
    fn default() -> Self {
        Self {
            domain_allowlist: Vec::new(),
            domain_denylist: Vec::new(),
            max_redirects: 5,
            max_response_bytes: 10 * 1024 * 1024,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            rate_limit: RateLimitConfig::default(),
        }
    }
}

impl SourcePolicy {
    pub fn host_allowed(&self, host: &str) -> Result<(), crate::ConnectorError> {
        let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
        if self
            .domain_denylist
            .iter()
            .any(|d| domain_matches(&host, d))
        {
            return Err(crate::ConnectorError::DomainDenied { host });
        }
        if self.domain_allowlist.is_empty() {
            return Ok(());
        }
        if self
            .domain_allowlist
            .iter()
            .any(|d| domain_matches(&host, d))
        {
            return Ok(());
        }
        Err(crate::ConnectorError::DomainNotAllowed { host })
    }
}

fn domain_matches(host: &str, rule: &str) -> bool {
    let rule = rule.trim().trim_end_matches('.').to_ascii_lowercase();
    if rule.is_empty() {
        return false;
    }
    host == rule || host.ends_with(&format!(".{rule}"))
}

/// Per-domain token bucket。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// 每秒補充的 token 數。
    pub per_second: f64,
    /// 桶容量。
    pub burst: f64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            per_second: 1.0,
            burst: 4.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denylist_wins() {
        let policy = SourcePolicy {
            domain_allowlist: vec!["example.com".into()],
            domain_denylist: vec!["evil.example.com".into()],
            ..SourcePolicy::default()
        };
        assert!(policy.host_allowed("example.com").is_ok());
        assert!(policy.host_allowed("evil.example.com").is_err());
        assert!(policy.host_allowed("www.example.com").is_ok());
    }

    #[test]
    fn empty_allowlist_allows_public_hosts() {
        let policy = SourcePolicy::default();
        assert!(policy.host_allowed("feeds.example.invalid").is_ok());
    }
}
