//! SSRF Guard：hard-deny + soft-deny + `NetworkRule`。
//!
//! 強制路徑：resolve 一次 → 分類每個 IP → 連到解析到的 IP（不在 connect 時再 resolve）。

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use core_model::{NetworkRule, SourceId};
use core_security::{AuditEntry, AuditLog, IpClass, classify_host, classify_ip};
use serde_json::json;
use url::Url;

use crate::ConnectorError;
use crate::dns::HostResolver;
use crate::network_rule::{MatchingRule, matching_rule};
use crate::policy::SourcePolicy;

/// 允許連線時的決策。`allowlisted` 時必須寫稽核。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsrfDecision {
    pub host: String,
    pub port: u16,
    pub ips: Vec<IpAddr>,
    pub allowlisted: Option<MatchingRule>,
}

/// 對單一 Source 的 SSRF 檢查。
pub struct SsrfGuard {
    source_id: SourceId,
    policy: SourcePolicy,
    rules: Vec<NetworkRule>,
    resolver: Arc<dyn HostResolver>,
    audit: Arc<dyn AuditLog>,
}

impl SsrfGuard {
    #[must_use]
    pub fn new(
        source_id: SourceId,
        policy: SourcePolicy,
        rules: Vec<NetworkRule>,
        resolver: Arc<dyn HostResolver>,
        audit: Arc<dyn AuditLog>,
    ) -> Self {
        Self {
            source_id,
            policy,
            rules,
            resolver,
            audit,
        }
    }

    #[must_use]
    pub fn policy(&self) -> &SourcePolicy {
        &self.policy
    }

    #[must_use]
    pub fn source_id(&self) -> SourceId {
        self.source_id
    }

    /// 檢查 URL（含每次 redirect hop）。允許時若走白名單會寫稽核。
    pub async fn check(
        &self,
        url: &Url,
        now: DateTime<Utc>,
    ) -> Result<SsrfDecision, ConnectorError> {
        if url.scheme() != "http" && url.scheme() != "https" {
            return Err(ConnectorError::UnsupportedScheme {
                url: url.to_string(),
                scheme: url.scheme().to_string(),
            });
        }
        let host = url.host_str().ok_or_else(|| ConnectorError::MissingHost {
            url: url.to_string(),
        })?;
        let host_norm = host.trim().trim_end_matches('.').to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .unwrap_or(if url.scheme() == "https" { 443 } else { 80 });

        self.policy.host_allowed(&host_norm)?;

        if classify_host(&host_norm) == Some(IpClass::HardDeny) {
            return Err(ConnectorError::HardDenied {
                host: host_norm,
                detail: "hostname 在硬拒絕清單".into(),
            });
        }

        let ips = if let Ok(ip) = host_norm.parse::<IpAddr>() {
            vec![ip]
        } else {
            self.resolver.resolve(&host_norm).await?
        };
        if ips.is_empty() {
            return Err(ConnectorError::DnsEmpty { host: host_norm });
        }

        let mut allowlisted: Option<MatchingRule> = None;
        for ip in &ips {
            match classify_ip(*ip) {
                IpClass::HardDeny => {
                    return Err(ConnectorError::HardDenied {
                        host: host_norm,
                        detail: format!("解析到硬拒絕 IP {ip}"),
                    });
                }
                IpClass::Public => {}
                IpClass::SoftDeny => match matching_rule(&self.rules, &host_norm, *ip, port, now) {
                    Some(rule) => {
                        allowlisted = Some(MatchingRule {
                            id: rule.id,
                            cidr_or_host: rule.cidr_or_host.clone(),
                            reason: rule.reason.clone(),
                            approved_by: rule.approved_by.clone(),
                        });
                    }
                    None => {
                        return Err(ConnectorError::SoftDenied {
                            host: host_norm,
                            ip: ip.to_string(),
                            source_id: self.source_id.to_string(),
                        });
                    }
                },
            }
        }

        if let Some(matched) = &allowlisted {
            self.audit
                .append(
                    AuditEntry::new(
                        matched.approved_by.clone(),
                        "connector.ssrf.allowlist",
                        "source",
                        Some(self.source_id.to_string()),
                        "allowed",
                    )
                    .with_metadata(json!({
                        "source_id": self.source_id,
                        "rule_id": matched.id,
                        "cidr_or_host": matched.cidr_or_host,
                        "host": host_norm,
                        "ips": ips.iter().map(ToString::to_string).collect::<Vec<_>>(),
                        "port": port,
                    })),
                )
                .await
                .map_err(|err| ConnectorError::Audit {
                    message: err.to_string(),
                })?;
        }

        Ok(SsrfDecision {
            host: host_norm,
            port,
            ips,
            allowlisted,
        })
    }

    /// 給 reqwest `resolve` 用：host 對應第一個通過檢查的 IP。
    #[must_use]
    pub fn pinned_addr(decision: &SsrfDecision) -> SocketAddr {
        SocketAddr::new(decision.ips[0], decision.port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dns::MapResolver;
    use core_security::MemoryAuditLog;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn guard(
        rules: Vec<NetworkRule>,
        map: HashMap<String, Vec<IpAddr>>,
    ) -> (SsrfGuard, Arc<MemoryAuditLog>) {
        let audit = Arc::new(MemoryAuditLog::new());
        let resolver = Arc::new(MapResolver::new(map));
        let g = SsrfGuard::new(
            Uuid::now_v7(),
            SourcePolicy::default(),
            rules,
            resolver,
            audit.clone(),
        );
        (g, audit)
    }

    #[tokio::test]
    async fn public_ip_allowed() {
        let mut map = HashMap::new();
        map.insert("example.com".into(), vec!["8.8.8.8".parse().unwrap()]);
        let (g, audit) = guard(vec![], map);
        let url = Url::parse("https://example.com/feed").unwrap();
        let d = g.check(&url, Utc::now()).await.unwrap();
        assert!(d.allowlisted.is_none());
        assert_eq!(d.ips[0].to_string(), "8.8.8.8");
        assert!(audit.entries().is_empty());
    }

    #[tokio::test]
    async fn loopback_denied_by_default() {
        let mut map = HashMap::new();
        map.insert("local.test".into(), vec!["127.0.0.1".parse().unwrap()]);
        let (g, _) = guard(vec![], map);
        let url = Url::parse("http://local.test/").unwrap();
        let err = g.check(&url, Utc::now()).await.unwrap_err();
        assert!(matches!(err, ConnectorError::SoftDenied { .. }));
    }

    #[tokio::test]
    async fn metadata_hard_denied_even_with_rule() {
        let now = Utc::now();
        let rules = vec![NetworkRule {
            id: Uuid::now_v7(),
            source_id: Uuid::now_v7(),
            cidr_or_host: "169.254.169.254".into(),
            ports: None,
            reason: "不該過".into(),
            approved_by: "alice".into(),
            expires_at: None,
            created_at: now,
            updated_at: now,
        }];
        let (g, _) = guard(rules, HashMap::new());
        let url = Url::parse("http://169.254.169.254/latest/meta-data").unwrap();
        let err = g.check(&url, now).await.unwrap_err();
        assert!(matches!(err, ConnectorError::HardDenied { .. }));
    }
}
