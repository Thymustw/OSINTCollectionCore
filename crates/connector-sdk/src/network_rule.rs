//! `NetworkRule` 寫入驗證與比對。Hard-deny 永遠不能被覆寫。

use std::net::IpAddr;

use chrono::{DateTime, Utc};
use core_model::NetworkRule;
use core_security::{IpClass, Principal, Role, classify_host, classify_ip};
use ipnet::IpNet;

use crate::ConnectorError;

/// 通過驗證的比對結果，給稽核用。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchingRule {
    pub id: uuid::Uuid,
    pub cidr_or_host: String,
    pub reason: String,
    pub approved_by: String,
}

/// 建立／編輯規則前的檢查。Viewer 會被拒絕；hard-deny 目標會被拒絕。
pub fn validate_network_rule(
    principal: &Principal,
    rule: &NetworkRule,
) -> Result<(), ConnectorError> {
    if !principal.role.includes(Role::Operator) {
        return Err(ConnectorError::NetworkRuleForbidden {
            role: principal.role.as_str().to_string(),
        });
    }
    if rule.reason.trim().is_empty() {
        return Err(ConnectorError::MissingRuleReason);
    }
    if rule.approved_by.trim().is_empty() {
        return Err(ConnectorError::MissingApprover);
    }
    if rule.cidr_or_host.contains('*') || rule.cidr_or_host.contains('?') {
        return Err(ConnectorError::InvalidNetworkRule {
            cidr_or_host: rule.cidr_or_host.clone(),
            message:
                "禁止萬用字元。請寫精確 hostname 或 CIDR，例如 10.0.0.0/8 或 api.internal.example"
                    .into(),
        });
    }
    let target = rule.cidr_or_host.trim();
    if target.is_empty() {
        return Err(ConnectorError::InvalidNetworkRule {
            cidr_or_host: rule.cidr_or_host.clone(),
            message: "cidr_or_host 不能是空的".into(),
        });
    }
    if let Ok(net) = target.parse::<IpNet>() {
        if classify_ip(net.addr()) == IpClass::HardDeny
            || classify_ip(net.network()) == IpClass::HardDeny
            || cidr_covers_hard_deny(net)
        {
            return Err(ConnectorError::HardDenied {
                host: target.to_string(),
                detail: "這條規則涵蓋 cloud metadata／硬拒絕位址".into(),
            });
        }
        return Ok(());
    }
    if let Ok(ip) = target.parse::<IpAddr>() {
        if classify_ip(ip) == IpClass::HardDeny {
            return Err(ConnectorError::HardDenied {
                host: target.to_string(),
                detail: "這是 cloud metadata 位址".into(),
            });
        }
        return Ok(());
    }
    if classify_host(target) == Some(IpClass::HardDeny) {
        return Err(ConnectorError::HardDenied {
            host: target.to_string(),
            detail: "這是 cloud metadata hostname".into(),
        });
    }
    Ok(())
}

fn cidr_covers_hard_deny(net: IpNet) -> bool {
    const HARD: &[&str] = &[
        "169.254.169.254",
        "169.254.169.253",
        "168.63.129.16",
        "100.100.100.200",
        "fd00:ec2::254",
    ];
    HARD.iter().any(|raw| {
        raw.parse::<IpAddr>()
            .map(|ip| net.contains(&ip))
            .unwrap_or(false)
    })
}

/// 找出一條未過期、涵蓋此 IP＋埠的規則。Hard-deny IP 永遠不匹配。
#[must_use]
pub fn matching_rule<'a>(
    rules: &'a [NetworkRule],
    host: &str,
    ip: IpAddr,
    port: u16,
    now: DateTime<Utc>,
) -> Option<&'a NetworkRule> {
    if classify_ip(ip) == IpClass::HardDeny {
        return None;
    }
    let host = host.trim().trim_end_matches('.').to_ascii_lowercase();
    rules
        .iter()
        .find(|rule| rule_matches(rule, &host, ip, port, now))
}

fn rule_matches(rule: &NetworkRule, host: &str, ip: IpAddr, port: u16, now: DateTime<Utc>) -> bool {
    if rule.is_expired(now) {
        return false;
    }
    if let Some(ports) = rule.ports.as_ref() {
        if !ports.contains(&port) {
            return false;
        }
    }
    let target = rule.cidr_or_host.trim();
    if let Ok(net) = target.parse::<IpNet>() {
        return net.contains(&ip);
    }
    if let Ok(rule_ip) = target.parse::<IpAddr>() {
        return rule_ip == ip;
    }
    let rule_host = target.trim_end_matches('.').to_ascii_lowercase();
    host == rule_host
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_security::{AuthMethod, Principal, Role};
    use uuid::Uuid;

    fn principal(role: Role) -> Principal {
        Principal {
            subject: "alice".into(),
            role,
            auth_method: AuthMethod::Jwt,
        }
    }

    fn rule(cidr_or_host: &str) -> NetworkRule {
        let now = Utc::now();
        NetworkRule {
            id: Uuid::now_v7(),
            source_id: Uuid::now_v7(),
            cidr_or_host: cidr_or_host.into(),
            ports: None,
            reason: "內部 API".into(),
            approved_by: "alice".into(),
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn viewer_cannot_write() {
        let err = validate_network_rule(&principal(Role::Viewer), &rule("10.0.0.0/8")).unwrap_err();
        assert!(matches!(err, ConnectorError::NetworkRuleForbidden { .. }));
    }

    #[test]
    fn operator_can_write_rfc1918() {
        validate_network_rule(&principal(Role::Operator), &rule("10.0.0.0/8")).unwrap();
        validate_network_rule(&principal(Role::Admin), &rule("127.0.0.1")).unwrap();
    }

    #[test]
    fn hard_deny_rejected_at_write() {
        for target in [
            "169.254.169.254",
            "169.254.0.0/16",
            "metadata.google.internal",
            "fd00:ec2::254",
        ] {
            let err = validate_network_rule(&principal(Role::Admin), &rule(target)).unwrap_err();
            assert!(
                matches!(err, ConnectorError::HardDenied { .. }),
                "{target}: {err}"
            );
        }
    }

    #[test]
    fn wildcard_rejected() {
        let err =
            validate_network_rule(&principal(Role::Admin), &rule("*.example.com")).unwrap_err();
        assert!(matches!(err, ConnectorError::InvalidNetworkRule { .. }));
    }
}
