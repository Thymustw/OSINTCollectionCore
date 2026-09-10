//! `/health` 與 `/ready` 的穩定回應型別。

use serde::{Deserialize, Serialize};

/// 單一檢查結果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckResult {
    pub name: String,
    pub healthy: bool,
    pub message: String,
}

impl CheckResult {
    #[must_use]
    pub fn ok(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            healthy: true,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn down(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            healthy: false,
            message: message.into(),
        }
    }
}

/// liveness：行程還活著即可。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: &'static str,
}

impl HealthStatus {
    #[must_use]
    pub fn alive() -> Self {
        Self { status: "ok" }
    }
}

/// readiness：下游依賴是否可服務。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyStatus {
    pub ready: bool,
    pub checks: Vec<CheckResult>,
}

impl ReadyStatus {
    #[must_use]
    pub fn from_checks(checks: Vec<CheckResult>) -> Self {
        let ready = checks.iter().all(|c| c.healthy);
        Self { ready, checks }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_requires_all_checks() {
        let ok = ReadyStatus::from_checks(vec![CheckResult::ok("postgres", "SELECT 1")]);
        assert!(ok.ready);
        let down = ReadyStatus::from_checks(vec![
            CheckResult::ok("postgres", "SELECT 1"),
            CheckResult::down("redpanda", "broker 連不上"),
        ]);
        assert!(!down.ready);
    }
}
