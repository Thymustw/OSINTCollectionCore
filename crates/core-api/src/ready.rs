//! `/ready` 探測。可注入 storage／broker 檢查。

use std::sync::Arc;

use async_trait::async_trait;
use core_observability::{CheckResult, ReadyStatus};
use storage_core::HealthProvider;

/// 單一 readiness 檢查。
#[async_trait]
pub trait ReadyCheck: Send + Sync {
    fn name(&self) -> &'static str;
    async fn check(&self) -> CheckResult;
}

/// 一組檢查。
#[derive(Clone, Default)]
pub struct ReadyProbe {
    checks: Vec<Arc<dyn ReadyCheck>>,
}

impl ReadyProbe {
    #[must_use]
    pub fn new(checks: Vec<Arc<dyn ReadyCheck>>) -> Self {
        Self { checks }
    }

    #[must_use]
    pub fn always_ready() -> Self {
        Self {
            checks: vec![Arc::new(StaticOk)],
        }
    }

    pub async fn status(&self) -> ReadyStatus {
        let mut results = Vec::with_capacity(self.checks.len());
        for check in &self.checks {
            results.push(check.check().await);
        }
        ReadyStatus::from_checks(results)
    }
}

struct StaticOk;

#[async_trait]
impl ReadyCheck for StaticOk {
    fn name(&self) -> &'static str {
        "process"
    }

    async fn check(&self) -> CheckResult {
        CheckResult::ok("process", "行程活著")
    }
}

/// Postgres SELECT 1。
pub struct PostgresReady {
    pub store: storage_postgres::PostgresCanonicalStore,
}

#[async_trait]
impl ReadyCheck for PostgresReady {
    fn name(&self) -> &'static str {
        "postgres"
    }

    async fn check(&self) -> CheckResult {
        match self.store.health().await {
            Ok(h) if h.healthy => CheckResult::ok("postgres", h.message),
            Ok(h) => CheckResult::down("postgres", h.message),
            Err(err) => CheckResult::down("postgres", err.to_string()),
        }
    }
}
