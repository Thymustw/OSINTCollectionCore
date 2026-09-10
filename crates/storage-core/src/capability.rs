//! Capability descriptor。Core 不可在未宣稱／未通過 conformance 的前提下假設功能存在。

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::health::HealthProvider;

/// Adapter 對外宣告的能力與限制。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilityDescriptor {
    pub backend: String,
    pub adapter_version: String,
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub features: serde_json::Map<String, Value>,
}

impl CapabilityDescriptor {
    #[must_use]
    pub fn new(
        backend: impl Into<String>,
        adapter_version: impl Into<String>,
        capabilities: &[&str],
    ) -> Self {
        Self {
            backend: backend.into(),
            adapter_version: adapter_version.into(),
            capabilities: capabilities.iter().map(|s| (*s).to_string()).collect(),
            features: serde_json::Map::new(),
        }
    }

    #[must_use]
    pub fn with_feature(mut self, key: &str, value: Value) -> Self {
        self.features.insert(key.to_string(), value);
        self
    }

    #[must_use]
    pub fn supports(&self, capability: &str) -> bool {
        self.capabilities.iter().any(|c| c == capability)
    }
}

/// 可查 descriptor 的 adapter。
pub trait StorageAdapter: HealthProvider {
    fn descriptor(&self) -> CapabilityDescriptor;
}
