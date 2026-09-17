//! Task type → model alias 的對映（SPEC_V0.3 §3 model registry）。
//!
//! 這不是強制路由：`ChatCompletionRequest.model` 仍然是呼叫端明確指定的欄位，
//! `ModelRegistry` 只是給呼叫端一個「這個 task 該用哪個 model」的查詢入口，
//! 查不到就退回呼叫端自己給的預設值——不是白名單，比照 `core_model::AI_TASK_TYPES`
//! 同一種「參考清單」精神。

use std::collections::HashMap;

/// Task type → model alias。空 map（[`ModelRegistry::empty`]）代表「完全不查表，
/// 一律用呼叫端指定的預設值」，是目前唯一有一個 model（`qwen-primary`）時的
/// 合理起點。
#[derive(Debug, Clone, Default)]
pub struct ModelRegistry {
    entries: HashMap<String, String>,
}

impl ModelRegistry {
    #[must_use]
    pub fn new(entries: HashMap<String, String>) -> Self {
        Self { entries }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// 查這個 `task_type` 該用哪個 model alias；查不到回傳 `default_model`。
    #[must_use]
    pub fn resolve<'a>(&'a self, task_type: &str, default_model: &'a str) -> &'a str {
        self.entries
            .get(task_type)
            .map(String::as_str)
            .unwrap_or(default_model)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry_always_falls_back_to_default() {
        let registry = ModelRegistry::empty();
        assert_eq!(
            registry.resolve("candidate_scoring", "qwen-primary"),
            "qwen-primary"
        );
    }

    #[test]
    fn resolves_configured_task_type() {
        let mut entries = HashMap::new();
        entries.insert("summarization".to_string(), "qwen-small".to_string());
        let registry = ModelRegistry::new(entries);
        assert_eq!(
            registry.resolve("summarization", "qwen-primary"),
            "qwen-small"
        );
    }

    #[test]
    fn unconfigured_task_type_falls_back_to_default() {
        let mut entries = HashMap::new();
        entries.insert("summarization".to_string(), "qwen-small".to_string());
        let registry = ModelRegistry::new(entries);
        assert_eq!(
            registry.resolve("candidate_scoring", "qwen-primary"),
            "qwen-primary"
        );
    }
}
