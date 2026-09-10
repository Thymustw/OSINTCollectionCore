//! SPEC §20 topic 清單。另加 `job.dispatched` 給 Job 派工。

use serde::{Deserialize, Serialize};

/// Envelope `schema_version`。
pub const SCHEMA_VERSION: &str = "1";

/// V0.1 broker topic。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventTopic {
    #[serde(rename = "raw.collected")]
    RawCollected,
    #[serde(rename = "raw.failed")]
    RawFailed,
    #[serde(rename = "object.normalized")]
    ObjectNormalized,
    #[serde(rename = "object.created")]
    ObjectCreated,
    #[serde(rename = "object.updated")]
    ObjectUpdated,
    #[serde(rename = "dedup.completed")]
    DedupCompleted,
    #[serde(rename = "entity.extracted")]
    EntityExtracted,
    #[serde(rename = "search.index.requested")]
    SearchIndexRequested,
    #[serde(rename = "search.index.completed")]
    SearchIndexCompleted,
    /// SPEC §20 沒列。Job 系統派工用，見回報。
    #[serde(rename = "job.dispatched")]
    JobDispatched,
}

impl EventTopic {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RawCollected => "raw.collected",
            Self::RawFailed => "raw.failed",
            Self::ObjectNormalized => "object.normalized",
            Self::ObjectCreated => "object.created",
            Self::ObjectUpdated => "object.updated",
            Self::DedupCompleted => "dedup.completed",
            Self::EntityExtracted => "entity.extracted",
            Self::SearchIndexRequested => "search.index.requested",
            Self::SearchIndexCompleted => "search.index.completed",
            Self::JobDispatched => "job.dispatched",
        }
    }
}

impl std::fmt::Display for EventTopic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_topics_stable() {
        assert_eq!(EventTopic::RawCollected.as_str(), "raw.collected");
        assert_eq!(EventTopic::JobDispatched.as_str(), "job.dispatched");
    }
}
