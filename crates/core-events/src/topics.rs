//! SPEC §20 topic 清單。另加 `job.dispatched` 給 Job 派工。
//!
//! V0.2 的九個 topic（`SPEC_V0.2.md` §20）、V0.3 的十個 topic
//! （`SPEC_V0.3.md` §18）也定義在這裡，與 V0.1 沒有任何名稱重疊。
//! **這一批只有定義，沒有生產者也沒有消費者**——同 V0.1 的
//! `search.index.requested`；V0.3 這十個要等 Phase 1（AI Gateway）與
//! Phase 3（Discovery Engine）的 worker/handler 實際發布/訂閱才會有人用。

use serde::{Deserialize, Serialize};

/// Envelope `schema_version`。
pub const SCHEMA_VERSION: &str = "1";

/// broker topic。V0.1 + V0.2（見 enum 內的 section divider）。
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

    // ===== V0.2（SPEC_V0.2 §20）=====
    #[serde(rename = "entity.resolution.requested")]
    EntityResolutionRequested,
    #[serde(rename = "entity.resolution.completed")]
    EntityResolutionCompleted,
    #[serde(rename = "entity.merged")]
    EntityMerged,
    /// ⚠️ 與 V0.1 的 `object.updated` 不同：`object.updated` 是「某個 canonical
    /// object 改了」，這個是「relationship 這條**邊**變了」。graph-worker
    /// （SPEC_V0.2 §8）訂的是後者——訂前者會收到一堆與圖無關的文件更新。
    #[serde(rename = "relationship.changed")]
    RelationshipChanged,
    #[serde(rename = "graph.sync.requested")]
    GraphSyncRequested,
    #[serde(rename = "graph.sync.completed")]
    GraphSyncCompleted,
    #[serde(rename = "embedding.requested")]
    EmbeddingRequested,
    #[serde(rename = "embedding.completed")]
    EmbeddingCompleted,
    #[serde(rename = "timeline.updated")]
    TimelineUpdated,

    // ===== V0.3（SPEC_V0.3 §18）=====
    #[serde(rename = "seed.created")]
    SeedCreated,
    #[serde(rename = "seed.updated")]
    SeedUpdated,
    #[serde(rename = "discovery.requested")]
    DiscoveryRequested,
    #[serde(rename = "discovery.completed")]
    DiscoveryCompleted,
    #[serde(rename = "candidate.created")]
    CandidateCreated,
    #[serde(rename = "candidate.approved")]
    CandidateApproved,
    #[serde(rename = "candidate.rejected")]
    CandidateRejected,
    #[serde(rename = "ai.requested")]
    AiRequested,
    #[serde(rename = "ai.completed")]
    AiCompleted,
    #[serde(rename = "ai.failed")]
    AiFailed,
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
            Self::EntityResolutionRequested => "entity.resolution.requested",
            Self::EntityResolutionCompleted => "entity.resolution.completed",
            Self::EntityMerged => "entity.merged",
            Self::RelationshipChanged => "relationship.changed",
            Self::GraphSyncRequested => "graph.sync.requested",
            Self::GraphSyncCompleted => "graph.sync.completed",
            Self::EmbeddingRequested => "embedding.requested",
            Self::EmbeddingCompleted => "embedding.completed",
            Self::TimelineUpdated => "timeline.updated",
            Self::SeedCreated => "seed.created",
            Self::SeedUpdated => "seed.updated",
            Self::DiscoveryRequested => "discovery.requested",
            Self::DiscoveryCompleted => "discovery.completed",
            Self::CandidateCreated => "candidate.created",
            Self::CandidateApproved => "candidate.approved",
            Self::CandidateRejected => "candidate.rejected",
            Self::AiRequested => "ai.requested",
            Self::AiCompleted => "ai.completed",
            Self::AiFailed => "ai.failed",
        }
    }

    /// 全部 topic。給「名稱不可重複／`as_str` 與 serde 名稱必須一致」的測試用。
    ///
    /// 沒有這個清單，漏掉一個 topic 的 `as_str` 分支只會在執行期才發現，
    /// 而且是以「送到錯誤 topic」的形式發現——那時訊息已經在別的 partition 上了。
    pub const ALL: [Self; 29] = [
        Self::RawCollected,
        Self::RawFailed,
        Self::ObjectNormalized,
        Self::ObjectCreated,
        Self::ObjectUpdated,
        Self::DedupCompleted,
        Self::EntityExtracted,
        Self::SearchIndexRequested,
        Self::SearchIndexCompleted,
        Self::JobDispatched,
        Self::EntityResolutionRequested,
        Self::EntityResolutionCompleted,
        Self::EntityMerged,
        Self::RelationshipChanged,
        Self::GraphSyncRequested,
        Self::GraphSyncCompleted,
        Self::EmbeddingRequested,
        Self::EmbeddingCompleted,
        Self::TimelineUpdated,
        Self::SeedCreated,
        Self::SeedUpdated,
        Self::DiscoveryRequested,
        Self::DiscoveryCompleted,
        Self::CandidateCreated,
        Self::CandidateApproved,
        Self::CandidateRejected,
        Self::AiRequested,
        Self::AiCompleted,
        Self::AiFailed,
    ];
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

        // SPEC_V0.2 §20 的九個。topic 名稱是跨服務契約，改名等於改契約。
        assert_eq!(
            EventTopic::EntityResolutionRequested.as_str(),
            "entity.resolution.requested"
        );
        assert_eq!(
            EventTopic::EntityResolutionCompleted.as_str(),
            "entity.resolution.completed"
        );
        assert_eq!(EventTopic::EntityMerged.as_str(), "entity.merged");
        assert_eq!(
            EventTopic::RelationshipChanged.as_str(),
            "relationship.changed"
        );
        assert_eq!(
            EventTopic::GraphSyncRequested.as_str(),
            "graph.sync.requested"
        );
        assert_eq!(
            EventTopic::GraphSyncCompleted.as_str(),
            "graph.sync.completed"
        );
        assert_eq!(
            EventTopic::EmbeddingRequested.as_str(),
            "embedding.requested"
        );
        assert_eq!(
            EventTopic::EmbeddingCompleted.as_str(),
            "embedding.completed"
        );
        assert_eq!(EventTopic::TimelineUpdated.as_str(), "timeline.updated");

        // SPEC_V0.3 §18 的十個。
        assert_eq!(EventTopic::SeedCreated.as_str(), "seed.created");
        assert_eq!(EventTopic::SeedUpdated.as_str(), "seed.updated");
        assert_eq!(
            EventTopic::DiscoveryRequested.as_str(),
            "discovery.requested"
        );
        assert_eq!(
            EventTopic::DiscoveryCompleted.as_str(),
            "discovery.completed"
        );
        assert_eq!(EventTopic::CandidateCreated.as_str(), "candidate.created");
        assert_eq!(EventTopic::CandidateApproved.as_str(), "candidate.approved");
        assert_eq!(EventTopic::CandidateRejected.as_str(), "candidate.rejected");
        assert_eq!(EventTopic::AiRequested.as_str(), "ai.requested");
        assert_eq!(EventTopic::AiCompleted.as_str(), "ai.completed");
        assert_eq!(EventTopic::AiFailed.as_str(), "ai.failed");
    }

    /// `as_str()` 與 serde 的 `rename` 必須是同一個字串。
    ///
    /// 兩邊各寫一次名字，寫錯一邊**不會編譯失敗**：produce 用 `as_str()` 決定
    /// topic，envelope 裡的 `event_type` 走 serde，consumer 會訂到一個空 topic
    /// 而且一則訊息都收不到——看起來就只是「目前沒有事件」。
    #[test]
    fn as_str_matches_serde_name() {
        for topic in EventTopic::ALL {
            let serialized = serde_json::to_string(&topic).expect("serialize");
            assert_eq!(
                serialized,
                format!("\"{}\"", topic.as_str()),
                "{topic:?} 的 as_str() 與 serde rename 不一致"
            );
        }
    }

    /// 沒有兩個 topic 共用同一個名稱，且 `ALL` 沒有漏掉任何一個 variant。
    #[test]
    fn topic_names_are_unique_and_all_is_complete() {
        let mut names: Vec<&str> = EventTopic::ALL.iter().map(|t| t.as_str()).collect();
        let total = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), total, "EventTopic::ALL 有重複的 topic 名稱");
        // V0.1 的 10 個 + V0.2 §20 的 9 個 + V0.3 §18 的 10 個。新增 variant
        // 時這個數字要一起改，否則 ALL 漏掉的那個不會被上面兩個測試檢查到。
        assert_eq!(total, 29);
    }
}
