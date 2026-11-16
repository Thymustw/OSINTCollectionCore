//! Event envelope v1 與 Redpanda 封裝。

mod envelope;
mod error;
mod kafka;
mod lag;
mod namespace;
mod payloads;
// 一次性 topic namespace 只給測試用（各 crate 的 dev-dependencies 開 `test-support`）。
#[cfg(feature = "test-support")]
mod test_topics;
mod topics;

pub use envelope::EventEnvelope;
pub use error::EventError;
pub use kafka::{EventConsumer, EventProducer};
pub use lag::{GroupLag, GroupLagProbe, PartitionLag};
pub use namespace::TopicNamespace;
pub use payloads::{
    AiCompleted, AiFailed, AiRequested, CandidateLifecycle, DiscoveryCompleted, SeedCreated,
    partition_key, to_payload_value,
};
#[cfg(feature = "test-support")]
pub use test_topics::{TEST_TOPIC_PREFIX, TestTopics};
pub use topics::{EventTopic, SCHEMA_VERSION};
