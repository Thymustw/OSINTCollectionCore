//! Event envelope v1 與 Redpanda 封裝。

mod envelope;
mod error;
mod kafka;
mod lag;
mod payloads;
mod topics;

pub use envelope::EventEnvelope;
pub use error::EventError;
pub use kafka::{EventConsumer, EventProducer};
pub use lag::{GroupLag, GroupLagProbe, PartitionLag};
pub use payloads::{
    AiCompleted, AiFailed, AiRequested, CandidateLifecycle, DiscoveryCompleted, SeedCreated,
    partition_key, to_payload_value,
};
pub use topics::{EventTopic, SCHEMA_VERSION};
