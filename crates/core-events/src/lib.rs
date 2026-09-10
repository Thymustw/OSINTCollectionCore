//! Event envelope v1 與 Redpanda 封裝。

mod envelope;
mod error;
mod kafka;
mod topics;

pub use envelope::EventEnvelope;
pub use error::EventError;
pub use kafka::{EventConsumer, EventProducer};
pub use topics::{EventTopic, SCHEMA_VERSION};
