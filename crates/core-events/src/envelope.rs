//! Versioned event envelope。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::topics::{EventTopic, SCHEMA_VERSION};

/// Event envelope v1。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub id: Uuid,
    pub event_type: String,
    pub schema_version: String,
    pub source_service: String,
    pub timestamp: DateTime<Utc>,
    pub correlation_id: Option<Uuid>,
    pub payload: Value,
}

impl EventEnvelope {
    #[must_use]
    pub fn new(
        topic: EventTopic,
        source_service: impl Into<String>,
        correlation_id: Option<Uuid>,
        payload: Value,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            event_type: topic.as_str().to_string(),
            schema_version: SCHEMA_VERSION.to_string(),
            source_service: source_service.into(),
            timestamp: Utc::now(),
            correlation_id,
            payload,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn round_trip() {
        let original = EventEnvelope::new(
            EventTopic::JobDispatched,
            "core-jobs",
            Some(Uuid::now_v7()),
            json!({"job_type": "collect"}),
        );
        let encoded = serde_json::to_string(&original).unwrap();
        let back: EventEnvelope = serde_json::from_str(&encoded).unwrap();
        assert_eq!(original, back);
        assert_eq!(back.schema_version, "1");
        assert_eq!(back.event_type, "job.dispatched");
    }
}
