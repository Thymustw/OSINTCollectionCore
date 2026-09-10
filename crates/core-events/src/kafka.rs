//! Redpanda producer／consumer。librdkafka 走 mklove（不開 cmake-build／ssl-vendored）。

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde_json::Value;

use crate::envelope::EventEnvelope;
use crate::error::EventError;
use crate::topics::EventTopic;

/// 有界 produce。`queue.buffering.max.messages` 避免無界堆積。
pub struct EventProducer {
    inner: FutureProducer,
    source_service: String,
}

impl EventProducer {
    pub fn connect(brokers: &str, source_service: impl Into<String>) -> Result<Self, EventError> {
        if brokers.trim().is_empty() {
            return Err(EventError::Configuration {
                message: "broker 位址是空的。請設 REDPANDA_BROKERS 或 config [broker].brokers"
                    .into(),
            });
        }
        let inner: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "10000")
            .set("queue.buffering.max.messages", "10000")
            .set("queue.buffering.max.kbytes", "16384")
            .set("acks", "all")
            .create()
            .map_err(|err| EventError::ProducerCreate {
                message: err.to_string(),
            })?;
        Ok(Self {
            inner,
            source_service: source_service.into(),
        })
    }

    pub async fn publish(
        &self,
        topic: EventTopic,
        partition_key: Option<&str>,
        correlation_id: Option<uuid::Uuid>,
        payload: Value,
    ) -> Result<EventEnvelope, EventError> {
        let envelope =
            EventEnvelope::new(topic, self.source_service.clone(), correlation_id, payload);
        self.publish_envelope(topic, partition_key, &envelope)
            .await?;
        Ok(envelope)
    }

    pub async fn publish_envelope(
        &self,
        topic: EventTopic,
        partition_key: Option<&str>,
        envelope: &EventEnvelope,
    ) -> Result<(), EventError> {
        self.publish_to_topic(topic.as_str(), partition_key, envelope)
            .await
    }

    /// 送到任意 topic 名。conformance 用一次性 topic，避免吃到歷史 `job.dispatched`。
    pub async fn publish_to_topic(
        &self,
        topic_name: &str,
        partition_key: Option<&str>,
        envelope: &EventEnvelope,
    ) -> Result<(), EventError> {
        if topic_name.trim().is_empty() {
            return Err(EventError::Configuration {
                message: "topic 名稱不可為空".into(),
            });
        }
        let body = serde_json::to_vec(envelope)?;
        let mut record = FutureRecord::to(topic_name).payload(&body);
        let key_owned: Option<String> = partition_key.map(str::to_string);
        if let Some(ref key) = key_owned {
            record = record.key(key.to_bytes());
        }
        self.inner
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(err, _)| EventError::Produce {
                topic: topic_name.to_string(),
                message: err.to_string(),
            })?;
        Ok(())
    }
}

/// 單一 consumer group 的 StreamConsumer 封裝。
pub struct EventConsumer {
    inner: StreamConsumer,
}

impl EventConsumer {
    pub fn connect(brokers: &str, group_id: &str, topics: &[&str]) -> Result<Self, EventError> {
        if brokers.trim().is_empty() {
            return Err(EventError::Configuration {
                message: "broker 位址是空的。請設 REDPANDA_BROKERS".into(),
            });
        }
        if group_id.trim().is_empty() {
            return Err(EventError::Configuration {
                message: "consumer group 不可為空".into(),
            });
        }
        let inner: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("group.id", group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "10000")
            // 新 topic 第一次 subscribe 時 broker 可能還沒建好；metadata 刷新後才能 recv。
            .set("allow.auto.create.topics", "true")
            .set("topic.metadata.refresh.interval.ms", "1000")
            .create()
            .map_err(|err| EventError::ConsumerCreate {
                message: err.to_string(),
            })?;
        inner
            .subscribe(topics)
            .map_err(|err| EventError::ConsumerCreate {
                message: format!("subscribe {topics:?} 失敗：{err}"),
            })?;
        Ok(Self { inner })
    }

    /// 等到下一則合法 envelope，或逾時。
    ///
    /// Topic 尚未建立時 librdkafka 會回 `UnknownTopicOrPartition`；這是暫時性錯誤，
    /// 在逾時內重試，不要立刻當成壞 envelope。
    pub async fn next_envelope(&self, timeout: Duration) -> Result<EventEnvelope, EventError> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(EventError::ConsumeTimeout {
                    topic: "(subscribed)".into(),
                    timeout_ms: timeout.as_millis() as u64,
                });
            }
            match tokio::time::timeout(remaining, self.inner.recv()).await {
                Err(_) => {
                    return Err(EventError::ConsumeTimeout {
                        topic: "(subscribed)".into(),
                        timeout_ms: timeout.as_millis() as u64,
                    });
                }
                Ok(Err(err)) => {
                    let text = err.to_string();
                    if is_temporary_consume_error(&text) {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                        continue;
                    }
                    return Err(EventError::InvalidEnvelope {
                        message: format!("rdkafka recv 失敗：{err}"),
                    });
                }
                Ok(Ok(msg)) => {
                    let payload = msg.payload().ok_or_else(|| EventError::InvalidEnvelope {
                        message: "訊息沒有 payload".into(),
                    })?;
                    return serde_json::from_slice(payload).map_err(|err| {
                        EventError::InvalidEnvelope {
                            message: err.to_string(),
                        }
                    });
                }
            }
        }
    }
}

fn is_temporary_consume_error(text: &str) -> bool {
    text.contains("UnknownTopicOrPartition")
        || text.contains("unknown topic")
        || text.contains("Unknown topic or partition")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_brokers_fail_closed() {
        match EventProducer::connect("", "core-events") {
            Err(EventError::Configuration { .. }) => {}
            Ok(_) => panic!("空 broker 不該連成功"),
            Err(err) => panic!("預期 Configuration，得到 {err}"),
        }
    }
}
