//! Redpanda producer／consumer。librdkafka 走 mklove（不開 cmake-build／ssl-vendored）。

use std::sync::Mutex;
use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
use rdkafka::message::{Message, ToBytes};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::topic_partition_list::Offset;
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

    /// 抓一次 cluster metadata，回 `(broker 數, topic 數)`。連不上時回 [`EventError::Produce`]。
    ///
    /// librdkafka 的 `fetch_metadata` 是同步阻塞呼叫，直接在 Tokio executor thread 上跑
    /// 會擋住整個 worker（連不上時會卡滿 `timeout`）。所以丟進 `spawn_blocking`。
    pub async fn cluster_metadata(&self, timeout: Duration) -> Result<(usize, usize), EventError> {
        let client = self.inner.clone();
        let result = tokio::task::spawn_blocking(move || {
            client
                .client()
                .fetch_metadata(None, timeout)
                .map(|md| (md.brokers().len(), md.topics().len()))
        })
        .await
        .map_err(|err| EventError::Produce {
            topic: "(metadata)".into(),
            message: format!("metadata 查詢任務中斷：{err}"),
        })?;
        result.map_err(|err| EventError::Produce {
            topic: "(metadata)".into(),
            message: err.to_string(),
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
///
/// `enable.auto.commit=false`：呼叫端處理完後自己 [`Self::commit_last`]。
pub struct EventConsumer {
    inner: StreamConsumer,
    last: Mutex<Option<TopicPartitionList>>,
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
        Ok(Self {
            inner,
            last: Mutex::new(None),
        })
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
                    let envelope = serde_json::from_slice(payload).map_err(|err| {
                        EventError::InvalidEnvelope {
                            message: err.to_string(),
                        }
                    })?;
                    remember_offset(&self.last, msg.topic(), msg.partition(), msg.offset());
                    return Ok(envelope);
                }
            }
        }
    }

    /// 這個 consumer group 在已指派 partition 上的總 lag（high watermark − 目前位置）。
    ///
    /// # 給誰用
    ///
    /// 高吞吐 consumer **自己**的 backpressure 判斷（CLAUDE.md §6）。也寫進
    /// `osint_queue_depth` gauge 曝露出去。
    ///
    /// V0.1 **沒有**「上游讀這個 gauge 自動降速」的機制：collector 不會讀它
    /// （見 `docs/developer/indexer.md`）。gauge 現在的用途是給人／Prometheus 看。
    ///
    /// # 為什麼要處理 runtime flavor
    ///
    /// librdkafka 的 `fetch_watermarks` 是**同步阻塞**呼叫（會發一次網路請求）。
    /// 直接在 Tokio executor thread 上跑會擋住同一條 thread 上的所有 task，
    /// 包含 health endpoint。`block_in_place` 可以把當前 thread 交還給 runtime，
    /// 但它在 current-thread runtime 上會 **panic**——`#[tokio::test]` 預設就是
    /// current-thread，所以必須先問 runtime 是哪一種。
    ///
    /// 尚未指派到任何 partition（剛啟動、還在 rebalance）時回 `Ok(0)`：
    /// 那不是錯誤，而是「還沒有東西可以落後」。
    pub fn consumer_lag(&self, timeout: Duration) -> Result<u64, EventError> {
        use tokio::runtime::{Handle, RuntimeFlavor};
        let compute = || self.consumer_lag_blocking(timeout);
        match Handle::try_current().map(|handle| handle.runtime_flavor()) {
            Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(compute),
            _ => compute(),
        }
    }

    fn consumer_lag_blocking(&self, timeout: Duration) -> Result<u64, EventError> {
        let assignment = self.inner.assignment().map_err(|err| EventError::Commit {
            message: format!("查詢 partition 指派失敗：{err}"),
        })?;
        let positions = self.inner.position().map_err(|err| EventError::Commit {
            message: format!("查詢 consumer 位置失敗：{err}"),
        })?;
        let mut lag = 0_u64;
        for element in assignment.elements() {
            let (_, high) = self
                .inner
                .fetch_watermarks(element.topic(), element.partition(), timeout)
                .map_err(|err| EventError::Commit {
                    message: format!(
                        "查詢 {}[{}] 的 watermark 失敗：{err}",
                        element.topic(),
                        element.partition()
                    ),
                })?;
            let current = positions
                .find_partition(element.topic(), element.partition())
                .and_then(|p| match p.offset() {
                    Offset::Offset(value) => Some(value),
                    // Invalid／Beginning／Stored 都代表「還沒讀過任何東西」，
                    // 這時 lag 就是整個 partition 的長度。用 0 當起點。
                    _ => None,
                })
                .unwrap_or(0);
            lag = lag.saturating_add(high.saturating_sub(current).max(0) as u64);
        }
        Ok(lag)
    }

    /// 提交上一則成功解析的 envelope 的 offset（offset+1）。沒有上一則時是 no-op。
    pub fn commit_last(&self) -> Result<(), EventError> {
        let stored = self
            .last
            .lock()
            .map_err(|_| EventError::Commit {
                message: "consumer offset lock 被毒化。請重啟行程".into(),
            })?
            .clone();
        let Some(list) = stored else {
            return Ok(());
        };
        self.inner
            .commit(&list, CommitMode::Sync)
            .map_err(|err| EventError::Commit {
                message: err.to_string(),
            })
    }

    /// 上一則成功解析的 envelope 的座標 `(topic, partition, offset)`。
    ///
    /// `next_envelope` 呼叫成功後才會有值；[`Self::commit_last`] 用的是同一份
    /// `last` 資料，這裡只是把它變成可讀，給失敗分支組 `FailedEvent` 用
    /// （ADR-008：錯誤分支要記錄是哪一則事件處理失敗）。
    ///
    /// `remember_offset` 存的是「下一筆要讀」的 offset（Kafka 慣例，offset+1），
    /// 這裡要還原成「剛剛處理的那一則」的真實 offset，所以要減 1。
    ///
    /// 沒有上一則（尚未成功讀到任何 envelope）或 lock 被毒化時回 `None`。
    pub fn last_coordinates(&self) -> Option<(String, i32, i64)> {
        // `remember_offset` 每次寫入的都是「只有一筆」的 list，所以空 list 等於
        // 「沒有上一則」。lock 被毒化也回 None，不要把失敗當成 panic。
        match self.last.lock() {
            Ok(guard) => first_coordinates(&guard.clone().unwrap_or(TopicPartitionList::new())),
            Err(_) => None,
        }
    }
}

fn remember_offset(
    last: &Mutex<Option<TopicPartitionList>>,
    topic: &str,
    partition: i32,
    offset: i64,
) {
    let mut list = TopicPartitionList::new();
    // Kafka 約定：提交的是「下一筆要讀」的 offset。
    let next = Offset::Offset(offset.saturating_add(1));
    if list.add_partition_offset(topic, partition, next).is_err() {
        return;
    }
    if let Ok(mut guard) = last.lock() {
        *guard = Some(list);
    }
}

/// 從 `TopicPartitionList` 取出第一筆的 `(topic, partition, offset)`。
///
/// `remember_offset` 每次都建一份全新、只有一筆的 list，所以取 elements 第一筆。
/// `offset` 減 1：`remember_offset` 存的是「下一筆要讀」的 offset（Kafka 慣例），
/// 這裡要還原成「剛剛處理的那一則」的真實 offset。
///
/// `Offset` 除了 `Offset::Offset(_)` 之外都是「沒有數值的記號」；理論上不會出現
/// （`remember_offset` 自己寫入的一定是 `Offset::Offset`），但不要 `unwrap`，
/// 用 match 對其他分支回 `None`。
fn first_coordinates(list: &TopicPartitionList) -> Option<(String, i32, i64)> {
    if let Some(element) = list.elements().into_iter().next() {
        let topic = element.topic();
        let partition = element.partition();
        match element.offset() {
            Offset::Offset(value) => {
                return Some((topic.to_string(), partition, value.saturating_sub(1)));
            }
            // Invalid／Beginning／End／Stored／OffsetTail 沒有數值，回 None。
            _ => return None,
        }
    }
    None
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

    #[test]
    fn remember_offset_stores_next_and_first_coordinates_restores_real_offset() {
        // 不連 broker：直接測 `remember_offset` 寫入 + `first_coordinates` 還原
        // 這段座標邏輯本身。`remember_offset` 存「下一筆要讀」的 offset（41 + 1），
        // `first_coordinates` 要還原成「剛剛處理的那一則」的真實 offset（41）。
        let last = Mutex::new(None);
        remember_offset(&last, "raw.collected", 3, 41);
        let Ok(guard) = last.lock() else {
            panic!("offset lock 不該被毒化")
        };
        let Some(list) = guard.clone() else {
            panic!("remember_offset 應該寫入座標")
        };
        let Some((topic, partition, offset)) = first_coordinates(&list) else {
            panic!("Offset::Offset 應該有數值")
        };
        assert_eq!(topic, "raw.collected");
        assert_eq!(partition, 3);
        assert_eq!(offset, 41);
    }

    #[test]
    fn empty_list_has_no_coordinates() {
        // 尚未成功讀到任何 envelope 時，記憶體裡沒有座標，回 None 而不是 panic。
        let list = TopicPartitionList::new();
        assert_eq!(first_coordinates(&list), None);
    }
}
