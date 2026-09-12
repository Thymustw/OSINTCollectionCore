//! 從**外部**查一個 consumer group 的 lag，給 `GET /api/v1/ops/queues` 用。
//!
//! # 為什麼不重用 [`crate::EventConsumer::consumer_lag`]
//!
//! 那個方法量的是「**我自己**這個 consumer 落後多少」，前提是呼叫者已經
//! subscribe 並被指派到 partition。Operations Center 不是 pipeline 的一員：
//! 它若用 `osint-normalizer` 這個 group id 去 subscribe，就會**加入那個 group**，
//! 觸發 rebalance 並從真正的 normalizer 手上搶走 partition。
//! 「打開監控頁面就讓正規化停一下」是不能接受的。
//!
//! 所以這裡走的是 `kafka-consumer-groups` 那條路：建一個帶 group id 但
//! **不 subscribe、不 assign** 的 consumer（不 subscribe 就不會加入 group），
//! 只問兩件事——
//!
//! 1. `committed_offsets`：這個 group 提交到哪裡了
//! 2. `fetch_watermarks`：partition 現在的 low／high watermark
//!
//! lag = high − committed。
//!
//! # 還沒有 committed offset 時
//!
//! 回 [`rdkafka::Offset::Invalid`]。這時 lag 用 `high − low`（整個仍保留的 backlog），
//! 因為所有 consumer 都設 `auto.offset.reset=earliest`（見 `kafka.rs`），
//! 一旦它上線就要從最舊的一筆開始追。把這種情況當成 lag=0 會讓「consumer 從來沒
//! 起來過」看起來跟「完全追上了」一模一樣——那正是最需要被看見的狀態。

use std::time::Duration;

use rdkafka::TopicPartitionList;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::topic_partition_list::Offset;
use rdkafka::types::RDKafkaErrorCode;
use serde::Serialize;

use crate::error::EventError;

/// 單一 partition 的落後量。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PartitionLag {
    pub partition: i32,
    /// 這個 group 已提交的 offset。`None` = 從未提交過（見模組說明）。
    pub committed: Option<i64>,
    pub low_watermark: i64,
    pub high_watermark: i64,
    pub lag: u64,
}

/// 一個 (group, topic) 的落後量。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GroupLag {
    pub group: String,
    pub topic: String,
    /// topic 是否存在。**不存在不是錯誤**：pipeline 還沒發過第一則事件時
    /// topic 本來就不存在，那時 lag 是 0 而不是「查不到」。
    pub topic_exists: bool,
    pub total_lag: u64,
    pub partitions: Vec<PartitionLag>,
}

/// 查 consumer group lag 的探針。每次查詢建一個短命的 client。
///
/// # 為什麼不快取 client
///
/// `BaseConsumer` 需要有人定期 `poll()` 才會消化內部事件佇列；一個放著不動的
/// 長命 client 在 broker 斷線時會累積事件。ops endpoint 的呼叫頻率很低
/// （人看頁面、Prometheus 抓取），省下的連線建立成本不值得換一個要維護生命週期的物件。
#[derive(Debug, Clone)]
pub struct GroupLagProbe {
    brokers: String,
    timeout: Duration,
}

impl GroupLagProbe {
    pub fn new(brokers: impl Into<String>, timeout: Duration) -> Result<Self, EventError> {
        let brokers = brokers.into();
        if brokers.trim().is_empty() {
            return Err(EventError::Configuration {
                message: "broker 位址是空的。請設 REDPANDA_BROKERS 或 config [broker].brokers"
                    .into(),
            });
        }
        Ok(Self { brokers, timeout })
    }

    #[must_use]
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// 查 `group` 在 `topic` 上的 lag。
    ///
    /// **同步阻塞**（librdkafka 的 metadata／watermark 查詢都是阻塞呼叫）。
    /// runtime flavor 的處理與 [`crate::EventConsumer::consumer_lag`] 相同：
    /// multi-thread runtime 用 `block_in_place` 把執行緒還給 runtime，
    /// current-thread（`#[tokio::test]` 預設）直接跑——在那裡呼叫 `block_in_place`
    /// 會 panic。
    pub fn group_lag(&self, group: &str, topic: &str) -> Result<GroupLag, EventError> {
        use tokio::runtime::{Handle, RuntimeFlavor};
        let compute = || self.group_lag_blocking(group, topic);
        match Handle::try_current().map(|handle| handle.runtime_flavor()) {
            Ok(RuntimeFlavor::MultiThread) => tokio::task::block_in_place(compute),
            _ => compute(),
        }
    }

    fn group_lag_blocking(&self, group: &str, topic: &str) -> Result<GroupLag, EventError> {
        if group.trim().is_empty() {
            return Err(EventError::Configuration {
                message: "consumer group 不可為空".into(),
            });
        }
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            // **不可以是 true**：只是想看 lag 的動作不該把 topic 生出來。
            .set("allow.auto.create.topics", "false")
            .create()
            .map_err(|err| EventError::ConsumerCreate {
                message: format!("建立 lag 探針（group={group}）失敗：{err}"),
            })?;

        let metadata = consumer
            .fetch_metadata(Some(topic), self.timeout)
            .map_err(|err| EventError::Commit {
                message: format!("查詢 {topic} 的 metadata 失敗：{err}"),
            })?;
        let Some(meta_topic) = metadata.topics().iter().find(|t| t.name() == topic) else {
            return Ok(GroupLag::absent(group, topic));
        };
        if let Some(code) = meta_topic.error() {
            // 尚未建立的 topic 會回這兩個碼。那是「還沒有人發過事件」，不是故障。
            if matches!(
                RDKafkaErrorCode::from(code),
                RDKafkaErrorCode::UnknownTopic | RDKafkaErrorCode::UnknownTopicOrPartition
            ) {
                return Ok(GroupLag::absent(group, topic));
            }
            return Err(EventError::Commit {
                message: format!("{topic} 的 metadata 回報錯誤碼 {code:?}"),
            });
        }
        if meta_topic.partitions().is_empty() {
            return Ok(GroupLag::absent(group, topic));
        }

        let mut wanted = TopicPartitionList::new();
        for partition in meta_topic.partitions() {
            wanted
                .add_partition_offset(topic, partition.id(), Offset::Invalid)
                .map_err(|err| EventError::Commit {
                    message: format!("組 {topic}[{}] 的查詢清單失敗：{err}", partition.id()),
                })?;
        }
        let committed = consumer
            .committed_offsets(wanted, self.timeout)
            .map_err(|err| EventError::Commit {
                message: format!(
                    "查詢 group={group} 在 {topic} 的 committed offset 失敗：{err}。\
                     請確認 Redpanda 在跑，以及 [broker].brokers 設定正確"
                ),
            })?;

        let mut partitions = Vec::with_capacity(meta_topic.partitions().len());
        let mut total_lag = 0_u64;
        for partition in meta_topic.partitions() {
            let id = partition.id();
            let (low, high) =
                consumer
                    .fetch_watermarks(topic, id, self.timeout)
                    .map_err(|err| EventError::Commit {
                        message: format!("查詢 {topic}[{id}] 的 watermark 失敗：{err}"),
                    })?;
            let committed_offset =
                committed
                    .find_partition(topic, id)
                    .and_then(|p| match p.offset() {
                        Offset::Offset(value) => Some(value),
                        _ => None,
                    });
            // 沒有 committed offset → 從 low 開始算（consumer 是 earliest）。
            let position = committed_offset.unwrap_or(low);
            let lag = high.saturating_sub(position).max(0) as u64;
            total_lag = total_lag.saturating_add(lag);
            partitions.push(PartitionLag {
                partition: id,
                committed: committed_offset,
                low_watermark: low,
                high_watermark: high,
                lag,
            });
        }

        Ok(GroupLag {
            group: group.to_string(),
            topic: topic.to_string(),
            topic_exists: true,
            total_lag,
            partitions,
        })
    }
}

impl GroupLag {
    fn absent(group: &str, topic: &str) -> Self {
        Self {
            group: group.to_string(),
            topic: topic.to_string(),
            topic_exists: false,
            total_lag: 0,
            partitions: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_brokers_fail_closed() {
        match GroupLagProbe::new("  ", Duration::from_secs(1)) {
            Err(EventError::Configuration { .. }) => {}
            other => panic!("空 broker 不該建得起來，實際 {other:?}"),
        }
    }

    #[test]
    fn empty_group_fail_closed() {
        let probe = GroupLagProbe::new("127.0.0.1:9092", Duration::from_secs(1)).expect("probe");
        match probe.group_lag("", "raw.collected") {
            Err(EventError::Configuration { .. }) => {}
            other => panic!("空 group 不該查得動，實際 {other:?}"),
        }
    }

    #[test]
    fn absent_topic_is_zero_lag_not_an_error() {
        let lag = GroupLag::absent("osint-normalizer", "raw.collected");
        assert!(!lag.topic_exists);
        assert_eq!(lag.total_lag, 0);
        assert!(lag.partitions.is_empty());
    }
}
