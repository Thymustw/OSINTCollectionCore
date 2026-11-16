//! 對本機真實 Redpanda produce／consume 一次，並驗證 [`TestTopics`] 的隔離與清理。
//!
//! # 一次性 topic 一定要刪
//!
//! 2026-10-15 實測：這個測試原本從不刪自己的 topic，累積了 119 個
//! `osint.conformance.*`（144 個 partition）。本機 Redpanda 用 `--memory 256M`
//! 啟動，partition 數量受記憶體限制，碰到上限後**任何新 topic 都建不出來**：
//! `rpk topic create` 回 `INVALID_PARTITIONS: Can not increase partition count due
//! to memory limit`，而 producer 端只看得到 `MessageTimedOut`——錯誤訊息完全不透露
//! 原因，這個測試因此被當成「既有環境問題」好幾週。正式服務的 topic 也受同一個上限。
//!
//! 2026-11-17 起改用 [`TestTopics`]：namespace `osinttest-<uuid>`，drop 時刪 topic 與 group，
//! 建立時清掃逾時的殘留。舊的 `osint.conformance.*` 前綴已不再產生。

use std::time::Duration;

use core_events::{EventEnvelope, EventTopic, TEST_TOPIC_PREFIX, TestTopics};
use rdkafka::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use uuid::Uuid;

fn brokers() -> String {
    load_workspace_dotenv();
    let brokers = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        brokers.contains("127.0.0.1") || brokers.contains("localhost"),
        "conformance 只連本機 Redpanda，實際 brokers={brokers}"
    );
    brokers
}

/// broker 上現有的 topic 與 group 名稱（不 subscribe，不會加入任何 group）。
fn broker_names(brokers: &str) -> (Vec<String>, Vec<String>) {
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("建立查詢 client");
    let topics = client
        .fetch_metadata(None, Duration::from_secs(10))
        .expect("讀 topic 清單")
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect();
    let groups = client
        .fetch_group_list(None, Duration::from_secs(10))
        .expect("讀 group 清單")
        .groups()
        .iter()
        .map(|g| g.name().to_string())
        .collect();
    (topics, groups)
}

#[tokio::test]
async fn produce_and_consume_job_dispatched() {
    let brokers = brokers();
    let topics = TestTopics::create(&brokers);
    let probe_id = Uuid::now_v7();

    let consumer = topics
        .consumer("roundtrip", &[EventTopic::JobDispatched.as_str()])
        .expect("建立 consumer。請確認 osint-core-redpanda-1 在跑（埠 9092）");
    // 等 group join。Redpanda 在本機通常 <1s，給 2s 緩衝。
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer = topics.producer("core-events-test").expect("建立 producer");
    let envelope = producer
        .publish(
            EventTopic::JobDispatched,
            Some(&probe_id.to_string()),
            Some(probe_id),
            json!({"probe": "phase2-events", "id": probe_id.to_string()}),
        )
        .await
        .expect(
            "produce。請確認 Redpanda healthy：docker compose ps。若 healthy 卻一直 \
             MessageTimedOut，用 `docker exec osint-core-redpanda-1 rpk topic create x` \
             看是否回 INVALID_PARTITIONS（partition 數碰到記憶體上限，見檔頭說明）",
        );

    let received: EventEnvelope = consumer
        .next_envelope(Duration::from_secs(15))
        .await
        .expect("consume 逾時。請確認 topic 可自動建立，且 consumer 已加入 group");

    assert_eq!(received.id, envelope.id);
    // envelope 與座標都是邏輯名稱：namespace 不外洩到 event_type／failed_events.topic。
    assert_eq!(received.event_type, "job.dispatched");
    assert_eq!(received.schema_version, "1");
    assert_eq!(received.payload, envelope.payload);
    assert_eq!(received.correlation_id, Some(probe_id));
    let (topic, _, _) = consumer.last_coordinates().expect("座標");
    assert_eq!(topic, "job.dispatched");
    consumer.commit_last().expect("commit");

    // 真的落在 namespace 底下，而不是共用的 job.dispatched。
    let broker_topic = topics.broker_topic("job.dispatched");
    let (names, _) = broker_names(&brokers);
    assert!(
        names.contains(&broker_topic),
        "broker 上應該有 `{broker_topic}`"
    );

    // lag 探針也走 namespace：剛 commit 完，lag 為 0 且 topic 存在。
    let lag = topics
        .lag_probe(Duration::from_secs(5))
        .expect("probe")
        .group_lag(&topics.group("roundtrip"), "job.dispatched")
        .expect("lag");
    assert!(lag.topic_exists, "namespace 下的 topic 應該存在");
    assert_eq!(lag.topic, "job.dispatched");
    assert_eq!(lag.total_lag, 0);
}

/// 共用的 `job.dispatched` 上一則都沒多：測試事件不會被執行中的服務收到。
#[tokio::test]
async fn namespaced_publish_does_not_touch_shared_topic() {
    let brokers = brokers();
    let watermark = |client: &BaseConsumer| {
        client
            .fetch_watermarks("job.dispatched", 0, Duration::from_secs(5))
            .map(|(_, high)| high)
            .unwrap_or(0)
    };
    let client: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("client");
    let before = watermark(&client);

    let topics = TestTopics::create(&brokers);
    let producer = topics.producer("core-events-test").expect("producer");
    producer
        .publish(
            EventTopic::JobDispatched,
            None,
            None,
            json!({"probe": "isolation"}),
        )
        .await
        .expect("produce");

    assert_eq!(
        watermark(&client),
        before,
        "namespaced publish 不該寫進共用的 job.dispatched"
    );
}

/// drop 後 namespace 的 topic 與 group 都不在 broker 上。
#[tokio::test]
async fn dropping_test_topics_deletes_topics_and_groups() {
    let brokers = brokers();
    let topics = TestTopics::create(&brokers);
    let prefix = topics.namespace().as_str().to_string();
    {
        let consumer = topics
            .consumer("cleanup", &[EventTopic::RawCollected.as_str()])
            .expect("consumer");
        tokio::time::sleep(Duration::from_millis(1500)).await;
        topics
            .producer("core-events-test")
            .expect("producer")
            .publish(
                EventTopic::RawCollected,
                None,
                None,
                json!({"probe": "cleanup"}),
            )
            .await
            .expect("produce");
        consumer
            .next_envelope(Duration::from_secs(15))
            .await
            .expect("consume");
        consumer.commit_last().expect("commit");
        // consumer 在這裡 drop：離開 group，group 變 Empty 才刪得掉。
    }
    let (names, groups) = broker_names(&brokers);
    assert!(
        names.iter().any(|n| n.starts_with(&prefix)),
        "drop 前 topic 應該存在"
    );
    assert!(
        groups.iter().any(|g| g.starts_with(&prefix)),
        "drop 前 group 應該存在"
    );

    drop(topics);
    let (names, groups) = broker_names(&brokers);
    let left_topics: Vec<_> = names.iter().filter(|n| n.starts_with(&prefix)).collect();
    let left_groups: Vec<_> = groups.iter().filter(|g| g.starts_with(&prefix)).collect();
    assert!(left_topics.is_empty(), "drop 後仍有 topic：{left_topics:?}");
    assert!(left_groups.is_empty(), "drop 後仍有 group：{left_groups:?}");
    assert!(prefix.starts_with(TEST_TOPIC_PREFIX));
}
