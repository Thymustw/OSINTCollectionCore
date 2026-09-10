//! 對本機真實 Redpanda produce／consume 一次。
//!
//! 用一次性 topic，不要寫進共用的 `job.dispatched`：unique group + earliest
//! 會把歷史訊息先讀出來，斷言就對到上一輪的 envelope。

use std::time::Duration;

use core_events::{EventConsumer, EventEnvelope, EventProducer, EventTopic};
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use uuid::Uuid;

#[tokio::test]
async fn produce_and_consume_job_dispatched() {
    load_workspace_dotenv();
    let brokers = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        brokers.contains("127.0.0.1") || brokers.contains("localhost"),
        "conformance 只連本機 Redpanda，實際 brokers={brokers}"
    );

    let probe_id = Uuid::now_v7();
    let topic_name = format!("osint.conformance.{probe_id}");
    let group = format!("osint-core-events-conformance-{probe_id}");
    let consumer = EventConsumer::connect(&brokers, &group, &[topic_name.as_str()])
        .expect("建立 consumer。請確認 osint-core-redpanda-1 在跑（埠 9092）");
    // 等 group join。Redpanda 在本機通常 <1s，給 2s 緩衝。
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer = EventProducer::connect(&brokers, "core-events-test").expect("建立 producer");
    let envelope = EventEnvelope::new(
        EventTopic::JobDispatched,
        "core-events-test",
        Some(probe_id),
        json!({"probe": "phase2-events", "id": probe_id.to_string()}),
    );
    producer
        .publish_to_topic(&topic_name, Some(&probe_id.to_string()), &envelope)
        .await
        .expect("produce。請確認 Redpanda healthy：docker compose ps");

    let received = consumer
        .next_envelope(Duration::from_secs(15))
        .await
        .expect("consume 逾時。請確認 topic 可自動建立，且 consumer 已加入 group");

    assert_eq!(received.id, envelope.id);
    assert_eq!(received.event_type, envelope.event_type);
    assert_eq!(received.schema_version, "1");
    assert_eq!(received.payload, envelope.payload);
    assert_eq!(received.correlation_id, Some(probe_id));
}
