//! 對本機真實 Redpanda produce／consume 一次。
//!
//! 用一次性 topic，不要寫進共用的 `job.dispatched`：unique group + earliest
//! 會把歷史訊息先讀出來，斷言就對到上一輪的 envelope。
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
//! 所以：開頭先清掉所有殘留的 `osint.conformance.*`（上一次 panic 沒走到結尾也會被
//! 收掉），結尾再刪掉這次的 topic。不放在 Drop 裡，因為刪除是 async。

use std::time::Duration;

use core_events::{EventConsumer, EventEnvelope, EventProducer, EventTopic};
use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use serde_json::json;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use uuid::Uuid;

const PROBE_PREFIX: &str = "osint.conformance.";

fn admin(brokers: &str) -> AdminClient<DefaultClientContext> {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .expect("建立 AdminClient")
}

/// 刪掉指定的一次性 topic。只接受 `osint.conformance.` 開頭的名字——
/// 防止哪天有人把正式 topic 傳進來。
async fn delete_probe_topics(admin: &AdminClient<DefaultClientContext>, topics: &[&str]) {
    assert!(
        topics.iter().all(|t| t.starts_with(PROBE_PREFIX)),
        "只能刪 {PROBE_PREFIX}* 的一次性 topic：{topics:?}"
    );
    if topics.is_empty() {
        return;
    }
    let opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(10)));
    let results = admin
        .delete_topics(topics, &opts)
        .await
        .expect("delete_topics 呼叫失敗");
    for result in results {
        if let Err((topic, code)) = result {
            // 已經不存在不算失敗（兩個測試行程同時清理時會發生）。
            assert!(
                code == rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition,
                "刪除一次性 topic `{topic}` 失敗：{code:?}"
            );
        }
    }
}

/// 清掉之前留下的 `osint.conformance.*`（上一次 panic 沒走到結尾時會殘留）。
async fn purge_leftover_probe_topics(admin: &AdminClient<DefaultClientContext>) {
    let metadata = admin
        .inner()
        .fetch_metadata(None, Duration::from_secs(10))
        .expect("讀取 topic 清單");
    let leftovers: Vec<String> = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .filter(|name| name.starts_with(PROBE_PREFIX))
        .collect();
    let refs: Vec<&str> = leftovers.iter().map(String::as_str).collect();
    delete_probe_topics(admin, &refs).await;
}

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

    let admin = admin(&brokers);
    purge_leftover_probe_topics(&admin).await;

    let probe_id = Uuid::now_v7();
    let topic_name = format!("{PROBE_PREFIX}{probe_id}");
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
        .expect(
            "produce。請確認 Redpanda healthy：docker compose ps。若 healthy 卻一直 \
             MessageTimedOut，用 `docker exec osint-core-redpanda-1 rpk topic create x` \
             看是否回 INVALID_PARTITIONS（partition 數碰到記憶體上限，見檔頭說明）",
        );

    let received = consumer
        .next_envelope(Duration::from_secs(15))
        .await
        .expect("consume 逾時。請確認 topic 可自動建立，且 consumer 已加入 group");

    assert_eq!(received.id, envelope.id);
    assert_eq!(received.event_type, envelope.event_type);
    assert_eq!(received.schema_version, "1");
    assert_eq!(received.payload, envelope.payload);
    assert_eq!(received.correlation_id, Some(probe_id));

    drop(consumer);
    delete_probe_topics(&admin, &[topic_name.as_str()]).await;
}
