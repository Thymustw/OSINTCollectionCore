//! 對本機 Redpanda 驗證 merge／undo 會發 `relationship.changed`。
//!
//! SQLite 當 canonical store（不碰共用 Postgres），broker 用本機容器。
//! 每個測試開自己的 sqlite 檔，跑完刪掉。

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use core_events::{EventConsumer, EventProducer, EventTopic};
use core_model::{Entity, EntityId, EntityType, Relationship, RelationshipType};
use merge::MergeService;
use serde_json::json;
use storage_core::RelationalStore;
use storage_core::conformance::{find_workspace_root, load_workspace_dotenv, required_env};
use storage_sqlite::SqliteEmbeddedStore;
use uuid::Uuid;

fn ts() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 13, 9, 0, 0).unwrap()
}

fn entity(name: &str, entity_type: EntityType) -> Entity {
    Entity {
        id: Uuid::now_v7(),
        entity_type,
        name: name.into(),
        normalized_name: name.to_ascii_lowercase(),
        description: None,
        confidence: 0.9,
        first_seen: ts(),
        last_seen: ts(),
        merged_into: None,
        attributes: json!({}),
    }
}

fn relationship(
    source: EntityId,
    ty: RelationshipType,
    target: EntityId,
    evidence_count: i32,
    confidence: f64,
) -> Relationship {
    Relationship {
        id: Uuid::now_v7(),
        source_object_id: source,
        relationship_type: ty,
        target_object_id: target,
        confidence,
        first_seen: ts(),
        last_seen: ts(),
        evidence_count,
        created_at: ts(),
        updated_at: ts(),
    }
}

struct Harness {
    service: MergeService<SqliteEmbeddedStore>,
    db: SqliteEmbeddedStore,
    path: PathBuf,
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
    }
}

fn brokers() -> String {
    load_workspace_dotenv();
    let brokers = required_env("REDPANDA_BROKERS")
        .or_else(|_| required_env("OSINT__BROKER__BROKERS"))
        .unwrap_or_else(|_| "127.0.0.1:9092".into());
    assert!(
        brokers.contains("127.0.0.1") || brokers.contains("localhost"),
        "e2e 只連本機 Redpanda，實際 brokers={brokers}"
    );
    brokers
}

async fn open_harness(producer: Arc<EventProducer>) -> Harness {
    let root = find_workspace_root().expect("workspace root");
    let path: PathBuf = root.join(format!("var/osint-merge-evt-{}.sqlite", Uuid::now_v7()));
    let writer = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite writer");
    writer.migrate().await.expect("migrate");
    let db = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite reader");
    Harness {
        service: MergeService::new(writer, Some(producer)),
        db,
        path,
    }
}

async fn wait_for(
    consumer: &EventConsumer,
    relationship_id: Uuid,
    change_kind: &str,
) -> core_events::EventEnvelope {
    let want = relationship_id.to_string();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "等 relationship.changed（id={want}, kind={change_kind}）逾時。\
             請確認 Redpanda 在跑、topic 可自動建立"
        );
        let envelope = consumer
            .next_envelope(remaining)
            .await
            .expect("consume 失敗。請確認 osint-core-redpanda-1 在跑（埠 9092）");
        if envelope.event_type != "relationship.changed" {
            continue;
        }
        let payload_id = envelope
            .payload
            .get("relationship_id")
            .and_then(|v| v.as_str());
        let payload_kind = envelope.payload.get("change_kind").and_then(|v| v.as_str());
        if payload_id == Some(want.as_str()) && payload_kind == Some(change_kind) {
            let _ = consumer.commit_last();
            return envelope;
        }
    }
}

#[tokio::test]
async fn execute_merge_publishes_upserted_for_safe_repoint() {
    let brokers = brokers();
    let run = Uuid::now_v7();
    let group = format!("osint-e2e-merge-rel-{run}");
    let consumer = EventConsumer::connect(
        &brokers,
        &group,
        &[EventTopic::RelationshipChanged.as_str()],
    )
    .expect("consumer");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer = Arc::new(EventProducer::connect(&brokers, "merge-e2e").expect("producer"));
    let h = open_harness(producer).await;

    let survivor = entity("s-evt", EntityType::Organization);
    let merged = entity("m-evt", EntityType::Organization);
    let third = entity("t-evt", EntityType::Organization);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();
    h.db.put_entity(&third).await.unwrap();

    let rel = relationship(merged.id, RelationshipType::Owns, third.id, 1, 0.4);
    h.db.put_relationship(&rel).await.unwrap();

    h.service
        .execute_merge(survivor.id, merged.id, "事件測試".into(), "tester".into())
        .await
        .expect("merge");

    let envelope = wait_for(&consumer, rel.id, "upserted").await;
    assert_eq!(envelope.payload["source_object_id"], json!(survivor.id));
    assert_eq!(envelope.payload["target_object_id"], json!(third.id));
    assert_eq!(envelope.payload["relationship_type"], json!("owns"));
    assert_eq!(
        envelope.correlation_id,
        Some(rel.id),
        "correlation_id 必須是 relationship_id"
    );
    // EventConsumer 不暴露 Kafka message key；partition key 在 produce 時
    // 設成 relationship_id.to_string()，與 correlation_id 同一值。
}

#[tokio::test]
async fn execute_and_undo_publish_deleted_then_upserted_for_self_loop() {
    let brokers = brokers();
    let run = Uuid::now_v7();
    let group = format!("osint-e2e-merge-loop-{run}");
    let consumer = EventConsumer::connect(
        &brokers,
        &group,
        &[EventTopic::RelationshipChanged.as_str()],
    )
    .expect("consumer");
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let producer = Arc::new(EventProducer::connect(&brokers, "merge-e2e-loop").expect("producer"));
    let h = open_harness(producer).await;

    let survivor = entity("s-loop-evt", EntityType::Organization);
    let merged = entity("m-loop-evt", EntityType::Organization);
    h.db.put_entity(&survivor).await.unwrap();
    h.db.put_entity(&merged).await.unwrap();

    let loop_rel = relationship(
        merged.id,
        RelationshipType::AssociatedWith,
        survivor.id,
        1,
        0.4,
    );
    h.db.put_relationship(&loop_rel).await.unwrap();

    let history = h
        .service
        .execute_merge(survivor.id, merged.id, "自迴圈事件".into(), "tester".into())
        .await
        .expect("merge");

    let deleted = wait_for(&consumer, loop_rel.id, "deleted").await;
    assert_eq!(deleted.payload["source_object_id"], json!(merged.id));
    assert_eq!(deleted.payload["target_object_id"], json!(survivor.id));
    assert_eq!(
        deleted.payload["relationship_type"],
        json!("associated_with")
    );

    h.service.undo_merge(history.id).await.expect("undo");
    let restored = wait_for(&consumer, loop_rel.id, "upserted").await;
    assert_eq!(restored.payload["source_object_id"], json!(merged.id));
    assert_eq!(restored.payload["target_object_id"], json!(survivor.id));
}
