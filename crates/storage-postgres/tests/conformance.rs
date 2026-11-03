use storage_core::conformance::{
    OutboxContractTracker, assert_candidate_discovery_concurrent, assert_candidate_natural_key,
    assert_canonical_health, assert_collection_member_links_atomic,
    assert_connector_run_state_partial_update, assert_event_outbox_contract_tracked,
    assert_evidence_collections, assert_jobs_purge, assert_list_sources_by_entity,
    assert_reclaim_orphan_collect_runs, assert_relational_round_trip_with_enabled_hook,
    assert_transactional_contract, load_workspace_dotenv, required_env,
};
use storage_postgres::{
    CleanupOutboxConformanceOnDrop, DisableConnectorsOnDrop, PostgresCanonicalStore,
};

#[tokio::test]
async fn postgres_canonical_conformance() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "conformance 只連本機 Postgres，實際 DSN host 不像本機：{}",
        storage_core::StorageError::sanitize(&dsn)
    );
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    assert_canonical_health(&store)
        .await
        .expect("canonical health");
    // 這支會暫時寫 enabled=true 且有 schedule 的 connector 驗 list_enabled_connectors。
    // 函式結尾才停用，panic 時不會跑；guard 在 Drop（含 panic）時停用。
    let guard = DisableConnectorsOnDrop::from_dsn(&dsn);
    assert_relational_round_trip_with_enabled_hook(&store, |id| guard.track(id))
        .await
        .expect("relational round-trip");
    assert_transactional_contract(&store, "postgres")
        .await
        .expect("transactional contract");
    let outbox_tracker = OutboxContractTracker::default();
    let _outbox_guard = CleanupOutboxConformanceOnDrop::new(
        &dsn,
        outbox_tracker.outbox_ids.clone(),
        outbox_tracker.entity_ids.clone(),
    );
    assert_event_outbox_contract_tracked(&store, "postgres", &outbox_tracker)
        .await
        .expect("event outbox contract");
}

#[tokio::test]
async fn postgres_list_sources_by_entity_dedup_and_empty() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    assert_list_sources_by_entity(&store)
        .await
        .expect("list_sources_by_entity：多 source 去重 + 無關聯回空");
}

#[tokio::test]
async fn postgres_evidence_collections_many_to_many() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    assert_evidence_collections(&store)
        .await
        .expect("evidence ↔ collections 多對多寫入／反查／unlink／刪文件清關聯");
}

async fn connect() -> PostgresCanonicalStore {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "conformance 只連本機 Postgres"
    );
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    store
}

#[tokio::test]
async fn postgres_collection_member_links_are_atomic() {
    let store = connect().await;
    assert_collection_member_links_atomic(&store)
        .await
        .expect("集合批次關聯：一筆無效整批不寫入");
}

#[tokio::test]
async fn postgres_connector_run_state_partial_update_does_not_clobber_patch() {
    let store = connect().await;
    assert_connector_run_state_partial_update(&store)
        .await
        .expect("update_connector_run_state 不得蓋掉 enabled/schedule");
}

#[tokio::test]
async fn postgres_candidate_natural_key_dedups_rediscovery() {
    let store = connect().await;
    assert_candidate_natural_key(&store)
        .await
        .expect("candidate 自然鍵：重跑不新增、被拒不復活、只附加證據");
}

#[tokio::test]
async fn postgres_jobs_purge_only_deletes_matching_terminal_rows() {
    let store = connect().await;
    assert_jobs_purge(&store)
        .await
        .expect("purge_jobs：只刪 collect+failed+夠舊，limit／非終態／count 對得上");
}

#[tokio::test]
async fn postgres_reclaim_orphan_collect_runs_marks_failed_and_idle() {
    let store = connect().await;
    // reclaim 收回全表 running collect job；與 collector shutdown 測試跨行程互斥。
    let _collect_lock = store
        .acquire_collect_state_test_lock()
        .await
        .expect("collect 狀態測試鎖");
    assert_reclaim_orphan_collect_runs(&store)
        .await
        .expect("reclaim：running collect job → failed（completed_at／固定 error）、running connector → idle（error_count 不動）");
}

#[tokio::test]
async fn postgres_candidate_discovery_concurrent_writers_do_not_duplicate_evidence() {
    let store = connect().await;
    assert_candidate_discovery_concurrent(&store, 20)
        .await
        .expect("兩個寫入者同時記錄同一份候選證據：只能留下一列證據");
}

/// 兩個連線同時 claim 同一列：只能一個人拿到，不掃共用表。
#[tokio::test]
async fn postgres_outbox_parallel_claim_does_not_double_send() {
    use chrono::Utc;
    use serde_json::json;
    use storage_core::{EventOutboxStore, EventOutboxTestSupport, OutboxEvent};
    use uuid::Uuid;

    let store = connect().await;
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let run = Uuid::now_v7();
    let mut event = OutboxEvent::pending(
        format!("osint.conformance.outbox.parallel.{run}"),
        run.to_string(),
        Some(run),
        json!({"kind": "parallel"}),
        "conformance",
    );
    let epoch = chrono::DateTime::<Utc>::UNIX_EPOCH;
    event.created_at = epoch;
    event.next_attempt_at = epoch;
    let id = event.id;
    let tracker = OutboxContractTracker::default();
    tracker.track_outbox(id);
    let _guard = CleanupOutboxConformanceOnDrop::new(
        &dsn,
        tracker.outbox_ids.clone(),
        tracker.entity_ids.clone(),
    );
    store.enqueue_outbox(&event).await.expect("enqueue");

    let now = Utc::now();
    let lease = now + chrono::Duration::seconds(30);
    let left = store.clone();
    let right = store.clone();
    let (a, b) = tokio::join!(
        left.claim_outbox_by_id(id, now, lease),
        right.claim_outbox_by_id(id, now, lease),
    );
    let a = a.expect("claim a");
    let b = b.expect("claim b");
    let got = [a.is_some(), b.is_some()]
        .into_iter()
        .filter(|seen| *seen)
        .count();
    assert_eq!(
        got, 1,
        "兩個連線同時 claim 同一列，應恰好一人拿到，實際 {got}"
    );
    store.delete_outbox(id).await.expect("cleanup outbox");
}

/// 兩個連線同時把同一筆 pending candidate 核准：條件 UPDATE 只能讓一筆寫入 outbox。
#[tokio::test]
async fn postgres_concurrent_candidate_approve_writes_single_outbox_row() {
    use chrono::Utc;
    use core_model::{Candidate, CandidateStatus, CandidateType};
    use serde_json::json;
    use storage_core::{EventOutboxTestSupport, OutboxEvent, RelationalStore, TransactionalStore};
    use uuid::Uuid;

    let store = connect().await;
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let now = Utc::now();
    let candidate = Candidate {
        id: Uuid::now_v7(),
        candidate_type: CandidateType::Account,
        value: format!("conformance-concurrent-approve-{}", Uuid::now_v7().simple()),
        normalized_value: "conformance-concurrent-approve".into(),
        collection_id: None,
        discovered_by: "conformance".into(),
        discovery_method: "account_expansion".into(),
        confidence: 0.8,
        score: 0.8,
        status: CandidateStatus::Pending,
        depth: 0,
        created_at: now,
        reviewed_at: None,
    };
    let candidate_id = candidate.id;
    let tracker = OutboxContractTracker::default();
    let guard = CleanupOutboxConformanceOnDrop::new(
        &dsn,
        tracker.outbox_ids.clone(),
        tracker.entity_ids.clone(),
    );
    guard.track_candidate(candidate_id);
    store
        .put_candidate(&candidate)
        .await
        .expect("put candidate");

    let approve = |handle: PostgresCanonicalStore| async move {
        let tx = handle.begin().await?;
        let changed = tx
            .store()
            .update_candidate_status(candidate_id, CandidateStatus::Approved, Utc::now())
            .await?;
        if changed {
            let event = OutboxEvent::pending(
                format!("osint.conformance.candidate.approve.{candidate_id}"),
                candidate_id.to_string(),
                Some(candidate_id),
                json!({"candidate_id": candidate_id}),
                "conformance",
            );
            tx.outbox().enqueue_outbox(&event).await?;
        }
        tx.commit().await?;
        Ok::<_, storage_core::StorageError>(changed)
    };

    let (left, right) = tokio::join!(approve(store.clone()), approve(store.clone()));
    let left = left.expect("left approve");
    let right = right.expect("right approve");
    assert_ne!(left, right, "兩個連線應恰好一個真的改到狀態");

    let rows = store
        .list_outbox_by_correlation_id(candidate_id)
        .await
        .expect("list outbox");
    for row in &rows {
        guard.track_outbox(row.id);
    }
    assert_eq!(
        rows.len(),
        1,
        "同時核准只能寫一列 outbox，實際 {}：{:?}",
        rows.len(),
        rows.iter().map(|r| r.id).collect::<Vec<_>>()
    );
    for row in &rows {
        store.delete_outbox(row.id).await.expect("cleanup outbox");
    }
    sqlx::query("DELETE FROM candidates WHERE id = $1")
        .bind(candidate_id)
        .execute(store.pool())
        .await
        .expect("cleanup candidate");
    drop(guard);
}
