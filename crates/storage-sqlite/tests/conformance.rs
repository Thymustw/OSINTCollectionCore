use std::path::PathBuf;

use storage_core::conformance::{
    assert_candidate_natural_key, assert_collection_member_links_atomic,
    assert_connector_run_state_partial_update, assert_embedded_health, assert_evidence_collections,
    assert_jobs_purge, assert_list_sources_by_entity, assert_reclaim_orphan_collect_runs,
    assert_relational_round_trip, assert_transactional_contract, find_workspace_root,
};
use storage_sqlite::SqliteEmbeddedStore;

#[tokio::test]
async fn sqlite_embedded_conformance() {
    let root = find_workspace_root().expect("workspace root");
    let path: PathBuf = root.join(format!(
        "var/osint-conformance-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite");
    store.migrate().await.expect("migrate");
    assert_embedded_health(&store)
        .await
        .expect("embedded health");
    assert_relational_round_trip(&store)
        .await
        .expect("relational round-trip");
    assert_transactional_contract(&store, "sqlite")
        .await
        .expect("transactional contract");
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[tokio::test]
async fn sqlite_list_sources_by_entity_dedup_and_empty() {
    let root = find_workspace_root().expect("workspace root");
    let path: PathBuf = root.join(format!(
        "var/osint-sources-by-entity-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite");
    store.migrate().await.expect("migrate");
    assert_list_sources_by_entity(&store)
        .await
        .expect("list_sources_by_entity：多 source 去重 + 無關聯回空");
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[tokio::test]
async fn sqlite_evidence_collections_many_to_many() {
    let root = find_workspace_root().expect("workspace root");
    let path: PathBuf = root.join(format!(
        "var/osint-evidence-collections-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite");
    store.migrate().await.expect("migrate");
    assert_evidence_collections(&store)
        .await
        .expect("evidence ↔ collections 多對多寫入／反查／unlink／刪文件清關聯");
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// 開一個一次性的 SQLite 檔，跑完 `check` 後把三個檔都刪掉（測試只刪自己建立的東西）。
async fn with_temp_store<F, Fut>(label: &str, check: F)
where
    F: FnOnce(SqliteEmbeddedStore) -> Fut,
    Fut: std::future::Future<Output = SqliteEmbeddedStore>,
{
    let root = find_workspace_root().expect("workspace root");
    let path: PathBuf = root.join(format!(
        "var/osint-{label}-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite");
    store.migrate().await.expect("migrate");
    let store = check(store).await;
    drop(store);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

#[tokio::test]
async fn sqlite_connector_run_state_partial_update_does_not_clobber_patch() {
    with_temp_store("run-state", |store| async move {
        assert_connector_run_state_partial_update(&store)
            .await
            .expect("update_connector_run_state 不得蓋掉 enabled/schedule");
        store
    })
    .await;
}

#[tokio::test]
async fn sqlite_collection_member_links_are_atomic() {
    with_temp_store("member-links", |store| async move {
        assert_collection_member_links_atomic(&store)
            .await
            .expect("集合批次關聯：一筆無效整批不寫入");
        store
    })
    .await;
}

#[tokio::test]
async fn sqlite_candidate_natural_key_dedups_rediscovery() {
    with_temp_store("candidate-nk", |store| async move {
        assert_candidate_natural_key(&store)
            .await
            .expect("candidate 自然鍵：重跑不新增、被拒不復活、只附加證據");
        store
    })
    .await;
}

#[tokio::test]
async fn sqlite_jobs_purge_only_deletes_matching_terminal_rows() {
    with_temp_store("jobs-purge", |store| async move {
        assert_jobs_purge(&store)
            .await
            .expect("purge_jobs：只刪 collect+failed+夠舊，limit／非終態／count 對得上");
        store
    })
    .await;
}

#[tokio::test]
async fn sqlite_reclaim_orphan_collect_runs_marks_failed_and_idle() {
    with_temp_store("reclaim-orphan", |store| async move {
        assert_reclaim_orphan_collect_runs(&store)
            .await
            .expect("reclaim：running collect job → failed（completed_at／固定 error）、running connector → idle（error_count 不動）");
        store
    })
    .await;
}
