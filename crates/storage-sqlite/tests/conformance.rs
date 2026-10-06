use std::path::PathBuf;

use storage_core::conformance::{
    assert_embedded_health, assert_evidence_collections, assert_list_sources_by_entity,
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
