use std::path::PathBuf;

use storage_core::conformance::{
    assert_embedded_health, assert_relational_round_trip, assert_transactional_contract,
    find_workspace_root,
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
