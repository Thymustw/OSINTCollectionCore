use storage_core::conformance::{
    assert_candidate_discovery_concurrent, assert_candidate_natural_key, assert_canonical_health,
    assert_collection_member_links_atomic, assert_evidence_collections,
    assert_list_sources_by_entity, assert_relational_round_trip, assert_transactional_contract,
    load_workspace_dotenv, required_env,
};
use storage_postgres::PostgresCanonicalStore;

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
    assert_relational_round_trip(&store)
        .await
        .expect("relational round-trip");
    assert_transactional_contract(&store, "postgres")
        .await
        .expect("transactional contract");
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
async fn postgres_candidate_natural_key_dedups_rediscovery() {
    let store = connect().await;
    assert_candidate_natural_key(&store)
        .await
        .expect("candidate 自然鍵：重跑不新增、被拒不復活、只附加證據");
}

#[tokio::test]
async fn postgres_candidate_discovery_concurrent_writers_do_not_duplicate_evidence() {
    let store = connect().await;
    assert_candidate_discovery_concurrent(&store, 20)
        .await
        .expect("兩個寫入者同時記錄同一份候選證據：只能留下一列證據");
}
