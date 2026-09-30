use storage_core::conformance::{
    assert_canonical_health, assert_list_sources_by_entity, assert_relational_round_trip,
    assert_transactional_contract, load_workspace_dotenv, required_env,
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
