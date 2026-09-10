use storage_core::conformance::{
    assert_canonical_health, assert_relational_round_trip, load_workspace_dotenv, required_env,
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
}
