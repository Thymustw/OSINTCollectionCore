use storage_core::conformance::{assert_kv_round_trip, load_workspace_dotenv, required_env};
use storage_redis::RedisKeyValueStore;

#[tokio::test]
async fn redis_kv_conformance() {
    load_workspace_dotenv();
    let url = required_env("REDIS_URL").expect("REDIS_URL");
    assert!(
        url.contains("127.0.0.1") || url.contains("localhost"),
        "conformance 只連本機 Redis，實際是 {url}"
    );
    let store = RedisKeyValueStore::connect(&url).expect("Redis URL");
    assert_kv_round_trip(&store).await.expect("kv round-trip");
}
