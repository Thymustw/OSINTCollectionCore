use storage_core::conformance::{
    assert_object_round_trip, load_workspace_dotenv, required_env, verify_not_opencti_s3,
};
use storage_s3::S3ObjectStore;

#[tokio::test]
async fn s3_object_conformance() {
    load_workspace_dotenv();
    let endpoint = required_env("S3_ENDPOINT").expect("S3_ENDPOINT");
    // 埠號本身不代表身分——只在本機開 OSINT_STRICT_PORT_ISOLATION 時才會擋 9000。
    // MinIO 沒有等同 OpenSearch 的身分驗證 API，這是目前唯一的防線，只在已知衝突的機器生效。
    let _parsed = verify_not_opencti_s3(&endpoint).expect("URL 格式或本機嚴格模式檢查失敗");

    let bucket = required_env("S3_BUCKET").unwrap_or_else(|_| "raw-evidence".into());
    let access = required_env("MINIO_ROOT_USER").expect("MINIO_ROOT_USER");
    let secret = required_env("MINIO_ROOT_PASSWORD").expect("MINIO_ROOT_PASSWORD");

    let store = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("S3 client");
    store.ensure_bucket().await.expect("ensure bucket");

    let health = storage_core::HealthProvider::health(&store)
        .await
        .expect("health");
    eprintln!(
        "s3 identity: healthy={} message={} details={}",
        health.healthy, health.message, health.details
    );
    assert!(health.healthy, "MinIO health 失敗：{}", health.message);

    let prefix = format!("conformance/{}", uuid::Uuid::now_v7());
    assert_object_round_trip(&store, &prefix)
        .await
        .expect("object round-trip");
}
