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

/// `object_store` 沒有 CreateBucket；這條是專門驗證 adapter 自己簽 SigV4 打 PUT /{bucket}。
#[tokio::test]
async fn ensure_bucket_creates_when_missing() {
    load_workspace_dotenv();
    let endpoint = required_env("S3_ENDPOINT").expect("S3_ENDPOINT");
    let _parsed = verify_not_opencti_s3(&endpoint).expect("URL 格式或本機嚴格模式檢查失敗");
    let access = required_env("MINIO_ROOT_USER").expect("MINIO_ROOT_USER");
    let secret = required_env("MINIO_ROOT_PASSWORD").expect("MINIO_ROOT_PASSWORD");

    let bucket = format!("osint-ensure-{}", uuid::Uuid::now_v7().simple());
    let store = S3ObjectStore::connect(&endpoint, &bucket, &access, &secret).expect("S3 client");

    let before = storage_core::HealthProvider::health(&store)
        .await
        .expect("health before");
    assert!(
        !before.healthy,
        "測試前 bucket `{bucket}` 不該已存在：{}",
        before.message
    );

    store.ensure_bucket().await.expect("ensure missing bucket");
    store
        .ensure_bucket()
        .await
        .expect("ensure_bucket 對已存在的 bucket 必須是冪等");

    let after = storage_core::HealthProvider::health(&store)
        .await
        .expect("health after");
    assert!(
        after.healthy,
        "ensure_bucket 之後 health 應為 true：{}",
        after.message
    );

    store
        .delete_empty_bucket()
        .await
        .expect("清掉測試用空 bucket");
}
