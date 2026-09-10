//! MinIO / S3 ObjectStore adapter。endpoint 由呼叫端／設定注入，不寫死 9000。

use async_trait::async_trait;
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, BucketConfiguration, Region};
use storage_core::{
    CapabilityDescriptor, HealthProvider, ObjectStore, StorageAdapter, StorageError, StorageHealth,
};

/// S3 相容物件儲存（本機 MinIO）。
#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    bucket: Box<Bucket>,
}

impl S3ObjectStore {
    pub fn connect(
        endpoint: &str,
        bucket: &str,
        access_key: &str,
        secret_key: &str,
    ) -> Result<Self, StorageError> {
        if bucket.is_empty() {
            return Err(StorageError::Configuration {
                message: "S3 bucket 名稱是空的。請設 S3_BUCKET 或 storage.object.bucket".into(),
            });
        }
        let region = Region::Custom {
            region: "us-east-1".into(),
            endpoint: endpoint.to_string(),
        };
        let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)
            .map_err(|err| StorageError::Configuration {
                message: format!(
                    "S3 憑證無效：{}。請檢查 MINIO_ROOT_USER / MINIO_ROOT_PASSWORD",
                    StorageError::sanitize(&err.to_string())
                ),
            })?;
        let mut bucket = Bucket::new(bucket, region, credentials).map_err(map_s3)?;
        bucket.set_path_style();
        Ok(Self { bucket })
    }

    /// 測試用：bucket 不存在就建立。不會刪既有物件。
    pub async fn ensure_bucket(&self) -> Result<(), StorageError> {
        match self.bucket.exists().await {
            Ok(true) => Ok(()),
            Ok(false) => create_bucket(&self.bucket).await,
            Err(err) => {
                let message = err.to_string();
                if message.contains("NoSuchBucket") || is_http_status(&err, 404) {
                    create_bucket(&self.bucket).await
                } else {
                    Err(map_s3(err))
                }
            }
        }
    }
}

async fn create_bucket(bucket: &Bucket) -> Result<(), StorageError> {
    let credentials = bucket.credentials().await.map_err(map_s3)?;
    match Bucket::create_with_path_style(
        bucket.name.as_str(),
        bucket.region.clone(),
        credentials,
        BucketConfiguration::default(),
    )
    .await
    {
        Ok(_) => Ok(()),
        Err(err)
            if is_http_status(&err, 409) || err.to_string().contains("BucketAlreadyOwnedByYou") =>
        {
            Ok(())
        }
        Err(err) => Err(map_s3(err)),
    }
}

fn is_http_status(err: &S3Error, status: u16) -> bool {
    matches!(err, S3Error::HttpFailWithBody(code, _) if *code == status)
        || ((400..500).contains(&status) && matches!(err, S3Error::HttpFail))
}

fn is_missing(err: &S3Error) -> bool {
    is_http_status(err, 404)
        || err.to_string().contains("NoSuchKey")
        || err.to_string().contains("Not Found")
}

fn map_s3(err: S3Error) -> StorageError {
    let message = StorageError::sanitize(&err.to_string());
    match &err {
        S3Error::HttpFailWithBody(404, _) => StorageError::NotFound { message },
        S3Error::HttpFailWithBody(403, _) => StorageError::PermissionDenied { message },
        S3Error::HttpFailWithBody(409, _) => StorageError::Conflict { message },
        S3Error::HttpFailWithBody(code, _) if *code >= 500 => StorageError::Unavailable {
            backend: "s3",
            message,
        },
        S3Error::HttpFail => StorageError::Unavailable {
            backend: "s3",
            message,
        },
        _ if message.to_ascii_lowercase().contains("timed out")
            || message.to_ascii_lowercase().contains("timeout") =>
        {
            StorageError::Timeout {
                backend: "s3",
                message,
            }
        }
        _ => StorageError::Unknown {
            backend: "s3",
            message,
        },
    }
}

#[async_trait]
impl HealthProvider for S3ObjectStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        match self.bucket.exists().await {
            Ok(true) => Ok(StorageHealth::ok("s3", "bucket 存在").with_details(
                serde_json::json!({
                    "bucket": self.bucket.name,
                }),
            )),
            Ok(false) => Ok(StorageHealth::down(
                "s3",
                format!(
                    "bucket `{}` 不存在。測試可呼叫 ensure_bucket()",
                    self.bucket.name
                ),
            )),
            Err(err) => Err(map_s3(err)),
        }
    }
}

impl StorageAdapter for S3ObjectStore {
    fn descriptor(&self) -> CapabilityDescriptor {
        CapabilityDescriptor::new("s3", env!("CARGO_PKG_VERSION"), &["object"])
    }
}

#[async_trait]
impl ObjectStore for S3ObjectStore {
    async fn put(
        &self,
        key: &str,
        bytes: &[u8],
        content_type: Option<&str>,
    ) -> Result<(), StorageError> {
        let content_type = content_type.unwrap_or("application/octet-stream");
        self.bucket
            .put_object_with_content_type(key, bytes, content_type)
            .await
            .map_err(map_s3)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        match self.bucket.get_object(key).await {
            Ok(resp) => {
                if resp.status_code() == 404 {
                    Ok(None)
                } else if (200..300).contains(&resp.status_code()) {
                    Ok(Some(resp.to_vec()))
                } else {
                    Err(StorageError::Unknown {
                        backend: "s3",
                        message: format!("get `{key}` 回 HTTP {}", resp.status_code()),
                    })
                }
            }
            Err(err) if is_missing(&err) => Ok(None),
            Err(err) => Err(map_s3(err)),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        if !self.exists(key).await? {
            return Ok(false);
        }
        self.bucket.delete_object(key).await.map_err(map_s3)?;
        Ok(true)
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        match self.bucket.head_object(key).await {
            Ok((_, code)) if (200..300).contains(&code) => Ok(true),
            Ok((_, 404)) => Ok(false),
            Ok((_, code)) => Err(StorageError::Unknown {
                backend: "s3",
                message: format!("head `{key}` 回 HTTP {code}"),
            }),
            Err(err) if is_missing(&err) => Ok(false),
            Err(err) => Err(map_s3(err)),
        }
    }
}
