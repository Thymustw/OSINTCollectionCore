//! MinIO / S3 ObjectStore adapter。endpoint 由呼叫端／設定注入，不寫死 9000。
//!
//! 底層是 `object_store` 的 `AmazonS3`（`aws` feature = reqwest/rustls + aws-lc-rs）。
//! `object_store` 不提供建立 bucket 的 API，`ensure_bucket` 另外用同一套 SigV4 + rustls
//! HTTP client 打 MinIO 的 CreateBucket（`PUT /{bucket}`）。

use async_trait::async_trait;
use http::{Method, StatusCode};
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer};
use object_store::client::{
    ClientOptions, HttpClient, HttpConnector, HttpErrorKind, HttpRequest, HttpRequestBody,
    ReqwestConnector,
};
use object_store::path::Path;
use object_store::{
    Attribute, Attributes, Error as ObjectStoreError, ObjectStore as ObjectStoreBackend,
    ObjectStoreExt, PutOptions, PutPayload,
};
use storage_core::{
    CapabilityDescriptor, HealthProvider, ObjectStore, StorageAdapter, StorageError, StorageHealth,
};

const REGION: &str = "us-east-1";

/// S3 相容物件儲存（本機 MinIO）。
#[derive(Debug, Clone)]
pub struct S3ObjectStore {
    inner: AmazonS3,
    bucket: String,
    endpoint: String,
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
        if endpoint.is_empty() {
            return Err(StorageError::Configuration {
                message: "S3 endpoint 是空的。請設 S3_ENDPOINT 或 storage.object.endpoint".into(),
            });
        }
        let endpoint = endpoint.trim_end_matches('/').to_string();
        let allow_http = endpoint.starts_with("http://");
        let inner = AmazonS3Builder::new()
            .with_region(REGION)
            .with_bucket_name(bucket)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_endpoint(&endpoint)
            .with_allow_http(allow_http)
            .with_virtual_hosted_style_request(false)
            .build()
            .map_err(|err| StorageError::Configuration {
                message: format!(
                    "S3 client 建立失敗：{}。請檢查 S3_ENDPOINT / MINIO_ROOT_USER / MINIO_ROOT_PASSWORD",
                    StorageError::sanitize(&err.to_string())
                ),
            })?;
        Ok(Self {
            inner,
            bucket: bucket.to_string(),
            endpoint,
        })
    }

    /// 測試用：bucket 不存在就建立。不會刪既有物件。
    ///
    /// `object_store` 沒有 CreateBucket API，這裡用同一套 rustls HTTP client 簽 SigV4
    /// 後打 MinIO 的 `PUT /{bucket}`。已存在（HTTP 409 / BucketAlreadyOwnedByYou）視為成功。
    pub async fn ensure_bucket(&self) -> Result<(), StorageError> {
        match self.inner.list_with_delimiter(None).await {
            Ok(_) => Ok(()),
            Err(err) if is_missing_bucket(&err) => self.create_bucket().await,
            Err(err) => Err(map_object_store(err)),
        }
    }

    /// 測試用：刪空的 bucket。裡面還有物件時 MinIO 會拒絕。
    ///
    /// 跟 `ensure_bucket` 一樣走 SigV4 `DELETE /{bucket}`，因為 `object_store` 也沒有 DeleteBucket。
    pub async fn delete_empty_bucket(&self) -> Result<(), StorageError> {
        let response =
            self.signed_bucket_request(Method::DELETE)
                .await
                .map_err(|err| match err {
                    BucketHttpError::Object(err) => map_object_store(err),
                    BucketHttpError::Transport(err) => map_http_error(err),
                })?;
        let status = response.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED {
            return Err(StorageError::PermissionDenied {
                message: format!(
                    "刪除 bucket `{}` 被拒絕（HTTP {status}）。請確認憑證有 DeleteBucket 權限",
                    self.bucket
                ),
            });
        }
        if status == StatusCode::CONFLICT {
            return Err(StorageError::Conflict {
                message: format!(
                    "bucket `{}` 不是空的，MinIO 拒絕刪除（HTTP {status}）。請先刪物件再刪 bucket",
                    self.bucket
                ),
            });
        }
        Err(StorageError::Unknown {
            backend: "s3",
            message: format!("刪除 bucket `{}` 失敗（HTTP {status}）", self.bucket),
        })
    }

    async fn create_bucket(&self) -> Result<(), StorageError> {
        let response = self
            .signed_bucket_request(Method::PUT)
            .await
            .map_err(|err| match err {
                BucketHttpError::Object(err) => map_object_store(err),
                BucketHttpError::Transport(err) => map_http_error(err),
            })?;
        let status = response.status();
        // 200/200-range = 新建成功。409 = BucketAlreadyOwnedByYou / BucketAlreadyExists。
        if status.is_success() || status == StatusCode::CONFLICT {
            return Ok(());
        }
        if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED {
            return Err(StorageError::PermissionDenied {
                message: format!(
                    "建立 bucket `{}` 被拒絕（HTTP {status}）。請檢查 MINIO_ROOT_USER / MINIO_ROOT_PASSWORD 是否有 CreateBucket 權限",
                    self.bucket
                ),
            });
        }
        // 其他狀態（含部分 MinIO 對已存在 bucket 回 405）：再 list 一次，已存在就當成功。
        match self.inner.list_with_delimiter(None).await {
            Ok(_) => Ok(()),
            Err(err) if is_missing_bucket(&err) => Err(StorageError::Unknown {
                backend: "s3",
                message: format!(
                    "建立 bucket `{}` 失敗（HTTP {status}），且後續 list 仍顯示不存在。請確認 MinIO 允許 CreateBucket，或先用 mc mb 建好 bucket",
                    self.bucket
                ),
            }),
            Err(err) => Err(map_object_store(err)),
        }
    }

    async fn bucket_exists(&self) -> Result<bool, StorageError> {
        match self.inner.list_with_delimiter(None).await {
            Ok(_) => Ok(true),
            Err(err) if is_missing_bucket(&err) => Ok(false),
            Err(err) => Err(map_object_store(err)),
        }
    }

    async fn signed_bucket_request(
        &self,
        method: Method,
    ) -> Result<http::Response<object_store::client::HttpResponseBody>, BucketHttpError> {
        let options = ClientOptions::new().with_allow_http(self.endpoint.starts_with("http://"));
        let client: HttpClient = ReqwestConnector {}
            .connect(&options)
            .map_err(BucketHttpError::Object)?;
        let url = format!("{}/{}", self.endpoint, self.bucket);
        let uri = url.parse::<http::Uri>().map_err(|err| {
            BucketHttpError::Object(ObjectStoreError::Generic {
                store: "s3",
                source: format!("bucket URL `{url}` 無效：{err}").into(),
            })
        })?;
        let mut request = HttpRequest::new(HttpRequestBody::empty());
        *request.method_mut() = method;
        *request.uri_mut() = uri;

        let credential = self
            .inner
            .credentials()
            .get_credential()
            .await
            .map_err(BucketHttpError::Object)?;
        AwsAuthorizer::new(&credential, "s3", REGION)
            .try_authorize(&mut request, None)
            .map_err(BucketHttpError::Object)?;

        client
            .execute(request)
            .await
            .map_err(BucketHttpError::Transport)
    }
}

enum BucketHttpError {
    Object(ObjectStoreError),
    Transport(object_store::client::HttpError),
}

fn is_missing_bucket(err: &ObjectStoreError) -> bool {
    matches!(err, ObjectStoreError::NotFound { .. })
        || err.to_string().contains("NoSuchBucket")
        || err
            .to_string()
            .contains("The specified bucket does not exist")
}

fn is_missing_object(err: &ObjectStoreError) -> bool {
    matches!(err, ObjectStoreError::NotFound { .. })
        || err.to_string().contains("NoSuchKey")
        || err.to_string().contains("Not Found")
}

fn map_http_error(err: object_store::client::HttpError) -> StorageError {
    let message = StorageError::sanitize(&err.to_string());
    match err.kind() {
        HttpErrorKind::Timeout => StorageError::Timeout {
            backend: "s3",
            message,
        },
        HttpErrorKind::Connect | HttpErrorKind::Request | HttpErrorKind::Interrupted => {
            StorageError::Unavailable {
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

fn map_object_store(err: ObjectStoreError) -> StorageError {
    let message = StorageError::sanitize(&err.to_string());
    match err {
        ObjectStoreError::NotFound { .. } => StorageError::NotFound { message },
        ObjectStoreError::AlreadyExists { .. } => StorageError::Conflict { message },
        ObjectStoreError::PermissionDenied { .. } | ObjectStoreError::Unauthenticated { .. } => {
            StorageError::PermissionDenied { message }
        }
        ObjectStoreError::NotSupported { .. } | ObjectStoreError::NotImplemented { .. } => {
            StorageError::UnsupportedCapability {
                backend: "s3",
                capability: "object",
            }
        }
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

fn object_path(key: &str) -> Result<Path, StorageError> {
    Path::parse(key).map_err(|err| StorageError::Configuration {
        message: format!(
            "S3 object key `{key}` 無效：{err}。請用不以 `/` 開頭、不含空 segment 的路徑"
        ),
    })
}

#[async_trait]
impl HealthProvider for S3ObjectStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        match self.bucket_exists().await {
            Ok(true) => Ok(StorageHealth::ok("s3", "bucket 存在").with_details(
                serde_json::json!({
                    "bucket": self.bucket,
                }),
            )),
            Ok(false) => Ok(StorageHealth::down(
                "s3",
                format!(
                    "bucket `{}` 不存在。測試可呼叫 ensure_bucket()",
                    self.bucket
                ),
            )),
            Err(err) => Err(err),
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
        let location = object_path(key)?;
        let content_type = content_type.unwrap_or("application/octet-stream");
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_string().into());
        let opts = PutOptions {
            attributes,
            ..PutOptions::default()
        };
        self.inner
            .put_opts(&location, PutPayload::from(bytes.to_vec()), opts)
            .await
            .map_err(map_object_store)?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let location = object_path(key)?;
        match ObjectStoreExt::get(&self.inner, &location).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(map_object_store)?;
                Ok(Some(bytes.to_vec()))
            }
            Err(err) if is_missing_object(&err) => Ok(None),
            Err(err) => Err(map_object_store(err)),
        }
    }

    async fn delete(&self, key: &str) -> Result<bool, StorageError> {
        if !self.exists(key).await? {
            return Ok(false);
        }
        let location = object_path(key)?;
        ObjectStoreExt::delete(&self.inner, &location)
            .await
            .map_err(map_object_store)?;
        Ok(true)
    }

    async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        let location = object_path(key)?;
        match ObjectStoreExt::head(&self.inner, &location).await {
            Ok(_) => Ok(true),
            Err(err) if is_missing_object(&err) => Ok(false),
            Err(err) => Err(map_object_store(err)),
        }
    }
}
