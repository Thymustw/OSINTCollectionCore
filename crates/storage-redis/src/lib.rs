//! Redis KeyValueStore adapter。連線 URL 由呼叫端／設定注入。

use std::time::Duration;

use async_trait::async_trait;
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, Client};
use storage_core::{
    CapabilityDescriptor, HealthProvider, KeyValueStore, StorageAdapter, StorageError,
    StorageHealth,
};

/// Redis 快取／暫存。
#[derive(Debug, Clone)]
pub struct RedisKeyValueStore {
    client: Client,
}

impl RedisKeyValueStore {
    pub fn connect(url: &str) -> Result<Self, StorageError> {
        let client = Client::open(url).map_err(|err| StorageError::Configuration {
            message: format!(
                "REDIS_URL 無效：{}。請用 redis://127.0.0.1:6379/0 這種格式",
                StorageError::sanitize(&err.to_string())
            ),
        })?;
        Ok(Self { client })
    }

    async fn conn(&self) -> Result<MultiplexedConnection, StorageError> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(map_redis)
    }
}

fn map_redis(err: redis::RedisError) -> StorageError {
    let message = StorageError::sanitize(&err.to_string());
    if err.is_timeout() || message.contains("timeout") {
        StorageError::Timeout {
            backend: "redis",
            message,
        }
    } else if err.is_connection_refusal() || err.is_connection_dropped() || err.is_io_error() {
        StorageError::Unavailable {
            backend: "redis",
            message,
        }
    } else if message.contains("NOAUTH")
        || message.contains("WRONGPASS")
        || message.contains("invalid password")
    {
        StorageError::PermissionDenied { message }
    } else {
        StorageError::Unknown {
            backend: "redis",
            message,
        }
    }
}

fn ttl_millis(ttl: Duration) -> Result<u64, StorageError> {
    let millis = ttl.as_millis();
    if millis == 0 {
        return Err(StorageError::Configuration {
            message: "KeyValueStore TTL 不可為 0。請傳正的 Duration".into(),
        });
    }
    u64::try_from(millis).map_err(|_| StorageError::Configuration {
        message: "TTL 太大，超出 u64 毫秒".into(),
    })
}

#[async_trait]
impl HealthProvider for RedisKeyValueStore {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let mut conn = self.conn().await?;
        let pong: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .map_err(map_redis)?;
        if pong.eq_ignore_ascii_case("PONG") {
            Ok(StorageHealth::ok("redis", "PING PONG"))
        } else {
            Ok(StorageHealth::down("redis", format!("PING 回 `{pong}`")))
        }
    }
}

impl StorageAdapter for RedisKeyValueStore {
    fn descriptor(&self) -> CapabilityDescriptor {
        CapabilityDescriptor::new("redis", env!("CARGO_PKG_VERSION"), &["key_value"])
    }
}

#[async_trait]
impl KeyValueStore for RedisKeyValueStore {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        let mut conn = self.conn().await?;
        conn.get(key).await.map_err(map_redis)
    }

    async fn set(&self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let mut conn = self.conn().await?;
        let _: () = conn.set(key, value).await.map_err(map_redis)?;
        Ok(())
    }

    async fn set_ex(&self, key: &str, value: &[u8], ttl: Duration) -> Result<(), StorageError> {
        let mut conn = self.conn().await?;
        let _: () = redis::cmd("PSETEX")
            .arg(key)
            .arg(ttl_millis(ttl)?)
            .arg(value)
            .query_async(&mut conn)
            .await
            .map_err(map_redis)?;
        Ok(())
    }

    async fn del(&self, key: &str) -> Result<bool, StorageError> {
        let mut conn = self.conn().await?;
        let deleted: i64 = conn.del(key).await.map_err(map_redis)?;
        Ok(deleted > 0)
    }

    async fn expire(&self, key: &str, ttl: Duration) -> Result<bool, StorageError> {
        let mut conn = self.conn().await?;
        let ok: i64 = redis::cmd("PEXPIRE")
            .arg(key)
            .arg(ttl_millis(ttl)?)
            .query_async(&mut conn)
            .await
            .map_err(map_redis)?;
        Ok(ok == 1)
    }
}
