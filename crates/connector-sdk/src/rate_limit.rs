//! Per-domain token bucket。

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

use crate::ConnectorError;
use crate::policy::RateLimitConfig;

/// 以 domain 為鍵的 token bucket。
#[derive(Clone)]
pub struct DomainRateLimiter {
    inner: Arc<Mutex<HashMap<String, Bucket>>>,
    config: RateLimitConfig,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl DomainRateLimiter {
    #[must_use]
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// 取不到 token 就回 `RateLimited`，呼叫端用 `RetryPolicy` 等。
    pub async fn acquire(&self, domain: &str) -> Result<(), ConnectorError> {
        let domain = domain.trim().trim_end_matches('.').to_ascii_lowercase();
        let mut map = self.inner.lock().await;
        let now = Instant::now();
        let burst = self.config.burst.max(1.0);
        let per_second = self.config.per_second.max(0.001);
        let bucket = map.entry(domain.clone()).or_insert_with(|| Bucket {
            tokens: burst,
            last: now,
        });
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * per_second).min(burst);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            Err(ConnectorError::RateLimited { domain })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn burst_then_block() {
        let limiter = DomainRateLimiter::new(RateLimitConfig {
            per_second: 1.0,
            burst: 1.0,
        });
        limiter.acquire("example.com").await.unwrap();
        let err = limiter.acquire("example.com").await.unwrap_err();
        assert!(matches!(err, ConnectorError::RateLimited { .. }));
        limiter.acquire("other.com").await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        limiter.acquire("example.com").await.unwrap();
    }
}
