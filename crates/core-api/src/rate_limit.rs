//! 全域 token bucket。Axum 的 Service 必須 Clone，不能直接用 `tower::limit::RateLimitLayer`。

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::Mutex;

/// 每秒補充 `per_second` 個 token，容量相同。
#[derive(Clone)]
pub struct RateLimiter {
    inner: Arc<Mutex<Bucket>>,
    per_second: f64,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    #[must_use]
    pub fn new(per_second: u32) -> Self {
        let per_second = f64::from(per_second.max(1));
        Self {
            inner: Arc::new(Mutex::new(Bucket {
                tokens: per_second,
                last: Instant::now(),
            })),
            per_second,
        }
    }

    pub async fn try_acquire(&self) -> bool {
        let mut bucket = self.inner.lock().await;
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.per_second).min(self.per_second);
        bucket.last = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// 超出配額回 429。
pub async fn rate_limit(
    axum::extract::State(state): axum::extract::State<crate::state::AppState>,
    request: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, crate::error::ApiError> {
    if state.rate_limiter.try_acquire().await {
        Ok(next.run(request).await)
    } else {
        Err(crate::error::ApiError::new(
            axum::http::StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            format!(
                "超過每秒 {} 次的全域上限。請降低請求頻率或把 config [http].rate_limit_per_second 調大",
                state.rate_limit_per_second
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn allows_then_rejects() {
        let limiter = RateLimiter::new(1);
        assert!(limiter.try_acquire().await);
        assert!(!limiter.try_acquire().await);
        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(limiter.try_acquire().await);
    }
}
