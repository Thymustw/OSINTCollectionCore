//! 指數退避。只重試暫時性錯誤。

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::ConnectorError;

/// 指數退避政策。
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// 第 `attempt` 次（從 0）失敗後要等多久；超過次數回 `None`。
    #[must_use]
    pub fn delay_after(&self, attempt: u32) -> Option<Duration> {
        if attempt + 1 >= self.max_attempts {
            return None;
        }
        let factor = 2u32.saturating_pow(attempt);
        let millis = self
            .base_delay
            .as_millis()
            .saturating_mul(u128::from(factor));
        let capped = millis.min(self.max_delay.as_millis());
        Some(Duration::from_millis(capped as u64))
    }

    /// 跑 `op`，暫時性錯誤才退避重試。
    pub async fn run<T, F, Fut>(&self, mut op: F) -> Result<T, ConnectorError>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ConnectorError>>,
    {
        let mut attempt = 0;
        loop {
            match op().await {
                Ok(value) => return Ok(value),
                Err(err) if err.is_retryable() => match self.delay_after(attempt) {
                    Some(delay) => {
                        tracing::warn!(
                            attempt,
                            delay_ms = delay.as_millis() as u64,
                            error = %err,
                            "暫時性錯誤，退避後重試"
                        );
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                    None => {
                        return Err(ConnectorError::RetryExhausted {
                            attempts: attempt + 1,
                            message: err.to_string(),
                        });
                    }
                },
                Err(err) => return Err(err),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[test]
    fn exponential() {
        let p = RetryPolicy {
            max_attempts: 4,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        assert_eq!(p.delay_after(0), Some(Duration::from_millis(100)));
        assert_eq!(p.delay_after(1), Some(Duration::from_millis(200)));
        assert_eq!(p.delay_after(2), Some(Duration::from_millis(400)));
        assert_eq!(p.delay_after(3), None);
    }

    #[tokio::test]
    async fn retries_timeout_then_ok() {
        let n = AtomicU32::new(0);
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        };
        let value = policy
            .run(|| async {
                let i = n.fetch_add(1, Ordering::SeqCst);
                if i < 2 {
                    Err(ConnectorError::Timeout {
                        url: "http://x".into(),
                        timeout: Duration::from_secs(1),
                    })
                } else {
                    Ok(7)
                }
            })
            .await
            .unwrap();
        assert_eq!(value, 7);
        assert_eq!(n.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn hard_deny_not_retried() {
        let n = AtomicU32::new(0);
        let policy = RetryPolicy::default();
        let err = policy
            .run(|| async {
                n.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(ConnectorError::HardDenied {
                    host: "169.254.169.254".into(),
                    detail: "imds".into(),
                })
            })
            .await
            .unwrap_err();
        assert!(matches!(err, ConnectorError::HardDenied { .. }));
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }
}
