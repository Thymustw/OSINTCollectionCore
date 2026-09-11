//! 全域與 per-domain 同時執行數上限。這層是「同時幾個在跑」，不是速率限制。

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// collector 併發上限。
#[derive(Clone)]
pub struct RunBounds {
    global: Arc<Semaphore>,
    global_limit: u32,
    per_domain: Arc<tokio::sync::Mutex<HashMap<String, Arc<Semaphore>>>>,
    per_domain_limit: u32,
}

impl RunBounds {
    #[must_use]
    pub fn new(global_inflight: u32, per_domain_inflight: u32) -> Self {
        let global_limit = global_inflight.max(1);
        let per_domain_limit = per_domain_inflight.max(1);
        Self {
            global: Arc::new(Semaphore::new(global_limit as usize)),
            global_limit,
            per_domain: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            per_domain_limit,
        }
    }

    #[must_use]
    pub fn global_limit(&self) -> u32 {
        self.global_limit
    }

    #[must_use]
    pub fn per_domain_limit(&self) -> u32 {
        self.per_domain_limit
    }

    #[must_use]
    pub fn global_available(&self) -> usize {
        self.global.available_permits()
    }

    async fn domain_sem(&self, domain: &str) -> Arc<Semaphore> {
        let key = domain.trim().trim_end_matches('.').to_ascii_lowercase();
        let mut map = self.per_domain.lock().await;
        map.entry(key)
            .or_insert_with(|| Arc::new(Semaphore::new(self.per_domain_limit as usize)))
            .clone()
    }

    /// 同時取得全域與 domain permit。drop 後釋放。
    pub async fn acquire(&self, domain: &str) -> RunPermit {
        let global = self
            .global
            .clone()
            .acquire_owned()
            .await
            .expect("collector 全域 semaphore 不會關閉");
        let domain_sem = self.domain_sem(domain).await;
        let domain = domain_sem
            .acquire_owned()
            .await
            .expect("collector domain semaphore 不會關閉");
        RunPermit { global, domain }
    }
}

/// 持有期間佔用一個全域 slot 與一個 domain slot。
pub struct RunPermit {
    #[allow(dead_code)]
    global: OwnedSemaphorePermit,
    #[allow(dead_code)]
    domain: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[tokio::test]
    async fn global_limit_is_enforced() {
        let bounds = RunBounds::new(2, 8);
        assert_eq!(bounds.global_limit(), 2);
        let peak = Arc::new(AtomicUsize::new(0));
        let current = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..6 {
            let bounds = bounds.clone();
            let peak = peak.clone();
            let current = current.clone();
            handles.push(tokio::spawn(async move {
                let _permit = bounds.acquire("a.example.invalid").await;
                let now = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(30)).await;
                current.fetch_sub(1, Ordering::SeqCst);
            }));
        }
        for handle in handles {
            handle.await.expect("join");
        }
        let seen = peak.load(Ordering::SeqCst);
        assert!(seen <= 2, "同時執行數應 ≤ 2，實際 {seen}");
        assert!(seen >= 1, "應至少跑過一次");
    }

    #[tokio::test]
    async fn per_domain_limit_is_independent() {
        let bounds = RunBounds::new(8, 1);
        let peak_a = Arc::new(AtomicUsize::new(0));
        let cur_a = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for i in 0..4 {
            let bounds = bounds.clone();
            let peak_a = peak_a.clone();
            let cur_a = cur_a.clone();
            let domain = if i < 2 {
                "a.example.invalid"
            } else {
                "b.example.invalid"
            };
            handles.push(tokio::spawn(async move {
                let _permit = bounds.acquire(domain).await;
                if domain.starts_with('a') {
                    let now = cur_a.fetch_add(1, Ordering::SeqCst) + 1;
                    peak_a.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(30)).await;
                    cur_a.fetch_sub(1, Ordering::SeqCst);
                } else {
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }
            }));
        }
        for handle in handles {
            handle.await.expect("join");
        }
        let seen = peak_a.load(Ordering::SeqCst);
        assert!(seen <= 1, "同一 domain 同時執行數應 ≤ 1，實際 {seen}");
    }
}
