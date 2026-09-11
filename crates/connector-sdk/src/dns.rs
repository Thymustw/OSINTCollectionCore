//! DNS 解析。SSRF 路徑只 resolve 一次，連線用解析到的 IP。

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::ConnectorError;

/// 可替換的 hostname 解析器。測試可注入固定／計數實作。
#[async_trait]
pub trait HostResolver: Send + Sync {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, ConnectorError>;
}

/// 系統 DNS（`tokio::net::lookup_host`）。
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemResolver;

#[async_trait]
impl HostResolver for SystemResolver {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, ConnectorError> {
        let lookup = format!("{host}:0");
        let addrs = tokio::net::lookup_host(&lookup)
            .await
            .map_err(|err| ConnectorError::Dns {
                host: host.to_string(),
                message: err.to_string(),
            })?;
        let ips: Vec<IpAddr> = addrs.map(|addr: SocketAddr| addr.ip()).collect();
        if ips.is_empty() {
            return Err(ConnectorError::DnsEmpty {
                host: host.to_string(),
            });
        }
        Ok(ips)
    }
}

/// 測試用：host → IP 對照。每次 resolve 會記次數。
#[derive(Debug, Default)]
pub struct MapResolver {
    map: HashMap<String, Vec<IpAddr>>,
    counts: Mutex<HashMap<String, u32>>,
}

impl MapResolver {
    #[must_use]
    pub fn new(map: HashMap<String, Vec<IpAddr>>) -> Self {
        let map = map
            .into_iter()
            .map(|(host, ips)| (host.to_ascii_lowercase(), ips))
            .collect();
        Self {
            map,
            counts: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(&mut self, host: impl Into<String>, ips: Vec<IpAddr>) {
        self.map.insert(host.into().to_ascii_lowercase(), ips);
    }

    #[must_use]
    pub fn resolve_count(&self, host: &str) -> u32 {
        let host = host.to_ascii_lowercase();
        self.counts
            .lock()
            .expect("resolver counts")
            .get(&host)
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait]
impl HostResolver for MapResolver {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, ConnectorError> {
        let key = host.trim().trim_end_matches('.').to_ascii_lowercase();
        {
            let mut counts = self.counts.lock().expect("resolver counts");
            *counts.entry(key.clone()).or_insert(0) += 1;
        }
        self.map
            .get(&key)
            .cloned()
            .ok_or_else(|| ConnectorError::Dns {
                host: host.to_string(),
                message: "測試 resolver 沒有這個 host。請在 MapResolver 先 insert".into(),
            })
    }
}
