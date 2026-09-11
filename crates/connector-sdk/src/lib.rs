//! Connector SDK：SSRF Guard、Source 政策、抓取與 RawEvidence 落地。
//!
//! IP 分類沿用 `core_security::ssrf`，這裡只加 `NetworkRule` 白名單與實際連線控制。

mod checkpoint;
mod connector;
mod dns;
mod error;
mod evidence;
mod fetch;
mod network_rule;
mod policy;
mod rate_limit;
mod retry;
mod ssrf;

pub use checkpoint::{CheckpointStore, ConnectorCheckpoint, RelationalCheckpointStore};
pub use connector::{
    CollectContext, CollectResult, ConnectorHealth, ConnectorTrait, DiscoverItem, ParsedItem,
};
pub use dns::{HostResolver, MapResolver, SystemResolver};
pub use error::ConnectorError;
pub use evidence::{EvidenceSink, NewRawEvidence, StoreEvidenceSink, sha256_hex, storage_path};
pub use fetch::{FetchedResponse, GuardedFetcher};
pub use network_rule::{MatchingRule, validate_network_rule};
pub use policy::{RateLimitConfig, SourcePolicy};
pub use rate_limit::DomainRateLimiter;
pub use reqwest::Method;
pub use retry::RetryPolicy;
pub use ssrf::{SsrfDecision, SsrfGuard};
