//! collector 排程服務。
//!
//! 讀 `enabled` connector，依 cron 觸發；未知 `connector_type` 記 log 後跳過。
//! 同時執行數受全域與 per-domain semaphore 限制。

mod bounds;
mod error;
mod health;
mod registry;
mod runner;
mod schedule;

pub use bounds::RunBounds;
pub use error::CollectorError;
pub use health::serve as serve_health;
pub use registry::{KnownConnectorKind, classify_connector_type};
pub use runner::{CollectOutcome, CollectorRunner};
pub use schedule::{is_due, parse_cron};
