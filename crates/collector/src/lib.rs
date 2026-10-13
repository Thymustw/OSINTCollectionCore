//! collector 排程服務。
//!
//! 讀 `enabled` connector，依 cron 觸發。匯入專用類型與空 schedule 直接略過、
//! 不打 warn；真正的錯誤只在狀態改變時打 warn／error。同時執行數受全域與
//! per-domain semaphore 限制。

mod bounds;
mod error;
mod health;
mod registry;
mod runner;
mod schedule;

pub use bounds::RunBounds;
pub use error::CollectorError;
pub use health::serve as serve_health;
pub use registry::{KnownConnectorKind, classify_connector_type, is_import_only_type};
pub use runner::{CollectOutcome, CollectorRunner};
pub use schedule::{is_due, is_unscheduled, parse_cron};
