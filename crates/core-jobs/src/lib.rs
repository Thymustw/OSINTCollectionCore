//! Job 狀態機與派工。儲存走 `RelationalStore`（生產路徑是 Postgres canonical），不直接寫 SQL。

mod error;
mod service;
mod transition;

pub use error::JobError;
pub use service::JobService;
pub use transition::can_transition;
