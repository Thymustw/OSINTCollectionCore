//! Job 狀態機。SPEC §21 列了狀態但沒寫轉換表；這裡是實作時補的。
//!
//! ```text
//! queued ──► running ──► completed
//!    │          │
//!    │          ├──► failed ──► retrying ──► running
//!    │          ├──► retrying ──► running
//!    │          └──► cancelled
//!    └──► cancelled
//! ```
//!
//! 終態：`completed`、`cancelled`。`failed` 可經 `retrying` 再進 `running`。

use core_model::JobStatus;

/// 是否允許 `from` → `to`。
#[must_use]
pub fn can_transition(from: JobStatus, to: JobStatus) -> bool {
    if from == to {
        return false;
    }
    matches!(
        (from, to),
        (JobStatus::Queued, JobStatus::Running)
            | (JobStatus::Queued, JobStatus::Cancelled)
            | (JobStatus::Running, JobStatus::Completed)
            | (JobStatus::Running, JobStatus::Failed)
            | (JobStatus::Running, JobStatus::Retrying)
            | (JobStatus::Running, JobStatus::Cancelled)
            | (JobStatus::Retrying, JobStatus::Running)
            | (JobStatus::Retrying, JobStatus::Failed)
            | (JobStatus::Retrying, JobStatus::Cancelled)
            | (JobStatus::Failed, JobStatus::Retrying)
            | (JobStatus::Failed, JobStatus::Cancelled)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path() {
        assert!(can_transition(JobStatus::Queued, JobStatus::Running));
        assert!(can_transition(JobStatus::Running, JobStatus::Completed));
        assert!(!can_transition(JobStatus::Completed, JobStatus::Running));
        assert!(!can_transition(JobStatus::Cancelled, JobStatus::Queued));
        assert!(can_transition(JobStatus::Failed, JobStatus::Retrying));
        assert!(can_transition(JobStatus::Retrying, JobStatus::Running));
        assert!(!can_transition(JobStatus::Queued, JobStatus::Completed));
        assert!(!can_transition(JobStatus::Queued, JobStatus::Queued));
    }
}
