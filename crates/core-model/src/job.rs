use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::JobStatus;
use crate::ids::{JobId, ObjectId};

/// 工作排程（SPEC §21）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    #[serde(rename = "type")]
    pub job_type: String,
    pub status: JobStatus,
    pub correlation_id: Option<ObjectId>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub retry_count: i32,
    pub error: Option<String>,
}
