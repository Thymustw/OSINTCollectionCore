use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::JobStatus;
use crate::ids::{JobId, ObjectId};

/// 工作排程（SPEC §21）。
///
/// 沒有 `Eq`：`parameters` 是 [`Value`]（內含 `f64`），只有 `PartialEq`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// 執行參數，語意依 `job_type` 而定。`None` = 無參數（沿用舊行為）。
    #[serde(default)]
    pub parameters: Option<Value>,
}
