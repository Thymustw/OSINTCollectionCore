use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::AiRunId;

/// SPEC_V0.3 §5 用「至少」列出的 AI task 名稱。跟 `RESOLUTION_METHODS`／
/// `DISCOVERY_METHODS` 同一種設計：參考清單，不是白名單。
pub const AI_TASK_TYPES: [&str; 8] = [
    "summarization",
    "entity_extraction",
    "relationship_extraction",
    "event_extraction",
    "classification",
    "query_expansion",
    "candidate_scoring",
    "source_classification",
];

/// 一次 AI 推論呼叫的完整紀錄（SPEC_V0.3 §4）。
///
/// # AI output 是 derived data（CLAUDE.md §5）
///
/// 這張表**只記錄**，不覆寫 source/raw——任何下游要不要採信 `output` 的內容，
/// 是呼叫端（例如 Discovery Engine、resolver）的判斷，不是這個型別的責任。
///
/// `input_reference`／`output` 用 `serde_json::Value` 而不是 `String`：不同
/// `task_type` 的輸入/輸出形狀差異很大（摘要是一段文字，entity extraction
/// 是一組結構化物件），比照 `ResolutionCandidate::evidence` 的既有慣例用
/// `Value` 保留原始結構，不在型別層假設某一種形狀。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiRun {
    pub id: AiRunId,
    /// 見 [`AI_TASK_TYPES`]。
    pub task_type: String,
    pub provider: String,
    pub model: String,
    pub model_version: String,
    pub prompt_version: String,
    pub input_reference: Value,
    pub output: Value,
    pub confidence: f64,
    pub tokens: i64,
    pub estimated_cost: f64,
    pub duration_ms: i64,
    pub created_at: DateTime<Utc>,
}
