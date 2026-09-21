//! LLM 生成式推論的抽象層（ADR-012 AI 輔助自動核准）。
//!
//! **這不是 V0.3 完整的 AI Gateway**（多模型路由、prompt 管理、NER、enrichment）。
//! 只提前借用其中最小的一塊：單一 OpenAI 相容 endpoint 的 chat completion。
//! 見 ADR-012「借用 V0.3 AI Gateway 的範圍界線」。
//!
//! V0.3 Phase 1 Step A 補上 retry／rate limit／cost accounting／model
//! registry／redaction／logging 六項 SPEC_V0.3 §3 要求的能力。prompt
//! version／model version 是呼叫端（`resolver::auto_approval`）的概念，
//! 留給 Phase 1 Step B。

mod cost;
mod mock;
mod openai;
mod redact;
mod registry;

pub use cost::Pricing;
pub use mock::{MockLlmProvider, UnsupportedLlmProvider};
pub use openai::{OpenAiCompatibleLlmProvider, OpenAiCompatibleLlmProviderConfig};
pub use redact::{redact_credentials, truncate};
pub use registry::ModelRegistry;

/// LLM 生成式推論的統一介面。呼叫端用泛型 `<L: LlmProvider>`，不用 `dyn`
/// （比照 `storage_core::EmbeddingProvider` 的用法）。
///
/// 刻意用原生 `async fn`、不用 `async-trait`。這個 trait 不會當 `dyn` 物件用，
/// 因此允許 `async_fn_in_trait`（該 lint 是為物件安全／`Send` 邊界無法在 dyn
/// 上指定而設的，這裡不適用）。
#[allow(async_fn_in_trait)]
pub trait LlmProvider: Send + Sync {
    /// 送出一個 chat completion 請求，回傳模型的文字回應。
    ///
    /// 失敗時呼叫端（`resolver::auto_approval`，Step 3）一律退回 `Pending`，
    /// 不會因為這裡失敗而阻擋任何 ingestion（CLAUDE.md §5「AI failure must
    /// not block base ingestion」）。
    async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, AiGatewayError>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    pub temperature: f64,
    pub max_tokens: u32,
    /// 是否允許模型展開推理過程（部分本地模型走 chain-of-thought，
    /// 回應會多一個 `reasoning` 欄位，`false` 時關閉這個行為）。
    ///
    /// 呼叫端**明確宣告**，不是由 task_type 自動推導——輸入已經結構化、
    /// 模型只需要做直接判斷的呼叫（例如 entity resolution 的二元
    /// same_entity 判斷）應該傳 `false`：更快、更省 token，且不需要
    /// 推理過程。輸入是未經抽取的自由文字、需要模型先拆解問題才能回答
    /// 的呼叫（例如從一段文章抽取實體/關係/事件）應該傳 `true`。
    ///
    /// 對不支援這個概念的 endpoint（不走 reasoning 的一般模型）沒有影響
    /// ——`OpenAiCompatibleLlmProvider` 只在 `false` 時額外送一個
    /// `chat_template_kwargs` 欄位，多數 OpenAI 相容 server 會忽略不認識
    /// 的欄位；`true` 時不送這個欄位，使用 endpoint 的預設行為。
    pub enable_reasoning: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatRole {
    System,
    User,
    Assistant,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ChatCompletionResponse {
    pub content: String,
    pub model: String,
    pub usage: Option<TokenUsage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
}

/// LLM 呼叫失敗的分類。命名與語意刻意跟 `StorageError` 分開（LLM 不是
/// storage capability，錯誤成因也不同：沒有「CorruptionSuspected」這種
/// 概念，但多了「回應格式不符預期」）。
#[derive(Debug, thiserror::Error, Clone, PartialEq)]
pub enum AiGatewayError {
    /// 暫時性：逾時、503、429。呼叫端可決定要不要重試（Step 3 的預設是不重試，
    /// 見 `AutoApprovalLlmSection::max_retries` 預設 0）。
    #[error("LLM 暫時性錯誤：{message}")]
    Transient { message: String },
    /// 永久性：401、400、模型不存在。重試無意義。
    #[error("LLM 永久性錯誤：{message}")]
    Permanent { message: String },
    /// 回應不是預期格式（不是 HTTP 層錯誤，是解析失敗）。
    #[error("LLM 回應格式不符預期：{message}")]
    InvalidResponse { message: String },
    /// LLM 功能未設定／未啟用。`UnsupportedLlmProvider` 一律回這個。
    #[error("LLM 功能未啟用")]
    Unsupported,
}
