//! 測試用 [`LlmProvider`] 實作。比照
//! `storage_core::mock::MockEmbeddingProvider` 的設計慣例：
//! `unsupported()` 建構子、`Arc<AtomicU64>` 記呼叫次數。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{AiGatewayError, ChatCompletionRequest, ChatCompletionResponse, LlmProvider};

/// 可設定固定回應的假 LLM。三種模式：
/// - `always_same_entity(true)` / `always_same_entity(false)`：忽略輸入，
///   固定回 `{"same_entity": <bool>, "reasoning": "mock"}` 格式的 JSON 內容
///   （Step 3 的 prompt 解析邏輯期待這個格式，這裡先給一個能通過解析的假回應，
///   不用等 Step 3 就能測 mock 本身）。
/// - `always_error(AiGatewayError)`：固定回傳指定錯誤，測 Step 3／Step 4 的
///   降級路徑（LLM 失敗 → Pending）。
///
/// 每次呼叫都會讓 `call_count()` 遞增，不管成功或失敗。
#[derive(Clone)]
pub struct MockLlmProvider {
    mode: MockMode,
    call_count: Arc<AtomicU64>,
}

#[derive(Clone)]
enum MockMode {
    SameEntity(bool),
    Error(AiGatewayError),
}

impl MockLlmProvider {
    #[must_use]
    pub fn always_same_entity(same: bool) -> Self {
        Self {
            mode: MockMode::SameEntity(same),
            call_count: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn always_error(err: AiGatewayError) -> Self {
        Self {
            mode: MockMode::Error(err),
            call_count: Arc::new(AtomicU64::new(0)),
        }
    }

    #[must_use]
    pub fn call_count(&self) -> u64 {
        self.call_count.load(Ordering::Relaxed)
    }
}

impl LlmProvider for MockLlmProvider {
    async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, AiGatewayError> {
        self.call_count.fetch_add(1, Ordering::Relaxed);
        match &self.mode {
            MockMode::SameEntity(same) => Ok(ChatCompletionResponse {
                content: format!(r#"{{"same_entity": {same}, "reasoning": "mock"}}"#),
                model: request.model.clone(),
                usage: None,
            }),
            MockMode::Error(err) => Err(err.clone()),
        }
    }
}

/// LLM 未設定／未啟用時注入這個。所有呼叫回 [`AiGatewayError::Unsupported`]，
/// 比照 `storage_core::mock::MockEmbeddingProvider::unsupported()` 的先例：
/// 呼叫端（`resolver::auto_approval`）看到這個錯誤要能正確降級成 `Pending`，
/// 不是把它當成一般的暫時性錯誤去重試。
#[derive(Clone, Default)]
pub struct UnsupportedLlmProvider;

impl UnsupportedLlmProvider {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl LlmProvider for UnsupportedLlmProvider {
    async fn chat_completion(
        &self,
        _request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, AiGatewayError> {
        Err(AiGatewayError::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChatMessage, ChatRole};

    fn sample_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "qwen-primary".to_string(),
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: "same entity?".to_string(),
            }],
            temperature: 0.0,
            max_tokens: 128,
            enable_reasoning: false,
        }
    }

    #[tokio::test]
    async fn always_same_entity_true_parses_as_json_true() {
        let provider = MockLlmProvider::always_same_entity(true);
        let response = provider
            .chat_completion(&sample_request())
            .await
            .expect("mock 成功路徑不該失敗");
        assert_eq!(response.model, "qwen-primary");
        let value: serde_json::Value =
            serde_json::from_str(&response.content).expect("mock 回應必須是合法 JSON");
        assert_eq!(value["same_entity"], true);
        assert_eq!(value["reasoning"], "mock");
    }

    #[tokio::test]
    async fn always_same_entity_false_parses_as_json_false() {
        let provider = MockLlmProvider::always_same_entity(false);
        let response = provider
            .chat_completion(&sample_request())
            .await
            .expect("mock 成功路徑不該失敗");
        let value: serde_json::Value =
            serde_json::from_str(&response.content).expect("mock 回應必須是合法 JSON");
        assert_eq!(value["same_entity"], false);
    }

    #[tokio::test]
    async fn always_error_returns_the_configured_error() {
        let expected = AiGatewayError::Transient {
            message: "逾時".to_string(),
        };
        let provider = MockLlmProvider::always_error(expected.clone());
        let actual = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("always_error 必須回傳錯誤");
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn call_count_includes_success_and_failure() {
        let success = MockLlmProvider::always_same_entity(true);
        let failure = MockLlmProvider::always_error(AiGatewayError::Permanent {
            message: "401".to_string(),
        });
        assert_eq!(success.call_count(), 0);
        assert_eq!(failure.call_count(), 0);

        success.chat_completion(&sample_request()).await.unwrap();
        success.chat_completion(&sample_request()).await.unwrap();
        let _ = failure.chat_completion(&sample_request()).await;
        let _ = failure.chat_completion(&sample_request()).await;
        let _ = failure.chat_completion(&sample_request()).await;

        assert_eq!(success.call_count(), 2);
        assert_eq!(failure.call_count(), 3);
    }

    #[tokio::test]
    async fn unsupported_provider_always_returns_unsupported() {
        let provider = UnsupportedLlmProvider::new();
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("未啟用的 LLM 必須回 Unsupported");
        assert_eq!(err, AiGatewayError::Unsupported);
        assert_eq!(err.to_string(), "LLM 功能未啟用");
    }
}
