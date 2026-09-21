//! OpenAI Chat Completions 相容的 HTTP client（ADR-012 Step 2）。
//!
//! `enabled == false` 時不建立 [`reqwest::Client`]，[`LlmProvider::chat_completion`]
//! 直接回 [`AiGatewayError::Unsupported`]，不會發出任何 HTTP 請求。
//!
//! 建構時**不做連線探活**——連不通的錯誤留給第一次呼叫，並分類成
//! [`AiGatewayError::Transient`]（比照 EmbeddingProvider 連線失敗時的降級精神）。
//!
//! # 格式假設未經真實環境驗證
//!
//! 本模組的單元測試全部打 wiremock 假 server，**沒有**對真實 vLLM／llama.cpp
//! 的 `/v1/chat/completions` 做過實測（本機沒有跑起來的推論服務）。請求／回應
//! 欄位依 OpenAI Chat Completions 公開格式組裝；若真實 serving 路徑回的是
//! 多段 `content` 陣列或其他變體，會被當成 [`AiGatewayError::InvalidResponse`]。

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tokio::sync::Mutex as AsyncMutex;

use crate::redact::{redact_credentials, truncate};
use crate::{
    AiGatewayError, ChatCompletionRequest, ChatCompletionResponse, ChatRole, LlmProvider,
    TokenUsage,
};

/// 組裝 [`OpenAiCompatibleLlmProvider`] 用的建構參數。
///
/// 這個 crate **不依賴** `core-config`，避免循環相依。呼叫端（Step 4 的
/// `core-api` 組裝）把 `AutoApprovalLlmSection` 的欄位轉過來即可。
///
/// `model`／`temperature`／`max_tokens` 走 [`ChatCompletionRequest`]，這裡只留
/// client 連線、並發控制、重試、限流需要的欄位。
#[derive(Debug, Clone)]
pub struct OpenAiCompatibleLlmProviderConfig {
    /// `false` 時不建 HTTP client，所有呼叫回 [`AiGatewayError::Unsupported`]。
    pub enabled: bool,
    /// OpenAI 相容 endpoint 的 base URL，例如 `http://ai-inference:8000/v1`。
    /// 實際請求打 `{base_url}/chat/completions`（會去掉尾端 `/`）。
    pub base_url: String,
    /// 單次 HTTP 請求逾時（含連線 + 讀 body）。
    pub timeout: Duration,
    /// 同時進行的 LLM 推論上限（`tokio::sync::Semaphore`）。
    /// `0` 會被當成 `1`，避免 semaphore 永遠發不出 permit。
    pub max_concurrent: usize,
    /// 除了第一次嘗試外，[`AiGatewayError::Transient`] 最多重試幾次。
    /// `0`＝不重試（V0.3 Phase 1 Step A 之前的既有行為）。
    /// `Permanent`／`InvalidResponse`／`Unsupported` 一律不重試——重試對
    /// 這三類錯誤沒有意義，白白多等一個逾時。
    pub max_retries: usize,
    /// 全域請求速率上限（次/秒），跟 `max_concurrent` 是兩個獨立機制：
    /// `max_concurrent` 限制「同時有幾個請求在飛」，這個限制「多快能發出
    /// 下一個請求」。`None` 代表不限制（只靠 `max_concurrent` 節流，
    /// V0.3 Phase 1 Step A 之前的既有行為）。
    pub rate_limit_per_second: Option<f64>,
}

/// OpenAI 相容 chat completion 的生產實作。
///
/// `inner` 為 `None` 代表未啟用（或 `enabled == false`），不是「client 建失敗」——
/// `reqwest::Client` 建不起來時仍會留下一個 Inner，第一次呼叫再以 Transient／
/// Permanent 失敗（建構永遠成功）。
#[derive(Clone)]
pub struct OpenAiCompatibleLlmProvider {
    inner: Option<Inner>,
}

#[derive(Clone)]
struct Inner {
    client: reqwest::Client,
    base_url: String,
    semaphore: Arc<tokio::sync::Semaphore>,
    max_retries: usize,
    rate_limiter: Option<RateLimiter>,
}

/// 簡單的固定間隔限流器：`next_allowed` 記錄下一個請求最早能出發的時間，
/// 每次請求把它往後推一個 `interval`。跟 `tokio::sync::Semaphore` 不同——
/// semaphore 限制「同時幾個」，這個限制「間隔多久一個」，兩者疊加使用。
#[derive(Clone)]
struct RateLimiter {
    interval: Duration,
    next_allowed: Arc<AsyncMutex<Instant>>,
}

impl RateLimiter {
    fn new(per_second: f64) -> Self {
        // per_second <= 0 視同極慢（每次間隔拉到最大合理值），不整個 panic
        // 或除以零——設定打錯值時退化成「很慢」比「直接崩潰」對生產環境友善。
        let per_second = if per_second > 0.0 { per_second } else { 0.001 };
        Self {
            interval: Duration::from_secs_f64(1.0 / per_second),
            next_allowed: Arc::new(AsyncMutex::new(Instant::now())),
        }
    }

    async fn wait_turn(&self) {
        let mut next = self.next_allowed.lock().await;
        let now = Instant::now();
        if *next > now {
            tokio::time::sleep(*next - now).await;
        }
        *next = next.max(now) + self.interval;
    }
}

impl OpenAiCompatibleLlmProvider {
    /// `enabled == false` 時回傳一個永遠回 Unsupported 的 provider（不建
    /// `reqwest::Client`）。`enabled == true` 時建立真的 client。
    ///
    /// 這裡不做連線探活。
    #[must_use]
    pub fn new(config: &OpenAiCompatibleLlmProviderConfig) -> Self {
        if !config.enabled {
            return Self { inner: None };
        }

        let timeout = config.timeout;
        let client = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let permits = config.max_concurrent.max(1);
        Self {
            inner: Some(Inner {
                client,
                base_url: config.base_url.clone(),
                semaphore: Arc::new(tokio::sync::Semaphore::new(permits)),
                max_retries: config.max_retries,
                rate_limiter: config.rate_limit_per_second.map(RateLimiter::new),
            }),
        }
    }
}

impl LlmProvider for OpenAiCompatibleLlmProvider {
    async fn chat_completion(
        &self,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, AiGatewayError> {
        let Some(inner) = self.inner.as_ref() else {
            return Err(AiGatewayError::Unsupported);
        };

        tracing::debug!(
            model = %request.model,
            message_count = request.messages.len(),
            "LLM 呼叫開始"
        );

        let mut attempt = 0usize;
        loop {
            match inner.send_once(request).await {
                Ok(response) => {
                    if attempt > 0 {
                        tracing::info!(attempt, model = %request.model, "LLM 呼叫重試後成功");
                    } else {
                        tracing::debug!(
                            model = %response.model,
                            tokens = ?response.usage,
                            "LLM 呼叫成功"
                        );
                    }
                    return Ok(response);
                }
                Err(AiGatewayError::Transient { message }) if attempt < inner.max_retries => {
                    let backoff = retry_backoff(attempt);
                    tracing::warn!(
                        attempt,
                        max_retries = inner.max_retries,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %redact_credentials(&message),
                        "LLM 暫時性錯誤，退避後重試"
                    );
                    tokio::time::sleep(backoff).await;
                    attempt += 1;
                }
                Err(err) => {
                    if attempt > 0 {
                        tracing::error!(
                            attempt,
                            error = %err,
                            "LLM 呼叫重試耗盡仍失敗"
                        );
                    }
                    return Err(err);
                }
            }
        }
    }
}

impl Inner {
    /// 單次嘗試：取 semaphore permit、等限流輪到、發 HTTP 請求、解析回應。
    /// 不含重試邏輯——重試迴圈在 [`LlmProvider::chat_completion`]。
    async fn send_once(
        &self,
        request: &ChatCompletionRequest,
    ) -> Result<ChatCompletionResponse, AiGatewayError> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| AiGatewayError::Permanent {
                message: "LLM 並發限制 semaphore 已關閉（這不該發生；請重啟服務）".to_string(),
            })?;

        if let Some(limiter) = &self.rate_limiter {
            limiter.wait_turn().await;
        }

        let url = chat_completions_url(&self.base_url);
        let mut payload = json!({
            "model": request.model,
            "messages": request.messages.iter().map(|m| {
                json!({
                    "role": role_as_str(m.role),
                    "content": m.content,
                })
            }).collect::<Vec<_>>(),
            "temperature": request.temperature,
            "max_tokens": request.max_tokens,
        });
        // `false` 才顯式關閉 thinking；`true` 不加這個欄位，維持跟舊版
        // 完全相同的 payload 形狀（多數 endpoint 預設就是開 thinking）。
        if !request.enable_reasoning {
            payload["chat_template_kwargs"] = json!({"enable_thinking": false});
        }
        let body = serde_json::to_vec(&payload).map_err(|err| AiGatewayError::Permanent {
            message: format!("序列化 LLM 請求失敗：{err}"),
        })?;

        let response = self
            .client
            .post(&url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(classify_reqwest)?;

        let status = response.status();
        let text = response.text().await.map_err(classify_reqwest)?;

        if !status.is_success() {
            return Err(classify_http(status.as_u16(), &text));
        }

        parse_chat_completion(&text, &request.model)
    }
}

/// 退避時間：100ms, 200ms, 400ms, ... 上限 5s。`attempt` 是第幾次重試
/// （從 0 起算，即第一次重試用 100ms）。
fn retry_backoff(attempt: usize) -> Duration {
    let millis = 100u64.saturating_mul(1u64 << attempt.min(6));
    Duration::from_millis(millis.min(5_000))
}

fn role_as_str(role: ChatRole) -> &'static str {
    match role {
        ChatRole::System => "system",
        ChatRole::User => "user",
        ChatRole::Assistant => "assistant",
    }
}

fn chat_completions_url(base_url: &str) -> String {
    format!("{}/chat/completions", base_url.trim_end_matches('/'))
}

fn classify_reqwest(err: reqwest::Error) -> AiGatewayError {
    // 刻意不用 `err.to_string()`：reqwest 的 Display 會帶完整 URL。
    if err.is_timeout() {
        return AiGatewayError::Transient {
            message: "LLM 請求逾時。請稍後再試，或把 auto_approval.llm.timeout_secs 調大"
                .to_string(),
        };
    }
    if err.is_connect() {
        return AiGatewayError::Transient {
            message:
                "連不上 LLM endpoint。請確認 auto_approval.llm.base_url 指向的服務有在跑，稍後再試"
                    .to_string(),
        };
    }
    AiGatewayError::Permanent {
        message: "LLM HTTP 傳輸失敗（非逾時、非連線錯誤）。請檢查 auto_approval.llm 設定"
            .to_string(),
    }
}

fn classify_http(status: u16, body: &str) -> AiGatewayError {
    let summary = truncate(&redact_credentials(body), 500);
    match status {
        429 | 502 | 503 => AiGatewayError::Transient {
            message: format!(
                "LLM endpoint 回 HTTP {status}（暫時性）。請稍後再試。回應摘要：{summary}"
            ),
        },
        400 | 401 | 403 => AiGatewayError::Permanent {
            message: format!(
                "LLM endpoint 回 HTTP {status}（永久性，重試無意義）。\
                 請檢查 auto_approval.llm.base_url／model。回應摘要：{summary}"
            ),
        },
        _ => AiGatewayError::Permanent {
            message: format!(
                "LLM endpoint 回 HTTP {status}。請檢查 auto_approval.llm 設定。回應摘要：{summary}"
            ),
        },
    }
}

fn parse_chat_completion(
    text: &str,
    fallback_model: &str,
) -> Result<ChatCompletionResponse, AiGatewayError> {
    let value: Value = serde_json::from_str(text).map_err(|_| AiGatewayError::InvalidResponse {
        message: format!(
            "LLM 回應不是合法 JSON。請確認 auto_approval.llm.base_url 指向 OpenAI 相容的 /v1\
             （vLLM 或 llama.cpp server）。回應摘要：{}",
            truncate(&redact_credentials(text), 500)
        ),
    })?;

    let content = value
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .ok_or_else(|| AiGatewayError::InvalidResponse {
            message: format!(
                "LLM 回應缺少 choices[0].message.content。請確認 endpoint 是 OpenAI Chat \
                 Completions 格式（字串 content，不是多段陣列）。回應摘要：{}",
                truncate(&redact_credentials(text), 500)
            ),
        })?;

    let model = value
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or(fallback_model)
        .to_string();

    let usage = value.get("usage").and_then(|usage| {
        let prompt_tokens = usage.get("prompt_tokens").and_then(Value::as_u64)?;
        let completion_tokens = usage.get("completion_tokens").and_then(Value::as_u64)?;
        Some(TokenUsage {
            prompt_tokens: u32::try_from(prompt_tokens).ok()?,
            completion_tokens: u32::try_from(completion_tokens).ok()?,
        })
    });

    Ok(ChatCompletionResponse {
        content: content.to_string(),
        model,
        usage,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;
    use crate::{ChatMessage, ChatRole};

    fn sample_request() -> ChatCompletionRequest {
        ChatCompletionRequest {
            model: "qwen-primary".to_string(),
            messages: vec![
                ChatMessage {
                    role: ChatRole::System,
                    content: "you are a judge".to_string(),
                },
                ChatMessage {
                    role: ChatRole::User,
                    content: "same entity?".to_string(),
                },
            ],
            temperature: 0.0,
            max_tokens: 128,
            enable_reasoning: false,
        }
    }

    fn enabled_config(
        base_url: String,
        timeout: Duration,
        max_concurrent: usize,
    ) -> OpenAiCompatibleLlmProviderConfig {
        OpenAiCompatibleLlmProviderConfig {
            enabled: true,
            base_url,
            timeout,
            max_concurrent,
            max_retries: 0,
            rate_limit_per_second: None,
        }
    }

    fn success_body() -> String {
        json!({
            "id": "chatcmpl-test",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": "same entity yes"},
                "finish_reason": "stop"
            }],
            "model": "qwen-primary",
            "usage": {"prompt_tokens": 10, "completion_tokens": 4}
        })
        .to_string()
    }

    async fn mount_json(server: &MockServer, status: u16, body: &str) {
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(status).set_body_raw(body.to_string(), "application/json"),
            )
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn success_parses_content_and_usage() {
        let server = MockServer::start().await;
        mount_json(&server, 200, &success_body()).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            2,
        ));

        let response = provider
            .chat_completion(&sample_request())
            .await
            .expect("200 + 合法 OpenAI JSON 必須成功");
        assert_eq!(response.content, "same entity yes");
        assert_eq!(response.model, "qwen-primary");
        assert_eq!(
            response.usage,
            Some(TokenUsage {
                prompt_tokens: 10,
                completion_tokens: 4,
            })
        );
    }

    #[tokio::test]
    async fn missing_usage_is_none_not_error() {
        let server = MockServer::start().await;
        let body = json!({
            "choices": [{
                "message": {"role": "assistant", "content": "ok"}
            }],
            "model": "qwen-primary"
        })
        .to_string();
        mount_json(&server, 200, &body).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));

        let response = provider
            .chat_completion(&sample_request())
            .await
            .expect("缺 usage 不該當錯誤");
        assert_eq!(response.content, "ok");
        assert_eq!(response.usage, None);
    }

    #[tokio::test]
    async fn http_429_is_transient() {
        let server = MockServer::start().await;
        mount_json(&server, 429, r#"{"error":"rate limited"}"#).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("429 必須失敗");
        assert!(
            matches!(err, AiGatewayError::Transient { .. }),
            "期望 Transient，實際 {err:?}"
        );
        assert!(!err.to_string().contains(&server.uri()));
    }

    #[tokio::test]
    async fn http_503_is_transient() {
        let server = MockServer::start().await;
        mount_json(&server, 503, r#"{"error":"unavailable"}"#).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("503 必須失敗");
        assert!(matches!(err, AiGatewayError::Transient { .. }));
    }

    #[tokio::test]
    async fn http_502_is_transient() {
        let server = MockServer::start().await;
        mount_json(&server, 502, "bad gateway").await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("502 必須失敗");
        assert!(matches!(err, AiGatewayError::Transient { .. }));
    }

    #[tokio::test]
    async fn http_401_is_permanent() {
        let server = MockServer::start().await;
        mount_json(&server, 401, r#"{"error":"unauthorized"}"#).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("401 必須失敗");
        assert!(
            matches!(err, AiGatewayError::Permanent { .. }),
            "期望 Permanent，實際 {err:?}"
        );
    }

    #[tokio::test]
    async fn http_400_and_500_are_permanent() {
        for status in [400_u16, 403, 500] {
            let server = MockServer::start().await;
            mount_json(&server, status, r#"{"error":"no"}"#).await;
            let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
                format!("{}/v1", server.uri()),
                Duration::from_secs(5),
                1,
            ));
            let err = provider
                .chat_completion(&sample_request())
                .await
                .expect_err("非 2xx 必須失敗");
            assert!(
                matches!(err, AiGatewayError::Permanent { .. }),
                "HTTP {status} 期望 Permanent，實際 {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn timeout_is_transient() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(5))
                    .set_body_raw(success_body(), "application/json"),
            )
            .mount(&server)
            .await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_millis(200),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("client timeout 必須失敗");
        assert!(
            matches!(err, AiGatewayError::Transient { .. }),
            "期望 Transient，實際 {err:?}"
        );
        assert!(err.to_string().contains("逾時"));
        assert!(!err.to_string().contains(&server.uri()));
    }

    #[tokio::test]
    async fn non_json_body_is_invalid_response() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
            .mount(&server)
            .await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("非 JSON 必須失敗");
        assert!(
            matches!(err, AiGatewayError::InvalidResponse { .. }),
            "期望 InvalidResponse，實際 {err:?}"
        );
    }

    #[tokio::test]
    async fn json_missing_choices_is_invalid_response() {
        let server = MockServer::start().await;
        mount_json(&server, 200, r#"{"model":"qwen-primary"}"#).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("缺 choices 必須失敗");
        assert!(matches!(err, AiGatewayError::InvalidResponse { .. }));
        assert!(err.to_string().contains("choices[0].message.content"));
    }

    #[tokio::test]
    async fn disabled_provider_returns_unsupported_and_sends_no_http() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string(success_body()))
            .expect(0)
            .mount(&server)
            .await;

        let provider = OpenAiCompatibleLlmProvider::new(&OpenAiCompatibleLlmProviderConfig {
            enabled: false,
            base_url: format!("{}/v1", server.uri()),
            timeout: Duration::from_secs(5),
            max_concurrent: 2,
            max_retries: 0,
            rate_limit_per_second: None,
        });
        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("未啟用必須回 Unsupported");
        assert_eq!(err, AiGatewayError::Unsupported);

        let received = server.received_requests().await.unwrap_or_default();
        assert_eq!(received.len(), 0, "enabled=false 時不該發出任何 HTTP 請求");
    }

    struct StartRecorder {
        starts: Arc<std::sync::Mutex<Vec<Instant>>>,
        delay: Duration,
        body: String,
    }

    impl Respond for StartRecorder {
        fn respond(&self, _request: &Request) -> ResponseTemplate {
            self.starts
                .lock()
                .expect("start recorder mutex")
                .push(Instant::now());
            ResponseTemplate::new(200)
                .set_delay(self.delay)
                .set_body_raw(self.body.clone(), "application/json")
        }
    }

    #[tokio::test]
    async fn semaphore_caps_in_flight_requests() {
        let server = MockServer::start().await;
        let delay = Duration::from_millis(250);
        let max_concurrent = 2;
        let starts = Arc::new(std::sync::Mutex::new(Vec::new()));

        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(StartRecorder {
                starts: Arc::clone(&starts),
                delay,
                body: success_body(),
            })
            .mount(&server)
            .await;

        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            max_concurrent,
        ));
        let req = sample_request();
        let started = Instant::now();
        let (a, b, c) = tokio::join!(
            provider.chat_completion(&req),
            provider.chat_completion(&req),
            provider.chat_completion(&req),
        );
        let elapsed = started.elapsed();

        a.expect("並發測試的請求應成功");
        b.expect("並發測試的請求應成功");
        c.expect("並發測試的請求應成功");

        let mut observed = starts.lock().expect("start recorder mutex").clone();
        observed.sort();
        assert_eq!(observed.len(), 3, "三個請求都該打到 mock server");
        // 上限 2：第三個 HTTP 請求必須等前兩個之一結束（delay 之後）才能出發。
        let third_lag = observed[2].duration_since(observed[0]);
        assert!(
            third_lag >= delay.saturating_sub(Duration::from_millis(50)),
            "第三個請求在 {third_lag:?} 就出發，semaphore 沒把並發限制在 {max_concurrent}"
        );
        assert!(
            elapsed >= delay + Duration::from_millis(100),
            "semaphore 若沒卡住，三個請求會幾乎同時結束；elapsed {elapsed:?} 太短"
        );
    }

    #[tokio::test]
    async fn retry_succeeds_after_one_transient_failure() {
        let server = MockServer::start().await;
        // 第一次回 503，第二次回成功——用 wiremock 的 up_to_n_times 各掛一個 Mock。
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_raw(success_body(), "application/json"),
            )
            .mount(&server)
            .await;

        let mut config = enabled_config(format!("{}/v1", server.uri()), Duration::from_secs(5), 1);
        config.max_retries = 2;
        let provider = OpenAiCompatibleLlmProvider::new(&config);

        let response = provider
            .chat_completion(&sample_request())
            .await
            .expect("第一次 503、第二次成功，重試後應該成功");
        assert_eq!(response.content, "same entity yes");

        let received = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            received.len(),
            2,
            "應該發出兩次 HTTP 請求（第一次失敗+一次重試）"
        );
    }

    #[tokio::test]
    async fn retry_exhausts_and_returns_last_transient_error() {
        let server = MockServer::start().await;
        mount_json(&server, 503, r#"{"error":"unavailable"}"#).await;

        let mut config = enabled_config(format!("{}/v1", server.uri()), Duration::from_secs(5), 1);
        config.max_retries = 2;
        let provider = OpenAiCompatibleLlmProvider::new(&config);

        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("一直 503，重試耗盡仍應失敗");
        assert!(matches!(err, AiGatewayError::Transient { .. }));

        let received = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            received.len(),
            3,
            "max_retries=2 應該發出 3 次 HTTP 請求（1 次原始 + 2 次重試）"
        );
    }

    #[tokio::test]
    async fn permanent_errors_are_never_retried() {
        let server = MockServer::start().await;
        mount_json(&server, 401, r#"{"error":"unauthorized"}"#).await;

        let mut config = enabled_config(format!("{}/v1", server.uri()), Duration::from_secs(5), 1);
        config.max_retries = 3;
        let provider = OpenAiCompatibleLlmProvider::new(&config);

        let err = provider
            .chat_completion(&sample_request())
            .await
            .expect_err("401 是 Permanent，不該重試");
        assert!(matches!(err, AiGatewayError::Permanent { .. }));

        let received = server.received_requests().await.unwrap_or_default();
        assert_eq!(
            received.len(),
            1,
            "Permanent 錯誤即使 max_retries=3 也只該發一次 HTTP 請求"
        );
    }

    #[tokio::test]
    async fn rate_limit_spaces_out_sequential_requests() {
        let server = MockServer::start().await;
        let starts = Arc::new(std::sync::Mutex::new(Vec::new()));
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(StartRecorder {
                starts: Arc::clone(&starts),
                delay: Duration::from_millis(0),
                body: success_body(),
            })
            .mount(&server)
            .await;

        // max_concurrent 給大值（不讓 semaphore 成為節流瓶頸），只測 rate_limit_per_second。
        let mut config = enabled_config(format!("{}/v1", server.uri()), Duration::from_secs(5), 10);
        config.rate_limit_per_second = Some(10.0); // 100ms 一個
        let provider = OpenAiCompatibleLlmProvider::new(&config);

        let req = sample_request();
        for _ in 0..3 {
            provider
                .chat_completion(&req)
                .await
                .expect("rate limit 測試的請求應成功");
        }

        let observed = starts.lock().expect("start recorder mutex").clone();
        assert_eq!(observed.len(), 3);
        let first_to_third = observed[2].duration_since(observed[0]);
        // 3 個請求、10 次/秒（間隔 100ms）→ 第三個至少比第一個晚 ~200ms。
        assert!(
            first_to_third >= Duration::from_millis(150),
            "rate_limit_per_second=10 應該讓 3 個請求間隔開，實際 {first_to_third:?}"
        );
    }

    #[tokio::test]
    async fn enable_reasoning_false_sends_chat_template_kwargs() {
        let server = MockServer::start().await;
        mount_json(&server, 200, &success_body()).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));

        let mut request = sample_request();
        request.enable_reasoning = false;
        provider
            .chat_completion(&request)
            .await
            .expect("200 + 合法 OpenAI JSON 必須成功");

        let received = server.received_requests().await.unwrap_or_default();
        assert_eq!(received.len(), 1, "應該只發出一次 HTTP 請求");
        let body: serde_json::Value =
            serde_json::from_slice(&received[0].body).expect("送出的 body 必須是合法 JSON");
        assert_eq!(
            body.get("chat_template_kwargs"),
            Some(&json!({"enable_thinking": false})),
            "enable_reasoning=false 必須送 chat_template_kwargs.enable_thinking=false，實際 {body}"
        );
    }

    #[tokio::test]
    async fn enable_reasoning_true_omits_chat_template_kwargs() {
        let server = MockServer::start().await;
        mount_json(&server, 200, &success_body()).await;
        let provider = OpenAiCompatibleLlmProvider::new(&enabled_config(
            format!("{}/v1", server.uri()),
            Duration::from_secs(5),
            1,
        ));

        let mut request = sample_request();
        request.enable_reasoning = true;
        provider
            .chat_completion(&request)
            .await
            .expect("200 + 合法 OpenAI JSON 必須成功");

        let received = server.received_requests().await.unwrap_or_default();
        assert_eq!(received.len(), 1, "應該只發出一次 HTTP 請求");
        let body: serde_json::Value =
            serde_json::from_slice(&received[0].body).expect("送出的 body 必須是合法 JSON");
        assert!(
            body.get("chat_template_kwargs").is_none(),
            "enable_reasoning=true 不該送 chat_template_kwargs，實際 {body}"
        );
        assert!(
            body.get("enable_thinking").is_none(),
            "enable_thinking 不該出現在 payload 頂層，實際 {body}"
        );
    }

    #[test]
    fn chat_completions_url_strips_trailing_slash() {
        assert_eq!(
            chat_completions_url("http://ai-inference:8000/v1/"),
            "http://ai-inference:8000/v1/chat/completions"
        );
        assert_eq!(
            chat_completions_url("http://ai-inference:8000/v1"),
            "http://ai-inference:8000/v1/chat/completions"
        );
    }
}
