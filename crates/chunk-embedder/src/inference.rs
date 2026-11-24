//! osint-inference 的 HTTP client（`POST /v1/embeddings`，OpenAI 相容子集）。
//!
//! 錯誤分兩類（CLAUDE.md 全域慣例：暫時性退避、永久性立刻拋）：
//! - [`InferenceError::Retryable`]：503（佇列滿）、連線失敗、逾時、5xx。呼叫端退避後重試同一批。
//! - [`InferenceError::Permanent`]：400／422（超過 512 token、筆數超限）、回應格式不符。
//!   重試只會得到同樣的結果；記 failed_event，不要卡住消費。

use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum InferenceError {
    #[error("推論服務暫時不可用（{reason}），退避後重試")]
    Retryable {
        reason: String,
        retry_after: Option<Duration>,
    },
    #[error("推論請求被拒絕（{status}）：{message}。重試不會改變結果")]
    Permanent { status: u16, message: String },
}

#[derive(Deserialize)]
struct Response {
    model: String,
    data: Vec<Item>,
}

#[derive(Deserialize)]
struct Item {
    index: usize,
    embedding: Vec<f32>,
}

#[derive(Clone)]
pub struct InferenceClient {
    http: reqwest::Client,
    url: String,
}

impl InferenceClient {
    pub fn new(base_url: &str, timeout: Duration) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .build()
            .map_err(|err| format!("建立推論 HTTP client 失敗：{err}"))?;
        Ok(Self {
            http,
            url: format!("{}/v1/embeddings", base_url.trim_end_matches('/')),
        })
    }

    /// 回傳 (模型名稱, 每筆向量，順序與 `inputs` 相同)。
    pub async fn embed(
        &self,
        inputs: &[String],
        dimensions: usize,
    ) -> Result<(String, Vec<Vec<f32>>), InferenceError> {
        let resp = self
            .http
            .post(&self.url)
            .json(&json!({ "input": inputs }))
            .send()
            .await
            .map_err(|err| InferenceError::Retryable {
                reason: format!("連線或逾時：{err}"),
                retry_after: None,
            })?;
        let status = resp.status();
        if status.as_u16() == 503 || status.is_server_error() || status.as_u16() == 429 {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs);
            return Err(InferenceError::Retryable {
                reason: format!("HTTP {status}"),
                retry_after,
            });
        }
        if !status.is_success() {
            let message = resp.text().await.unwrap_or_default();
            return Err(InferenceError::Permanent {
                status: status.as_u16(),
                message: message.chars().take(500).collect(),
            });
        }
        let body: Response = resp.json().await.map_err(|err| InferenceError::Permanent {
            status: status.as_u16(),
            message: format!("回應不是預期的 embeddings 格式：{err}"),
        })?;
        if body.data.len() != inputs.len() {
            return Err(InferenceError::Permanent {
                status: status.as_u16(),
                message: format!("送 {} 筆、回 {} 筆", inputs.len(), body.data.len()),
            });
        }
        let mut out = vec![Vec::new(); inputs.len()];
        for item in body.data {
            if item.index >= out.len() || item.embedding.len() != dimensions {
                return Err(InferenceError::Permanent {
                    status: status.as_u16(),
                    message: format!(
                        "第 {} 筆向量 {} 維（預期 {dimensions}）或 index 越界",
                        item.index,
                        item.embedding.len()
                    ),
                });
            }
            out[item.index] = item.embedding;
        }
        if out.iter().any(Vec::is_empty) {
            return Err(InferenceError::Permanent {
                status: status.as_u16(),
                message: "回應的 index 有重複，部分輸入沒有向量".into(),
            });
        }
        Ok((body.model, out))
    }
}
