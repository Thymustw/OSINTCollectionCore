//! 受 SSRF Guard 保護的 HTTP 抓取。每次 hop 都重跑 Guard；連線釘在解析 IP。

use std::collections::BTreeMap;
use std::time::Instant;

use chrono::Utc;
use reqwest::redirect::Policy as RedirectPolicy;
use reqwest::{Client, Method, StatusCode};
use serde_json::{Value, json};
use url::Url;

use crate::ConnectorError;
use crate::rate_limit::DomainRateLimiter;
use crate::ssrf::SsrfGuard;

/// 一次成功（或 304）的回應。
#[derive(Debug, Clone)]
pub struct FetchedResponse {
    pub url: Url,
    pub status: u16,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
    pub elapsed_ms: u64,
}

impl FetchedResponse {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let want = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(&want))
            .map(|(_, v)| v.as_str())
    }

    #[must_use]
    pub fn headers_json(&self) -> Value {
        json!(self.headers)
    }

    #[must_use]
    pub fn is_not_modified(&self) -> bool {
        self.status == StatusCode::NOT_MODIFIED.as_u16()
    }
}

/// 手動跟隨 redirect、釘 IP、限制 body 大小。
pub struct GuardedFetcher {
    guard: SsrfGuard,
    limiter: DomainRateLimiter,
}

impl GuardedFetcher {
    #[must_use]
    pub fn new(guard: SsrfGuard, limiter: DomainRateLimiter) -> Self {
        Self { guard, limiter }
    }

    #[must_use]
    pub fn guard(&self) -> &SsrfGuard {
        &self.guard
    }

    pub async fn get(
        &self,
        url: &str,
        extra_headers: &[(&str, &str)],
    ) -> Result<FetchedResponse, ConnectorError> {
        let mut current = Url::parse(url).map_err(|err| ConnectorError::InvalidUrl {
            url: url.to_string(),
            message: err.to_string(),
        })?;
        let mut hops = 0;
        let started = Instant::now();
        loop {
            let now = Utc::now();
            let decision = self.guard.check(&current, now).await?;
            self.limiter.acquire(&decision.host).await?;

            let client = Client::builder()
                .redirect(RedirectPolicy::none())
                .timeout(self.guard.policy().request_timeout)
                .connect_timeout(self.guard.policy().connect_timeout)
                .resolve(&decision.host, SsrfGuard::pinned_addr(&decision))
                .build()
                .map_err(|err| ConnectorError::Fetch {
                    url: current.to_string(),
                    message: format!("建立 HTTP client 失敗：{err}"),
                })?;

            let mut req = client.request(Method::GET, current.clone());
            for (k, v) in extra_headers {
                req = req.header(*k, *v);
            }

            let response = req
                .send()
                .await
                .map_err(|err| map_reqwest(&current, self.guard.policy().request_timeout, err))?;
            let status = response.status();
            let headers = collect_headers(&response);

            // 304 Not Modified 屬於 3xx（`is_redirection()` 對它也回 true），但它不是
            // redirect——沒有 Location 是正常語意（沿用快取），不能被底下的 redirect
            // 分支當成「redirect 卻缺 Location」而報錯。必須先判斷、排除在 redirect 之外。
            if status == StatusCode::NOT_MODIFIED {
                return Ok(FetchedResponse {
                    url: current,
                    status: status.as_u16(),
                    headers,
                    body: Vec::new(),
                    elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                });
            }

            if status.is_redirection() {
                hops += 1;
                if hops > self.guard.policy().max_redirects {
                    return Err(ConnectorError::TooManyRedirects {
                        limit: self.guard.policy().max_redirects,
                    });
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| ConnectorError::Fetch {
                        url: current.to_string(),
                        message: format!(
                            "HTTP {status} 沒有合法 Location。請檢查來源伺服器的 redirect"
                        ),
                    })?;
                current = current
                    .join(location)
                    .map_err(|err| ConnectorError::InvalidUrl {
                        url: location.to_string(),
                        message: format!("redirect Location 無效：{err}"),
                    })?;
                continue;
            }

            if !status.is_success() {
                return Err(ConnectorError::Fetch {
                    url: current.to_string(),
                    message: format!("HTTP {status}。請確認來源 URL 可讀，或稍後再試"),
                });
            }

            let body =
                read_body(&current, response, self.guard.policy().max_response_bytes).await?;
            return Ok(FetchedResponse {
                url: current,
                status: status.as_u16(),
                headers,
                body,
                elapsed_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
            });
        }
    }
}

fn collect_headers(response: &reqwest::Response) -> BTreeMap<String, String> {
    let mut headers = BTreeMap::new();
    for (k, v) in response.headers() {
        if let Ok(value) = v.to_str() {
            headers.insert(k.as_str().to_ascii_lowercase(), value.to_string());
        }
    }
    headers
}

async fn read_body(
    url: &Url,
    mut response: reqwest::Response,
    limit: u64,
) -> Result<Vec<u8>, ConnectorError> {
    if let Some(len) = response.content_length() {
        if len > limit {
            return Err(ConnectorError::ResponseTooLarge { limit });
        }
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| ConnectorError::Fetch {
            url: url.to_string(),
            message: format!("讀 body 失敗：{err}"),
        })?
    {
        let next = body.len() as u64 + chunk.len() as u64;
        if next > limit {
            return Err(ConnectorError::ResponseTooLarge { limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn map_reqwest(url: &Url, timeout: std::time::Duration, err: reqwest::Error) -> ConnectorError {
    if err.is_timeout() {
        return ConnectorError::Timeout {
            url: url.to_string(),
            timeout,
        };
    }
    ConnectorError::Fetch {
        url: url.to_string(),
        message: err.to_string(),
    }
}
