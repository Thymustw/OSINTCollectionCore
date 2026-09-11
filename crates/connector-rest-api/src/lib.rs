//! REST API connector。
//!
//! 抓取走 `connector-sdk` 的 SSRF Guard／rate limit。回應 JSON 整份存成 RawEvidence
//! （`content_type=application/json`）。V0.1 不把 JSON 正規化成 Document。
//!
//! 憑證只從 `credential_reference`（SecretRef）解析，禁止寫進 `configuration`。
//! 分頁預設只抓第一頁（`pagination.max_pages` 預設 1）；多頁時把每頁 JSON 包成 array 存成
//! 單一 RawEvidence，避免一次 collect 寫出無界筆數。

use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use connector_sdk::{
    CheckpointStore, CollectContext, CollectResult, ConnectorCheckpoint, ConnectorError,
    ConnectorHealth, ConnectorTrait, DiscoverItem, EvidenceSink, GuardedFetcher, Method,
    NewRawEvidence, ParsedItem, sha256_hex,
};
use core_config::SecretRef;
use core_model::{Connector, RawEvidence, Source};
use serde::Deserialize;
use serde_json::{Value, json};
use url::Url;

/// REST API connector。
pub struct RestApiConnector<E, C> {
    fetcher: GuardedFetcher,
    evidence: E,
    checkpoints: C,
}

impl<E, C> RestApiConnector<E, C> {
    pub const CONNECTOR_TYPE: &'static str = "rest_api";
    pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    pub fn new(fetcher: GuardedFetcher, evidence: E, checkpoints: C) -> Self {
        Self {
            fetcher,
            evidence,
            checkpoints,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
struct RestApiConfig {
    /// 覆寫 Source.base_url。未填就用 Source.base_url。
    #[serde(default)]
    base_url: Option<String>,
    /// 相對於 base_url 的 path，例如 `/v1/items`。
    #[serde(default)]
    path: Option<String>,
    /// 完整 URL。設定時優先於 base_url + path。
    #[serde(default)]
    url: Option<String>,
    /// HTTP method，預設 GET。
    #[serde(default)]
    method: Option<String>,
    /// 靜態 header（值必須是字面，不可放 token／password）。
    #[serde(default)]
    headers: BTreeMap<String, String>,
    /// 查詢參數。
    #[serde(default)]
    query: BTreeMap<String, String>,
    /// 請求 body（JSON）。只在 POST／PUT／PATCH 送出。
    #[serde(default)]
    body: Option<Value>,
    /// 把 `credential_reference` 解析後的密文塞進哪個 header。
    #[serde(default)]
    auth: Option<AuthConfig>,
    /// 分頁。未填則只抓一頁。
    #[serde(default)]
    pagination: PaginationConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct AuthConfig {
    /// header 名稱，預設 `Authorization`。
    #[serde(default = "default_auth_header")]
    header: String,
    /// 加在密文前面，例如 `Bearer `。預設空字串。
    #[serde(default)]
    prefix: String,
}

fn default_auth_header() -> String {
    "Authorization".into()
}

#[derive(Debug, Clone, Deserialize)]
struct PaginationConfig {
    /// 這次 collect 最多跟幾頁。預設 1（V0.1 有界）。上限 20。
    #[serde(default = "default_max_pages")]
    max_pages: u32,
    /// JSON pointer，指向下一頁 URL。例如 `/links/next`。
    #[serde(default)]
    next_url_pointer: Option<String>,
    /// 用 query 參數分頁時的參數名，例如 `page`。
    #[serde(default)]
    page_param: Option<String>,
    /// `page_param` 的起始值。預設 1。
    #[serde(default = "default_page_start")]
    page_start: u32,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            max_pages: default_max_pages(),
            next_url_pointer: None,
            page_param: None,
            page_start: default_page_start(),
        }
    }
}

fn default_max_pages() -> u32 {
    1
}

fn default_page_start() -> u32 {
    1
}

const FORBIDDEN_HEADER_NAMES: &[&str] = &[
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "api-key",
    "x-auth-token",
];

const FORBIDDEN_VALUE_MARKERS: &[&str] = &[
    "bearer ", "token ", "basic ", "password", "secret", "api_key", "apikey",
];

fn parse_config(connector: &Connector) -> Result<RestApiConfig, ConnectorError> {
    serde_json::from_value(connector.configuration.clone()).map_err(|err| {
        ConnectorError::Policy {
            message: format!(
                "configuration JSON 無法解析：{err}。請提供 base_url／path／method／headers／pagination 物件"
            ),
        }
    })
}

fn reject_inline_secrets(headers: &BTreeMap<String, String>) -> Result<(), ConnectorError> {
    for (name, value) in headers {
        let lower = name.to_ascii_lowercase();
        if FORBIDDEN_HEADER_NAMES.contains(&lower.as_str()) {
            return Err(ConnectorError::Policy {
                message: format!(
                    "configuration.headers 含 `{name}`。憑證請改放 connector.credential_reference（SecretRef，例如 env:API_TOKEN），不要寫進 configuration"
                ),
            });
        }
        let v = value.to_ascii_lowercase();
        if FORBIDDEN_VALUE_MARKERS.iter().any(|m| v.contains(m)) {
            return Err(ConnectorError::Policy {
                message: format!(
                    "configuration.headers.`{name}` 看起來含憑證。請改用 credential_reference + configuration.auth"
                ),
            });
        }
    }
    Ok(())
}

fn parse_method(raw: Option<&str>) -> Result<Method, ConnectorError> {
    let Some(raw) = raw.filter(|s| !s.trim().is_empty()) else {
        return Ok(Method::GET);
    };
    match raw.trim().to_ascii_uppercase().as_str() {
        "GET" => Ok(Method::GET),
        "POST" => Ok(Method::POST),
        "PUT" => Ok(Method::PUT),
        "PATCH" => Ok(Method::PATCH),
        "HEAD" => Ok(Method::HEAD),
        other => Err(ConnectorError::Policy {
            message: format!("HTTP method `{other}` 不支援。請用 GET／POST／PUT／PATCH／HEAD"),
        }),
    }
}

fn join_url(base: &str, path: Option<&str>) -> Result<Url, ConnectorError> {
    let mut url = Url::parse(base).map_err(|err| ConnectorError::InvalidUrl {
        url: base.to_string(),
        message: err.to_string(),
    })?;
    if let Some(path) = path.filter(|p| !p.is_empty()) {
        if path.starts_with("http://") || path.starts_with("https://") {
            return Url::parse(path).map_err(|err| ConnectorError::InvalidUrl {
                url: path.to_string(),
                message: err.to_string(),
            });
        }
        let joined = if path.starts_with('/') {
            url.set_path(path);
            url
        } else {
            url.join(path).map_err(|err| ConnectorError::InvalidUrl {
                url: path.to_string(),
                message: format!("path 無法接到 base_url：{err}"),
            })?
        };
        return Ok(joined);
    }
    Ok(url)
}

fn start_url(source: &Source, cfg: &RestApiConfig) -> Result<Url, ConnectorError> {
    if let Some(url) = cfg.url.as_deref().filter(|u| !u.trim().is_empty()) {
        return Url::parse(url).map_err(|err| ConnectorError::InvalidUrl {
            url: url.to_string(),
            message: err.to_string(),
        });
    }
    let base = cfg
        .base_url
        .as_deref()
        .filter(|u| !u.trim().is_empty())
        .or(source.base_url.as_deref())
        .ok_or_else(|| ConnectorError::Policy {
            message:
                "Source.base_url 與 configuration.base_url／url 都是空的。REST API connector 需要一個 API URL"
                    .into(),
        })?;
    let mut url = join_url(base, cfg.path.as_deref())?;
    {
        let mut pairs = url.query_pairs_mut();
        for (k, v) in &cfg.query {
            pairs.append_pair(k, v);
        }
    }
    Ok(url)
}

fn apply_page_param(url: &mut Url, param: &str, page: u32) {
    let mut pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .filter(|(k, _)| k != param)
        .collect();
    pairs.push((param.to_string(), page.to_string()));
    url.query_pairs_mut().clear();
    url.query_pairs_mut()
        .extend_pairs(pairs.iter().map(|(k, v)| (k.as_str(), v.as_str())));
}

fn next_url_from_pointer(body: &[u8], pointer: &str, current: &Url) -> Option<Url> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let next = value.pointer(pointer)?;
    let raw = next.as_str()?;
    if raw.trim().is_empty() {
        return None;
    }
    current.join(raw).ok().or_else(|| Url::parse(raw).ok())
}

fn resolve_auth_header(
    connector: &Connector,
    cfg: &RestApiConfig,
) -> Result<Option<(String, String)>, ConnectorError> {
    let Some(auth) = &cfg.auth else {
        if connector.credential_reference.is_some() {
            return Err(ConnectorError::Policy {
                message: "設了 credential_reference 但 configuration.auth 是空的。請填 auth.header（例如 Authorization）與可選的 auth.prefix（例如 \"Bearer \"）".into(),
            });
        }
        return Ok(None);
    };
    let Some(reference) = connector.credential_reference.as_deref() else {
        return Err(ConnectorError::Policy {
            message: "configuration.auth 已設定，但 credential_reference 是空的。請填 SecretRef（例如 env:API_TOKEN），不要把 token 寫進 configuration".into(),
        });
    };
    let secret_ref = SecretRef::parse(reference).map_err(|err| ConnectorError::Policy {
        message: format!(
            "credential_reference 不是合法 SecretRef：{err}。請用 env:VAR、file:/path 或 store:locator"
        ),
    })?;
    let secret = secret_ref.resolve().map_err(|err| ConnectorError::Policy {
        message: format!("無法解析憑證：{err}。請確認環境變數或檔案存在"),
    })?;
    let value = format!("{}{secret}", auth.prefix);
    Ok(Some((auth.header.clone(), value)))
}

#[async_trait]
impl<E, C> ConnectorTrait for RestApiConnector<E, C>
where
    E: EvidenceSink,
    C: CheckpointStore,
{
    fn connector_type(&self) -> &'static str {
        Self::CONNECTOR_TYPE
    }

    fn version(&self) -> &'static str {
        Self::VERSION
    }

    async fn discover(&self, source: &Source) -> Result<Vec<DiscoverItem>, ConnectorError> {
        let url = source
            .base_url
            .as_ref()
            .filter(|u| !u.trim().is_empty())
            .ok_or_else(|| ConnectorError::Policy {
                message: "Source.base_url 是空的。請在 Source 填 REST API base URL".into(),
            })?;
        Ok(vec![DiscoverItem {
            url: url.clone(),
            title: Some(source.name.clone()),
        }])
    }

    async fn collect(&self, ctx: &CollectContext) -> Result<CollectResult, ConnectorError> {
        let cfg = parse_config(&ctx.connector)?;
        reject_inline_secrets(&cfg.headers)?;
        let method = parse_method(cfg.method.as_deref())?;
        let auth = resolve_auth_header(&ctx.connector, &cfg)?;
        let mut start = start_url(&ctx.source, &cfg)?;
        let max_pages = cfg.pagination.max_pages.clamp(1, 20);
        if let Some(param) = cfg.pagination.page_param.as_deref() {
            apply_page_param(&mut start, param, cfg.pagination.page_start);
        }

        let mut extra_owned: Vec<(String, String)> = cfg
            .headers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if let Some((name, value)) = &auth {
            extra_owned.push((name.clone(), value.clone()));
        }
        let etag = ctx.checkpoint.etag.clone();
        let last_modified = ctx.checkpoint.last_modified.clone();
        if let Some(value) = &etag {
            extra_owned.push(("If-None-Match".into(), value.clone()));
        }
        if let Some(value) = &last_modified {
            extra_owned.push(("If-Modified-Since".into(), value.clone()));
        }
        let extra: Vec<(&str, &str)> = extra_owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let request_body = if method == Method::GET || method == Method::HEAD {
            None
        } else {
            cfg.body
                .as_ref()
                .map(|v| serde_json::to_vec(v).unwrap_or_default())
        };

        let mut page_bodies: Vec<Vec<u8>> = Vec::new();
        let mut seen = HashSet::new();
        let mut current = start.clone();
        let mut last_fetched: Option<connector_sdk::FetchedResponse> = None;
        let mut first_not_modified = false;

        for page_idx in 0..max_pages {
            if !seen.insert(current.to_string()) {
                break;
            }
            let hop_headers: Vec<(&str, &str)> = if page_idx == 0 {
                extra.clone()
            } else {
                extra
                    .iter()
                    .copied()
                    .filter(|(k, _)| {
                        !k.eq_ignore_ascii_case("If-None-Match")
                            && !k.eq_ignore_ascii_case("If-Modified-Since")
                    })
                    .collect()
            };
            let fetched = self
                .fetcher
                .request(
                    method.clone(),
                    current.as_str(),
                    &hop_headers,
                    request_body.as_deref(),
                )
                .await?;
            if fetched.is_not_modified() && page_idx == 0 {
                first_not_modified = true;
                last_fetched = Some(fetched);
                break;
            }
            let next = if let Some(pointer) = cfg.pagination.next_url_pointer.as_deref() {
                next_url_from_pointer(&fetched.body, pointer, &fetched.url)
            } else if let Some(param) = cfg.pagination.page_param.as_deref() {
                if page_idx + 1 < max_pages {
                    let mut nxt = fetched.url.clone();
                    apply_page_param(&mut nxt, param, cfg.pagination.page_start + page_idx + 1);
                    Some(nxt)
                } else {
                    None
                }
            } else {
                None
            };
            page_bodies.push(fetched.body.clone());
            last_fetched = Some(fetched);
            match next {
                Some(n) => current = n,
                None => break,
            }
        }

        let mut checkpoint = ctx.checkpoint.clone();
        checkpoint.last_retrieved_at = Some(ctx.now);
        if let Some(fetched) = &last_fetched {
            if let Some(value) = fetched.header("etag").map(str::to_string) {
                checkpoint.etag = Some(value);
            }
            if let Some(value) = fetched.header("last-modified").map(str::to_string) {
                checkpoint.last_modified = Some(value);
            }
        }

        if first_not_modified {
            return Ok(CollectResult {
                fetched: false,
                evidence: None,
                checkpoint,
            });
        }

        let page_count = page_bodies.len();
        let body = if page_bodies.len() <= 1 {
            page_bodies
                .into_iter()
                .next()
                .unwrap_or_else(|| b"null".to_vec())
        } else {
            let pages: Vec<Value> = page_bodies
                .iter()
                .map(|raw| {
                    serde_json::from_slice(raw)
                        .unwrap_or_else(|_| json!({ "_raw": String::from_utf8_lossy(raw) }))
                })
                .collect();
            serde_json::to_vec(&pages).unwrap_or_else(|_| b"[]".to_vec())
        };
        let digest = sha256_hex(&body);
        if ctx
            .checkpoint
            .content_sha256
            .as_deref()
            .is_some_and(|prev| prev == digest)
        {
            checkpoint.content_sha256 = Some(digest);
            return Ok(CollectResult {
                fetched: false,
                evidence: None,
                checkpoint,
            });
        }
        checkpoint.content_sha256 = Some(digest);

        let fetched = last_fetched.ok_or_else(|| ConnectorError::Fetch {
            url: current.to_string(),
            message: "REST API collect 沒有拿到任何回應。請確認 URL 與 NetworkRule".into(),
        })?;
        let content_type = fetched
            .header("content-type")
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_string())
            .or_else(|| Some("application/json".into()));
        let mime_type = Some("application/json".into());
        let evidence = NewRawEvidence {
            source_id: ctx.source.id,
            connector_id: ctx.connector.id,
            collection_id: ctx.collection_id,
            external_id: Some(start_url(&ctx.source, &cfg)?.to_string()),
            source_url: fetched.url.to_string(),
            retrieved_at: ctx.now,
            content_type,
            mime_type,
            http_status: Some(i32::from(fetched.status)),
            http_headers: fetched.headers_json(),
            metadata: json!({
                "connector_type": Self::CONNECTOR_TYPE,
                "elapsed_ms": fetched.elapsed_ms,
                "page_count": page_count,
            }),
            collector_version: Self::VERSION.to_string(),
            body,
        };
        Ok(CollectResult {
            fetched: true,
            evidence: Some(evidence),
            checkpoint,
        })
    }

    async fn parse(&self, body: &[u8]) -> Result<Vec<ParsedItem>, ConnectorError> {
        parse_json_envelope(body)
    }

    async fn create_raw_evidence(
        &self,
        evidence: NewRawEvidence,
    ) -> Result<RawEvidence, ConnectorError> {
        self.evidence.persist(evidence).await
    }

    async fn update_checkpoint(
        &self,
        connector: &Connector,
        checkpoint: &ConnectorCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.checkpoints.save(connector, checkpoint).await
    }

    async fn health(&self) -> ConnectorHealth {
        ConnectorHealth {
            healthy: true,
            message: format!(
                "{} {} 就緒（遠端探活由 collect 執行，health 不對外發請求）",
                Self::CONNECTOR_TYPE,
                Self::VERSION
            ),
        }
    }
}

/// JSON 不當 Document 處理。parse 只確認是 JSON，不拆欄位。
pub fn parse_json_envelope(body: &[u8]) -> Result<Vec<ParsedItem>, ConnectorError> {
    serde_json::from_slice::<Value>(body).map_err(|err| ConnectorError::Parse {
        message: format!(
            "回應不是合法 JSON：{err}。REST API connector 只接受 JSON；請檢查來源或改用 Static Web"
        ),
    })?;
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_ok_returns_no_items() {
        let items = parse_json_envelope(br#"{"ok":true}"#).unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn parse_json_rejects_html() {
        let err = parse_json_envelope(b"<html></html>").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("JSON"), "{msg}");
    }

    #[test]
    fn rejects_authorization_in_configuration() {
        let mut headers = BTreeMap::new();
        headers.insert("Authorization".into(), "Bearer abc".into());
        let err = reject_inline_secrets(&headers).unwrap_err();
        assert!(err.to_string().contains("credential_reference"));
    }

    #[test]
    fn join_path_absolute() {
        let url = join_url("http://127.0.0.1:9/base/", Some("/v1/items")).unwrap();
        assert_eq!(url.path(), "/v1/items");
    }
}
