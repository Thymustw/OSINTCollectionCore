//! Static Web connector。
//!
//! 抓取走 `connector-sdk` 的 SSRF Guard／rate limit；解析用 `scraper` 抽 title／description／正文。
//! 一次 collect 把整份 HTML 存成一筆 RawEvidence。
//!
//! Checkpoint：優先 HTTP ETag／Last-Modified（304 不算新內容）；沒有條件標頭時用 body SHA-256
//! 比對。規格沒指定 Static Web 增量策略，選這組是因為多數靜態頁至少會給其中一種訊號，
//! 而且比「每次都當新證據」少寫重複 blob。

use async_trait::async_trait;
use connector_sdk::{
    CheckpointStore, CollectContext, CollectResult, ConnectorCheckpoint, ConnectorError,
    ConnectorHealth, ConnectorTrait, DiscoverItem, EvidenceSink, GuardedFetcher, NewRawEvidence,
    ParsedItem, sha256_hex,
};
use core_model::{Connector, RawEvidence, Source};
use scraper::{Html, Selector};
use serde::Deserialize;
use serde_json::json;

/// 從 HTML 抽出的主要欄位。不是完整 readability。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractedPage {
    pub title: Option<String>,
    pub description: Option<String>,
    pub body_text: String,
}

/// Static Web connector。
pub struct StaticWebConnector<E, C> {
    fetcher: GuardedFetcher,
    evidence: E,
    checkpoints: C,
}

impl<E, C> StaticWebConnector<E, C> {
    pub const CONNECTOR_TYPE: &'static str = "static_web";
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
struct StaticWebConfig {
    /// 覆寫 Source.base_url。未填就用 Source.base_url。
    url: Option<String>,
}

fn page_url(source: &Source, connector: &Connector) -> Result<String, ConnectorError> {
    let cfg: StaticWebConfig =
        serde_json::from_value(connector.configuration.clone()).unwrap_or_default();
    if let Some(url) = cfg.url.filter(|u| !u.trim().is_empty()) {
        return Ok(url);
    }
    source
        .base_url
        .as_ref()
        .filter(|u| !u.trim().is_empty())
        .cloned()
        .ok_or_else(|| ConnectorError::Policy {
            message:
                "Source.base_url 與 configuration.url 都是空的。Static Web connector 需要一個頁面 URL"
                    .into(),
        })
}

#[async_trait]
impl<E, C> ConnectorTrait for StaticWebConnector<E, C>
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
                message: "Source.base_url 是空的。請在 Source 填靜態頁 URL".into(),
            })?;
        Ok(vec![DiscoverItem {
            url: url.clone(),
            title: Some(source.name.clone()),
        }])
    }

    async fn collect(&self, ctx: &CollectContext) -> Result<CollectResult, ConnectorError> {
        let url = page_url(&ctx.source, &ctx.connector)?;
        let etag = ctx.checkpoint.etag.clone();
        let last_modified = ctx.checkpoint.last_modified.clone();
        let mut extra: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = etag.as_deref() {
            extra.push(("If-None-Match", value));
        }
        if let Some(value) = last_modified.as_deref() {
            extra.push(("If-Modified-Since", value));
        }

        let fetched = self.fetcher.get(&url, &extra).await?;
        let mut checkpoint = ctx.checkpoint.clone();
        checkpoint.last_retrieved_at = Some(ctx.now);
        if let Some(value) = fetched.header("etag").map(str::to_string) {
            checkpoint.etag = Some(value);
        }
        if let Some(value) = fetched.header("last-modified").map(str::to_string) {
            checkpoint.last_modified = Some(value);
        }

        if fetched.is_not_modified() {
            return Ok(CollectResult {
                fetched: false,
                evidence: None,
                checkpoint,
            });
        }

        let digest = sha256_hex(&fetched.body);
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

        let content_type = fetched
            .header("content-type")
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_string())
            .or_else(|| Some("text/html".into()));
        let mime_type = content_type.clone();
        let evidence = NewRawEvidence {
            source_id: ctx.source.id,
            connector_id: ctx.connector.id,
            collection_id: ctx.collection_id,
            external_id: Some(url.clone()),
            source_url: fetched.url.to_string(),
            retrieved_at: ctx.now,
            content_type,
            mime_type,
            http_status: Some(i32::from(fetched.status)),
            http_headers: fetched.headers_json(),
            metadata: json!({
                "connector_type": Self::CONNECTOR_TYPE,
                "elapsed_ms": fetched.elapsed_ms,
            }),
            collector_version: Self::VERSION.to_string(),
            body: fetched.body,
        };
        Ok(CollectResult {
            fetched: true,
            evidence: Some(evidence),
            checkpoint,
        })
    }

    async fn parse(&self, body: &[u8]) -> Result<Vec<ParsedItem>, ConnectorError> {
        parse_html(body)
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

/// 給測試與 normalizer 共用的 HTML 抽取。
pub fn parse_html(body: &[u8]) -> Result<Vec<ParsedItem>, ConnectorError> {
    let page = extract_page(body);
    if page.title.is_none() && page.body_text.is_empty() {
        return Err(ConnectorError::Parse {
            message:
                "HTML 抽不到 title 也抽不到正文。請確認內容是 HTML，或改用 REST／RSS connector"
                    .into(),
        });
    }
    Ok(vec![ParsedItem {
        external_id: None,
        url: None,
        title: page.title,
        published_at: None,
        summary: page.description,
        attributes: json!({
            "body_text": page.body_text,
            "connector_type": "static_web",
        }),
    }])
}

/// 合理但不完美的內容抽取：title、meta description、可見文字。
pub fn extract_page(body: &[u8]) -> ExtractedPage {
    let html = String::from_utf8_lossy(body);
    let document = Html::parse_document(&html);
    let title = first_text(&document, "title")
        .or_else(|| meta_content(&document, "og:title"))
        .or_else(|| meta_content(&document, "twitter:title"));
    let description = meta_content(&document, "description")
        .or_else(|| meta_content(&document, "og:description"))
        .or_else(|| meta_content(&document, "twitter:description"));
    let body_text = visible_text(&document);
    ExtractedPage {
        title,
        description,
        body_text,
    }
}

fn first_text(document: &Html, selector: &str) -> Option<String> {
    let sel = Selector::parse(selector).ok()?;
    let text = document
        .select(&sel)
        .next()?
        .text()
        .collect::<Vec<_>>()
        .join(" ");
    let trimmed = collapse_ws(&text);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

fn meta_content(document: &Html, property: &str) -> Option<String> {
    let sel = Selector::parse("meta").ok()?;
    for el in document.select(&sel) {
        let prop = el
            .value()
            .attr("property")
            .or_else(|| el.value().attr("name"))
            .unwrap_or("");
        if prop.eq_ignore_ascii_case(property) {
            let content = el.value().attr("content").unwrap_or("").trim();
            if !content.is_empty() {
                return Some(content.to_string());
            }
        }
    }
    None
}

fn visible_text(document: &Html) -> String {
    let skip = Selector::parse("script, style, noscript, svg, template").ok();
    let root_sel = Selector::parse("article, main, [role=main], body").ok();
    let Some(root_sel) = root_sel else {
        return String::new();
    };
    let root = document.select(&root_sel).next();
    let Some(root) = root else {
        return collapse_ws(&document.root_element().text().collect::<String>());
    };
    let mut parts = Vec::new();
    collect_visible(&root, skip.as_ref(), &mut parts);
    collapse_ws(&parts.join(" "))
}

fn collect_visible(
    element: &scraper::ElementRef<'_>,
    skip: Option<&Selector>,
    parts: &mut Vec<String>,
) {
    if let Some(skip) = skip {
        if skip.matches(element) {
            return;
        }
    }
    for child in element.children() {
        match child.value() {
            scraper::node::Node::Text(text) => {
                let t = text.text.trim();
                if !t.is_empty() {
                    parts.push(t.to_string());
                }
            }
            scraper::node::Node::Element(_) => {
                if let Some(el) = scraper::ElementRef::wrap(child) {
                    collect_visible(&el, skip, parts);
                }
            }
            _ => {}
        }
    }
}

fn collapse_ws(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const HTML: &str = r#"<!doctype html>
<html>
<head>
  <title>CVE-2026-0001 advisory</title>
  <meta name="description" content="fixture page">
</head>
<body>
  <script>alert(1)</script>
  <article>
    <h1>CVE-2026-0001</h1>
    <p>This is the main body text.</p>
  </article>
</body>
</html>
"#;

    #[test]
    fn extracts_title_description_and_body() {
        let page = extract_page(HTML.as_bytes());
        assert_eq!(page.title.as_deref(), Some("CVE-2026-0001 advisory"));
        assert_eq!(page.description.as_deref(), Some("fixture page"));
        assert!(page.body_text.contains("This is the main body text."));
        assert!(
            !page.body_text.contains("alert"),
            "script 不該進正文：{}",
            page.body_text
        );
    }

    #[test]
    fn parse_html_returns_one_item() {
        let items = parse_html(HTML.as_bytes()).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title.as_deref(), Some("CVE-2026-0001 advisory"));
        assert_eq!(items[0].summary.as_deref(), Some("fixture page"));
        assert!(
            items[0].attributes["body_text"]
                .as_str()
                .unwrap()
                .contains("main body")
        );
    }

    #[test]
    fn empty_html_explains_next_step() {
        let err = parse_html(b"<html><body></body></html>").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("HTML"), "{msg}");
    }
}
