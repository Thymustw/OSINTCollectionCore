//! RSS 2.0／Atom 1.0 connector。
//!
//! 抓取走 `connector-sdk` 的 SSRF Guard／rate limit；解析用 `feed-rs`。
//! 一次 collect 把整份 feed body 存成一筆 RawEvidence（V0.1 不做 per-item blob）。

use std::io::Cursor;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use connector_sdk::{
    CheckpointStore, CollectContext, CollectResult, ConnectorCheckpoint, ConnectorError,
    ConnectorHealth, ConnectorTrait, DiscoverItem, EvidenceSink, GuardedFetcher, NewRawEvidence,
    ParsedItem,
};
use core_model::{Connector, RawEvidence, Source};
use feed_rs::model::{FeedType, Text};
use serde_json::json;

/// RSS／Atom connector。
pub struct RssConnector<E, C> {
    fetcher: GuardedFetcher,
    evidence: E,
    checkpoints: C,
}

impl<E, C> RssConnector<E, C> {
    pub const CONNECTOR_TYPE: &'static str = "rss";
    pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    pub fn new(fetcher: GuardedFetcher, evidence: E, checkpoints: C) -> Self {
        Self {
            fetcher,
            evidence,
            checkpoints,
        }
    }
}

#[async_trait]
impl<E, C> ConnectorTrait for RssConnector<E, C>
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
        discover_feed(source)
    }

    async fn collect(&self, ctx: &CollectContext) -> Result<CollectResult, ConnectorError> {
        let url = ctx
            .source
            .base_url
            .as_ref()
            .ok_or_else(|| ConnectorError::Policy {
                message: "Source.base_url 是空的。RSS／Atom connector 需要一個 feed URL".into(),
            })?;

        let etag = ctx.checkpoint.etag.clone();
        let last_modified = ctx.checkpoint.last_modified.clone();
        let mut extra: Vec<(&str, &str)> = Vec::new();
        if let Some(value) = etag.as_deref() {
            extra.push(("If-None-Match", value));
        }
        if let Some(value) = last_modified.as_deref() {
            extra.push(("If-Modified-Since", value));
        }

        let fetched = self.fetcher.get(url, &extra).await?;
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

        let content_type = fetched
            .header("content-type")
            .map(|value| value.split(';').next().unwrap_or(value).trim().to_string());
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
        parse_feed(body)
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

fn discover_feed(source: &Source) -> Result<Vec<DiscoverItem>, ConnectorError> {
    let url = source
        .base_url
        .as_ref()
        .ok_or_else(|| ConnectorError::Policy {
            message: "Source.base_url 是空的。請在 Source 填 RSS／Atom feed URL".into(),
        })?;
    Ok(vec![DiscoverItem {
        url: url.clone(),
        title: Some(source.name.clone()),
    }])
}

/// 給測試與不需要完整 connector 的呼叫端。
pub fn parse_feed(body: &[u8]) -> Result<Vec<ParsedItem>, ConnectorError> {
    let feed = feed_rs::parser::parse(Cursor::new(body)).map_err(|err| ConnectorError::Parse {
        message: format!("feed-rs 無法解析這份 RSS／Atom：{err}。請確認內容是 RSS 2.0 或 Atom 1.0"),
    })?;
    let feed_kind = match feed.feed_type {
        FeedType::Atom => "atom",
        FeedType::JSON => "json_feed",
        FeedType::RSS0 | FeedType::RSS1 | FeedType::RSS2 => "rss",
    };
    let items = feed
        .entries
        .into_iter()
        .map(|entry| {
            let url = entry.links.first().map(|link| link.href.clone());
            let title = entry.title.as_ref().map(text_value);
            let summary = entry
                .summary
                .as_ref()
                .map(text_value)
                .or_else(|| entry.content.as_ref().and_then(|c| c.body.clone()));
            let published_at: Option<DateTime<Utc>> =
                entry.published.map(|ts| ts.with_timezone(&Utc));
            ParsedItem {
                external_id: Some(entry.id),
                url,
                title,
                published_at,
                summary,
                attributes: json!({ "feed_type": feed_kind }),
            }
        })
        .collect();
    Ok(items)
}

fn text_value(text: &Text) -> String {
    text.content.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    const RSS: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <link>http://127.0.0.1/feed</link>
    <description>fixture</description>
    <item>
      <title>CVE-2026-0001</title>
      <link>http://127.0.0.1/cve</link>
      <guid>CVE-2026-0001</guid>
      <pubDate>Wed, 10 Sep 2026 12:00:00 GMT</pubDate>
      <description>test item</description>
    </item>
  </channel>
</rss>
"#;

    const ATOM: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Atom Fixture</title>
  <id>urn:osint:atom-fixture</id>
  <updated>2026-09-10T12:00:00Z</updated>
  <entry>
    <title>Atom Item</title>
    <id>urn:osint:atom-1</id>
    <link href="http://127.0.0.1/atom-1"/>
    <updated>2026-09-10T12:00:00Z</updated>
    <summary>atom summary</summary>
  </entry>
</feed>
"#;

    #[test]
    fn parses_rss2() {
        let items = parse_feed(RSS.as_bytes()).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title.as_deref(), Some("CVE-2026-0001"));
        assert_eq!(items[0].external_id.as_deref(), Some("CVE-2026-0001"));
        assert_eq!(items[0].attributes["feed_type"], "rss");
    }

    #[test]
    fn parses_atom() {
        let items = parse_feed(ATOM.as_bytes()).unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].title.as_deref(), Some("Atom Item"));
        assert_eq!(items[0].attributes["feed_type"], "atom");
    }

    #[test]
    fn malformed_feed_explains_next_step() {
        let err = parse_feed(b"<not-a-feed/>").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("RSS"), "{msg}");
    }
}
