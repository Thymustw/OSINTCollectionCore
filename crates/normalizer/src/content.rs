//! 內容型別判斷。未知型別記錄後跳過，不讓 consumer 掛掉。

/// 這次要不要當 RSS／Atom 處理。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentClass {
    RssOrAtom,
    Unsupported,
}

/// 依 content-type／mime／路徑副檔名判斷。規格沒有列舉 MIME，這裡用常見 feed 型別。
#[must_use]
pub fn classify_content(
    content_type: Option<&str>,
    mime_type: Option<&str>,
    path: &str,
) -> ContentClass {
    if looks_like_feed(content_type) || looks_like_feed(mime_type) || path_looks_like_feed(path) {
        ContentClass::RssOrAtom
    } else {
        ContentClass::Unsupported
    }
}

fn looks_like_feed(value: Option<&str>) -> bool {
    let Some(value) = value else {
        return false;
    };
    let lower = value
        .split(';')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "application/rss+xml"
            | "application/atom+xml"
            | "application/xml"
            | "text/xml"
            | "application/rdf+xml"
            | "text/rss"
    ) || lower.contains("rss")
        || lower.contains("atom")
}

fn path_looks_like_feed(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".xml") || lower.ends_with(".rss") || lower.ends_with(".atom")
}

/// body 看起來像 RSS／Atom（content-type 缺失時的後備）。
#[must_use]
pub fn body_looks_like_feed(body: &[u8]) -> bool {
    let sample = std::str::from_utf8(body).unwrap_or("");
    let head = sample
        .chars()
        .take(512)
        .collect::<String>()
        .to_ascii_lowercase();
    head.contains("<rss")
        || head.contains("<feed")
        || head.contains("xmlns=\"http://www.w3.org/2005/atom\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_mime_is_supported() {
        assert_eq!(
            classify_content(Some("application/rss+xml; charset=utf-8"), None, "x"),
            ContentClass::RssOrAtom
        );
        assert_eq!(
            classify_content(Some("application/atom+xml"), None, "x"),
            ContentClass::RssOrAtom
        );
        assert_eq!(
            classify_content(Some("text/xml"), None, "/rss.xml"),
            ContentClass::RssOrAtom
        );
    }

    #[test]
    fn unknown_type_does_not_panic() {
        assert_eq!(
            classify_content(Some("application/pdf"), None, "file.pdf"),
            ContentClass::Unsupported
        );
        assert_eq!(
            classify_content(None, None, "blob.bin"),
            ContentClass::Unsupported
        );
    }

    #[test]
    fn body_sniff_rss() {
        assert!(body_looks_like_feed(
            b"<?xml version=\"1.0\"?><rss version=\"2.0\">"
        ));
        assert!(!body_looks_like_feed(b"%PDF-1.4"));
    }
}
