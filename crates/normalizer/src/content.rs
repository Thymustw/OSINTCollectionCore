//! 內容型別判斷。未知型別記錄後跳過，不讓 consumer 掛掉。

/// 這次要不要當 RSS／Atom／HTML 處理。
///
/// `Json`／`Csv` 單看 content-type 仍然是「不知道怎麼拆成 Document」——
/// 任意 REST API 回傳的 JSON 沒有欄位對映可用。只有走 `POST /api/v1/import`
/// 進來、`metadata.import` 帶著 `ImportSpec` 的那些才知道怎麼拆，
/// 那條路徑由 `service.rs` 另外處理，不依賴這裡的分類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentClass {
    RssOrAtom,
    Html,
    Json,
    Csv,
    Unsupported,
}

/// 依 content-type／mime／路徑副檔名判斷。規格沒有列舉 MIME，這裡用常見型別。
#[must_use]
pub fn classify_content(
    content_type: Option<&str>,
    mime_type: Option<&str>,
    path: &str,
) -> ContentClass {
    if looks_like_json(content_type) || looks_like_json(mime_type) || path_looks_like_json(path) {
        return ContentClass::Json;
    }
    if looks_like_csv(content_type) || looks_like_csv(mime_type) || path_looks_like_csv(path) {
        return ContentClass::Csv;
    }
    if looks_like_feed(content_type) || looks_like_feed(mime_type) || path_looks_like_feed(path) {
        return ContentClass::RssOrAtom;
    }
    if looks_like_html(content_type) || looks_like_html(mime_type) || path_looks_like_html(path) {
        return ContentClass::Html;
    }
    ContentClass::Unsupported
}

fn media_type(value: Option<&str>) -> Option<String> {
    let value = value?;
    Some(
        value
            .split(';')
            .next()
            .unwrap_or(value)
            .trim()
            .to_ascii_lowercase(),
    )
}

fn looks_like_json(value: Option<&str>) -> bool {
    let Some(lower) = media_type(value) else {
        return false;
    };
    lower == "application/json" || lower.ends_with("+json") || lower == "text/json"
}

fn looks_like_html(value: Option<&str>) -> bool {
    let Some(lower) = media_type(value) else {
        return false;
    };
    matches!(
        lower.as_str(),
        "text/html" | "application/xhtml+xml" | "application/xhtml"
    )
}

fn looks_like_feed(value: Option<&str>) -> bool {
    let Some(lower) = media_type(value) else {
        return false;
    };
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

fn path_looks_like_html(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower.ends_with(".html") || lower.ends_with(".htm") || lower.ends_with(".xhtml")
}

fn path_looks_like_json(path: &str) -> bool {
    path.to_ascii_lowercase().ends_with(".json")
}

fn looks_like_csv(value: Option<&str>) -> bool {
    let Some(lower) = media_type(value) else {
        return false;
    };
    matches!(
        lower.as_str(),
        "text/csv" | "application/csv" | "text/comma-separated-values"
    )
}

fn path_looks_like_csv(path: &str) -> bool {
    path.to_ascii_lowercase().ends_with(".csv")
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

/// body 看起來像 HTML（content-type 缺失時的後備）。JSON／feed 優先，避免誤判。
#[must_use]
pub fn body_looks_like_html(body: &[u8]) -> bool {
    if body_looks_like_feed(body) {
        return false;
    }
    let sample = std::str::from_utf8(body).unwrap_or("");
    let head = sample
        .chars()
        .take(512)
        .collect::<String>()
        .to_ascii_lowercase();
    let trimmed = head.trim_start();
    trimmed.starts_with("<!doctype html")
        || trimmed.starts_with("<html")
        || head.contains("<html")
        || head.contains("<head")
        || head.contains("<body")
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
    fn html_mime_is_supported() {
        assert_eq!(
            classify_content(Some("text/html; charset=utf-8"), None, "x"),
            ContentClass::Html
        );
        assert_eq!(
            classify_content(None, None, "page.html"),
            ContentClass::Html
        );
    }

    #[test]
    fn json_is_unsupported_for_document() {
        assert_eq!(
            classify_content(Some("application/json"), None, "api"),
            ContentClass::Json
        );
        assert_eq!(
            classify_content(None, None, "payload.json"),
            ContentClass::Json
        );
    }

    #[test]
    fn csv_is_recognised() {
        assert_eq!(
            classify_content(Some("text/csv; charset=utf-8"), None, "x"),
            ContentClass::Csv
        );
        assert_eq!(classify_content(None, None, "table.csv"), ContentClass::Csv);
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

    #[test]
    fn body_sniff_html() {
        assert!(body_looks_like_html(
            b"<!doctype html><html><title>t</title>"
        ));
        assert!(!body_looks_like_html(b"{\"ok\":true}"));
        assert!(!body_looks_like_html(b"<rss version=\"2.0\">"));
    }
}
