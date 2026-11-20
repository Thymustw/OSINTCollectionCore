//! 文件的「串接文字」：抽取（entity-worker）與切段（chunk-worker）**共用同一支函式**。
//!
//! ADR-016 決定 #11：段落↔實體在切段時對應、用同一份串接文字。兩邊各寫一份的話，
//! 只要分隔符或欄位順序差一個字，段落 offset 與實體 offset 就會靜默錯位。
//!
//! 格式（2026-11-18 凍結，改了要連 `EXTRACTOR_VERSION` 與切段策略版本一起升）：
//! `title`、`summary`、`body` 依序，**只取有值的欄位**，每個欄位後面都接一個 `\n`
//! （最後一個也接）。欄位內容原樣保留，不 trim。

use core_model::Document;

/// 串接 `title`／`summary`／`body`。`max_bytes = None` 表示不截斷（切段用；
/// 抽取路徑仍帶上限，外部輸入不可信，CLAUDE.md §5）。
///
/// 有上限時在**串接當下**就截（截在字元邊界上），不是串完再截——`body` 沒有長度限制，
/// 先複製一份 50 MB 再丟掉 99% 是白花的配置。截斷後仍在該欄位尾端補 `\n`，
/// 與抽取器既有行為一致（換行而不是空白：避免標題尾的網域與正文首的 TLD 被 regex 黏成一個 token）。
#[must_use]
pub fn chunk_source_text(document: &Document, max_bytes: Option<usize>) -> String {
    let mut text = String::new();
    for part in [
        document.title.as_deref(),
        document.summary.as_deref(),
        document.body.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        match max_bytes {
            None => text.push_str(part),
            Some(limit) => {
                let remaining = limit.saturating_sub(text.len());
                if remaining == 0 {
                    break;
                }
                text.push_str(truncate_on_char_boundary(part, remaining));
            }
        }
        text.push('\n');
    }
    text
}

/// 取 `text` 不超過 `max_bytes` 的最長前綴，且結尾落在字元邊界。
#[must_use]
pub fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(title: Option<&str>, summary: Option<&str>, body: Option<&str>) -> Document {
        let now = chrono::Utc::now();
        Document {
            id: uuid::Uuid::now_v7(),
            object_type: core_model::DocumentType::Article,
            schema_version: "1".into(),
            title: title.map(Into::into),
            body: body.map(Into::into),
            summary: summary.map(Into::into),
            language: None,
            author: None,
            published_at: None,
            modified_at: None,
            observed_at: now,
            collected_at: now,
            source_url: None,
            canonical_url: None,
            normalized_content_hash: None,
            confidence: 1.0,
            labels: Vec::new(),
            attributes: serde_json::json!({}),
            external_key: None,
            simhash: None,
            duplicate_of: None,
        }
    }

    #[test]
    fn concatenates_present_fields_with_trailing_newlines() {
        let d = doc(Some("標題"), None, Some("正文"));
        assert_eq!(chunk_source_text(&d, None), "標題\n正文\n");
        let d = doc(None, None, None);
        assert_eq!(chunk_source_text(&d, None), "");
    }

    #[test]
    fn limit_truncates_on_char_boundary_and_stops() {
        let d = doc(Some("ab"), Some("中文字"), Some("never"));
        // "ab\n" 是 3 bytes；剩 4 bytes 只裝得下「中」（3 bytes）。
        assert_eq!(chunk_source_text(&d, Some(7)), "ab\n中\n");
    }
}
