//! Cursor pagination。cursor 是上一頁最後一筆 UUID（標準字串，可選 base64url）。

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::ApiError;

const DEFAULT_LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 100;

/// list query。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Pagination {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
}

impl Pagination {
    /// 解成 `(after_id, limit)`。limit 夾在 1..=100。
    pub fn decode(&self) -> Result<(Option<Uuid>, u32), ApiError> {
        let limit = self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
        let after = match self.cursor.as_deref() {
            None | Some("") => None,
            Some(raw) => Some(decode_cursor(raw)?),
        };
        Ok((after, limit))
    }
}

/// list 回應。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CursorPage<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
}

impl<T> CursorPage<T> {
    #[must_use]
    pub fn from_items(items: Vec<T>, limit: u32, id_of: impl Fn(&T) -> Uuid) -> Self {
        let next_cursor = if items.len() as u32 >= limit {
            items.last().map(|item| id_of(item).to_string())
        } else {
            None
        };
        Self { items, next_cursor }
    }
}

fn decode_cursor(raw: &str) -> Result<Uuid, ApiError> {
    if let Ok(id) = Uuid::parse_str(raw) {
        return Ok(id);
    }
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(raw)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(raw))
        .map_err(|_| {
            ApiError::bad_request(format!(
                "cursor `{raw}` 不是 UUID 也不是 base64url。請用上一頁回傳的 next_cursor，不要手改"
            ))
        })?;
    let text = String::from_utf8(bytes).map_err(|_| {
        ApiError::bad_request("cursor 解碼後不是 UTF-8。請用 API 回傳的 next_cursor".to_string())
    })?;
    Uuid::parse_str(text.trim()).map_err(|_| {
        ApiError::bad_request(format!(
            "cursor 解碼後 `{text}` 不是 UUID。請用 API 回傳的 next_cursor"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_cursor() {
        let id = Uuid::now_v7();
        let p = Pagination {
            cursor: Some(id.to_string()),
            limit: Some(5),
        };
        let (after, limit) = p.decode().unwrap();
        assert_eq!(after, Some(id));
        assert_eq!(limit, 5);
    }

    #[test]
    fn clamps_limit() {
        let p = Pagination {
            cursor: None,
            limit: Some(9999),
        };
        assert_eq!(p.decode().unwrap().1, 100);
    }

    #[test]
    fn next_cursor_when_full_page() {
        let ids = vec![Uuid::now_v7(), Uuid::now_v7()];
        let page = CursorPage::from_items(ids.clone(), 2, |id| *id);
        assert_eq!(
            page.next_cursor.as_deref(),
            Some(ids[1].to_string().as_str())
        );
        let short = CursorPage::from_items(vec![ids[0]], 2, |id| *id);
        assert!(short.next_cursor.is_none());
    }
}
