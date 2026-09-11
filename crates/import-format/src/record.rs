//! 解析後的中性紀錄。JSON 與 CSV 共用，normalizer 再轉成 Document。

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};

use crate::error::ImportError;
use crate::spec::{Field, ImportLimits};

/// 一筆匯入資料，已套用欄位對映。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImportRecord {
    /// 在原始檔案中的序號（從 0 起算，跳過的空白筆也會佔號碼）。
    pub index: usize,
    pub title: Option<String>,
    pub body: Option<String>,
    pub summary: Option<String>,
    pub url: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    /// 原始 `published_at` 字串。解析不出時間時保留，方便之後回頭查。
    pub published_at_raw: Option<String>,
    pub external_id: Option<String>,
    pub language: Option<String>,
    pub author: Option<String>,
}

impl ImportRecord {
    /// title／body／summary 全空視為沒有內容，會被跳過。
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.title.is_none() && self.body.is_none() && self.summary.is_none()
    }

    /// 依欄位設定值（已去除前後空白；空字串當作沒填）。
    pub(crate) fn set(
        &mut self,
        field: Field,
        value: String,
        limits: &ImportLimits,
    ) -> Result<(), ImportError> {
        if value.len() > limits.max_field_bytes {
            return Err(ImportError::FieldTooLarge {
                index: self.index,
                field: field.as_str(),
                size: value.len(),
                limit: limits.max_field_bytes,
            });
        }
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Ok(());
        }
        let owned = trimmed.to_string();
        match field {
            Field::Title => self.title = Some(owned),
            Field::Body => self.body = Some(owned),
            Field::Summary => self.summary = Some(owned),
            Field::Url => self.url = Some(owned),
            Field::ExternalId => self.external_id = Some(owned),
            Field::Language => self.language = Some(owned),
            Field::Author => self.author = Some(owned),
            Field::PublishedAt => {
                self.published_at = parse_timestamp(&owned);
                self.published_at_raw = Some(owned);
            }
        }
        Ok(())
    }
}

/// 一次解析的結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseOutcome {
    /// 有內容的紀錄。
    pub records: Vec<ImportRecord>,
    /// 讀到的總筆數（含被跳過的空白筆）。
    pub total: usize,
    /// title／body／summary 全空而跳過的筆數。
    pub skipped_empty: usize,
}

/// 常見時間格式。解析不出來不算失敗——`published_at` 只是選填欄位，
/// 讓整份匯入因為一個怪日期而失敗並不合理，原字串會留在 `published_at_raw`。
#[must_use]
pub fn parse_timestamp(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(dt) = DateTime::parse_from_rfc2822(value) {
        return Some(dt.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(value, format) {
            return Some(Utc.from_utc_datetime(&naive));
        }
    }
    for format in ["%Y-%m-%d", "%Y/%m/%d"] {
        if let Ok(date) = NaiveDate::parse_from_str(value, format) {
            return Some(Utc.from_utc_datetime(&date.and_hms_opt(0, 0, 0)?));
        }
    }
    // Unix epoch 秒。只接受 10 位數字，避免把 ID 當成時間。
    if value.len() == 10 && value.bytes().all(|b| b.is_ascii_digit()) {
        if let Ok(secs) = value.parse::<i64>() {
            return DateTime::from_timestamp(secs, 0);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_timestamp_shapes() {
        assert!(parse_timestamp("2026-09-10T08:00:00Z").is_some());
        assert!(parse_timestamp("2026-09-10 08:00:00").is_some());
        assert!(parse_timestamp("2026-09-10").is_some());
        assert!(parse_timestamp("Thu, 10 Sep 2026 08:00:00 +0000").is_some());
        assert!(parse_timestamp("1757491200").is_some());
    }

    #[test]
    fn nonsense_timestamp_is_none_not_error() {
        assert!(parse_timestamp("not a date").is_none());
        assert!(parse_timestamp("").is_none());
        // 12 位數字是 ID 不是 epoch。
        assert!(parse_timestamp("123456789012").is_none());
    }

    #[test]
    fn oversized_field_is_rejected_with_field_name() {
        let limits = ImportLimits {
            max_field_bytes: 8,
            ..ImportLimits::default()
        };
        let mut record = ImportRecord {
            index: 3,
            ..ImportRecord::default()
        };
        let err = record
            .set(Field::Title, "x".repeat(9), &limits)
            .unwrap_err();
        assert_eq!(
            err,
            ImportError::FieldTooLarge {
                index: 3,
                field: "title",
                size: 9,
                limit: 8,
            }
        );
    }
}
