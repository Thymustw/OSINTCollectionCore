//! 匯入描述：種類、欄位對映、解析上限。
//!
//! 這整包會原封不動寫進 `RawEvidence.metadata["import"]`，normalizer 之後用同一份設定
//! 重跑解析。刻意連 limits 一起存：上傳當下驗證通過的資料，之後正規化不能因為有人調
//! 小 config 就變成「解析失敗」，那會讓已接受的證據永遠卡住。

use core_model::DocumentType;
use serde::{Deserialize, Serialize};

/// 匯入種類。由上傳者明確指定，不從副檔名或 `Content-Type` header 推測。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ImportKind {
    /// 任意檔案 + 使用者填的 metadata。V0.1 不拆 Document。
    Manual,
    /// 物件陣列或 NDJSON。
    Json,
    /// 第一列為 header 的 CSV。
    Csv,
}

impl ImportKind {
    /// 對應的 `Connector.connector_type`。
    #[must_use]
    pub fn connector_type(self) -> &'static str {
        match self {
            Self::Manual => "manual_upload",
            Self::Json => "json_import",
            Self::Csv => "csv_import",
        }
    }

    /// 對應的 `Source.source_type`（字串形式，與 `core_model::SourceType` 的 serde 名稱一致）。
    #[must_use]
    pub fn source_type(self) -> &'static str {
        match self {
            Self::Manual => "manual_upload",
            Self::Json => "json_import",
            Self::Csv => "csv_import",
        }
    }

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "manual",
            Self::Json => "json",
            Self::Csv => "csv",
        }
    }
}

/// 解析上限。每一項都是有界的，不存在「無上限」設定值。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportLimits {
    /// 單次匯入最多幾筆。
    pub max_records: usize,
    /// 單筆（NDJSON 一行／JSON 陣列一個元素／CSV 一列）最多幾 bytes。
    pub max_record_bytes: usize,
    /// 單一對映欄位最多幾 bytes。
    pub max_field_bytes: usize,
    /// JSON 巢狀深度上限。
    pub max_depth: usize,
    /// CSV 欄位數上限。
    pub max_columns: usize,
}

impl Default for ImportLimits {
    fn default() -> Self {
        Self {
            max_records: 10_000,
            max_record_bytes: 262_144,
            max_field_bytes: 65_536,
            max_depth: 32,
            max_columns: 512,
        }
    }
}

/// 欄位對映。值是「來源鍵」：CSV 是 header 名稱；JSON 是頂層鍵，或以 `/` 開頭的 JSON Pointer。
///
/// 每個欄位都可以不填，未填時套用 `default_aliases` 的慣用名稱（大小寫不敏感）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMapping {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

/// 邏輯欄位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    Title,
    Body,
    Summary,
    Url,
    PublishedAt,
    ExternalId,
    Language,
    Author,
}

impl Field {
    /// 全部邏輯欄位，順序固定（錯誤訊息與測試都依賴這個順序）。
    pub const ALL: [Field; 8] = [
        Field::Title,
        Field::Body,
        Field::Summary,
        Field::Url,
        Field::PublishedAt,
        Field::ExternalId,
        Field::Language,
        Field::Author,
    ];

    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Title => "title",
            Self::Body => "body",
            Self::Summary => "summary",
            Self::Url => "url",
            Self::PublishedAt => "published_at",
            Self::ExternalId => "external_id",
            Self::Language => "language",
            Self::Author => "author",
        }
    }

    /// 未指定 mapping 時依序嘗試的慣用鍵名（大小寫不敏感）。
    #[must_use]
    pub fn default_aliases(self) -> &'static [&'static str] {
        match self {
            Self::Title => &["title", "headline", "name", "subject"],
            Self::Body => &["body", "content", "text", "description_full"],
            Self::Summary => &["summary", "description", "abstract", "excerpt"],
            Self::Url => &["url", "link", "source_url", "permalink"],
            Self::PublishedAt => &["published_at", "published", "date", "pubdate", "timestamp"],
            Self::ExternalId => &["external_id", "id", "guid", "uuid"],
            Self::Language => &["language", "lang"],
            Self::Author => &["author", "creator", "byline"],
        }
    }
}

impl FieldMapping {
    /// 取得某欄位使用者指定的來源鍵。
    #[must_use]
    pub fn explicit(&self, field: Field) -> Option<&str> {
        let value = match field {
            Field::Title => &self.title,
            Field::Body => &self.body,
            Field::Summary => &self.summary,
            Field::Url => &self.url,
            Field::PublishedAt => &self.published_at,
            Field::ExternalId => &self.external_id,
            Field::Language => &self.language,
            Field::Author => &self.author,
        };
        value.as_deref().map(str::trim).filter(|s| !s.is_empty())
    }
}

fn default_object_type() -> DocumentType {
    DocumentType::Report
}

/// 一次匯入的完整描述。寫進 `RawEvidence.metadata["import"]`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImportSpec {
    pub kind: ImportKind,
    #[serde(default)]
    pub mapping: FieldMapping,
    #[serde(default)]
    pub limits: ImportLimits,
    /// 產出的 Document `object_type`。預設 `report`：匯入資料是操作者提供的資料集紀錄，
    /// 不是發佈者的文章（`article` 留給 RSS／Atom）。
    #[serde(default = "default_object_type")]
    pub object_type: DocumentType,
}

impl ImportSpec {
    #[must_use]
    pub fn new(kind: ImportKind) -> Self {
        Self {
            kind,
            mapping: FieldMapping::default(),
            limits: ImportLimits::default(),
            object_type: default_object_type(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_round_trips_through_json() {
        let mut spec = ImportSpec::new(ImportKind::Csv);
        spec.mapping.title = Some("headline".into());
        let value = serde_json::to_value(&spec).unwrap();
        assert_eq!(value["kind"], "csv");
        assert_eq!(value["object_type"], "report");
        assert_eq!(value["mapping"]["title"], "headline");
        let back: ImportSpec = serde_json::from_value(value).unwrap();
        assert_eq!(back, spec);
    }

    #[test]
    fn unknown_mapping_key_is_rejected() {
        // mapping 打錯字必須當場失敗，否則使用者會拿到「全部欄位都空」的靜默結果。
        let err = serde_json::from_str::<FieldMapping>(r#"{"titel":"x"}"#).unwrap_err();
        assert!(err.to_string().contains("titel"), "{err}");
    }

    #[test]
    fn limits_have_bounded_defaults() {
        let limits = ImportLimits::default();
        assert_eq!(limits.max_records, 10_000);
        assert_eq!(limits.max_depth, 32);
    }
}
