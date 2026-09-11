//! JSON／CSV 匯入的欄位對映與有界解析。
//!
//! 這個 crate 被兩處共用，而且刻意共用：
//!
//! - `core-api` 的 `POST /api/v1/import`：上傳當下就解析一次，讓格式錯誤在 HTTP 回應
//!   當場講清楚，而不是收下之後在 normalizer 靜默跳過。
//! - `normalizer`：真正產生 Document 時再解析一次，用的是寫進
//!   `RawEvidence.metadata["import"]` 的同一份 `ImportSpec`。
//!
//! 兩邊共用同一份實作，才不會出現「API 收得下、normalizer 解不開」的落差。
//!
//! 所有解析都是有界的：筆數、單筆 bytes、單欄位 bytes、JSON 巢狀深度、CSV 欄位數。
//! 上傳大小本身不在這裡管，那是 `core-api` 的 multipart 串流上限。

mod csv_import;
mod error;
mod json;
mod record;
mod spec;

pub use csv_import::parse_csv;
pub use error::ImportError;
pub use json::parse_json;
pub use record::{ImportRecord, ParseOutcome, parse_timestamp};
pub use spec::{Field, FieldMapping, ImportKind, ImportLimits, ImportSpec};

/// 依 `spec.kind` 選解析器。`Manual` 沒有結構可拆，回 `None`。
///
/// 回傳 `Option` 而不是 error：manual 上傳「不產生 Document」是預期行為，不是失敗。
pub fn parse(bytes: &[u8], spec: &ImportSpec) -> Option<Result<ParseOutcome, ImportError>> {
    match spec.kind {
        ImportKind::Manual => None,
        ImportKind::Json => Some(parse_json(bytes, spec)),
        ImportKind::Csv => Some(parse_csv(bytes, spec)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_has_no_parser() {
        assert!(parse(b"%PDF-1.4", &ImportSpec::new(ImportKind::Manual)).is_none());
    }

    #[test]
    fn dispatch_matches_kind() {
        let json = parse(br#"[{"title":"a"}]"#, &ImportSpec::new(ImportKind::Json));
        assert_eq!(json.unwrap().unwrap().records.len(), 1);
        let csv = parse(b"title\na\n", &ImportSpec::new(ImportKind::Csv));
        assert_eq!(csv.unwrap().unwrap().records.len(), 1);
    }
}
