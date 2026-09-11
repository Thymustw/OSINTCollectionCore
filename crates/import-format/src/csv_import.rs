//! CSV 匯入解析。第一列一律是 header。
//!
//! `flexible(false)`：欄位數與 header 不同的列直接報錯，不靜默補空值——
//! 欄位錯位的匯入會產生「看起來成功但內容整排位移」的資料，比失敗難發現得多。

use std::collections::BTreeMap;

use crate::error::ImportError;
use crate::record::{ImportRecord, ParseOutcome};
use crate::spec::{Field, ImportSpec};

/// 解析 CSV 匯入內容。
pub fn parse_csv(bytes: &[u8], spec: &ImportSpec) -> Result<ParseOutcome, ImportError> {
    let limits = &spec.limits;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(ImportError::Empty);
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(false)
        .trim(csv::Trim::All)
        .from_reader(bytes);

    let headers = reader.headers().map_err(|err| map_csv_error(&err))?.clone();
    if headers.is_empty() {
        return Err(ImportError::MissingHeader);
    }
    if headers.len() > limits.max_columns {
        return Err(ImportError::TooManyColumns {
            count: headers.len(),
            limit: limits.max_columns,
        });
    }

    let columns = resolve_columns(&headers, spec)?;
    if columns.is_empty() {
        return Err(ImportError::UnknownColumn {
            column: "title／body／summary".into(),
            available: headers.iter().collect::<Vec<_>>().join(", "),
        });
    }

    let mut records = Vec::new();
    let mut total = 0usize;
    let mut skipped_empty = 0usize;
    for row in reader.records() {
        let row = row.map_err(|err| map_csv_error(&err))?;
        let index = total;
        total += 1;
        if total > limits.max_records {
            return Err(ImportError::TooManyRecords {
                count: total,
                limit: limits.max_records,
            });
        }
        let row_bytes = row.as_slice().len();
        if row_bytes > limits.max_record_bytes {
            return Err(ImportError::RecordTooLarge {
                index,
                size: row_bytes,
                limit: limits.max_record_bytes,
            });
        }

        let mut record = ImportRecord {
            index,
            ..ImportRecord::default()
        };
        for (field, column) in columns.values() {
            if let Some(cell) = row.get(*column) {
                record.set(*field, cell.to_string(), limits)?;
            }
        }
        if record.is_empty() {
            skipped_empty += 1;
            continue;
        }
        records.push(record);
    }

    if total == 0 {
        return Err(ImportError::Empty);
    }
    if records.is_empty() {
        return Err(ImportError::NothingUsable { total });
    }
    Ok(ParseOutcome {
        records,
        total,
        skipped_empty,
    })
}

/// 邏輯欄位 → header 欄位索引。明確指定的 header 找不到時直接報錯（不靜默忽略）。
fn resolve_columns(
    headers: &csv::StringRecord,
    spec: &ImportSpec,
) -> Result<BTreeMap<usize, (Field, usize)>, ImportError> {
    let mut resolved = BTreeMap::new();
    for (slot, field) in Field::ALL.into_iter().enumerate() {
        let column = match spec.mapping.explicit(field) {
            Some(name) => {
                let found =
                    find_header(headers, name).ok_or_else(|| ImportError::UnknownColumn {
                        column: name.to_string(),
                        available: headers.iter().collect::<Vec<_>>().join(", "),
                    })?;
                Some(found)
            }
            None => field
                .default_aliases()
                .iter()
                .find_map(|alias| find_header(headers, alias)),
        };
        if let Some(column) = column {
            resolved.insert(slot, (field, column));
        }
    }
    Ok(resolved)
}

fn find_header(headers: &csv::StringRecord, name: &str) -> Option<usize> {
    headers
        .iter()
        .position(|header| header.trim().eq_ignore_ascii_case(name.trim()))
}

/// csv crate 的錯誤只轉成「第幾行 + 原因」，不外流檔案路徑等內部資訊。
fn map_csv_error(err: &csv::Error) -> ImportError {
    let line = err.position().map_or(1, |pos| pos.line() as usize);
    let detail = match err.kind() {
        csv::ErrorKind::UnequalLengths {
            expected_len, len, ..
        } => format!("欄位數是 {len}，header 有 {expected_len} 欄"),
        csv::ErrorKind::Utf8 { .. } => "內容不是合法 UTF-8".to_string(),
        _ => "格式不正確".to_string(),
    };
    ImportError::MalformedCsv { line, detail }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{ImportKind, ImportSpec};

    fn spec() -> ImportSpec {
        ImportSpec::new(ImportKind::Csv)
    }

    const CSV: &str = "title,description,link,published_at,id\n\
CVE-2026-0001,第一筆,http://127.0.0.1/a,2026-09-10,a-1\n\
CVE-2026-0002,第二筆,http://127.0.0.1/b,2026-09-11,a-2\n";

    #[test]
    fn header_row_drives_mapping() {
        let out = parse_csv(CSV.as_bytes(), &spec()).unwrap();
        assert_eq!(out.total, 2);
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].title.as_deref(), Some("CVE-2026-0001"));
        assert_eq!(out.records[0].summary.as_deref(), Some("第一筆"));
        assert_eq!(out.records[0].url.as_deref(), Some("http://127.0.0.1/a"));
        assert_eq!(out.records[1].external_id.as_deref(), Some("a-2"));
        assert!(out.records[0].published_at.is_some());
    }

    #[test]
    fn explicit_mapping_overrides_conventional_names() {
        let mut spec = spec();
        spec.mapping.title = Some("headline".into());
        spec.mapping.body = Some("full_text".into());
        let csv = "headline,full_text\n標題,內文\n";
        let out = parse_csv(csv.as_bytes(), &spec).unwrap();
        assert_eq!(out.records[0].title.as_deref(), Some("標題"));
        assert_eq!(out.records[0].body.as_deref(), Some("內文"));
    }

    #[test]
    fn mapping_to_missing_column_fails_loudly() {
        let mut spec = spec();
        spec.mapping.title = Some("nope".into());
        let err = parse_csv(CSV.as_bytes(), &spec).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("nope"), "{message}");
        assert!(message.contains("title"), "可用 header 要列出來：{message}");
    }

    #[test]
    fn record_count_limit_is_enforced() {
        let mut spec = spec();
        spec.limits.max_records = 1;
        assert_eq!(
            parse_csv(CSV.as_bytes(), &spec).unwrap_err(),
            ImportError::TooManyRecords { count: 2, limit: 1 }
        );
    }

    #[test]
    fn single_row_size_limit_is_enforced() {
        let mut spec = spec();
        spec.limits.max_record_bytes = 16;
        let csv = format!("title\n{}\n", "x".repeat(64));
        let err = parse_csv(csv.as_bytes(), &spec).unwrap_err();
        assert!(
            matches!(err, ImportError::RecordTooLarge { index: 0, .. }),
            "{err:?}"
        );
    }

    #[test]
    fn oversized_cell_is_rejected() {
        let mut spec = spec();
        spec.limits.max_field_bytes = 8;
        let csv = format!("title\n{}\n", "x".repeat(32));
        let err = parse_csv(csv.as_bytes(), &spec).unwrap_err();
        assert!(
            matches!(err, ImportError::FieldTooLarge { field: "title", .. }),
            "{err:?}"
        );
    }

    #[test]
    fn too_many_columns_is_rejected() {
        let mut spec = spec();
        spec.limits.max_columns = 3;
        let csv = "a,b,c,d,title\n1,2,3,4,x\n";
        assert_eq!(
            parse_csv(csv.as_bytes(), &spec).unwrap_err(),
            ImportError::TooManyColumns { count: 5, limit: 3 }
        );
    }

    #[test]
    fn ragged_row_fails_instead_of_shifting_columns() {
        let csv = "title,description\nonly-one-cell\n";
        let err = parse_csv(csv.as_bytes(), &spec()).unwrap_err();
        assert!(matches!(err, ImportError::MalformedCsv { .. }), "{err:?}");
    }

    #[test]
    fn header_only_file_is_empty() {
        assert_eq!(
            parse_csv(b"title,description\n", &spec()).unwrap_err(),
            ImportError::Empty
        );
    }

    #[test]
    fn quoted_commas_and_newlines_survive() {
        let csv = "title,description\n\"a, b\",\"line1\nline2\"\n";
        let out = parse_csv(csv.as_bytes(), &spec()).unwrap();
        assert_eq!(out.records[0].title.as_deref(), Some("a, b"));
        assert_eq!(
            out.records[0].summary.as_deref(),
            Some("line1\nline2"),
            "引號內的換行屬於同一格"
        );
    }
}
