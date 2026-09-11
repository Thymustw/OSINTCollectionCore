//! JSON 匯入解析。
//!
//! 支援兩種形狀，由內容本身決定（不看副檔名、不看 `Content-Type`）：
//!
//! - 物件陣列 `[{…}, {…}]`：手工整理的資料最常見的形狀。
//! - NDJSON（每行一個物件）：匯出工具常見的形狀，而且可以逐行套上單筆大小上限，
//!   記憶體用量與筆數無關。
//!
//! 兩種都只接受「物件」當一筆紀錄。純量陣列（`[1,2,3]`）沒有欄位可對映，直接拒絕。

use serde_json::Value;

use crate::error::ImportError;
use crate::record::{ImportRecord, ParseOutcome};
use crate::spec::{Field, ImportLimits, ImportSpec};

/// 解析 JSON 匯入內容。
pub fn parse_json(bytes: &[u8], spec: &ImportSpec) -> Result<ParseOutcome, ImportError> {
    let limits = &spec.limits;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Err(ImportError::Empty);
    }
    // 先做深度掃描再交給 serde_json：serde_json 自己有遞迴上限，但那是保護它自己的堆疊，
    // 不是我們的政策上限，而且錯誤訊息對使用者沒有意義。
    check_depth(bytes, limits.max_depth)?;

    let first = bytes
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .copied()
        .unwrap_or(b'\0');
    let values = if first == b'[' {
        parse_array(bytes, limits)?
    } else {
        parse_ndjson(bytes, limits)?
    };

    if values.len() > limits.max_records {
        return Err(ImportError::TooManyRecords {
            count: values.len(),
            limit: limits.max_records,
        });
    }

    let total = values.len();
    let mut records = Vec::with_capacity(total);
    let mut skipped_empty = 0;
    for (index, value) in values.into_iter().enumerate() {
        let mut record = ImportRecord {
            index,
            ..ImportRecord::default()
        };
        for field in Field::ALL {
            if let Some(text) = lookup(&value, field, spec) {
                record.set(field, text, limits)?;
            }
        }
        if record.is_empty() {
            skipped_empty += 1;
            continue;
        }
        records.push(record);
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

/// 陣列形狀：整份解析後逐一量測單筆大小。整體記憶體由上傳大小上限夾住。
fn parse_array(bytes: &[u8], limits: &ImportLimits) -> Result<Vec<Value>, ImportError> {
    let parsed: Value =
        serde_json::from_slice(bytes).map_err(|err| ImportError::MalformedJson {
            line: err.line(),
            detail: err.to_string(),
        })?;
    let Value::Array(items) = parsed else {
        return Err(ImportError::NotAnObject { line: 1 });
    };
    if items.len() > limits.max_records {
        return Err(ImportError::TooManyRecords {
            count: items.len(),
            limit: limits.max_records,
        });
    }
    for (index, item) in items.iter().enumerate() {
        if !item.is_object() {
            return Err(ImportError::NotAnObject { line: index + 1 });
        }
        let size = serde_json::to_vec(item).map(|v| v.len()).unwrap_or(0);
        if size > limits.max_record_bytes {
            return Err(ImportError::RecordTooLarge {
                index,
                size,
                limit: limits.max_record_bytes,
            });
        }
    }
    Ok(items)
}

/// NDJSON：逐行套上單筆大小與筆數上限，讀到超限就停，不會把整份都解析完。
fn parse_ndjson(bytes: &[u8], limits: &ImportLimits) -> Result<Vec<Value>, ImportError> {
    let text = std::str::from_utf8(bytes).map_err(|err| ImportError::MalformedJson {
        line: 1,
        detail: format!("內容不是合法 UTF-8（第 {} byte 起）", err.valid_up_to()),
    })?;
    let mut values = Vec::new();
    for (offset, line) in text.lines().enumerate() {
        let line_no = offset + 1;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed.len() > limits.max_record_bytes {
            return Err(ImportError::RecordTooLarge {
                index: values.len(),
                size: trimmed.len(),
                limit: limits.max_record_bytes,
            });
        }
        if values.len() >= limits.max_records {
            return Err(ImportError::TooManyRecords {
                count: values.len() + 1,
                limit: limits.max_records,
            });
        }
        let value: Value =
            serde_json::from_str(trimmed).map_err(|err| ImportError::MalformedJson {
                line: line_no,
                detail: err.to_string(),
            })?;
        if !value.is_object() {
            return Err(ImportError::NotAnObject { line: line_no });
        }
        values.push(value);
    }
    if values.is_empty() {
        return Err(ImportError::Empty);
    }
    Ok(values)
}

/// 掃 bytes 算巢狀深度。字串內的括號不算，跳脫字元要跳過。
fn check_depth(bytes: &[u8], limit: usize) -> Result<(), ImportError> {
    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in bytes {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        match byte {
            b'"' => in_string = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > limit {
                    return Err(ImportError::TooDeep { limit });
                }
            }
            b'}' | b']' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok(())
}

/// 取一個欄位的文字值。mapping 指定 `/` 開頭時走 JSON Pointer，否則比對頂層鍵。
fn lookup(value: &Value, field: Field, spec: &ImportSpec) -> Option<String> {
    if let Some(key) = spec.mapping.explicit(field) {
        if key.starts_with('/') {
            return value.pointer(key).and_then(as_text);
        }
        return top_level(value, key).and_then(as_text);
    }
    for alias in field.default_aliases() {
        if let Some(found) = top_level(value, alias).and_then(as_text) {
            return Some(found);
        }
    }
    None
}

fn top_level<'a>(value: &'a Value, key: &str) -> Option<&'a Value> {
    let object = value.as_object()?;
    if let Some(found) = object.get(key) {
        return Some(found);
    }
    object
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(key))
        .map(|(_, found)| found)
}

/// 只接受純量。物件／陣列沒有單一文字表示法，當作沒填——
/// 硬塞 `{"a":1}` 的字面 JSON 進 title 只會產生看不懂的 Document。
fn as_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::ImportKind;

    fn spec() -> ImportSpec {
        ImportSpec::new(ImportKind::Json)
    }

    #[test]
    fn array_of_objects_uses_conventional_keys() {
        let body = r#"[
          {"title":"CVE-2026-0001","description":"摘要","link":"http://127.0.0.1/a","published":"2026-09-10T00:00:00Z","id":"a-1"},
          {"title":"CVE-2026-0002","content":"內文"}
        ]"#;
        let out = parse_json(body.as_bytes(), &spec()).unwrap();
        assert_eq!(out.total, 2);
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[0].title.as_deref(), Some("CVE-2026-0001"));
        assert_eq!(out.records[0].summary.as_deref(), Some("摘要"));
        assert_eq!(out.records[0].url.as_deref(), Some("http://127.0.0.1/a"));
        assert_eq!(out.records[0].external_id.as_deref(), Some("a-1"));
        assert!(out.records[0].published_at.is_some());
        assert_eq!(out.records[1].body.as_deref(), Some("內文"));
    }

    #[test]
    fn ndjson_is_accepted() {
        let body = b"{\"title\":\"one\"}\n\n{\"title\":\"two\"}\n";
        let out = parse_json(body, &spec()).unwrap();
        assert_eq!(out.records.len(), 2);
        assert_eq!(out.records[1].title.as_deref(), Some("two"));
    }

    #[test]
    fn explicit_mapping_and_pointer() {
        let mut spec = spec();
        spec.mapping.title = Some("headline".into());
        spec.mapping.body = Some("/attributes/full_text".into());
        let body = r#"[{"headline":"標題","attributes":{"full_text":"深層內文"}}]"#;
        let out = parse_json(body.as_bytes(), &spec).unwrap();
        assert_eq!(out.records[0].title.as_deref(), Some("標題"));
        assert_eq!(out.records[0].body.as_deref(), Some("深層內文"));
    }

    #[test]
    fn record_count_limit_is_enforced() {
        let mut spec = spec();
        spec.limits.max_records = 2;
        let body = br#"[{"title":"a"},{"title":"b"},{"title":"c"}]"#;
        assert_eq!(
            parse_json(body, &spec).unwrap_err(),
            ImportError::TooManyRecords { count: 3, limit: 2 }
        );
        let ndjson = b"{\"title\":\"a\"}\n{\"title\":\"b\"}\n{\"title\":\"c\"}\n";
        assert_eq!(
            parse_json(ndjson, &spec).unwrap_err(),
            ImportError::TooManyRecords { count: 3, limit: 2 }
        );
    }

    #[test]
    fn single_record_size_limit_is_enforced() {
        let mut spec = spec();
        spec.limits.max_record_bytes = 40;
        let big = format!(r#"[{{"title":"{}"}}]"#, "x".repeat(100));
        let err = parse_json(big.as_bytes(), &spec).unwrap_err();
        assert!(
            matches!(err, ImportError::RecordTooLarge { index: 0, .. }),
            "{err:?}"
        );
    }

    #[test]
    fn deep_nesting_is_rejected_before_parsing() {
        let mut spec = spec();
        spec.limits.max_depth = 8;
        let body = format!("[{}{}]", "[".repeat(20), "]".repeat(20));
        assert_eq!(
            parse_json(body.as_bytes(), &spec).unwrap_err(),
            ImportError::TooDeep { limit: 8 }
        );
    }

    #[test]
    fn brackets_inside_strings_do_not_count_as_depth() {
        let mut spec = spec();
        spec.limits.max_depth = 3;
        let body = br#"[{"title":"[[[[[[[[[[ not nesting \" still not"}]"#;
        let out = parse_json(body, &spec).unwrap();
        assert_eq!(out.records.len(), 1);
    }

    #[test]
    fn oversized_field_is_rejected() {
        let mut spec = spec();
        spec.limits.max_field_bytes = 16;
        let body = format!(r#"[{{"title":"{}"}}]"#, "x".repeat(64));
        let err = parse_json(body.as_bytes(), &spec).unwrap_err();
        assert!(
            matches!(
                err,
                ImportError::FieldTooLarge {
                    field: "title",
                    limit: 16,
                    ..
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn scalar_array_is_rejected_with_actionable_message() {
        let err = parse_json(b"[1,2,3]", &spec()).unwrap_err();
        let message = err.to_string();
        assert!(message.contains("NDJSON"), "{message}");
    }

    #[test]
    fn empty_body_is_rejected() {
        assert_eq!(
            parse_json(b"   \n", &spec()).unwrap_err(),
            ImportError::Empty
        );
    }

    #[test]
    fn records_without_any_text_are_reported() {
        let err = parse_json(br#"[{"foo":1},{"bar":2}]"#, &spec()).unwrap_err();
        assert_eq!(err, ImportError::NothingUsable { total: 2 });
    }

    #[test]
    fn partially_empty_records_are_skipped_not_fatal() {
        let out = parse_json(br#"[{"title":"ok"},{"foo":1}]"#, &spec()).unwrap();
        assert_eq!(out.total, 2);
        assert_eq!(out.skipped_empty, 1);
        assert_eq!(out.records.len(), 1);
    }
}
