//! 輸出格式：預設人類可讀表格，`--json` 切成 JSON。
//!
//! 表格用 comfy-table，但關掉 default features（不帶 crossterm）：這支工具的輸出常被
//! 導向檔案或 pipe，偵測終端機寬度沒有意義，欄寬由這裡自己截斷控制。

use comfy_table::{Cell, ContentArrangement, Table, presets::UTF8_FULL};
use serde::Serialize;

use crate::error::CliError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Table,
    Json,
}

impl Format {
    #[must_use]
    pub fn from_flag(json: bool) -> Self {
        if json { Self::Json } else { Self::Table }
    }
}

/// JSON 輸出（pretty，末尾換行）。
pub fn print_json<T: Serialize>(value: &T) -> Result<(), CliError> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

/// 表格輸出。`rows` 為空時印一行提示而不是空表格——空表格看起來像壞掉。
pub fn print_table(headers: &[&str], rows: Vec<Vec<String>>, empty_hint: &str) {
    if rows.is_empty() {
        println!("（沒有資料）{empty_hint}");
        return;
    }
    let mut table = Table::new();
    table
        .load_style(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Disabled)
        .set_header(headers.iter().map(|h| Cell::new(*h)));
    for row in rows {
        table.add_row(row);
    }
    println!("{table}");
    println!("共 {} 筆", table.row_count());
}

/// key-value 縱向表格，給 `show` 子命令用（欄位多，橫向排不下）。
pub fn print_detail(pairs: Vec<(&str, String)>) {
    let mut table = Table::new();
    table
        .load_style(UTF8_FULL)
        .set_content_arrangement(ContentArrangement::Disabled)
        .set_header(vec![Cell::new("欄位"), Cell::new("值")]);
    for (key, value) in pairs {
        table.add_row(vec![key.to_string(), value]);
    }
    println!("{table}");
}

/// 截斷成最多 `max` 個字元（以 char 計，不切破 UTF-8），超過補 `…`。
#[must_use]
pub fn truncate(text: &str, max: usize) -> String {
    let single_line = text.replace(['\n', '\r'], " ");
    if single_line.chars().count() <= max {
        return single_line;
    }
    let kept: String = single_line.chars().take(max.saturating_sub(1)).collect();
    format!("{kept}…")
}

/// `Option<String>` → 顯示字串。`None` 一律顯示成 `-`，避免和空字串混淆。
#[must_use]
pub fn opt(value: Option<&str>) -> String {
    value.map_or_else(|| "-".to_string(), ToString::to_string)
}

/// 時間一律 RFC3339（秒精度）。`None` → `-`。
#[must_use]
pub fn ts(value: Option<chrono::DateTime<chrono::Utc>>) -> String {
    value.map_or_else(
        || "-".to_string(),
        |t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_keeps_short_text() {
        assert_eq!(truncate("abc", 10), "abc");
    }

    #[test]
    fn truncate_adds_ellipsis() {
        assert_eq!(truncate("abcdef", 4), "abc…");
    }

    #[test]
    fn truncate_does_not_split_multibyte() {
        // 以 char 計數；若改成 byte slice 會在這裡 panic。
        assert_eq!(truncate("漏洞揭露公告", 3), "漏洞…");
    }

    #[test]
    fn truncate_flattens_newlines() {
        assert_eq!(truncate("a\nb", 10), "a b");
    }

    #[test]
    fn none_shows_dash() {
        assert_eq!(opt(None), "-");
        assert_eq!(ts(None), "-");
    }
}
