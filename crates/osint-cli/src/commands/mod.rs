//! 各子命令的實作。全部唯讀。

pub mod connectors;
pub mod documents;
pub mod entities;
pub mod health;
pub mod jobs;
pub mod raw;
pub mod sources;

/// 用 cursor 連續翻頁，直到湊滿 `limit` 筆或後端回了不滿一頁。
///
/// 為什麼不直接把 `limit` 丟給 store：`RelationalStore` 的 list 方法會把 limit 夾在
/// 1..=100（避免無界查詢）。傳 500 進去不會報錯，只會**靜默**回 100 筆——
/// 使用者以為看到全部了。要更多筆就必須自己翻頁。
///
/// 用法：`collect_paged!(store, list_sources(), limit)`、
/// `collect_paged!(store, list_raw_evidence_by_source(source_id), limit)`；
/// cursor 與 limit 由巨集補在參數尾端。
macro_rules! collect_paged {
    ($store:expr, $method:ident ( $($arg:expr),* ), $limit:expr) => {{
        let limit: u32 = $limit;
        let mut out = Vec::new();
        let mut cursor: Option<uuid::Uuid> = None;
        while (out.len() as u32) < limit {
            let want = (limit - out.len() as u32).min($crate::cli::PAGE_SIZE);
            let page = $store.$method($($arg,)* cursor, want).await?;
            let got = page.len() as u32;
            cursor = page.last().map(|item| item.id);
            out.extend(page);
            if got < want {
                break;
            }
        }
        out
    }};
}

pub(crate) use collect_paged;
