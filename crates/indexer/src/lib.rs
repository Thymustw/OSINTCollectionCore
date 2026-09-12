//! `entity.extracted` → OpenSearch `osint-documents` 投影（SPEC §18）
//! ＋ 面向使用者的搜尋查詢語法。
//!
//! # 這個 crate 為什麼同時放「索引」與「查詢語法」
//!
//! index mapping（欄位型別、analyzer、nested 結構）與查詢（要查哪些欄位、
//! 過濾條件掛在哪一欄、怎麼排序）是同一份契約的兩面。分開放的後果是：
//! 改了 analyzer 卻沒改查詢欄位清單、或改了欄位名卻只改了一邊——
//! 兩者都**不會編譯失敗，也不會執行失敗**，只會讓搜尋悄悄變得不準。
//!
//! 所以：
//!
//! * [`schema`]：index 名稱、欄位名常數、mapping、analyzer 取捨、搜尋欄位權重。
//! * [`query`]：使用者語法（keyword／phrase／boolean）→ 後端中立的語法樹。
//! * [`search`]：使用者請求（八種搜尋）→ [`storage_core::StructuredSearch`]。
//! * [`projection`]：Document + Entity → index 的 `_source`。
//! * [`service`]：consumer 端的索引與重建。
//! * [`batch`]：批次與 backpressure 的決策。
//!
//! core-api 的 `POST /api/v1/search` 與 `osint-cli search` 都只呼叫 [`search::build`]，
//! 不自己組查詢。
//!
//! # PostgreSQL 是 truth，OpenSearch 是 projection
//!
//! index 裡的每一個欄位都能從 PostgreSQL 重算出來，`osint-indexer --rebuild` 就是
//! 那個重算。不要在投影階段產生「只存在於 index」的資訊——它會在下一次重建時消失。
//!
//! # 查詢注入
//!
//! 使用者字串**永遠不會**被交給 OpenSearch 的查詢語言。[`query::parse`] 把它變成
//! [`storage_core::QueryExpr`] 的 Term／Phrase，adapter 再翻成 `multi_match`。
//! `title:*` 只是一個要比對的詞。理由與對照表見 [`query`] 的模組說明。

mod error;
mod health;

pub mod batch;
pub mod projection;
pub mod query;
pub mod schema;
pub mod search;
pub mod service;

pub use error::IndexerError;
pub use health::serve as serve_health;
pub use projection::{IndexEntity, Provenance};
pub use query::{QueryParseError, parse as parse_query};
pub use schema::{DEFAULT_INDEX, DateField};
pub use search::{
    EntityFilter, SearchRequest, SearchRequestError, build as build_search, next_cursor,
};
pub use service::{
    FlushReport, IndexBounds, Indexer, PrepareOutcome, RebuildOptions, RebuildReport,
};
