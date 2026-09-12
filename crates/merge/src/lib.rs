//! 執行一次 Entity merge 決策（SPEC_V0.2 §7）。
//!
//! 這個 crate **不產生候選**——那是 `crates/resolver` 的事。呼叫端已經決定
//! 「把 `merged` 併進 `survivor`」，這裡負責：
//!
//! 1. 前置檢查（存在、型別相同、兩端都還沒被併掉）。
//! 2. 在一筆交易裡 repoint 參照、處理 relationship UNIQUE 撞號、標記
//!    `entities.merged_into`、寫 [`MergeHistory`]。
//! 3. 依 [`MergeHistory`] 做可逆的 [`MergeService::undo_merge`]。
//! 4. `tx.commit()` 成功後發 `relationship.changed`（沒接 Kafka 時跳過）。
//!
//! # 交易邊界
//!
//! 前置檢查在交易外做（少開一筆空交易）。真正改資料一律走
//! [`storage_core::TransactionalStore::begin`]：中途失敗讓 `Transaction` drop
//! 回滾，狀態只剩「全在」或「全不在」。
//!
//! # 收集上限
//!
//! 參照收集走既有 `list_*` 方法，上限是 [`MERGE_REF_CAP`]（100）。
//! adapter 會把更大的 `limit` 夾回 100，傳 1000 只是自己騙自己。
//! 達到上限就中止並回 [`MergeError::CollectionTruncated`]，不允許半改的圖。
//!
//! [`MergeHistory`]: core_model::MergeHistory

mod collision;
mod error;
mod repoint;
mod service;

pub use collision::{Collision, classify_relationships};
pub use error::MergeError;
pub use repoint::{relationship_repoint_column, repointed, repointed_ends};
pub use service::{MERGE_REF_CAP, MergeService};
