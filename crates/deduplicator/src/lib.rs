//! `object.normalized` → 五階段去重（SPEC §15）→ `DuplicateGroup`（SPEC §16）
//! → `dedup.completed`。
//!
//! | Stage | 判準 | 相似度 |
//! |---|---|---|
//! | 1 | `platform + external_id` 完全相同 | 1.0 |
//! | 2 | 正規化後的 canonical URL 完全相同 | 1.0 |
//! | 3 | `SHA256(normalized content)` 完全相同 | 1.0 |
//! | 4 | 64-bit SimHash 的 Hamming 距離 ≤ 門檻（預設 3） | `1 - d/64` |
//! | 5 | 語意重複 | OpenSearch k-NN；基礎設施失敗降級為 `Unsupported` |
//!
//! **SPEC §16 的硬性規則：不得刪掉 duplicate evidence。** 這個 crate 沒有任何
//! `delete_*` 呼叫，判定重複只會新增一列 `DuplicateGroup` 並在 Document 上標
//! `duplicate_of`。設計說明與已知限制見 `docs/developer/deduplicator.md`。

mod error;
mod health;
mod service;

pub mod semantic;
pub mod semantic_real;
pub mod simhash;

/// Stage 2 的 canonical URL 正規化。
///
/// 實作已搬到 `core_model::url_norm`，讓 entity-worker 抽 URL Entity 時能用**同一套**規則
/// （理由見該模組開頭）。這裡保留 `deduplicator::url_norm` 這個路徑，既有呼叫端不必改。
pub use core_model::url_norm;

pub use error::DeduplicatorError;
pub use health::serve as serve_health;
pub use semantic::{SemanticDuplicateDetector, SemanticOutcome, UnsupportedSemanticDetector};
pub use semantic_real::VectorSemanticDetector;
pub use service::{
    ACTION_DEDUPLICATED, DedupBounds, DedupOutcome, DedupStage, Deduplicator, PROCESSOR,
    duplicate_group_id,
};
