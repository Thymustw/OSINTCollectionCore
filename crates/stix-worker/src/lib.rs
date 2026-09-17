//! `job.dispatched` → STIX 2.1 bundle 的匯入／匯出。
//!
//! # 執行 `stix_import` 與 `stix_export`
//!
//! `job_type` 不是 [`JOB_TYPE_IMPORT`] 也不是 [`JOB_TYPE_EXPORT`] 的事件
//! （例如 `graph_rebuild`）一律 [`JobDispatchOutcome::Ignored`] 並提交 offset。
//! 匯入寫進 Postgres（見 `service` 模組），匯出從 Postgres 查出來組 bundle
//! 寫進物件儲存（見 `export` 模組）。
//!
//! # PostgreSQL 是 truth
//!
//! Bundle 從物件儲存讀出後，Entity／Relationship／Provenance **寫進 Postgres 同一筆
//! 交易**。超過 [`StixWorkerOptions::max_objects_per_tx`] 整批 Failed，不拆交易。
//! Entity／Relationship 的主鍵重用 [`entity_worker::entity_id`]／
//! [`entity_worker::relationship_id`]，不要在這裡另發明一套 UUID。
//!
//! # 自動核准是加值路徑
//!
//! 交易提交後才跑 resolver + [`resolver::AutoApprovalEvaluator`]。失敗只記 log，
//! 不把已經匯入成功的 Job 改成 Failed（CLAUDE.md §5：AI failure must not block
//! base ingestion）。`max_auto_merges_per_resolve` 是**整個 bundle 共用**一個計數器。

mod error;
mod export;
mod health;
pub mod service;

pub use error::{ImportError, StixWorkerError};
pub use health::serve as serve_health;
pub use service::{
    ACTION_STIX_IMPORTED, JOB_TYPE_EXPORT, JOB_TYPE_IMPORT, JobDispatchOutcome, PROCESSOR,
    STIX_DEFAULT_CONFIDENCE, StixWorker, StixWorkerOptions,
};
