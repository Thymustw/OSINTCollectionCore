//! `osint-discovery-worker` 函式庫面。Phase 3 Step A：只有骨架，Discovery
//! method 執行邏輯留給 Step B。

pub mod error;
mod health;

pub use error::DiscoveryWorkerError;
pub use health::serve as serve_health;

/// `job.dispatched` 事件裡 `payload.job_type` 的值，`POST /discovery/run`
/// （Phase 3 Step C，尚未實作）會用這個字串建立 Job。集中定義在這裡而不是
/// 讓呼叫端（`core-api`）各自重複宣告一份同值常數——V0.2 的
/// `stix-worker::JOB_TYPE_IMPORT` 與 `core-api/resources/stix.rs` 各自宣告
/// 一份同值常數是已知的重複，Discovery 這裡不要延續那個模式。
pub const JOB_TYPE_DISCOVERY_RUN: &str = "discovery_run";
