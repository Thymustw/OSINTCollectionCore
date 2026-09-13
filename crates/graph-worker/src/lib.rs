//! `relationship.changed` → Neo4j 圖投影（SPEC_V0.2 §8）。
//!
//! # PostgreSQL 是 truth，Neo4j 是 projection
//!
//! 事件只用來知道「哪條邊變了」。`confidence`／`first_seen`／`last_seen`／
//! `relationship_type`／兩端 id **一律重新從 PostgreSQL 讀**。直接拿 payload
//! 當內容會讓亂序或重放的事件覆蓋掉較新的狀態。
//!
//! # 只有 Entity → Entity 的邊才進圖
//!
//! `relationship.changed` 混了兩種邊：
//!
//! * Document → Entity（例如 `mentions`）：`source_object_id` 是 Document，
//!   Document 不是圖節點。
//! * Entity → Entity（例如 `belongs_to`／`associated_with`）：兩端都是 Entity。
//!
//! `Neo4jStore` 只認 `:Entity` 節點。任一端 `get_entity` 回 `None` 就整條跳過
//! （debug log，不是錯誤）。merge 發的事件全部是 Entity-to-Entity，走同一套
//! 「兩端都查一次」自然正確。
//!
//! # 為什麼沒有批次
//!
//! `GraphStore` 沒有 bulk API。indexer 那套 `BatchController` 在這裡沒有意義——
//! 一則事件處理完就可以決定要不要 commit offset。

mod error;
mod health;
pub mod service;

pub use error::GraphWorkerError;
pub use health::serve as serve_health;
pub use service::{
    GRAPH_REBUILD_JOB_TYPE, GraphWorker, JobDispatchOutcome, ProcessOutcome, RebuildOptions,
    RebuildReport, apply_relationship, parse_change,
};
