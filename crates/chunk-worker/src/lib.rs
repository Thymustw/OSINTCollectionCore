//! `dedup.completed` → 段落切分（ADR-016 階段 5 M1）→ `document_chunks` → `document.chunked`。
//!
//! 只處理非重複文件（事件 `is_duplicate=false` 且 DB `duplicate_of IS NULL`）。
//! 切段規則在 `chunker` crate；這裡只負責讀文件、比對、寫庫、發事件。
//! 設計與營運細節見 `docs/developer/chunk-worker.md`。

mod error;
mod health;
mod service;

pub use error::ChunkWorkerError;
pub use health::serve as serve_health;
pub use service::{ChunkOutcome, ChunkWorker, RebuildReport};
