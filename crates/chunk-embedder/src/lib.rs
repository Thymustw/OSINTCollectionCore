//! `document.chunked` → osint-inference 段落向量 → OpenSearch `osint-chunks`（ADR-016 階段 5 M2-2）。
//!
//! 段落文字與 id 一律從 PostgreSQL 重讀（事件內容不可信）。重切後段數變少時，刪掉 index 裡
//! 多出來的舊向量。推論暫時不可用時退避、不放棄；永久性錯誤記 failed_event。
//! 設計與營運細節見 `docs/developer/chunk-embedder.md`。

mod error;
mod health;
pub mod inference;
pub mod schema;
mod service;

pub use error::ChunkEmbedderError;
pub use health::serve as serve_health;
pub use inference::{InferenceClient, InferenceError};
pub use service::{ChunkEmbedder, EmbedOutcome, EmbedderConfig, RebuildReport};
