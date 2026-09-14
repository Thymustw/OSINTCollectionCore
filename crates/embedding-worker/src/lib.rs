//! `entity.extracted` → Document／Entity 向量投影（SPEC_V0.2 §11–§14）。
//!
//! # PostgreSQL 是 truth，OpenSearch 是 projection
//!
//! 事件只用來知道「哪一份 Document、哪些 Entity 剛抽完」。文字與 metadata
//! **一律重新從 PostgreSQL 讀**。直接拿 payload 當內容會讓亂序或重放的事件
//! 覆蓋掉較新的狀態。
//!
//! # 兩個目標、兩個 index
//!
//! * Document title／body → 向量疊加進既有 `osint-documents` 的
//!   `embedding_en`／`embedding_multi`。用 [`storage_core::SearchStore::update_fields`]，
//!   **永不** `index()`——index API 會把 title／body／entities 整份清掉。
//! * Entity description → 向量寫進獨立的 `osint-entities`。用
//!   [`storage_core::SearchStore::index`] 整份覆寫。不 nested 進 documents：
//!   一份 Entity 會活在上百份文件的 nested 陣列裡，違反「PostgreSQL 是
//!   canonical，OpenSearch 是 rebuildable projection」。
//!
//! # 不處理的目標
//!
//! [`core_model::EmbeddingTarget::EventDescription`] 在 trait／schema 裡存在，
//! 但 V0.2 沒有 Event 抽取管線（`core_model::Event` 只有 CRUD）。本服務
//! **不會**掃 Event、也不會假裝處理後靜默跳過一則 event payload——根本沒有
//! 那種事件。等有 Event 資料源再接，不要在這裡先寫一個永遠走不到的分支。
//!
//! # Entity 語言永遠未知
//!
//! [`core_model::Entity`] 沒有 `language` 欄位。`EmbeddingRequest.language = None`
//! 代表未知，路由到多語 e5，**不是** MiniLM。V0.2 **只寫**
//! `description_vector_multi`；`description_vector_en` 在 mapping 裡保留但
//! 永遠是空的（accepted limitation，直到 NER 為 Entity 加上語言）。
//!
//! # 為什麼訂 `entity.extracted` 而不是 `embedding.requested`
//!
//! `embedding.requested` 是 SPEC §20 的保留名，目前零生產者／零消費者，
//! 與 V0.1 的 `search.index.requested` 同一狀態。硬去訂一則沒人發的事件
//! 不算完成。indexer 與本服務是同一個 topic 上的獨立 consumer group，
//! 沒有順序保證——`update_fields` 碰到 NotFound 當 indexer lag，重試後
//! 仍提交 offset（漏寫靠 `--rebuild` 回填）。

mod error;
mod health;
pub mod schema;
pub mod service;

pub use error::EmbeddingWorkerError;
pub use health::serve as serve_health;
pub use service::{
    EmbeddingBounds, EmbeddingWorker, ProcessReport, RebuildOptions, RebuildReport, parse_extracted,
};
