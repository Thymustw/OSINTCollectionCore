//! 給 Entity 跑 resolution method，產出 [`ResolutionCandidate`]。
//!
//! 這個 crate **不做事件消費／發布**——那是之後 Phase 1h 的事。呼叫端把
//! [`EntityId`] 丟進來，這裡查 canonical store、跑目前已實作的方法、把新產生的
//! candidate 寫回去。
//!
//! # 目前真正有資料可比對的方法只有 `normalized_name`
//!
//! SPEC_V0.2 §6 列了十種方法。這裡只實作「跨 `EntityType`、相同
//! `normalized_name`」那一條——資料來源是既有的 `entities` 表，
//! [`storage_core::RelationalStore::find_entity_by_normalized_name`] 已經能查。
//!
//! 其餘九種（`exact_identifier`／`alias`／`domain`／`url`／`account_handle`／
//! `email`／`external_id`／`semantic_similarity`／`graph_context`）**這次刻意
//! 不做**：它們依賴 `entity_identifiers`／`entity_aliases` 的寫入者，而
//! entity-worker 還沒寫那些表。空跑一圈只會得到永遠為空的結果，看起來像
//! 「這方法沒命中」，其實是「根本沒資料」。那是獨立技術債，不是這個 crate
//! 該順手補的範圍。
//!
//! `exact_identifier` 有一個**純函式 helper**
//! [`resolution_candidate_from_identifier_conflict`]：給未來的 identifier
//! 寫入者在撞到 `StorageError::Conflict` 時組出候選。目前沒有呼叫端，
//! 也不在這裡寫入 store。
//!
//! [`ResolutionCandidate`]: core_model::ResolutionCandidate
//! [`EntityId`]: core_model::EntityId
//! [`resolution_candidate_from_identifier_conflict`]:
//!     crate::conflict::resolution_candidate_from_identifier_conflict

mod conflict;
mod error;
mod service;

pub use conflict::resolution_candidate_from_identifier_conflict;
pub use error::ResolverError;
pub use service::{ALL_ENTITY_TYPES, ResolverService};
