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
//! 其餘方法裡，`exact_identifier`、`alias`、`domain`、`url`、
//! `semantic_similarity` 與 `graph_context` 有**自由函式**（尚未接進
//! [`ResolverService::resolve_entity`] 聚合）：
//!
//! * [`resolution_candidate_from_identifier_conflict`]：identifier 寫入衝突時組候選。
//!   entity-worker 的 `upsert_entity` 是第一個生產呼叫端。
//! * [`check_alias`]／[`check_domain`]／[`check_url`]：查
//!   `entity_aliases`／`entity_identifiers`。`entity_identifiers` 已由
//!   entity-worker 為 Domain／Ip／Url／Email／CVE 寫入；`entity_aliases` 仍空。
//!   空 Vec 是「沒資料」或「沒命中」，兩者要靠表是否有列來分辨。
//! * [`check_semantic_similarity`]：同 type Entity 用 embedding cosine 比對。
//! * [`check_graph_context`]：用一跳鄰居的 Jaccard 相似度找 2-hop 候選。
//!
//! `account_handle`／`email`／`external_id` 仍未做：同樣依賴 identifier
//! 寫入者，且 SPEC §6 明文禁止只因同 username 就判定同一真實人物，
//! 需要額外設計 score 上限，不是照抄 `check_domain` 的形狀就能做。
//!
//! [`ResolutionCandidate`]: core_model::ResolutionCandidate
//! [`EntityId`]: core_model::EntityId
//! [`resolution_candidate_from_identifier_conflict`]:
//!     crate::conflict::resolution_candidate_from_identifier_conflict
//! [`check_alias`]: crate::identifier_methods::check_alias
//! [`check_domain`]: crate::identifier_methods::check_domain
//! [`check_url`]: crate::identifier_methods::check_url
//! [`check_semantic_similarity`]: crate::semantic::check_semantic_similarity
//! [`check_graph_context`]: crate::graph_context::check_graph_context
//! [`ResolverService::resolve_entity`]: crate::ResolverService::resolve_entity

mod conflict;
mod error;
mod graph_context;
mod identifier_methods;
mod semantic;
mod service;

pub use conflict::resolution_candidate_from_identifier_conflict;
pub use error::ResolverError;
pub use graph_context::check_graph_context;
pub use identifier_methods::{check_alias, check_domain, check_url};
pub use semantic::check_semantic_similarity;
pub use service::{ALL_ENTITY_TYPES, ResolverService};
