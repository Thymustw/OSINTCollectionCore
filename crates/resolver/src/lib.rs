//! 給 Entity 跑 resolution method，產出 [`ResolutionCandidate`]。
//!
//! 這個 crate **不做事件消費／發布**——那是之後 Phase 1h 的事。呼叫端把
//! [`EntityId`] 丟進來，這裡查 canonical store、跑目前已實作的方法、把新產生的
//! candidate 寫回去。
//!
//! # [`ResolverService::resolve_entity`] 目前聚合六種掃描方法
//!
//! SPEC_V0.2 §6 列了十種方法。掃描式聚合目前跑：
//!
//! * [`check_normalized_name`]（跨 `EntityType`、相同 `normalized_name`）
//! * [`check_alias`]（共用 alias 文字）
//! * [`check_domain`]（兩個 Entity 都連到同一個衍生 Domain Entity）
//! * [`check_semantic_similarity`]（同 type embedding cosine）
//! * [`check_graph_context`]（一跳鄰居 Jaccard）
//! * [`check_account_handle`]（同一個 handle 出現在不同平台 namespace）
//!
//! 舊版 `check_url` 已退場：URL 與 Email 共用網域都由 entity-worker 寫成
//! Relationship，走 [`check_domain`] 即可。舊版靠 `entity_identifiers` 反查
//! 的 `check_domain` 在 UNIQUE 限制下是結構性死碼（已用真實 PostgreSQL 驗證）。
//!
//! [`resolution_candidate_from_identifier_conflict`] **不是**掃描式方法：
//! identifier 寫入衝突時組候選，entity-worker 的 `upsert_entity` 是第一個
//! 生產呼叫端。性質不同，保持獨立。
//!
//! `email`／`external_id` **刻意不做**：entity-worker 寫 `email`／`cve`
//! namespace 時，`(namespace, normalized_value)` UNIQUE 撞號就會觸發
//! `exact_identifier` 候選。再做一個批次掃描版本會是跟舊版 `check_domain`
//! 一樣的結構性死碼——同一個 UNIQUE 保證「查自己 identifier 的目前 owner」
//! 永遠是自己。`account_handle` 能做，是因為跨平台 namespace 字串不同，
//! UNIQUE 擋不住「github_handle=alice 與 twitter_handle=alice」。SPEC §6
//! 仍禁止只因同 username 就判定同一真實人物，所以這個方法的分數刻意壓低。
//!
//! [`ResolutionCandidate`]: core_model::ResolutionCandidate
//! [`EntityId`]: core_model::EntityId
//! [`resolution_candidate_from_identifier_conflict`]:
//!     crate::conflict::resolution_candidate_from_identifier_conflict
//! [`check_normalized_name`]: crate::ResolverService::check_normalized_name
//! [`check_alias`]: crate::identifier_methods::check_alias
//! [`check_domain`]: crate::identifier_methods::check_domain
//! [`check_account_handle`]: crate::identifier_methods::check_account_handle
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
pub use identifier_methods::{check_account_handle, check_alias, check_domain};
pub use semantic::check_semantic_similarity;
pub use service::{
    ALL_ENTITY_TYPES, GRAPH_CONTEXT_THRESHOLD, ResolverService, SEMANTIC_SIMILARITY_THRESHOLD,
};
