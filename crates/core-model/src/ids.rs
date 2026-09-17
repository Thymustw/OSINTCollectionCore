//! 識別碼別名。全部使用 UUID v7，對 PostgreSQL B-tree 插入較友善。

use uuid::Uuid;

pub type SourceId = Uuid;
pub type NetworkRuleId = Uuid;
pub type ConnectorId = Uuid;
pub type CollectionId = Uuid;
pub type WorkspaceId = Uuid;
pub type RawEvidenceId = Uuid;
pub type DocumentId = Uuid;
pub type EntityId = Uuid;
pub type RelationshipId = Uuid;
pub type EventId = Uuid;
pub type ProvenanceId = Uuid;
pub type JobId = Uuid;
pub type DuplicateGroupId = Uuid;
pub type RelationshipEvidenceId = Uuid;
pub type EntityExtractionId = Uuid;
pub type ObjectId = Uuid;

// ===== V0.2（SPEC_V0.2 §3／§4／§5／§7 + ADR-008）=====

pub type EntityAliasId = Uuid;
pub type EntityIdentifierId = Uuid;
pub type ResolutionCandidateId = Uuid;
pub type MergeHistoryId = Uuid;
pub type FailedEventId = Uuid;
pub type EmbeddingId = Uuid;

// ===== V0.3（SPEC_V0.3 §2／§4／§6／§7）=====

pub type SeedId = Uuid;
pub type CandidateId = Uuid;
pub type CandidateEvidenceId = Uuid;
pub type AiRunId = Uuid;
