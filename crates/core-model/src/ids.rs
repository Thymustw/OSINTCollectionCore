//! 識別碼別名。全部使用 UUID v7，對 PostgreSQL B-tree 插入較友善。

use uuid::Uuid;

pub type SourceId = Uuid;
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
