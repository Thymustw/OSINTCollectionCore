//! Capability-based storage ports。
//!
//! Domain 只依賴這裡的 trait，不依賴具體後端。
//! 後端只實作它真正支援的 capability，不要做成萬能 `UniversalDatabase`。

pub mod capability;
pub mod codec;
pub mod conformance;
pub mod error;
pub mod health;
pub mod mock;
pub mod traits;

pub use capability::{CapabilityDescriptor, StorageAdapter};
pub use error::StorageError;
pub use health::{HealthProvider, StorageHealth};
pub use traits::{
    BudgetConsumption, BulkFailure, BulkIndexResult, CandidateDiscoveryOutcome, CanonicalStore,
    CheckpointUpdate, ConnectorRunStateUpdate, DailyUsage, EMBEDDING_CACHE_KEY_PREFIX,
    EmbeddedStore, EmbeddingKind, EmbeddingModelRef, EmbeddingProvider, EmbeddingRequest,
    EmbeddingVector, GraphEdge, GraphNode, GraphPath, GraphPattern, GraphQuery, GraphStore,
    GraphTraversalOptions, JobPurgeFilter, KeyValueStore, ObjectStore, ProjectionCheckpoint,
    ProjectionLag, ProjectionStore, QueryExpr, RECLAIM_ORPHAN_COLLECT_JOB_ERROR, RebuildState,
    RebuildStatus, RelationalStore, SearchDocument, SearchField, SearchFilter, SearchHit,
    SearchHits, SearchQuery, SearchStore, SimhashCandidate, SortField, StructuredSearch,
    Transaction, TransactionalStore, VectorSearch, embedding_cache_key, embedding_content_hash,
    merge_json_objects,
};
