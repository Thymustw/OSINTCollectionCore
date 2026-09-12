//! Capability-based storage ports。
//!
//! Domain 只依賴這裡的 trait，不依賴具體後端。
//! 後端只實作它真正支援的 capability，不要做成萬能 `UniversalDatabase`。

pub mod capability;
pub mod codec;
pub mod conformance;
pub mod error;
pub mod health;
pub mod traits;

pub use capability::{CapabilityDescriptor, StorageAdapter};
pub use error::StorageError;
pub use health::{HealthProvider, StorageHealth};
pub use traits::{
    BulkFailure, BulkIndexResult, CanonicalStore, EmbeddedStore, KeyValueStore, ObjectStore,
    QueryExpr, RelationalStore, SearchDocument, SearchField, SearchFilter, SearchHit, SearchHits,
    SearchQuery, SearchStore, SimhashCandidate, SortField, StructuredSearch, Transaction,
    TransactionalStore,
};
