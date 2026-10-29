//! PostgreSQL canonical adapter。SQL 只留在這個 crate。

mod error;
mod mapping;
mod security;
mod store;
// 測試輔助（drop 時停用 connector、跨行程測試鎖）只在 `test-support` feature 下編譯，
// 由各 crate 的 dev-dependencies 開啟；正式 binary 不會帶著「drop 就改 DB」的型別。
#[cfg(feature = "test-support")]
mod test_guard;

pub use security::{PostgresApiTokenStore, PostgresAuditLog};
#[cfg(feature = "test-support")]
pub use store::CollectStateTestLock;
pub use store::{PostgresCanonicalStore, PostgresTransaction};
#[cfg(feature = "test-support")]
pub use test_guard::DisableConnectorsOnDrop;
