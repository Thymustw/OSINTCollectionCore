//! PostgreSQL canonical adapter。SQL 只留在這個 crate。

mod error;
mod mapping;
mod store;

pub use store::PostgresCanonicalStore;
