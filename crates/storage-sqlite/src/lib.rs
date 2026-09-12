//! SQLite embedded / App / projection adapter。不是 Core canonical store。

mod error;
mod mapping;
mod store;

pub use store::{SqliteEmbeddedStore, SqliteTransaction};
