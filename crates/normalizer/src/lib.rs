//! raw.collected → Document 正規化。

mod content;
mod error;
mod health;
mod service;

pub use content::{ContentClass, classify_content};
pub use error::NormalizerError;
pub use health::serve as serve_health;
pub use service::{NormalizeOutcome, Normalizer};
