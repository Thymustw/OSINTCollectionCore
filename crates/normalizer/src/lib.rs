//! raw.collected → Document 正規化。
//!
//! 支援：RSS／Atom、HTML，以及走 `POST /api/v1/import` 進來、
//! `metadata.import` 帶有 `ImportSpec` 的 JSON／CSV。
//!
//! 不支援（`SkippedUnsupported`）：沒有 `ImportSpec` 的 JSON／CSV（例如 REST API
//! connector 抓回來的任意 JSON，沒有欄位對映無從拆解）、manual 上傳的任意檔案（PDF 等）。

mod content;
mod error;
mod health;
mod service;

pub use content::{ContentClass, classify_content};
pub use error::NormalizerError;
pub use health::serve as serve_health;
pub use service::{NormalizeOutcome, Normalizer};
