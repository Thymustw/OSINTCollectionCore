//! `dedup.completed` → SPEC §17 entity extraction → SPEC §11／§12 Relationship 與
//! RelationshipEvidence → SPEC §14 Provenance → `entity.extracted`。
//!
//! | Entity type | 抽取器 | 正規化 |
//! |---|---|---|
//! | Vulnerability（CVE） | `regex-cve` | 轉大寫 |
//! | IP | `regex-ipv4` / `regex-ipv6` | `IpAddr` 標準形式（IPv6 壓縮） |
//! | URL | `regex-url` | `core_model::url_norm`（與 dedup Stage 2 同一套） |
//! | Email | `regex-email` | 轉小寫 |
//! | Domain | `regex-domain` / `derived-url-host` / `derived-email-domain` | 轉小寫、去尾端 `.` |
//! | Hash | `regex-hash` | 轉小寫 |
//! | Person | `field-author` | 小寫、空白正規化 |
//! | Organization | `field-organization` | 小寫、空白正規化 |
//!
//! # V0.1 的兩個硬性範圍限制
//!
//! 1. **沒有 AI／NER。** 只有確定性規則。Person／Organization 只從結構化欄位
//!    （`Document.author`、`Document.attributes`）抽，不從自由文本抽。NER 是 V0.3。
//! 2. **只處理 canonical Document。** `dedup.completed` 的 `is_duplicate=true` 一律跳過。
//!
//! 規則細節、public suffix 的取捨與已知限制（例如版本號 `1.2.3.4` 會被當成 IPv4）
//! 見 `docs/developer/entity-worker.md`。

mod error;
mod health;
mod service;

pub mod extract;
pub mod suffix;

pub use error::EntityWorkerError;
pub use extract::{
    EXCERPT_RADIUS, EXTRACTOR_VERSION, Extracted, ExtractionBounds, ExtractionInput,
    ExtractionResult,
};
pub use health::serve as serve_health;
pub use service::{
    ACTION_ENTITY_EXTRACTED, EntityWorker, ExtractOutcome, PROCESSOR, entity_id, extraction_id,
    rel_evidence_id, relationship_id,
};
