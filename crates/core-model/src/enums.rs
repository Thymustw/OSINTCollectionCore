//! SPEC_V0.1 有列舉值的欄位。規格沒列值的 status 維持字串，避免發明狀態機。

use serde::{Deserialize, Serialize};

/// V0.1 支援的來源種類（SPEC §3 / §5 `source_type`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Rss,
    Atom,
    StaticWeb,
    RestApi,
    ManualUpload,
    JsonImport,
    CsvImport,
}

/// Canonical document 種類（SPEC §9）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DocumentType {
    Article,
    WebPage,
    Post,
    Message,
    Report,
    File,
    Advisory,
}

/// Entity 種類（SPEC §10）。`vulnerability` 對應 Vulnerability/CVE。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Person,
    Organization,
    Account,
    Domain,
    Hostname,
    Ip,
    Url,
    Email,
    Vulnerability,
    Software,
    Repository,
    Hash,
    Location,
}

/// Relationship 種類（SPEC §11）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipType {
    Mentions,
    References,
    PublishedBy,
    AuthoredBy,
    LinksTo,
    Affects,
    BelongsTo,
    MemberOf,
    Owns,
    Uses,
    LocatedAt,
    AssociatedWith,
    DerivedFrom,
}

/// Job 狀態（SPEC §21）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Completed,
    Retrying,
    Failed,
    Cancelled,
}
