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
    StixImport,
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
    ThreatActor,
    Malware,
    Indicator,
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
    Indicates,
    AttributedTo,
    Targets,
    Mitigates,
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

// ===== V0.2 =====

/// Resolution candidate 的審核狀態（SPEC_V0.2 §5）。
///
/// 這是 V0.2 少數**規格有明確列舉值**的欄位，所以做成 enum 而不是 `String`。
///
/// `confirmed` 與 `auto_confirmed` 分開是有意義的：前者有人看過，後者是門檻夠高
/// 由系統自己決定的。混成同一個值之後就再也分不出「哪些合併沒有人類看過」——
/// 那正是出事時第一個要問的問題。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionStatus {
    Pending,
    Confirmed,
    Rejected,
    AutoConfirmed,
}

/// Embedding 目標種類（SPEC §12）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EmbeddingTarget {
    DocumentTitle,
    DocumentBody,
    EntityDescription,
    EventDescription,
}

// ===== V0.3 =====

/// Seed 種類（SPEC_V0.3 §2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedType {
    Keyword,
    Topic,
    Person,
    Organization,
    Account,
    Channel,
    Url,
    Domain,
    Ip,
    Email,
    Vulnerability,
    Software,
    Repository,
    Hashtag,
    Location,
}

/// Seed 的來源（SPEC_V0.3 §2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedOrigin {
    Manual,
    Connector,
    Ai,
    Graph,
    Entity,
    /// SPEC 原文是 "external import"（兩個字），`snake_case` 序列化成
    /// `external_import`。
    ExternalImport,
    Discovery,
}

/// Candidate 種類（SPEC_V0.3 §6）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateType {
    Entity,
    Relationship,
    Account,
    Channel,
    Source,
    Url,
    Domain,
    Repository,
    Keyword,
    Topic,
    Seed,
}

/// Candidate 的審核狀態（SPEC_V0.3 §6）。跟 `ResolutionStatus` 同一種理由：
/// `approved` 與 `auto_approved` 分開，才留得住「這筆有沒有人看過」的區別。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateStatus {
    Pending,
    Approved,
    AutoApproved,
    Rejected,
    Expired,
}
