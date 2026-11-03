//! V0.3 transactional outbox 的 typed payload。
//!
//! 這八個結構是 `event_outbox.payload`／envelope `payload` 的契約。
//! **不**發 `seed.updated`（沒有更新 seed 的 API）與 `discovery.requested`
//! （`job.dispatched` 已經覆蓋派工；兩邊都發會讓未來的消費者跑兩次）。

use chrono::{DateTime, Utc};
use core_model::{Candidate, Job, JobStatus, Seed};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::topics::EventTopic;

/// 預設 partition key：有 collection 就用它（同一調查的事件進同一 partition），
/// 否則用這筆事件的主體 id。見 `TECH_STACK.md` §6。
#[must_use]
pub fn partition_key(collection_id: Option<Uuid>, fallback: Uuid) -> String {
    collection_id.unwrap_or(fallback).to_string()
}

/// 序列化失敗在這些結構上幾乎不可能（欄位都是基本型別）。呼叫端仍應處理錯誤。
pub fn to_payload_value<T: Serialize>(payload: &T) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::to_value(payload)
}

/// `seed.created`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SeedCreated {
    pub seed_id: Uuid,
    pub collection_id: Option<Uuid>,
    pub seed_type: String,
    pub value: String,
    pub entity_id: Option<Uuid>,
    pub origin: String,
    pub status: String,
    pub priority: i32,
    pub confidence: f64,
    pub depth: i32,
    pub created_at: DateTime<Utc>,
}

impl SeedCreated {
    #[must_use]
    pub fn from_seed(seed: &Seed) -> Self {
        Self {
            seed_id: seed.id,
            collection_id: seed.collection_id,
            seed_type: serde_name(&seed.seed_type),
            value: seed.value.clone(),
            entity_id: seed.entity_id,
            origin: serde_name(&seed.origin),
            status: seed.status.clone(),
            priority: seed.priority,
            confidence: seed.confidence,
            depth: seed.depth,
            created_at: seed.created_at,
        }
    }

    #[must_use]
    pub fn topic() -> EventTopic {
        EventTopic::SeedCreated
    }

    #[must_use]
    pub fn partition_key(&self) -> String {
        partition_key(self.collection_id, self.seed_id)
    }
}

/// `candidate.created`／`candidate.approved`／`candidate.rejected` 共用欄位。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CandidateLifecycle {
    pub candidate_id: Uuid,
    pub collection_id: Option<Uuid>,
    pub candidate_type: String,
    pub value: String,
    pub normalized_value: String,
    pub status: String,
    pub discovery_method: String,
    pub discovered_by: String,
    pub confidence: f64,
    pub score: f64,
    pub depth: i32,
    pub created_at: DateTime<Utc>,
    pub reviewed_at: Option<DateTime<Utc>>,
}

impl CandidateLifecycle {
    #[must_use]
    pub fn from_candidate(candidate: &Candidate) -> Self {
        Self {
            candidate_id: candidate.id,
            collection_id: candidate.collection_id,
            candidate_type: serde_name(&candidate.candidate_type),
            value: candidate.value.clone(),
            normalized_value: candidate.normalized_value.clone(),
            status: serde_name(&candidate.status),
            discovery_method: candidate.discovery_method.clone(),
            discovered_by: candidate.discovered_by.clone(),
            confidence: candidate.confidence,
            score: candidate.score,
            depth: candidate.depth,
            created_at: candidate.created_at,
            reviewed_at: candidate.reviewed_at,
        }
    }

    #[must_use]
    pub fn partition_key(&self) -> String {
        partition_key(self.collection_id, self.candidate_id)
    }
}

/// `discovery.completed`（成功與失敗都走這個 topic，用 `status` 區分）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiscoveryCompleted {
    pub job_id: Uuid,
    /// `completed` 或 `failed`。
    pub status: String,
    pub collection_id: Option<Uuid>,
    pub entity_id: Option<Uuid>,
    pub error: Option<String>,
    pub completed_at: DateTime<Utc>,
}

impl DiscoveryCompleted {
    #[must_use]
    pub fn from_job(job: &Job) -> Self {
        let params = job.parameters.as_ref();
        Self {
            job_id: job.id,
            status: serde_name(&job.status),
            collection_id: uuid_from_params(params, "collection_id"),
            entity_id: uuid_from_params(params, "entity_id"),
            error: job.error.clone(),
            completed_at: job.completed_at.unwrap_or_else(Utc::now),
        }
    }

    #[must_use]
    pub fn topic() -> EventTopic {
        EventTopic::DiscoveryCompleted
    }

    #[must_use]
    pub fn partition_key(&self) -> String {
        partition_key(self.collection_id, self.job_id)
    }

    /// 只在終態 `completed`／`failed` 時該發這個事件。
    #[must_use]
    pub fn should_emit(status: JobStatus) -> bool {
        matches!(status, JobStatus::Completed | JobStatus::Failed)
    }
}

/// `ai.requested`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiRequested {
    pub ai_run_id: Uuid,
    pub task_type: String,
    pub model: String,
    pub collection_id: Option<Uuid>,
    pub resolution_candidate_id: Option<Uuid>,
    pub requested_at: DateTime<Utc>,
}

impl AiRequested {
    #[must_use]
    pub fn topic() -> EventTopic {
        EventTopic::AiRequested
    }

    #[must_use]
    pub fn partition_key(&self) -> String {
        partition_key(self.collection_id, self.ai_run_id)
    }
}

/// `ai.completed`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiCompleted {
    pub ai_run_id: Uuid,
    pub task_type: String,
    pub model: String,
    pub tokens: i64,
    pub duration_ms: i64,
    pub confidence: f64,
    pub parsed_same_entity: Option<bool>,
    pub collection_id: Option<Uuid>,
}

impl AiCompleted {
    #[must_use]
    pub fn topic() -> EventTopic {
        EventTopic::AiCompleted
    }

    #[must_use]
    pub fn partition_key(&self) -> String {
        partition_key(self.collection_id, self.ai_run_id)
    }
}

/// `ai.failed`
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AiFailed {
    pub ai_run_id: Uuid,
    pub task_type: String,
    pub model: String,
    /// `admission`／`http`／`parse`／`quota_exceeded`／`quota_unavailable`／
    /// `unsupported`／`other`。
    pub error_kind: String,
    pub error_message: String,
    pub duration_ms: i64,
    pub collection_id: Option<Uuid>,
}

impl AiFailed {
    #[must_use]
    pub fn topic() -> EventTopic {
        EventTopic::AiFailed
    }

    #[must_use]
    pub fn partition_key(&self) -> String {
        partition_key(self.collection_id, self.ai_run_id)
    }
}

fn serde_name<T: Serialize>(value: &T) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(s)) => s,
        Ok(other) => other.to_string(),
        Err(_) => "unknown".into(),
    }
}

fn uuid_from_params(params: Option<&serde_json::Value>, key: &str) -> Option<Uuid> {
    params
        .and_then(|p| p.get(key))
        .and_then(serde_json::Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use core_model::{CandidateStatus, CandidateType, SeedOrigin, SeedType};
    use serde_json::json;

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 4, 12, 0, 0).unwrap()
    }

    #[test]
    fn partition_key_prefers_collection() {
        let collection = Uuid::now_v7();
        let fallback = Uuid::now_v7();
        assert_eq!(
            partition_key(Some(collection), fallback),
            collection.to_string()
        );
        assert_eq!(partition_key(None, fallback), fallback.to_string());
    }

    #[test]
    fn seed_created_round_trip() {
        let seed = Seed {
            id: Uuid::now_v7(),
            collection_id: Some(Uuid::now_v7()),
            seed_type: SeedType::Domain,
            value: "example.com".into(),
            entity_id: None,
            priority: 1,
            confidence: 0.9,
            origin: SeedOrigin::Manual,
            status: "pending".into(),
            depth: 0,
            created_at: ts(),
        };
        let payload = SeedCreated::from_seed(&seed);
        let value = to_payload_value(&payload).unwrap();
        assert_eq!(value["seed_id"], json!(seed.id));
        assert_eq!(value["seed_type"], json!("domain"));
        assert_eq!(value["origin"], json!("manual"));
        assert_eq!(
            payload.partition_key(),
            seed.collection_id.unwrap().to_string()
        );
        let back: SeedCreated = serde_json::from_value(value).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn candidate_lifecycle_round_trip() {
        let candidate = Candidate {
            id: Uuid::now_v7(),
            candidate_type: CandidateType::Url,
            value: "https://example.com/a".into(),
            normalized_value: "https://example.com/a".into(),
            collection_id: None,
            discovered_by: "test".into(),
            discovery_method: "graph_expansion".into(),
            confidence: 0.4,
            score: 0.4,
            status: CandidateStatus::Approved,
            depth: 1,
            created_at: ts(),
            reviewed_at: Some(ts()),
        };
        let payload = CandidateLifecycle::from_candidate(&candidate);
        let value = to_payload_value(&payload).unwrap();
        assert_eq!(value["status"], json!("approved"));
        assert_eq!(value["candidate_type"], json!("url"));
        assert_eq!(payload.partition_key(), candidate.id.to_string());
        let back: CandidateLifecycle = serde_json::from_value(value).unwrap();
        assert_eq!(back, payload);
    }

    #[test]
    fn discovery_completed_reads_job_parameters() {
        let collection_id = Uuid::now_v7();
        let entity_id = Uuid::now_v7();
        let job = Job {
            id: Uuid::now_v7(),
            job_type: "discovery_run".into(),
            status: JobStatus::Failed,
            correlation_id: None,
            created_at: ts(),
            started_at: Some(ts()),
            completed_at: Some(ts()),
            retry_count: 0,
            error: Some("depth 超限".into()),
            parameters: Some(json!({
                "collection_id": collection_id,
                "entity_id": entity_id,
                "depth": 3
            })),
        };
        let payload = DiscoveryCompleted::from_job(&job);
        assert_eq!(payload.status, "failed");
        assert_eq!(payload.collection_id, Some(collection_id));
        assert_eq!(payload.entity_id, Some(entity_id));
        assert_eq!(payload.partition_key(), collection_id.to_string());
        assert!(DiscoveryCompleted::should_emit(JobStatus::Completed));
        assert!(DiscoveryCompleted::should_emit(JobStatus::Failed));
        assert!(!DiscoveryCompleted::should_emit(JobStatus::Running));
    }

    #[test]
    fn ai_payloads_use_run_id_when_no_collection() {
        let id = Uuid::now_v7();
        let requested = AiRequested {
            ai_run_id: id,
            task_type: "classification".into(),
            model: "local".into(),
            collection_id: None,
            resolution_candidate_id: Some(Uuid::now_v7()),
            requested_at: ts(),
        };
        assert_eq!(requested.partition_key(), id.to_string());
        let failed = AiFailed {
            ai_run_id: id,
            task_type: "classification".into(),
            model: "local".into(),
            error_kind: "admission".into(),
            error_message: "AI admission 被拒".into(),
            duration_ms: 12,
            collection_id: None,
        };
        assert_eq!(failed.partition_key(), id.to_string());
    }
}
