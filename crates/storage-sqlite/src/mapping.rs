use chrono::{DateTime, Utc};
use core_model::{
    Collection, Connector, Document, DuplicateGroup, Entity, EntityExtraction, Event, Job,
    NetworkRule, Provenance, RawEvidence, Relationship, RelationshipEvidence, Source,
};
use serde_json::Value;
use sqlx::Row;
use sqlx::sqlite::SqliteRow;
use storage_core::codec::{decode_enum, decode_json, decode_string_vec};
use storage_core::{SimhashCandidate, StorageError};
use uuid::Uuid;

use crate::error::map_sqlx;

fn get_str(row: &SqliteRow, col: &str) -> Result<String, StorageError> {
    row.try_get::<String, _>(col).map_err(map_sqlx)
}

fn get_opt_str(row: &SqliteRow, col: &str) -> Result<Option<String>, StorageError> {
    row.try_get::<Option<String>, _>(col).map_err(map_sqlx)
}

fn get_i64(row: &SqliteRow, col: &str) -> Result<i64, StorageError> {
    row.try_get::<i64, _>(col).map_err(map_sqlx)
}

fn get_opt_i64(row: &SqliteRow, col: &str) -> Result<Option<i64>, StorageError> {
    row.try_get::<Option<i64>, _>(col).map_err(map_sqlx)
}

fn get_f64(row: &SqliteRow, col: &str) -> Result<f64, StorageError> {
    row.try_get::<f64, _>(col).map_err(map_sqlx)
}

fn uuid_from(row: &SqliteRow, col: &str) -> Result<Uuid, StorageError> {
    parse_uuid(&get_str(row, col)?, col)
}

fn opt_uuid(row: &SqliteRow, col: &str) -> Result<Option<Uuid>, StorageError> {
    match get_opt_str(row, col)? {
        Some(s) => Ok(Some(parse_uuid(&s, col)?)),
        None => Ok(None),
    }
}

/// 只取一欄 UUID 的查詢（dedup 候選 id 清單）用。
pub fn uuid_column(row: &SqliteRow, col: &str) -> Result<Uuid, StorageError> {
    uuid_from(row, col)
}

fn parse_uuid(raw: &str, col: &str) -> Result<Uuid, StorageError> {
    Uuid::parse_str(raw).map_err(|err| StorageError::CorruptionSuspected {
        message: format!("欄位 `{col}` 不是合法 UUID：{err}"),
    })
}

fn ts(row: &SqliteRow, col: &str) -> Result<DateTime<Utc>, StorageError> {
    parse_ts(&get_str(row, col)?, col)
}

fn opt_ts(row: &SqliteRow, col: &str) -> Result<Option<DateTime<Utc>>, StorageError> {
    match get_opt_str(row, col)? {
        Some(s) => Ok(Some(parse_ts(&s, col)?)),
        None => Ok(None),
    }
}

fn parse_ts(raw: &str, col: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| StorageError::CorruptionSuspected {
            message: format!("欄位 `{col}` 不是 RFC3339 時間：{err}"),
        })
}

fn bool_int(row: &SqliteRow, col: &str) -> Result<bool, StorageError> {
    Ok(get_i64(row, col)? != 0)
}

fn json(row: &SqliteRow, col: &str) -> Result<Value, StorageError> {
    decode_json(&get_str(row, col)?, col)
}

pub fn source(row: &SqliteRow) -> Result<Source, StorageError> {
    Ok(Source {
        id: uuid_from(row, "id")?,
        name: get_str(row, "name")?,
        source_type: decode_enum(&get_str(row, "source_type")?, "source_type")?,
        platform: get_opt_str(row, "platform")?,
        base_url: get_opt_str(row, "base_url")?,
        description: get_opt_str(row, "description")?,
        language: get_opt_str(row, "language")?,
        country: get_opt_str(row, "country")?,
        enabled: bool_int(row, "enabled")?,
        collection_policy: json(row, "collection_policy")?,
        created_at: ts(row, "created_at")?,
        updated_at: ts(row, "updated_at")?,
        last_seen: opt_ts(row, "last_seen")?,
    })
}

pub fn network_rule(row: &SqliteRow) -> Result<NetworkRule, StorageError> {
    Ok(NetworkRule {
        id: uuid_from(row, "id")?,
        source_id: uuid_from(row, "source_id")?,
        cidr_or_host: get_str(row, "cidr_or_host")?,
        ports: decode_ports(get_opt_str(row, "ports")?)?,
        reason: get_str(row, "reason")?,
        approved_by: get_str(row, "approved_by")?,
        expires_at: opt_ts(row, "expires_at")?,
        created_at: ts(row, "created_at")?,
        updated_at: ts(row, "updated_at")?,
    })
}

fn decode_ports(raw: Option<String>) -> Result<Option<Vec<u16>>, StorageError> {
    match raw {
        None => Ok(None),
        Some(s) if s.is_empty() || s == "null" => Ok(None),
        Some(s) => {
            let value: Value =
                serde_json::from_str(&s).map_err(|err| StorageError::CorruptionSuspected {
                    message: format!("source_network_rules.ports 不是合法 JSON：{err}"),
                })?;
            match value {
                Value::Array(items) => {
                    let mut ports = Vec::with_capacity(items.len());
                    for item in items {
                        let n = item
                            .as_u64()
                            .ok_or_else(|| StorageError::CorruptionSuspected {
                                message: format!("source_network_rules.ports 含非數字：{item}"),
                            })?;
                        let port =
                            u16::try_from(n).map_err(|_| StorageError::CorruptionSuspected {
                                message: format!("source_network_rules.ports 含超出 u16 的值：{n}"),
                            })?;
                        ports.push(port);
                    }
                    Ok(Some(ports))
                }
                other => Err(StorageError::CorruptionSuspected {
                    message: format!(
                        "source_network_rules.ports 應為 JSON 陣列或 NULL，實際是 {other}"
                    ),
                }),
            }
        }
    }
}

pub fn connector(row: &SqliteRow) -> Result<Connector, StorageError> {
    Ok(Connector {
        id: uuid_from(row, "id")?,
        source_id: uuid_from(row, "source_id")?,
        name: get_str(row, "name")?,
        connector_type: get_str(row, "type")?,
        version: get_str(row, "version")?,
        enabled: bool_int(row, "enabled")?,
        configuration: json(row, "configuration")?,
        credential_reference: get_opt_str(row, "credential_reference")?,
        schedule: get_opt_str(row, "schedule")?,
        rate_limit: json(row, "rate_limit")?,
        timeout: json(row, "timeout")?,
        proxy_reference: get_opt_str(row, "proxy_reference")?,
        checkpoint: json(row, "checkpoint")?,
        last_run: opt_ts(row, "last_run")?,
        last_success: opt_ts(row, "last_success")?,
        status: get_str(row, "status")?,
        error_count: i32_from(row, "error_count")?,
    })
}

fn i32_from(row: &SqliteRow, col: &str) -> Result<i32, StorageError> {
    i32::try_from(get_i64(row, col)?).map_err(|_| StorageError::CorruptionSuspected {
        message: format!("欄位 `{col}` 超出 i32"),
    })
}

pub fn collection(row: &SqliteRow) -> Result<Collection, StorageError> {
    Ok(Collection {
        id: uuid_from(row, "id")?,
        workspace_id: opt_uuid(row, "workspace_id")?,
        name: get_str(row, "name")?,
        description: get_opt_str(row, "description")?,
        status: get_str(row, "status")?,
        priority: i32_from(row, "priority")?,
        created_at: ts(row, "created_at")?,
        updated_at: ts(row, "updated_at")?,
    })
}

pub fn raw_evidence(row: &SqliteRow) -> Result<RawEvidence, StorageError> {
    Ok(RawEvidence {
        id: uuid_from(row, "id")?,
        source_id: uuid_from(row, "source_id")?,
        connector_id: uuid_from(row, "connector_id")?,
        collection_id: opt_uuid(row, "collection_id")?,
        external_id: get_opt_str(row, "external_id")?,
        source_url: get_str(row, "source_url")?,
        retrieved_at: ts(row, "retrieved_at")?,
        content_type: get_opt_str(row, "content_type")?,
        mime_type: get_opt_str(row, "mime_type")?,
        content_length: get_opt_i64(row, "content_length")?,
        sha256: get_str(row, "sha256")?,
        storage_path: get_str(row, "storage_path")?,
        http_status: match get_opt_i64(row, "http_status")? {
            Some(v) => Some(
                i32::try_from(v).map_err(|_| StorageError::CorruptionSuspected {
                    message: "http_status 超出 i32".into(),
                })?,
            ),
            None => None,
        },
        http_headers: json(row, "http_headers")?,
        metadata: json(row, "metadata")?,
        collector_version: get_str(row, "collector_version")?,
    })
}

pub fn document(row: &SqliteRow) -> Result<Document, StorageError> {
    Ok(Document {
        id: uuid_from(row, "id")?,
        object_type: decode_enum(&get_str(row, "object_type")?, "object_type")?,
        schema_version: get_str(row, "schema_version")?,
        title: get_opt_str(row, "title")?,
        body: get_opt_str(row, "body")?,
        summary: get_opt_str(row, "summary")?,
        language: get_opt_str(row, "language")?,
        author: get_opt_str(row, "author")?,
        published_at: opt_ts(row, "published_at")?,
        modified_at: opt_ts(row, "modified_at")?,
        observed_at: ts(row, "observed_at")?,
        collected_at: ts(row, "collected_at")?,
        source_url: get_opt_str(row, "source_url")?,
        canonical_url: get_opt_str(row, "canonical_url")?,
        normalized_content_hash: get_opt_str(row, "normalized_content_hash")?,
        confidence: get_f64(row, "confidence")?,
        labels: decode_string_vec(&get_str(row, "labels")?, "labels")?,
        attributes: json(row, "attributes")?,
        external_key: get_opt_str(row, "external_key")?,
        simhash: get_opt_i64(row, "simhash")?,
        duplicate_of: opt_uuid(row, "duplicate_of")?,
    })
}

pub fn simhash_candidate(row: &SqliteRow) -> Result<SimhashCandidate, StorageError> {
    Ok(SimhashCandidate {
        id: uuid_from(row, "id")?,
        simhash: get_i64(row, "simhash")?,
    })
}

pub fn entity(row: &SqliteRow) -> Result<Entity, StorageError> {
    Ok(Entity {
        id: uuid_from(row, "id")?,
        entity_type: decode_enum(&get_str(row, "entity_type")?, "entity_type")?,
        name: get_str(row, "name")?,
        normalized_name: get_str(row, "normalized_name")?,
        description: get_opt_str(row, "description")?,
        confidence: get_f64(row, "confidence")?,
        first_seen: ts(row, "first_seen")?,
        last_seen: ts(row, "last_seen")?,
        attributes: json(row, "attributes")?,
    })
}

pub fn relationship(row: &SqliteRow) -> Result<Relationship, StorageError> {
    Ok(Relationship {
        id: uuid_from(row, "id")?,
        source_object_id: uuid_from(row, "source_object_id")?,
        relationship_type: decode_enum(&get_str(row, "relationship_type")?, "relationship_type")?,
        target_object_id: uuid_from(row, "target_object_id")?,
        confidence: get_f64(row, "confidence")?,
        first_seen: ts(row, "first_seen")?,
        last_seen: ts(row, "last_seen")?,
        evidence_count: i32_from(row, "evidence_count")?,
        created_at: ts(row, "created_at")?,
        updated_at: ts(row, "updated_at")?,
    })
}

pub fn relationship_evidence(row: &SqliteRow) -> Result<RelationshipEvidence, StorageError> {
    Ok(RelationshipEvidence {
        id: uuid_from(row, "id")?,
        relationship_id: uuid_from(row, "relationship_id")?,
        object_id: uuid_from(row, "object_id")?,
        raw_evidence_id: opt_uuid(row, "raw_evidence_id")?,
        excerpt: get_opt_str(row, "excerpt")?,
        confidence: get_f64(row, "confidence")?,
        created_at: ts(row, "created_at")?,
    })
}

pub fn event(row: &SqliteRow) -> Result<Event, StorageError> {
    Ok(Event {
        id: uuid_from(row, "id")?,
        event_type: get_str(row, "event_type")?,
        title: get_str(row, "title")?,
        description: get_opt_str(row, "description")?,
        start_time: opt_ts(row, "start_time")?,
        end_time: opt_ts(row, "end_time")?,
        confidence: get_f64(row, "confidence")?,
        status: get_str(row, "status")?,
        attributes: json(row, "attributes")?,
        created_at: ts(row, "created_at")?,
        updated_at: ts(row, "updated_at")?,
    })
}

pub fn provenance(row: &SqliteRow) -> Result<Provenance, StorageError> {
    Ok(Provenance {
        id: uuid_from(row, "id")?,
        subject_id: uuid_from(row, "subject_id")?,
        action: get_str(row, "action")?,
        parent_id: opt_uuid(row, "parent_id")?,
        raw_evidence_id: opt_uuid(row, "raw_evidence_id")?,
        processor: get_str(row, "processor")?,
        processor_version: get_str(row, "processor_version")?,
        timestamp: ts(row, "timestamp")?,
        metadata: json(row, "metadata")?,
    })
}

pub fn job(row: &SqliteRow) -> Result<Job, StorageError> {
    Ok(Job {
        id: uuid_from(row, "id")?,
        job_type: get_str(row, "type")?,
        status: decode_enum(&get_str(row, "status")?, "status")?,
        correlation_id: opt_uuid(row, "correlation_id")?,
        created_at: ts(row, "created_at")?,
        started_at: opt_ts(row, "started_at")?,
        completed_at: opt_ts(row, "completed_at")?,
        retry_count: i32_from(row, "retry_count")?,
        error: get_opt_str(row, "error")?,
    })
}

pub fn duplicate_group(row: &SqliteRow) -> Result<DuplicateGroup, StorageError> {
    Ok(DuplicateGroup {
        id: uuid_from(row, "id")?,
        canonical_object_id: uuid_from(row, "canonical_object_id")?,
        member_object_id: opt_uuid(row, "member_object_id")?,
        member_raw_evidence_id: opt_uuid(row, "member_raw_evidence_id")?,
        method: get_str(row, "method")?,
        similarity: get_f64(row, "similarity")?,
        first_seen: ts(row, "first_seen")?,
    })
}

pub fn entity_extraction(row: &SqliteRow) -> Result<EntityExtraction, StorageError> {
    Ok(EntityExtraction {
        id: uuid_from(row, "id")?,
        object_id: uuid_from(row, "object_id")?,
        entity_id: uuid_from(row, "entity_id")?,
        extractor: get_str(row, "extractor")?,
        extractor_version: get_str(row, "extractor_version")?,
        confidence: get_f64(row, "confidence")?,
        text_offset: match get_opt_i64(row, "text_offset")? {
            Some(v) => Some(
                i32::try_from(v).map_err(|_| StorageError::CorruptionSuspected {
                    message: "text_offset 超出 i32".into(),
                })?,
            ),
            None => None,
        },
        excerpt: get_opt_str(row, "excerpt")?,
    })
}
