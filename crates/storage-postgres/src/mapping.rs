use core_model::{
    Collection, Connector, Document, DuplicateGroup, Entity, EntityExtraction, Event, Job,
    NetworkRule, Provenance, RawEvidence, Relationship, RelationshipEvidence, Source,
};
use serde_json::Value;
use sqlx::Row;
use sqlx::postgres::PgRow;
use storage_core::codec::decode_enum;
use storage_core::{SimhashCandidate, StorageError};

use crate::error::map_sqlx;

fn get<'a, T>(row: &'a PgRow, col: &str) -> Result<T, StorageError>
where
    T: sqlx::Decode<'a, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(col).map_err(map_sqlx)
}

pub fn source(row: &PgRow) -> Result<Source, StorageError> {
    Ok(Source {
        id: get(row, "id")?,
        name: get(row, "name")?,
        source_type: decode_enum(&get::<String>(row, "source_type")?, "source_type")?,
        platform: get(row, "platform")?,
        base_url: get(row, "base_url")?,
        description: get(row, "description")?,
        language: get(row, "language")?,
        country: get(row, "country")?,
        enabled: get(row, "enabled")?,
        collection_policy: get(row, "collection_policy")?,
        created_at: get(row, "created_at")?,
        updated_at: get(row, "updated_at")?,
        last_seen: get(row, "last_seen")?,
    })
}

pub fn network_rule(row: &PgRow) -> Result<NetworkRule, StorageError> {
    Ok(NetworkRule {
        id: get(row, "id")?,
        source_id: get(row, "source_id")?,
        cidr_or_host: get(row, "cidr_or_host")?,
        ports: decode_ports(get::<Option<Value>>(row, "ports")?)?,
        reason: get(row, "reason")?,
        approved_by: get(row, "approved_by")?,
        expires_at: get(row, "expires_at")?,
        created_at: get(row, "created_at")?,
        updated_at: get(row, "updated_at")?,
    })
}

fn decode_ports(value: Option<Value>) -> Result<Option<Vec<u16>>, StorageError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(items)) => {
            let mut ports = Vec::with_capacity(items.len());
            for item in items {
                let n = item
                    .as_u64()
                    .ok_or_else(|| StorageError::CorruptionSuspected {
                        message: format!("source_network_rules.ports 含非數字：{item}"),
                    })?;
                let port = u16::try_from(n).map_err(|_| StorageError::CorruptionSuspected {
                    message: format!("source_network_rules.ports 含超出 u16 的值：{n}"),
                })?;
                ports.push(port);
            }
            Ok(Some(ports))
        }
        Some(other) => Err(StorageError::CorruptionSuspected {
            message: format!("source_network_rules.ports 應為 JSON 陣列或 NULL，實際是 {other}"),
        }),
    }
}

pub fn connector(row: &PgRow) -> Result<Connector, StorageError> {
    Ok(Connector {
        id: get(row, "id")?,
        source_id: get(row, "source_id")?,
        name: get(row, "name")?,
        connector_type: get(row, "type")?,
        version: get(row, "version")?,
        enabled: get(row, "enabled")?,
        configuration: get(row, "configuration")?,
        credential_reference: get(row, "credential_reference")?,
        schedule: get(row, "schedule")?,
        rate_limit: get(row, "rate_limit")?,
        timeout: get(row, "timeout")?,
        proxy_reference: get(row, "proxy_reference")?,
        checkpoint: get(row, "checkpoint")?,
        last_run: get(row, "last_run")?,
        last_success: get(row, "last_success")?,
        status: get(row, "status")?,
        error_count: get(row, "error_count")?,
    })
}

pub fn collection(row: &PgRow) -> Result<Collection, StorageError> {
    Ok(Collection {
        id: get(row, "id")?,
        workspace_id: get(row, "workspace_id")?,
        name: get(row, "name")?,
        description: get(row, "description")?,
        status: get(row, "status")?,
        priority: get(row, "priority")?,
        created_at: get(row, "created_at")?,
        updated_at: get(row, "updated_at")?,
    })
}

pub fn raw_evidence(row: &PgRow) -> Result<RawEvidence, StorageError> {
    Ok(RawEvidence {
        id: get(row, "id")?,
        source_id: get(row, "source_id")?,
        connector_id: get(row, "connector_id")?,
        collection_id: get(row, "collection_id")?,
        external_id: get(row, "external_id")?,
        source_url: get(row, "source_url")?,
        retrieved_at: get(row, "retrieved_at")?,
        content_type: get(row, "content_type")?,
        mime_type: get(row, "mime_type")?,
        content_length: get(row, "content_length")?,
        sha256: get(row, "sha256")?,
        storage_path: get(row, "storage_path")?,
        http_status: get(row, "http_status")?,
        http_headers: get(row, "http_headers")?,
        metadata: get(row, "metadata")?,
        collector_version: get(row, "collector_version")?,
    })
}

pub fn document(row: &PgRow) -> Result<Document, StorageError> {
    let labels_json: Value = get(row, "labels")?;
    let labels = match labels_json {
        Value::Array(_) => serde_json::from_value(labels_json).map_err(|err| {
            StorageError::CorruptionSuspected {
                message: format!("documents.labels 不是字串陣列：{err}"),
            }
        })?,
        other => {
            return Err(StorageError::CorruptionSuspected {
                message: format!("documents.labels 應為 JSON 陣列，實際是 {other}"),
            });
        }
    };
    Ok(Document {
        id: get(row, "id")?,
        object_type: decode_enum(&get::<String>(row, "object_type")?, "object_type")?,
        schema_version: get(row, "schema_version")?,
        title: get(row, "title")?,
        body: get(row, "body")?,
        summary: get(row, "summary")?,
        language: get(row, "language")?,
        author: get(row, "author")?,
        published_at: get(row, "published_at")?,
        modified_at: get(row, "modified_at")?,
        observed_at: get(row, "observed_at")?,
        collected_at: get(row, "collected_at")?,
        source_url: get(row, "source_url")?,
        canonical_url: get(row, "canonical_url")?,
        normalized_content_hash: get(row, "normalized_content_hash")?,
        confidence: get(row, "confidence")?,
        labels,
        attributes: get(row, "attributes")?,
        external_key: get(row, "external_key")?,
        simhash: get(row, "simhash")?,
        duplicate_of: get(row, "duplicate_of")?,
    })
}

pub fn simhash_candidate(row: &PgRow) -> Result<SimhashCandidate, StorageError> {
    Ok(SimhashCandidate {
        id: get(row, "id")?,
        simhash: get(row, "simhash")?,
    })
}

pub fn entity(row: &PgRow) -> Result<Entity, StorageError> {
    Ok(Entity {
        id: get(row, "id")?,
        entity_type: decode_enum(&get::<String>(row, "entity_type")?, "entity_type")?,
        name: get(row, "name")?,
        normalized_name: get(row, "normalized_name")?,
        description: get(row, "description")?,
        confidence: get(row, "confidence")?,
        first_seen: get(row, "first_seen")?,
        last_seen: get(row, "last_seen")?,
        attributes: get(row, "attributes")?,
    })
}

pub fn relationship(row: &PgRow) -> Result<Relationship, StorageError> {
    Ok(Relationship {
        id: get(row, "id")?,
        source_object_id: get(row, "source_object_id")?,
        relationship_type: decode_enum(
            &get::<String>(row, "relationship_type")?,
            "relationship_type",
        )?,
        target_object_id: get(row, "target_object_id")?,
        confidence: get(row, "confidence")?,
        first_seen: get(row, "first_seen")?,
        last_seen: get(row, "last_seen")?,
        evidence_count: get(row, "evidence_count")?,
        created_at: get(row, "created_at")?,
        updated_at: get(row, "updated_at")?,
    })
}

pub fn relationship_evidence(row: &PgRow) -> Result<RelationshipEvidence, StorageError> {
    Ok(RelationshipEvidence {
        id: get(row, "id")?,
        relationship_id: get(row, "relationship_id")?,
        object_id: get(row, "object_id")?,
        raw_evidence_id: get(row, "raw_evidence_id")?,
        excerpt: get(row, "excerpt")?,
        confidence: get(row, "confidence")?,
        created_at: get(row, "created_at")?,
    })
}

pub fn event(row: &PgRow) -> Result<Event, StorageError> {
    Ok(Event {
        id: get(row, "id")?,
        event_type: get(row, "event_type")?,
        title: get(row, "title")?,
        description: get(row, "description")?,
        start_time: get(row, "start_time")?,
        end_time: get(row, "end_time")?,
        confidence: get(row, "confidence")?,
        status: get(row, "status")?,
        attributes: get(row, "attributes")?,
        created_at: get(row, "created_at")?,
        updated_at: get(row, "updated_at")?,
    })
}

pub fn provenance(row: &PgRow) -> Result<Provenance, StorageError> {
    Ok(Provenance {
        id: get(row, "id")?,
        subject_id: get(row, "subject_id")?,
        action: get(row, "action")?,
        parent_id: get(row, "parent_id")?,
        raw_evidence_id: get(row, "raw_evidence_id")?,
        processor: get(row, "processor")?,
        processor_version: get(row, "processor_version")?,
        timestamp: get(row, "timestamp")?,
        metadata: get(row, "metadata")?,
    })
}

pub fn job(row: &PgRow) -> Result<Job, StorageError> {
    Ok(Job {
        id: get(row, "id")?,
        job_type: get(row, "type")?,
        status: decode_enum(&get::<String>(row, "status")?, "status")?,
        correlation_id: get(row, "correlation_id")?,
        created_at: get(row, "created_at")?,
        started_at: get(row, "started_at")?,
        completed_at: get(row, "completed_at")?,
        retry_count: get(row, "retry_count")?,
        error: get(row, "error")?,
    })
}

pub fn duplicate_group(row: &PgRow) -> Result<DuplicateGroup, StorageError> {
    Ok(DuplicateGroup {
        id: get(row, "id")?,
        canonical_object_id: get(row, "canonical_object_id")?,
        member_object_id: get(row, "member_object_id")?,
        member_raw_evidence_id: get(row, "member_raw_evidence_id")?,
        method: get(row, "method")?,
        similarity: get(row, "similarity")?,
        first_seen: get(row, "first_seen")?,
    })
}

pub fn entity_extraction(row: &PgRow) -> Result<EntityExtraction, StorageError> {
    Ok(EntityExtraction {
        id: get(row, "id")?,
        object_id: get(row, "object_id")?,
        entity_id: get(row, "entity_id")?,
        extractor: get(row, "extractor")?,
        extractor_version: get(row, "extractor_version")?,
        confidence: get(row, "confidence")?,
        text_offset: get(row, "text_offset")?,
        excerpt: get(row, "excerpt")?,
    })
}
