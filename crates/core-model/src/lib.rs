//! V0.1 canonical domain types。
//!
//! 欄位對齊 `docs/specs/SPEC_V0.1.md`。識別碼使用 UUID v7。
//! 規格未列舉值的 status 維持 `String`，避免發明狀態機。

pub mod collection;
pub mod connector;
pub mod document;
pub mod duplicate;
pub mod entity;
pub mod enums;
pub mod event;
pub mod extraction;
pub mod ids;
pub mod job;
pub mod network_rule;
pub mod provenance;
pub mod raw_evidence;
pub mod relationship;
pub mod source;

pub use collection::Collection;
pub use connector::Connector;
pub use document::Document;
pub use duplicate::DuplicateGroup;
pub use entity::Entity;
pub use enums::{DocumentType, EntityType, JobStatus, RelationshipType, SourceType};
pub use event::Event;
pub use extraction::EntityExtraction;
pub use ids::*;
pub use job::Job;
pub use network_rule::NetworkRule;
pub use provenance::Provenance;
pub use raw_evidence::RawEvidence;
pub use relationship::{Relationship, RelationshipEvidence};
pub use source::Source;

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::{Value, json};
    use uuid::Uuid;

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap()
    }

    fn id() -> Uuid {
        Uuid::parse_str("01993c6a-7c3e-7a11-8000-7c3e7a110001").unwrap()
    }

    fn round_trip<T>(value: &T) -> T
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
    {
        let json = serde_json::to_string(value).expect("serialize");
        serde_json::from_str(&json).expect("deserialize")
    }

    #[test]
    fn source_round_trip() {
        let original = Source {
            id: id(),
            name: "NVD RSS".into(),
            source_type: SourceType::Rss,
            platform: Some("nvd".into()),
            base_url: Some("https://nvd.nist.gov/feeds/xml/cve/misc/nvd-rss.xml".into()),
            description: Some("NVD CVE feed".into()),
            language: Some("en".into()),
            country: Some("US".into()),
            enabled: true,
            collection_policy: json!({"interval": "15m"}),
            created_at: ts(),
            updated_at: ts(),
            last_seen: Some(ts()),
        };
        let back = round_trip(&original);
        assert_eq!(original, back);
        let v: Value = serde_json::to_value(&original).unwrap();
        assert_eq!(v["source_type"], "rss");
    }

    #[test]
    fn connector_round_trip_keeps_type_field() {
        let original = Connector {
            id: id(),
            source_id: id(),
            name: "nvd-rss".into(),
            connector_type: "rss".into(),
            version: "0.1.0".into(),
            enabled: true,
            configuration: json!({"url": "https://example.invalid/rss"}),
            credential_reference: Some("env:NVD_TOKEN".into()),
            schedule: Some("*/15 * * * *".into()),
            rate_limit: json!({"rps": 1}),
            timeout: json!({"connect_ms": 5000, "read_ms": 15000}),
            proxy_reference: None,
            checkpoint: json!({"etag": "abc"}),
            last_run: Some(ts()),
            last_success: Some(ts()),
            status: "idle".into(),
            error_count: 0,
        };
        let json = serde_json::to_value(&original).unwrap();
        assert_eq!(json["type"], "rss");
        assert!(json.get("connector_type").is_none());
        assert_eq!(round_trip(&original), original);
    }

    #[test]
    fn raw_evidence_and_document_round_trip() {
        let raw = RawEvidence {
            id: id(),
            source_id: id(),
            connector_id: id(),
            collection_id: Some(id()),
            external_id: Some("CVE-2026-0001".into()),
            source_url: "https://example.invalid/cve".into(),
            retrieved_at: ts(),
            content_type: Some("text/xml".into()),
            mime_type: Some("application/xml".into()),
            content_length: Some(2048),
            sha256: "a".repeat(64),
            storage_path: "s3://raw-evidence/ab/cd".into(),
            http_status: Some(200),
            http_headers: json!({"etag": "1"}),
            metadata: json!({"feed": "nvd"}),
            collector_version: "0.1.0".into(),
        };
        assert_eq!(round_trip(&raw), raw);

        let doc = Document {
            id: id(),
            object_type: DocumentType::Advisory,
            schema_version: "1.0".into(),
            title: Some("CVE-2026-0001".into()),
            body: Some("body".into()),
            summary: None,
            language: Some("en".into()),
            author: None,
            published_at: Some(ts()),
            modified_at: None,
            observed_at: ts(),
            collected_at: ts(),
            source_url: Some("https://example.invalid/cve".into()),
            canonical_url: Some("https://example.invalid/cve".into()),
            normalized_content_hash: Some("b".repeat(64)),
            confidence: 0.9,
            labels: vec!["cve".into()],
            attributes: json!({"cve": "CVE-2026-0001"}),
        };
        let v = serde_json::to_value(&doc).unwrap();
        assert_eq!(v["object_type"], "advisory");
        assert_eq!(round_trip(&doc), doc);
    }

    #[test]
    fn entity_relationship_event_job_round_trip() {
        let entity = Entity {
            id: id(),
            entity_type: EntityType::Vulnerability,
            name: "CVE-2026-0001".into(),
            normalized_name: "cve-2026-0001".into(),
            description: None,
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
            attributes: json!({}),
        };
        assert_eq!(round_trip(&entity), entity);

        let rel = Relationship {
            id: id(),
            source_object_id: id(),
            relationship_type: RelationshipType::Affects,
            target_object_id: id(),
            confidence: 0.8,
            first_seen: ts(),
            last_seen: ts(),
            evidence_count: 2,
            created_at: ts(),
            updated_at: ts(),
        };
        assert_eq!(round_trip(&rel), rel);

        let event = Event {
            id: id(),
            event_type: "advisory_published".into(),
            title: "published".into(),
            description: None,
            start_time: Some(ts()),
            end_time: None,
            confidence: 0.5,
            status: "open".into(),
            attributes: json!({}),
            created_at: ts(),
            updated_at: ts(),
        };
        assert_eq!(round_trip(&event), event);

        let job = Job {
            id: id(),
            job_type: "collect".into(),
            status: JobStatus::Queued,
            correlation_id: Some(id()),
            created_at: ts(),
            started_at: None,
            completed_at: None,
            retry_count: 0,
            error: None,
        };
        let v = serde_json::to_value(&job).unwrap();
        assert_eq!(v["type"], "collect");
        assert_eq!(v["status"], "queued");
        assert_eq!(round_trip(&job), job);
    }

    #[test]
    fn provenance_duplicate_extraction_round_trip() {
        let provenance = Provenance {
            id: id(),
            subject_id: id(),
            action: "normalized".into(),
            parent_id: None,
            raw_evidence_id: Some(id()),
            processor: "normalizer".into(),
            processor_version: "0.1.0".into(),
            timestamp: ts(),
            metadata: json!({}),
        };
        assert_eq!(round_trip(&provenance), provenance);

        let dup = DuplicateGroup {
            id: id(),
            canonical_object_id: id(),
            member_object_id: Some(id()),
            member_raw_evidence_id: None,
            method: "sha256".into(),
            similarity: 1.0,
            first_seen: ts(),
        };
        assert_eq!(round_trip(&dup), dup);

        let extraction = EntityExtraction {
            id: id(),
            object_id: id(),
            entity_id: id(),
            extractor: "regex-cve".into(),
            extractor_version: "0.1.0".into(),
            confidence: 0.99,
            text_offset: Some(12),
            excerpt: Some("CVE-2026-0001".into()),
        };
        assert_eq!(round_trip(&extraction), extraction);

        let collection = Collection {
            id: id(),
            workspace_id: None,
            name: "default".into(),
            description: None,
            status: "active".into(),
            priority: 10,
            created_at: ts(),
            updated_at: ts(),
        };
        assert_eq!(round_trip(&collection), collection);

        let rule = NetworkRule {
            id: id(),
            source_id: id(),
            cidr_or_host: "10.0.0.0/8".into(),
            ports: Some(vec![443, 8443]),
            reason: "內部 REST API".into(),
            approved_by: "operator@example.invalid".into(),
            expires_at: Some(ts()),
            created_at: ts(),
            updated_at: ts(),
        };
        assert_eq!(round_trip(&rule), rule);
        assert!(rule.is_expired(ts()));
        assert!(!rule.is_expired(ts() - chrono::Duration::seconds(1)));
    }

    #[test]
    fn uuid_v7_parses_and_serializes_as_string() {
        let generated = Uuid::now_v7();
        assert_eq!(generated.get_version(), Some(uuid::Version::SortRand));
        let json = serde_json::to_string(&generated).unwrap();
        assert!(json.starts_with('"'));
        let back: Uuid = serde_json::from_str(&json).unwrap();
        assert_eq!(generated, back);
    }
}
