//! V0.1 canonical domain types。
//!
//! 欄位對齊 `docs/specs/SPEC_V0.1.md`。識別碼使用 UUID v7。
//! 規格未列舉值的 status 維持 `String`，避免發明狀態機。

pub mod collection;
pub mod connector;
pub mod content;
pub mod document;
pub mod duplicate;
pub mod embedding;
pub mod entity;
pub mod entity_alias;
pub mod entity_identifier;
pub mod enums;
pub mod event;
pub mod extraction;
pub mod failed_event;
pub mod ids;
pub mod job;
pub mod merge;
pub mod network_rule;
pub mod provenance;
pub mod raw_evidence;
pub mod relationship;
pub mod resolution;
pub mod source;
pub mod url_norm;

pub use collection::Collection;
pub use connector::Connector;
pub use content::{content_hash, normalize_content};
pub use document::Document;
pub use duplicate::DuplicateGroup;
pub use embedding::Embedding;
pub use entity::Entity;
pub use entity_alias::EntityAlias;
pub use entity_identifier::EntityIdentifier;
pub use enums::{
    DocumentType, EmbeddingTarget, EntityType, JobStatus, RelationshipType, ResolutionStatus,
    SourceType,
};
pub use event::Event;
pub use extraction::EntityExtraction;
pub use failed_event::FailedEvent;
pub use ids::*;
pub use job::Job;
pub use merge::{AbsorberSnapshot, MergeHistory, MergedRelationship, RepointedReference};
pub use network_rule::NetworkRule;
pub use provenance::Provenance;
pub use raw_evidence::RawEvidence;
pub use relationship::{Relationship, RelationshipEvidence};
pub use resolution::{RESOLUTION_METHODS, ResolutionCandidate};
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
            external_key: Some("nvd|CVE-2026-0001".into()),
            simhash: Some(-1),
            duplicate_of: Some(id()),
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
            merged_into: None,
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
            model: None,
        };
        assert_eq!(round_trip(&dup), dup);

        let semantic = DuplicateGroup {
            method: "semantic".into(),
            similarity: 0.92,
            model: Some("intfloat/multilingual-e5-small-int8".into()),
            ..dup.clone()
        };
        assert_eq!(round_trip(&semantic), semantic);
        let v = serde_json::to_value(&semantic).unwrap();
        assert_eq!(v["model"], "intfloat/multilingual-e5-small-int8");

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
    fn v0_2_alias_and_identifier_round_trip() {
        let alias = EntityAlias {
            id: id(),
            entity_id: id(),
            alias: "微軟".into(),
            alias_type: "localized_name".into(),
            source_id: Some(id()),
            confidence: 0.8,
            first_seen: ts(),
            last_seen: ts(),
        };
        assert_eq!(round_trip(&alias), alias);

        // source_id 可為 None：resolver 自己推導的 alias 沒有來源可指。
        let derived = EntityAlias {
            source_id: None,
            ..alias.clone()
        };
        assert_eq!(round_trip(&derived), derived);
        let v = serde_json::to_value(&derived).unwrap();
        assert!(v["source_id"].is_null());

        let identifier = EntityIdentifier {
            id: id(),
            entity_id: id(),
            // T10 的歧義就是靠 namespace 分開的：同樣 40 位 hex，
            // `git_commit` 與 `sha1` 是兩個不同的識別碼。
            namespace: "git_commit".into(),
            value: "A".repeat(40),
            normalized_value: "a".repeat(40),
            confidence: 0.9,
            source_id: None,
            first_seen: ts(),
            last_seen: ts(),
        };
        assert_eq!(round_trip(&identifier), identifier);
        let v = serde_json::to_value(&identifier).unwrap();
        assert_eq!(v["namespace"], "git_commit");
    }

    #[test]
    fn v0_2_resolution_candidate_round_trip() {
        let candidate = ResolutionCandidate {
            id: id(),
            entity_a_id: Uuid::parse_str("01993c6a-7c3e-7a11-8000-7c3e7a110001").unwrap(),
            entity_b_id: Uuid::parse_str("01993c6a-7c3e-7a11-8000-7c3e7a110002").unwrap(),
            score: 0.93,
            method: "exact_identifier".into(),
            evidence: json!({"namespace": "domain", "value": "microsoft.com"}),
            status: ResolutionStatus::Pending,
            created_at: ts(),
            reviewed_at: None,
        };
        assert_eq!(round_trip(&candidate), candidate);
        let v = serde_json::to_value(&candidate).unwrap();
        assert_eq!(v["status"], "pending");

        let reviewed = ResolutionCandidate {
            status: ResolutionStatus::AutoConfirmed,
            reviewed_at: Some(ts()),
            ..candidate.clone()
        };
        assert_eq!(round_trip(&reviewed), reviewed);
        // 四個狀態的線上字串是 schema 的一部分，改名等於改 schema。
        assert_eq!(
            serde_json::to_value(reviewed.status).unwrap(),
            "auto_confirmed"
        );

        // ordered_pair 必須符合 migration 0007 的 CHECK（entity_a_id < entity_b_id）。
        let (a, b) =
            ResolutionCandidate::ordered_pair(candidate.entity_b_id, candidate.entity_a_id);
        assert_eq!((a, b), (candidate.entity_a_id, candidate.entity_b_id));
        assert!(a < b);
    }

    #[test]
    fn v0_2_merge_history_round_trip_keeps_previous_values() {
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
        let history = MergeHistory {
            id: id(),
            survivor_id: id(),
            merged_id: id(),
            reason: "同一個 microsoft.com 識別碼".into(),
            operator: "operator@example.invalid".into(),
            timestamp: ts(),
            repointed_references: vec![RepointedReference {
                table: "relationships".into(),
                row_id: id(),
                column: "target_object_id".into(),
                previous_value: id(),
            }],
            merged_relationships: vec![MergedRelationship {
                absorbed_relationship_id: id(),
                absorber_relationship_id: Some(id()),
                absorbed_snapshot: rel.clone(),
                absorber_pre_merge: Some(AbsorberSnapshot {
                    evidence_count: 2,
                    confidence: 0.8,
                    first_seen: ts(),
                    last_seen: ts(),
                }),
                moved_evidence_ids: vec![id()],
            }],
            undone_at: None,
        };
        assert_eq!(round_trip(&history), history);

        // Acceptance C：undo 之後這一列還在，只是被標記——歷史不可以消失。
        let undone = MergeHistory {
            undone_at: Some(ts()),
            ..history.clone()
        };
        let back = round_trip(&undone);
        assert_eq!(back, undone);
        assert_eq!(back.repointed_references, history.repointed_references);
    }

    #[test]
    fn v0_2_failed_event_round_trip() {
        let failed = FailedEvent {
            id: id(),
            topic: "object.normalized".into(),
            partition: 3,
            // 超過 i32 的 offset：長期執行的 topic 真的會走到這裡，
            // 欄位若是 i32 會在這一行溢位。
            offset: 5_000_000_000,
            consumer_group: "osint-deduplicator".into(),
            failure_reason: "payload 缺少 document_id，無法定位文件".into(),
            attempt_count: 2,
            envelope: json!({"id": "01993c6a-7c3e-7a11-8000-7c3e7a110001"}),
            first_seen: ts(),
            last_seen: ts(),
            replayed_at: None,
        };
        assert_eq!(round_trip(&failed), failed);
        let v = serde_json::to_value(&failed).unwrap();
        assert_eq!(v["offset"], 5_000_000_000_i64);

        let replayed = FailedEvent {
            replayed_at: Some(ts()),
            ..failed
        };
        assert_eq!(round_trip(&replayed), replayed);
    }

    #[test]
    fn resolution_method_names_are_unique() {
        let mut sorted = RESOLUTION_METHODS.to_vec();
        sorted.sort_unstable();
        let before = sorted.len();
        sorted.dedup();
        assert_eq!(sorted.len(), before, "RESOLUTION_METHODS 有重複名稱");
        // SPEC §6 要求「至少」這十種。少一個就代表清單被改壞了。
        assert_eq!(RESOLUTION_METHODS.len(), 10);
    }

    #[test]
    fn embedding_round_trip() {
        let original = Embedding {
            id: id(),
            target_id: id(),
            target_type: EmbeddingTarget::DocumentTitle,
            model: "huggingface/sentence-transformers/all-MiniLM-L6-v2".into(),
            model_version: "c".repeat(64),
            dimensions: 384,
            content_hash: "d".repeat(64),
            created_at: ts(),
        };
        assert_eq!(round_trip(&original), original);
        let v: Value = serde_json::to_value(&original).unwrap();
        assert_eq!(v["target_type"], "document_title");
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
