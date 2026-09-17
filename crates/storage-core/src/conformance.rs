//! Capability conformance。adapter crate 的整合測試呼叫這些函式，針對真實後端驗證契約。
//!
//! 關聯式測試用 UUID v7 當資料，不 TRUNCATE 共用表。
//! OpenSearch / S3 測試必須先通過 [`verify_not_opencti_search`] / [`verify_not_opencti_s3`]。

use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{NaiveDate, TimeZone, Utc};
use core_model::{
    AbsorberSnapshot, AiRun, Candidate, CandidateEvidence, CandidateStatus, CandidateType,
    Collection, CollectionBudget, Connector, Document, DocumentType, DuplicateGroup, Embedding,
    EmbeddingTarget, Entity, EntityAlias, EntityExtraction, EntityIdentifier, EntityType, Event,
    FailedEvent, Job, JobStatus, MergeHistory, MergedRelationship, NetworkRule, Provenance,
    RawEvidence, Relationship, RelationshipEvidence, RelationshipType, RepointedReference,
    ResolutionCandidate, ResolutionStatus, Seed, SeedOrigin, SeedType, Source, SourceType,
};
use serde_json::{Value, json};
use url::Url;
use uuid::Uuid;

use crate::error::StorageError;
use crate::traits::{
    CanonicalStore, EmbeddedStore, GraphEdge, GraphNode, GraphPattern, GraphQuery, GraphStore,
    GraphTraversalOptions, KeyValueStore, ObjectStore, ProjectionCheckpoint, ProjectionStore,
    RebuildState, RebuildStatus, RelationalStore, SearchDocument, SearchQuery, SearchStore,
    StructuredSearch, TransactionalStore,
};

/// 從 workspace 根目錄載入 `.env`（若存在）。已設定的環境變數不被覆蓋。
pub fn load_workspace_dotenv() {
    if let Some(root) = find_workspace_root() {
        let path = root.join(".env");
        if path.is_file() {
            let _ = dotenvy::from_path(&path);
        }
    }
}

/// 由 `CARGO_MANIFEST_DIR` 往上找含 `config/default.toml` 的目錄。
#[must_use]
pub fn find_workspace_root() -> Option<PathBuf> {
    let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    loop {
        if dir.join("config/default.toml").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

/// 測試用環境變數。缺值時回傳可讀錯誤，指出要設哪個變數。
pub fn required_env(name: &str) -> Result<String, StorageError> {
    env::var(name).map_err(|_| StorageError::Configuration {
        message: format!(
            "環境變數 `{name}` 未設定。請在 repo 根目錄放 `.env`（可從 `.env.example` 複製），並確認 OpenSearch 用 19200、MinIO 用 19000，不要連到主機 9200/9000 的其他堆疊"
        ),
    })
}

/// 只有本機開發機（同時跑 OpenCTI，9200/9000 canonical 埠被占走）才需要開嚴格埠號檢查。
/// 設 `OSINT_STRICT_PORT_ISOLATION=1`（已寫在本機 `.env`，不進版本庫）才會擋 9200/9000；
/// CI 或其他沒有這個埠衝突的機器不要設這個變數，9200/9000 在那裡通常就是我們自己的服務。
///
/// 這條規則原本寫死「9200/9000 一律拒絕」，在 GitHub Actions 的第一次 CI run 上就直接把
/// 我們自己乾淨的 OpenSearch/MinIO（canonical 埠,沒有 OpenCTI 衝突）誤判成 OpenCTI 擋下來——
/// 埠號在不同環境代表不同東西,不能寫死。真正該信任的是連線後的身分驗證
/// （見 [`assert_opensearch_identity`]),埠號檢查只在已知有衝突的這台機器上當額外防線。
fn strict_port_isolation_enabled() -> bool {
    env::var("OSINT_STRICT_PORT_ISOLATION")
        .map(|v| v == "1")
        .unwrap_or(false)
}

/// 驗證 URL 格式正確；只有在本機開了 `OSINT_STRICT_PORT_ISOLATION` 時才額外擋 9200
/// （本機的 OpenCTI Elasticsearch）。實際的服務身分一律以 [`assert_opensearch_identity`] 為準。
pub fn verify_not_opencti_search(url: &str) -> Result<Url, StorageError> {
    let parsed = Url::parse(url).map_err(|err| StorageError::Configuration {
        message: format!("OPENSEARCH_URL `{url}` 不是合法 URL：{err}"),
    })?;
    if strict_port_isolation_enabled() && parsed.port_or_known_default() == Some(9200) {
        return Err(StorageError::Configuration {
            message: format!(
                "OPENSEARCH_URL `{url}` 指向埠 9200。本機 9200 是 OpenCTI Elasticsearch，不是 osint-core-opensearch-1。請改成 http://127.0.0.1:19200"
            ),
        });
    }
    Ok(parsed)
}

/// 驗證 URL 格式正確；只有在本機開了 `OSINT_STRICT_PORT_ISOLATION` 時才額外擋 9000
/// （本機的 OpenCTI MinIO）。MinIO 目前沒有等同 [`assert_opensearch_identity`] 的身分驗證，
/// 所以這台已知衝突的機器上，埠號檢查仍是唯一防線，其餘環境不受影響。
pub fn verify_not_opencti_s3(endpoint: &str) -> Result<Url, StorageError> {
    let parsed = Url::parse(endpoint).map_err(|err| StorageError::Configuration {
        message: format!("S3_ENDPOINT `{endpoint}` 不是合法 URL：{err}"),
    })?;
    if strict_port_isolation_enabled() && parsed.port_or_known_default() == Some(9000) {
        return Err(StorageError::Configuration {
            message: format!(
                "S3_ENDPOINT `{endpoint}` 指向埠 9000。本機 9000 是 OpenCTI MinIO，不是 osint-core-minio-1。請改成 http://127.0.0.1:19000"
            ),
        });
    }
    Ok(parsed)
}

/// 檢查 GET `/` 回應確實是 OpenSearch，且不是 Elasticsearch。
pub fn assert_opensearch_identity(cluster_json: &serde_json::Value) -> Result<(), StorageError> {
    let tagline = cluster_json
        .get("tagline")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let distribution = cluster_json
        .pointer("/version/distribution")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    if tagline.contains("You Know, for Search")
        || distribution.eq_ignore_ascii_case("elasticsearch")
    {
        return Err(StorageError::Configuration {
            message: format!(
                "連到的是 Elasticsearch（tagline={tagline:?}），不是 osint-core OpenSearch。請改用埠 19200"
            ),
        });
    }
    if !distribution.eq_ignore_ascii_case("opensearch") && !tagline.contains("OpenSearch") {
        return Err(StorageError::Configuration {
            message: format!(
                "無法確認是 OpenSearch（distribution={distribution:?}, tagline={tagline:?}）。拒絕繼續，以免寫到錯誤叢集"
            ),
        });
    }
    Ok(())
}

fn fixture_ts() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 10, 12, 0, 0).unwrap()
}

/// 關聯式 CRUD + Raw Evidence 不可變。
pub async fn assert_relational_round_trip<S: RelationalStore>(
    store: &S,
) -> Result<(), StorageError> {
    store.health().await?;

    let source = Source {
        id: Uuid::now_v7(),
        name: "conformance-rss".into(),
        source_type: SourceType::Rss,
        platform: Some("nvd".into()),
        base_url: Some("https://example.invalid/rss".into()),
        description: Some("conformance".into()),
        language: Some("en".into()),
        country: Some("US".into()),
        enabled: true,
        collection_policy: json!({"interval": "15m"}),
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
        last_seen: Some(fixture_ts()),
    };
    store.put_source(&source).await?;
    let got = store
        .get_source(source.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 source 讀不到".into(),
        })?;
    assert_eq_debug("source", &source, &got);
    // 共用表裡有其他測試同時寫入的列，不能假設「剛寫入的一定在第一頁」。
    // 改用 cursor = id+1：strictly-less-than 之下，第一筆必定就是這筆。
    let listed = store.list_sources(Some(next_uuid(source.id)), 10).await?;
    assert_cursor_page("list_sources", source.id, listed.iter().map(|i| i.id))?;
    // cursor 是 strictly-less-than：用自己的 id 當 cursor 就不該再看到自己。
    if store
        .list_sources(Some(source.id), 10)
        .await?
        .iter()
        .any(|item| item.id == source.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_sources 的 cursor 應嚴格小於 after，自己不該再出現".into(),
        });
    }
    // limit 必須真的限制筆數，且 0 被夾成 1（不可變成無界查詢）。
    let clamped = store.list_sources(None, 0).await?;
    if clamped.len() != 1 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "list_sources(limit=0) 應夾成 1 筆，實際 {} 筆。limit 沒有被夾住等於無界查詢",
                clamped.len()
            ),
        });
    }

    let rule = NetworkRule {
        id: Uuid::now_v7(),
        source_id: source.id,
        cidr_or_host: "10.1.2.0/24".into(),
        ports: Some(vec![443]),
        reason: "conformance private API".into(),
        approved_by: "operator@example.invalid".into(),
        expires_at: None,
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
    };
    store.put_network_rule(&rule).await?;
    let got = store
        .get_network_rule(rule.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 network_rule 讀不到".into(),
        })?;
    assert_eq_debug("network_rule", &rule, &got);
    let listed = store.list_network_rules(source.id).await?;
    if listed.len() != 1 || listed[0] != rule {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "list_network_rules 應回 1 筆相符規則，實際 {} 筆",
                listed.len()
            ),
        });
    }
    if !store.delete_network_rule(rule.id).await? {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "delete_network_rule 應刪到剛寫入的規則".into(),
        });
    }
    if store.get_network_rule(rule.id).await?.is_some() {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "刪除後 get_network_rule 仍讀得到".into(),
        });
    }

    let connector = Connector {
        id: Uuid::now_v7(),
        source_id: source.id,
        name: "conformance-connector".into(),
        connector_type: "rss".into(),
        version: "0.1.0".into(),
        enabled: true,
        configuration: json!({"url": "https://example.invalid/rss"}),
        credential_reference: Some("env:NVD_TOKEN".into()),
        schedule: Some("*/15 * * * *".into()),
        rate_limit: json!({"rps": 1}),
        timeout: json!({"connect_ms": 5000}),
        proxy_reference: None,
        checkpoint: json!({"etag": "abc"}),
        last_run: Some(fixture_ts()),
        last_success: Some(fixture_ts()),
        status: "idle".into(),
        error_count: 0,
    };
    store.put_connector(&connector).await?;
    let got = store
        .get_connector(connector.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 connector 讀不到".into(),
        })?;
    assert_eq_debug("connector", &connector, &got);
    let enabled = store.list_enabled_connectors().await?;
    if !enabled.iter().any(|item| item.id == connector.id) {
        return Err(StorageError::NotFound {
            message: "剛寫入且 enabled 的 connector 沒有出現在 list_enabled_connectors".into(),
        });
    }
    let listed = store
        .list_connectors(Some(next_uuid(connector.id)), 10)
        .await?;
    assert_cursor_page("list_connectors", connector.id, listed.iter().map(|i| i.id))?;
    // list_connectors 必須含停用的 connector——這正是它和 list_enabled_connectors 的差別。
    let mut disabled = connector.clone();
    disabled.id = Uuid::now_v7();
    disabled.name = "conformance-connector-disabled".into();
    disabled.enabled = false;
    store.put_connector(&disabled).await?;
    let listed = store
        .list_connectors(Some(next_uuid(disabled.id)), 10)
        .await?;
    assert_cursor_page("list_connectors", disabled.id, listed.iter().map(|i| i.id))?;
    if store
        .list_enabled_connectors()
        .await?
        .iter()
        .any(|item| item.id == disabled.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "停用的 connector 不該出現在 list_enabled_connectors".into(),
        });
    }

    let collection = Collection {
        id: Uuid::now_v7(),
        workspace_id: None,
        name: "conformance-collection".into(),
        description: Some("phase2".into()),
        status: "active".into(),
        priority: 3,
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
    };
    store.put_collection(&collection).await?;
    let listed = store
        .list_collections(Some(next_uuid(collection.id)), 10)
        .await?;
    assert_cursor_page(
        "list_collections",
        collection.id,
        listed.iter().map(|i| i.id),
    )?;
    store
        .link_collection_source(collection.id, source.id)
        .await?;
    store
        .link_collection_connector(collection.id, connector.id)
        .await?;

    let evidence = RawEvidence {
        id: Uuid::now_v7(),
        source_id: source.id,
        connector_id: connector.id,
        collection_id: Some(collection.id),
        external_id: Some("CVE-2026-0001".into()),
        source_url: "https://example.invalid/cve".into(),
        retrieved_at: fixture_ts(),
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
    store.insert_raw_evidence(&evidence).await?;
    let got = store
        .get_raw_evidence(evidence.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 raw_evidence 讀不到".into(),
        })?;
    assert_eq_debug("raw_evidence", &evidence, &got);

    let again = store.insert_raw_evidence(&evidence).await;
    match again {
        Err(StorageError::Conflict { .. }) => {}
        other => {
            return Err(StorageError::Unknown {
                backend: "conformance",
                message: format!(
                    "重複 insert_raw_evidence 應回 Conflict，實際是 {other:?}。Raw Evidence 必須不可變"
                ),
            });
        }
    }

    let listed = store
        .list_raw_evidence(Some(next_uuid(evidence.id)), 10)
        .await?;
    assert_cursor_page(
        "list_raw_evidence",
        evidence.id,
        listed.iter().map(|i| i.id),
    )?;
    let by_source = store
        .list_raw_evidence_by_source(source.id, Some(next_uuid(evidence.id)), 10)
        .await?;
    assert_cursor_page(
        "list_raw_evidence_by_source",
        evidence.id,
        by_source.iter().map(|i| i.id),
    )?;
    if by_source.iter().any(|item| item.source_id != source.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_raw_evidence_by_source 回了不屬於該 source 的列".into(),
        });
    }
    // 換一個不存在的 source id 必須是空結果，不能退化成「全部列出」。
    if !store
        .list_raw_evidence_by_source(Uuid::now_v7(), None, 10)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_raw_evidence_by_source 用不存在的 source_id 應回空頁".into(),
        });
    }

    let document = Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::Advisory,
        schema_version: "1.0".into(),
        title: Some("CVE-2026-0001".into()),
        body: Some("body".into()),
        summary: None,
        language: Some("en".into()),
        author: None,
        published_at: Some(fixture_ts()),
        modified_at: None,
        observed_at: fixture_ts(),
        collected_at: fixture_ts(),
        source_url: Some("https://example.invalid/cve".into()),
        canonical_url: Some("https://example.invalid/cve".into()),
        normalized_content_hash: Some("b".repeat(64)),
        confidence: 0.9,
        labels: vec!["cve".into()],
        attributes: json!({"cve": "CVE-2026-0001"}),
        // dedup 欄位在這裡刻意留空：模擬「normalizer 剛寫完、deduplicator 還沒處理」
        // 的狀態，下面會另外寫一份有值的來驗證 round-trip。
        external_key: None,
        simhash: None,
        duplicate_of: None,
    };
    store.put_document(&document).await?;
    let mut updated = document.clone();
    updated.title = Some("updated-title".into());
    store.put_document(&updated).await?;
    let got = store
        .get_document(document.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 document 讀不到".into(),
        })?;
    assert_eq_debug("document", &updated, &got);
    let listed = store
        .list_documents(Some(next_uuid(document.id)), 10)
        .await?;
    assert_cursor_page("list_documents", document.id, listed.iter().map(|i| i.id))?;
    store
        .link_collection_object(collection.id, document.id)
        .await?;

    assert_dedup_queries(store, &updated).await?;

    // ⚠️ `normalized_name` 必須含 run-specific UUID。0005 之後
    // `(entity_type, normalized_name)` 是 UNIQUE，寫死字串的話第二次跑（或 PG 與 SQLite
    // 共用同一顆 DB 時）會直接違反唯一鍵——那不是測到了什麼，只是 fixture 自己撞自己。
    let entity_name = format!("CVE-2026-{}", Uuid::now_v7().simple());
    let entity = Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Vulnerability,
        name: entity_name.clone(),
        normalized_name: entity_name.to_ascii_lowercase(),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    store.put_entity(&entity).await?;
    assert_eq_debug(
        "entity",
        &entity,
        &store.get_entity(entity.id).await?.expect("entity"),
    );
    assert_entity_queries(store, &entity).await?;

    let relationship = Relationship {
        id: Uuid::now_v7(),
        source_object_id: document.id,
        relationship_type: RelationshipType::Affects,
        target_object_id: entity.id,
        confidence: 0.8,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        evidence_count: 1,
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
    };
    store.put_relationship(&relationship).await?;
    assert_eq_debug(
        "relationship",
        &relationship,
        &store
            .get_relationship(relationship.id)
            .await?
            .expect("relationship"),
    );

    let rel_ev = RelationshipEvidence {
        id: Uuid::now_v7(),
        relationship_id: relationship.id,
        object_id: document.id,
        raw_evidence_id: Some(evidence.id),
        excerpt: Some("CVE-2026-0001".into()),
        confidence: 0.7,
        created_at: fixture_ts(),
    };
    store.put_relationship_evidence(&rel_ev).await?;
    assert_eq_debug(
        "relationship_evidence",
        &rel_ev,
        &store
            .get_relationship_evidence(rel_ev.id)
            .await?
            .expect("rel_ev"),
    );
    assert_relationship_queries(store, &relationship, &rel_ev, entity.id).await?;

    let event = Event {
        id: Uuid::now_v7(),
        event_type: "advisory_published".into(),
        title: "published".into(),
        description: None,
        start_time: Some(fixture_ts()),
        end_time: None,
        confidence: 0.5,
        status: "open".into(),
        attributes: json!({}),
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
    };
    store.put_event(&event).await?;
    assert_eq_debug(
        "event",
        &event,
        &store.get_event(event.id).await?.expect("event"),
    );
    let listed = store.list_events(Some(next_uuid(event.id)), 10).await?;
    assert_cursor_page("list_events", event.id, listed.iter().map(|i| i.id))?;

    let provenance = Provenance {
        id: Uuid::now_v7(),
        subject_id: document.id,
        action: "normalized".into(),
        parent_id: None,
        raw_evidence_id: Some(evidence.id),
        processor: "normalizer".into(),
        processor_version: "0.1.0".into(),
        timestamp: fixture_ts(),
        metadata: json!({}),
    };
    store.put_provenance(&provenance).await?;
    assert_eq_debug(
        "provenance",
        &provenance,
        &store.get_provenance(provenance.id).await?.expect("prov"),
    );
    let by_raw = store.list_provenance_by_raw_evidence(evidence.id).await?;
    if !by_raw.iter().any(|item| item.id == provenance.id) {
        return Err(StorageError::NotFound {
            message: "剛寫入的 provenance 沒有出現在 list_provenance_by_raw_evidence".into(),
        });
    }
    let by_subject = store.list_provenance_by_subject(document.id).await?;
    if !by_subject.iter().any(|item| item.id == provenance.id) {
        return Err(StorageError::NotFound {
            message: "剛寫入的 provenance 沒有出現在 list_provenance_by_subject".into(),
        });
    }
    if by_subject.iter().any(|item| item.subject_id != document.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_provenance_by_subject 回了不屬於該 subject 的列".into(),
        });
    }

    let job = Job {
        id: Uuid::now_v7(),
        job_type: "collect".into(),
        status: JobStatus::Queued,
        correlation_id: Some(document.id),
        created_at: fixture_ts(),
        started_at: None,
        completed_at: None,
        retry_count: 0,
        error: None,
        parameters: Some(json!({
            "source_id": document.id,
            "raw_evidence_id": evidence.id,
        })),
    };
    store.put_job(&job).await?;
    assert_eq_debug("job", &job, &store.get_job(job.id).await?.expect("job"));
    let listed = store.list_jobs(None, 10).await?;
    if !listed.iter().any(|item| item.id == job.id) {
        return Err(StorageError::NotFound {
            message: "剛寫入的 job 沒有出現在 list_jobs 結果".into(),
        });
    }
    assert_job_status_filter(store, &job).await?;

    let dup = DuplicateGroup {
        id: Uuid::now_v7(),
        canonical_object_id: document.id,
        member_object_id: Some(document.id),
        member_raw_evidence_id: None,
        method: "sha256".into(),
        similarity: 1.0,
        first_seen: fixture_ts(),
        model: None,
    };
    store.put_duplicate_group(&dup).await?;
    assert_eq_debug(
        "duplicate_group",
        &dup,
        &store.get_duplicate_group(dup.id).await?.expect("dup"),
    );

    let extraction = EntityExtraction {
        id: Uuid::now_v7(),
        object_id: document.id,
        entity_id: entity.id,
        extractor: "regex-cve".into(),
        extractor_version: "0.1.0".into(),
        confidence: 0.99,
        text_offset: Some(12),
        excerpt: Some("CVE-2026-0001".into()),
    };
    store.put_entity_extraction(&extraction).await?;
    assert_eq_debug(
        "entity_extraction",
        &extraction,
        &store
            .get_entity_extraction(extraction.id)
            .await?
            .expect("extraction"),
    );
    assert_extraction_queries(store, &extraction).await?;

    assert_v0_2_resolution_queries(store, &entity, source.id).await?;
    assert_v0_3_discovery_queries(store, &collection, &entity).await?;
    assert_v0_3_budget_queries(store, &collection).await?;
    assert_v0_2_failed_events(store).await?;
    assert_embedding_queries(store, document.id).await?;

    let missing = Uuid::now_v7();
    if store.get_document(missing).await?.is_some() {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "不存在的 document id 不該讀到資料".into(),
        });
    }
    if store.delete_document(missing).await? {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "刪除不存在的 document 應回 false".into(),
        });
    }

    Ok(())
}

/// SPEC §17 的 Entity 反查能力：自然鍵查詢 + cursor 列出。
///
/// 這兩個方法是 entity-worker「同一個 CVE 只建一個 Entity」的**唯一**依據。
/// 少了自然鍵查詢，worker 只能每次都建新的——不會報錯，只會讓 Entity 表無聲地長出
/// 一堆同名列，直到有人去數才發現。
async fn assert_entity_queries<S: RelationalStore>(
    store: &S,
    entity: &Entity,
) -> Result<(), StorageError> {
    let found = store
        .find_entity_by_normalized_name(entity.entity_type, &entity.normalized_name)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: format!(
                "find_entity_by_normalized_name({:?}, `{}`) 查不到剛寫入的 Entity。\
                 entity-worker 靠這個方法重用既有 Entity，查不到就會每篇文章都建一個新的",
                entity.entity_type, entity.normalized_name
            ),
        })?;
    assert_eq_debug("find_entity_by_normalized_name", entity, &found);

    // 同名但 entity_type 不同不可以命中——自然鍵是 (type, name) 兩欄，
    // 只比 name 的話 `example.invalid` 這種 Domain 會跟同名的 Hostname 混在一起。
    if store
        .find_entity_by_normalized_name(EntityType::Location, &entity.normalized_name)
        .await?
        .is_some()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_by_normalized_name 忽略了 entity_type，只比對 normalized_name"
                .into(),
        });
    }

    // 不存在的名字要回 None，不能回「隨便一列」。
    if store
        .find_entity_by_normalized_name(entity.entity_type, &format!("{}-absent", Uuid::now_v7()))
        .await?
        .is_some()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_by_normalized_name 對不存在的名稱回了資料".into(),
        });
    }

    let listed = store.list_entities(None, 100).await?;
    if listed.len() > 100 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_entities 沒有把 limit 夾在 1..=100".into(),
        });
    }
    // cursor 契約：`after` 是「嚴格小於」，所以傳 id+1 必須還看得到自己。
    let page = store.list_entities(Some(next_uuid(entity.id)), 10).await?;
    assert_cursor_page("list_entities", entity.id, page.iter().map(|i| i.id))
}

/// `list_jobs_by_status` 必須在 SQL 裡過濾，而且要維持 cursor 契約。
///
/// 兩個斷言缺一不可：
/// * 用**自己的** status 查得到 → 過濾條件沒有把對的列擋掉。
/// * 用**別的** status 查不到 → 過濾條件真的有生效。少了這一條，
///   一個完全忽略 `status` 參數的實作（等同 `list_jobs`）也會通過測試。
async fn assert_job_status_filter<S: RelationalStore>(
    store: &S,
    job: &Job,
) -> Result<(), StorageError> {
    let page = store
        .list_jobs_by_status(job.status, Some(next_uuid(job.id)), 10)
        .await?;
    assert_cursor_page("list_jobs_by_status", job.id, page.iter().map(|i| i.id))?;
    if page.iter().any(|item| item.status != job.status) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_jobs_by_status 回了其他狀態的 job".into(),
        });
    }

    // fixture 的 job 是 Queued，所以 Cancelled 這一頁不該有它。
    let other = JobStatus::Cancelled;
    if store
        .list_jobs_by_status(other, Some(next_uuid(job.id)), 10)
        .await?
        .iter()
        .any(|item| item.id == job.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "list_jobs_by_status({other:?}) 回了 status={:?} 的 job——status 參數沒有生效",
                job.status
            ),
        });
    }
    Ok(())
}

/// SPEC §11／§12：Relationship 的物件反查，以及「任何 relationship 必須能回查 evidence」。
async fn assert_relationship_queries<S: RelationalStore>(
    store: &S,
    relationship: &Relationship,
    evidence: &RelationshipEvidence,
    entity_id: Uuid,
) -> Result<(), StorageError> {
    // source 端（Document）與 target 端（Entity）都要查得到同一條邊。
    for (label, object_id) in [
        ("source_object_id", relationship.source_object_id),
        ("target_object_id", entity_id),
    ] {
        let rows = store.list_relationships_by_object(object_id, 100).await?;
        if !rows.iter().any(|r| r.id == relationship.id) {
            return Err(StorageError::NotFound {
                message: format!(
                    "list_relationships_by_object 從 {label} 這一端查不到剛寫入的 Relationship。\
                     SPEC §26 Acceptance E 要從 Entity 往回走，只支援 source 端等於走不通"
                ),
            });
        }
    }

    // 不綁任何一端的全表列出，cursor 契約與其他 list_* 相同。
    let page = store
        .list_relationships(Some(next_uuid(relationship.id)), 10)
        .await?;
    assert_cursor_page(
        "list_relationships",
        relationship.id,
        page.iter().map(|i| i.id),
    )?;
    if store.list_relationships(None, 0).await?.len() != 1 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_relationships(limit=0) 應夾成 1 筆，limit 沒被夾住等於無界查詢".into(),
        });
    }

    let rows = store
        .list_relationship_evidence(relationship.id, 100)
        .await?;
    if !rows.iter().any(|e| e.id == evidence.id) {
        return Err(StorageError::NotFound {
            message: "list_relationship_evidence 查不到剛寫入的 RelationshipEvidence。\
                      SPEC §12 要求任何 relationship 都能回查 evidence"
                .into(),
        });
    }
    if rows.iter().any(|e| e.relationship_id != relationship.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_relationship_evidence 回了不屬於該 relationship 的列".into(),
        });
    }
    // 不存在的 relationship 要回空陣列，不是回全部。
    if !store
        .list_relationship_evidence(Uuid::now_v7(), 100)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_relationship_evidence 對不存在的 relationship_id 回了資料".into(),
        });
    }
    Ok(())
}

/// SPEC §17：extraction 的兩個反查（依 object、依 entity）。
async fn assert_extraction_queries<S: RelationalStore>(
    store: &S,
    extraction: &EntityExtraction,
) -> Result<(), StorageError> {
    let by_object = store
        .list_entity_extractions_by_object(extraction.object_id, 100)
        .await?;
    if !by_object.iter().any(|e| e.id == extraction.id) {
        return Err(StorageError::NotFound {
            message: "list_entity_extractions_by_object 查不到剛寫入的 EntityExtraction".into(),
        });
    }
    if by_object
        .iter()
        .any(|e| e.object_id != extraction.object_id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_entity_extractions_by_object 回了不屬於該 object 的列".into(),
        });
    }

    let by_entity = store
        .list_entity_extractions_by_entity(extraction.entity_id, 100)
        .await?;
    if !by_entity.iter().any(|e| e.id == extraction.id) {
        return Err(StorageError::NotFound {
            message: "list_entity_extractions_by_entity 查不到剛寫入的 EntityExtraction".into(),
        });
    }
    if by_entity
        .iter()
        .any(|e| e.entity_id != extraction.entity_id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_entity_extractions_by_entity 回了不屬於該 entity 的列".into(),
        });
    }

    if !store
        .list_entity_extractions_by_object(Uuid::now_v7(), 100)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_entity_extractions_by_object 對不存在的 object_id 回了資料".into(),
        });
    }
    Ok(())
}

/// SPEC §15／§16 的查詢能力：Stage 1～4 的候選查詢 + duplicate group 的兩個反查。
///
/// 每次呼叫用新的 UUID 當鍵（`external_key`／`canonical_url`／content hash 都含 UUID），
/// 因為 e2e 共用同一個 Postgres，寫死字串會被其他測試的殘留資料干擾。
async fn assert_dedup_queries<S: RelationalStore>(
    store: &S,
    base: &Document,
) -> Result<(), StorageError> {
    let run = Uuid::now_v7();
    let key = format!("conformance-platform|{run}");
    let url = format!("https://example.invalid/conformance/{run}");
    // 假裝成 SHA256（64 個十六進位字元）：真實資料長這樣，欄位長度限制才測得到。
    let hash = format!("{}{}", run.simple(), run.simple());

    // canonical 先寫（id 較小），duplicate 後寫。查詢契約是「依 id 升序」，
    // 所以 canonical 必須是候選清單的第一筆。
    let mut canonical = base.clone();
    canonical.id = Uuid::now_v7();
    canonical.external_key = Some(key.clone());
    canonical.canonical_url = Some(url.clone());
    canonical.normalized_content_hash = Some(hash.clone());
    // 0x0F0F... 與下面 duplicate 的 fingerprint 只差 1 個 bit。
    canonical.simhash = Some(0x0F0F_0F0F_0F0F_0F0F);
    canonical.duplicate_of = None;
    store.put_document(&canonical).await?;

    let mut duplicate = base.clone();
    duplicate.id = Uuid::now_v7();
    duplicate.external_key = Some(key.clone());
    duplicate.canonical_url = Some(url.clone());
    duplicate.normalized_content_hash = Some(hash.clone());
    duplicate.simhash = Some(0x0F0F_0F0F_0F0F_0F0E);
    duplicate.duplicate_of = Some(canonical.id);
    store.put_document(&duplicate).await?;

    // 三個 dedup 欄位必須能原樣讀回來——特別是 simhash：它是 u64 的位元重解讀，
    // backend 若把它當數值轉換（例如走浮點）會靜默改值。
    let got = store
        .get_document(duplicate.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 duplicate document 讀不到".into(),
        })?;
    assert_eq_debug("document_dedup_fields", &duplicate, &got);

    let by_key = store
        .find_document_ids_by_external_key(&key, duplicate.id, 10)
        .await?;
    assert_only(&by_key, canonical.id, "find_document_ids_by_external_key")?;
    let by_url = store
        .find_document_ids_by_canonical_url(&url, duplicate.id, 10)
        .await?;
    assert_only(&by_url, canonical.id, "find_document_ids_by_canonical_url")?;
    let by_hash = store
        .find_document_ids_by_content_hash(&hash, duplicate.id, 10)
        .await?;
    assert_only(&by_hash, canonical.id, "find_document_ids_by_content_hash")?;

    // `before` 是嚴格小於：用 canonical 自己當 before 時，比它新的 duplicate 不該出現
    // （否則 duplicate_of 會形成環），它自己當然也不該出現。
    let earlier_than_canonical = store
        .find_document_ids_by_external_key(&key, canonical.id, 10)
        .await?;
    if earlier_than_canonical.contains(&canonical.id)
        || earlier_than_canonical.contains(&duplicate.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "find_document_ids_by_external_key 的 before 必須是嚴格小於，實際回 {earlier_than_canonical:?}"
            ),
        });
    }

    // Stage 4：距離 1 的候選要找得到；距離上限 0 時同一筆不該出現。
    let near = store
        .find_simhash_candidates(duplicate.simhash.expect("simhash"), 3, duplicate.id, 500)
        .await?;
    if !near.iter().any(|c| c.id == canonical.id) {
        return Err(StorageError::NotFound {
            message: format!(
                "find_simhash_candidates(max_distance=3) 應找到距離 1 的 canonical {}，實際回 {} 筆",
                canonical.id,
                near.len()
            ),
        });
    }
    let exact = store
        .find_simhash_candidates(duplicate.simhash.expect("simhash"), 0, duplicate.id, 500)
        .await?;
    if exact.iter().any(|c| c.id == canonical.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_simhash_candidates(max_distance=0) 不該回距離 1 的候選".into(),
        });
    }

    let group = DuplicateGroup {
        id: Uuid::now_v7(),
        canonical_object_id: canonical.id,
        member_object_id: Some(duplicate.id),
        member_raw_evidence_id: None,
        method: "content_sha256".into(),
        similarity: 1.0,
        first_seen: fixture_ts(),
        model: None,
    };
    store.put_duplicate_group(&group).await?;
    let by_member = store
        .get_duplicate_group_by_member(duplicate.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "get_duplicate_group_by_member 讀不到剛寫入的 group".into(),
        })?;
    assert_eq_debug("duplicate_group_by_member", &group, &by_member);
    let by_canonical = store
        .list_duplicate_groups_by_canonical(canonical.id, 10)
        .await?;
    if by_canonical.len() != 1 || by_canonical[0] != group {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "list_duplicate_groups_by_canonical 應回 1 筆相符 group，實際 {} 筆",
                by_canonical.len()
            ),
        });
    }
    Ok(())
}

/// V0.2 §3／§4／§5／§7：alias／identifier／resolution candidate／merge history。
///
/// 這裡驗的重點不是「寫得進去讀得回來」，而是**做錯了不會報錯的地方**：
///
/// 1. `(namespace, normalized_value)` 的唯一鍵真的存在——沒有它，兩個 Entity
///    可以各自宣稱同一個 domain，「exact identifier」就不再是合併依據。
/// 2. `entity_a_id < entity_b_id` 的 CHECK 真的擋得住——沒有它，同一對候選
///    會存成兩列，Review 畫面出現重複項目而且不會有任何錯誤。
/// 3. `(a, b, method)` 的唯一鍵真的存在——沒有它，每跑一次 resolver 就長一批新列。
/// 4. `repointed_references` 原樣讀得回來——它是 undo 的**全部**依據，
///    少一筆就少還原一個參照（SPEC Acceptance C）。
/// 5. `merged_into` 與 `merged_relationships` 原樣讀得回來（Phase 1e）。
///    漏掉的話下一棒 merge 實作寫進去、讀出來卻永遠是空，而且不會報錯。
/// 6. `update_resolution_candidate_status` 真的改到列、不存在的 id 回 false
///    （ADR-012 Step 0）。漏掉的話下一棒自動核准寫進去、讀出來卻永遠是 Pending。
/// 7. `auto_approval_audit` 原樣讀得回來（ADR-012 Step 0）。漏掉的話稽核 JSON
///    寫進去讀出來永遠是 None，出事時分不出哪些合併沒有人類看過。
async fn assert_v0_2_resolution_queries<S: RelationalStore>(
    store: &S,
    entity: &Entity,
    source_id: Uuid,
) -> Result<(), StorageError> {
    let run = Uuid::now_v7();

    // --- §3 alias ---------------------------------------------------------
    let alias = EntityAlias {
        id: Uuid::now_v7(),
        entity_id: entity.id,
        alias: format!("conformance-alias-{run}"),
        alias_type: "localized_name".into(),
        source_id: Some(source_id),
        confidence: 0.75,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
    };
    store.put_entity_alias(&alias).await?;
    assert_eq_debug(
        "entity_alias",
        &alias,
        &store
            .get_entity_alias(alias.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 entity_alias 讀不到".into(),
            })?,
    );
    // source_id 可為 NULL——resolver 自己推導的 alias 沒有來源可指。
    let derived = EntityAlias {
        id: Uuid::now_v7(),
        source_id: None,
        ..alias.clone()
    };
    store.put_entity_alias(&derived).await?;
    assert_eq_debug(
        "entity_alias_without_source",
        &derived,
        &store.get_entity_alias(derived.id).await?.expect("alias"),
    );

    let listed = store.list_entity_aliases_by_entity(entity.id, 100).await?;
    if !listed.iter().any(|a| a.id == alias.id) {
        return Err(StorageError::NotFound {
            message: "list_entity_aliases_by_entity 查不到剛寫入的 alias".into(),
        });
    }
    if listed.iter().any(|a| a.entity_id != entity.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_entity_aliases_by_entity 回了不屬於該 entity 的列".into(),
        });
    }
    if !store
        .list_entity_aliases_by_entity(Uuid::now_v7(), 100)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_entity_aliases_by_entity 對不存在的 entity_id 回了資料".into(),
        });
    }

    // --- §4 identifier ----------------------------------------------------
    let namespace = format!("conformance-ns-{run}");
    // `normalized_value` 必須帶 run id：conformance 不 TRUNCATE 共用表，
    // `find_entity_identifiers_by_normalized_value` 的 limit 夾在 1..=100，
    // 固定用 `example.com` 時舊列累積超過 100 筆，新列就被 ASC LIMIT 擠掉，
    // 看起來像「剛寫入的識別碼查不到」。2026-09-16 本機 Postgres 實測
    // `normalized_value = 'example.com'` 已有 102 列。
    let normalized_value = format!("example.com-{run}");
    let identifier = EntityIdentifier {
        id: Uuid::now_v7(),
        entity_id: entity.id,
        namespace: namespace.clone(),
        value: format!("Example.COM-{run}"),
        normalized_value: normalized_value.clone(),
        confidence: 0.95,
        source_id: None,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
    };
    store.put_entity_identifier(&identifier).await?;
    assert_eq_debug(
        "entity_identifier",
        &identifier,
        &store
            .get_entity_identifier(identifier.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 entity_identifier 讀不到".into(),
            })?,
    );
    let listed = store
        .list_entity_identifiers_by_entity(entity.id, 100)
        .await?;
    if !listed.iter().any(|i| i.id == identifier.id) {
        return Err(StorageError::NotFound {
            message: "list_entity_identifiers_by_entity 查不到剛寫入的識別碼".into(),
        });
    }

    // 同一個 (namespace, normalized_value) 換一個 id 再寫 → 必須是 Conflict。
    // 這個衝突是 resolution 的訊號，不是可以忽略的雜訊（見 trait 說明）。
    let clashing = EntityIdentifier {
        id: Uuid::now_v7(),
        ..identifier.clone()
    };
    match store.put_entity_identifier(&clashing).await {
        Err(StorageError::Conflict { .. }) => {}
        other => {
            return Err(StorageError::Unknown {
                backend: "conformance",
                message: format!(
                    "(namespace, normalized_value) 應為 UNIQUE，重複寫入卻得到 {other:?}。\
                     少了這個唯一鍵，兩個 Entity 可以各自宣稱同一個識別碼，\
                     SPEC §6 的 exact identifier 就不再是合併依據"
                ),
            });
        }
    }
    // 同一個值換 namespace 必須寫得進去——namespace 存在的意義就是把
    // 「40 位 hex 是 SHA-1 還是 git commit」這種歧義分開（V0.1 報告 T10）。
    let other_namespace = EntityIdentifier {
        id: Uuid::now_v7(),
        namespace: format!("{namespace}-other"),
        ..identifier.clone()
    };
    store.put_entity_identifier(&other_namespace).await?;

    // 反查既有 owner：UNIQUE (namespace, normalized_value) 保證最多一筆。
    // 兩個欄位都要比到——只比其中一個會讓 resolver 把別人的識別碼當成自己的。
    let owner = store
        .find_entity_identifier_owner(&namespace, &normalized_value)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "find_entity_identifier_owner 查不到剛寫入的識別碼".into(),
        })?;
    assert_eq_debug("find_entity_identifier_owner", &identifier, &owner);
    if store
        .find_entity_identifier_owner(&namespace, &format!("absent-{run}"))
        .await?
        .is_some()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_identifier_owner 對不存在的 normalized_value 回了資料".into(),
        });
    }
    if store
        .find_entity_identifier_owner(&format!("{namespace}-absent"), &normalized_value)
        .await?
        .is_some()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_identifier_owner 只比對了 normalized_value、忽略了 namespace"
                .into(),
        });
    }

    // 不限 namespace：同一個 normalized_value 掛在兩個 namespace 下都要回。
    // 這是 account_handle 的查詢形狀，不是 exact_identifier 的精確 owner 查詢。
    let by_value = store
        .find_entity_identifiers_by_normalized_value(&normalized_value, 100)
        .await?;
    if !by_value.iter().any(|i| i.id == identifier.id) {
        return Err(StorageError::NotFound {
            message: "find_entity_identifiers_by_normalized_value 查不到剛寫入的識別碼".into(),
        });
    }
    if !by_value.iter().any(|i| i.id == other_namespace.id) {
        return Err(StorageError::NotFound {
            message:
                "find_entity_identifiers_by_normalized_value 必須回所有 namespace 底下同值的列，\
                 不能只回第一筆"
                    .into(),
        });
    }
    if !store
        .find_entity_identifiers_by_normalized_value(&format!("absent-{run}"), 100)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_identifiers_by_normalized_value 對不存在的值回了資料".into(),
        });
    }

    // --- §5 resolution candidate -----------------------------------------
    // 需要第二個 Entity。normalized_name 必須含 run-specific UUID，理由同上面
    // 的 entity fixture：(entity_type, normalized_name) 是 UNIQUE。
    let other_name = format!("conformance-entity-{run}");
    let other_entity = Entity {
        id: Uuid::now_v7(),
        entity_type: entity.entity_type,
        name: other_name.clone(),
        normalized_name: other_name.to_ascii_lowercase(),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    store.put_entity(&other_entity).await?;

    // 反向查詢 alias 文字：兩個不同 Entity 可以共用同一個別名（「Apple」可以是
    // 公司也可以是水果）。SPEC §6 的 alias 方法就是靠這條找出候選對。
    let shared_alias_text = format!("shared-alias-{run}");
    let alias_on_first = EntityAlias {
        id: Uuid::now_v7(),
        entity_id: entity.id,
        alias: shared_alias_text.clone(),
        alias_type: "shared".into(),
        source_id: Some(source_id),
        confidence: 0.6,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
    };
    let alias_on_second = EntityAlias {
        id: Uuid::now_v7(),
        entity_id: other_entity.id,
        alias: shared_alias_text.clone(),
        alias_type: "shared".into(),
        source_id: None,
        confidence: 0.55,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
    };
    store.put_entity_alias(&alias_on_first).await?;
    store.put_entity_alias(&alias_on_second).await?;
    let by_text = store
        .find_entity_aliases_by_text(&shared_alias_text, 100)
        .await?;
    if !by_text.iter().any(|a| a.id == alias_on_first.id)
        || !by_text.iter().any(|a| a.id == alias_on_second.id)
    {
        return Err(StorageError::NotFound {
            message: format!(
                "find_entity_aliases_by_text 應回兩個 Entity 的同名 alias，實際 {} 筆",
                by_text.len()
            ),
        });
    }
    if by_text.iter().any(|a| a.alias != shared_alias_text) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_aliases_by_text 回了 alias 文字不符的列".into(),
        });
    }
    if !store
        .find_entity_aliases_by_text(&format!("absent-alias-{run}"), 100)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_entity_aliases_by_text 對不存在的文字回了資料".into(),
        });
    }

    let (a, b) = ResolutionCandidate::ordered_pair(entity.id, other_entity.id);
    let candidate = ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id: a,
        entity_b_id: b,
        score: 0.91,
        method: "exact_identifier".into(),
        evidence: json!({"namespace": namespace, "normalized_value": "example.com"}),
        status: ResolutionStatus::Pending,
        created_at: fixture_ts(),
        reviewed_at: None,
    };
    store.put_resolution_candidate(&candidate).await?;
    assert_eq_debug(
        "resolution_candidate",
        &candidate,
        &store
            .get_resolution_candidate(candidate.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 resolution_candidate 讀不到".into(),
            })?,
    );

    // CHECK constraint：反過來寫必須被擋下。擋不住的話同一對會有兩列。
    let reversed = ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id: b,
        entity_b_id: a,
        ..candidate.clone()
    };
    match store.put_resolution_candidate(&reversed).await {
        Err(StorageError::ConstraintViolation { .. }) => {}
        other => {
            return Err(StorageError::Unknown {
                backend: "conformance",
                message: format!(
                    "entity_a_id < entity_b_id 的 CHECK 應擋下反向的候選對，實際得到 {other:?}。\
                     候選對是無向的，(A,B) 與 (B,A) 存成兩列會讓 Review 出現重複項目"
                ),
            });
        }
    }

    // UNIQUE (entity_a_id, entity_b_id, method)：換 id 重寫同一組必須是 Conflict。
    // 少了它，每跑一次 resolver 就長一批新列。
    let duplicate_method = ResolutionCandidate {
        id: Uuid::now_v7(),
        ..candidate.clone()
    };
    match store.put_resolution_candidate(&duplicate_method).await {
        Err(StorageError::Conflict { .. }) => {}
        other => {
            return Err(StorageError::Unknown {
                backend: "conformance",
                message: format!(
                    "(entity_a_id, entity_b_id, method) 應為 UNIQUE，實際得到 {other:?}"
                ),
            });
        }
    }
    // 換一個 method 則是不同的候選，必須寫得進去。
    let other_method = ResolutionCandidate {
        id: Uuid::now_v7(),
        method: "normalized_name".into(),
        ..candidate.clone()
    };
    store.put_resolution_candidate(&other_method).await?;

    // cursor + status 過濾。
    let page = store
        .list_resolution_candidates(None, Some(next_uuid(other_method.id)), 10)
        .await?;
    assert_cursor_page(
        "list_resolution_candidates",
        other_method.id,
        page.iter().map(|i| i.id),
    )?;
    let pending = store
        .list_resolution_candidates(
            Some(ResolutionStatus::Pending),
            Some(next_uuid(other_method.id)),
            10,
        )
        .await?;
    assert_cursor_page(
        "list_resolution_candidates(pending)",
        other_method.id,
        pending.iter().map(|i| i.id),
    )?;
    if pending
        .iter()
        .any(|c| c.status != ResolutionStatus::Pending)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_resolution_candidates 回了其他狀態的候選".into(),
        });
    }
    // 反向斷言：少了它，一個完全忽略 status 參數的實作也會通過上面那一條。
    if store
        .list_resolution_candidates(
            Some(ResolutionStatus::Rejected),
            Some(next_uuid(other_method.id)),
            10,
        )
        .await?
        .iter()
        .any(|c| c.id == other_method.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message:
                "list_resolution_candidates(Rejected) 回了 pending 的候選——status 參數沒有生效"
                    .into(),
        });
    }
    if store.list_resolution_candidates(None, None, 0).await?.len() != 1 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_resolution_candidates(limit=0) 應夾成 1 筆，limit 沒被夾住等於無界查詢"
                .into(),
        });
    }

    // 依 entity 反查：entity_a 或 entity_b 命中都算，不相關的那筆不能出現，
    // status 過濾要在 SQL 裡生效。
    let partner_small = Entity {
        id: Uuid::now_v7(),
        entity_type: entity.entity_type,
        name: format!("by-entity-small-{run}"),
        normalized_name: format!("by-entity-small-{run}"),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    let target = Entity {
        id: Uuid::now_v7(),
        entity_type: entity.entity_type,
        name: format!("by-entity-target-{run}"),
        normalized_name: format!("by-entity-target-{run}"),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    let partner_large = Entity {
        id: Uuid::now_v7(),
        entity_type: entity.entity_type,
        name: format!("by-entity-large-{run}"),
        normalized_name: format!("by-entity-large-{run}"),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    let unrelated_a = Entity {
        id: Uuid::now_v7(),
        entity_type: entity.entity_type,
        name: format!("by-entity-unrelated-a-{run}"),
        normalized_name: format!("by-entity-unrelated-a-{run}"),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    let unrelated_b = Entity {
        id: Uuid::now_v7(),
        entity_type: entity.entity_type,
        name: format!("by-entity-unrelated-b-{run}"),
        normalized_name: format!("by-entity-unrelated-b-{run}"),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    };
    store.put_entity(&partner_small).await?;
    store.put_entity(&target).await?;
    store.put_entity(&partner_large).await?;
    store.put_entity(&unrelated_a).await?;
    store.put_entity(&unrelated_b).await?;

    let hit_as_a = ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id: target.id,
        entity_b_id: partner_large.id,
        score: 0.4,
        method: "normalized_name".into(),
        evidence: json!({"side": "a"}),
        status: ResolutionStatus::Pending,
        created_at: fixture_ts(),
        reviewed_at: None,
    };
    let hit_as_b = ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id: partner_small.id,
        entity_b_id: target.id,
        score: 0.5,
        method: "alias".into(),
        evidence: json!({"side": "b"}),
        status: ResolutionStatus::Rejected,
        created_at: fixture_ts(),
        reviewed_at: None,
    };
    let unrelated = ResolutionCandidate {
        id: Uuid::now_v7(),
        entity_a_id: unrelated_a.id,
        entity_b_id: unrelated_b.id,
        score: 0.9,
        method: "exact_identifier".into(),
        evidence: json!({"side": "unrelated"}),
        status: ResolutionStatus::Pending,
        created_at: fixture_ts(),
        reviewed_at: None,
    };
    store.put_resolution_candidate(&hit_as_a).await?;
    store.put_resolution_candidate(&hit_as_b).await?;
    store.put_resolution_candidate(&unrelated).await?;

    let by_entity = store
        .list_resolution_candidates_by_entity(target.id, None, None, 100)
        .await?;
    if !by_entity.iter().any(|c| c.id == hit_as_a.id) {
        return Err(StorageError::NotFound {
            message: "list_resolution_candidates_by_entity 從 entity_a_id 這一端查不到候選".into(),
        });
    }
    if !by_entity.iter().any(|c| c.id == hit_as_b.id) {
        return Err(StorageError::NotFound {
            message: "list_resolution_candidates_by_entity 從 entity_b_id 這一端查不到候選".into(),
        });
    }
    if by_entity.iter().any(|c| c.id == unrelated.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_resolution_candidates_by_entity 回了完全不相關的候選".into(),
        });
    }
    let pending_only = store
        .list_resolution_candidates_by_entity(target.id, Some(ResolutionStatus::Pending), None, 100)
        .await?;
    if !pending_only.iter().any(|c| c.id == hit_as_a.id) {
        return Err(StorageError::NotFound {
            message:
                "list_resolution_candidates_by_entity(Pending) 應回 entity_a 那筆 pending 候選"
                    .into(),
        });
    }
    if pending_only.iter().any(|c| c.id == hit_as_b.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message:
                "list_resolution_candidates_by_entity(Pending) 回了 Rejected 的候選——status 參數沒有生效"
                    .into(),
        });
    }

    // ADR-012 Step 0：`update_resolution_candidate_status` 必須真的改到列，
    // 不存在的 id 回 false（比照 mark_replayed）。這支方法現在還沒有生產呼叫端，
    // 漏掉的話下一棒自動核准會寫進去、讀出來卻永遠是 Pending，而且不會報錯。
    let reviewed_at = fixture_ts() + chrono::Duration::seconds(30);
    if !store
        .update_resolution_candidate_status(
            hit_as_a.id,
            ResolutionStatus::AutoConfirmed,
            reviewed_at,
        )
        .await?
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "update_resolution_candidate_status 對既有列應回 true".into(),
        });
    }
    let after_status = store
        .get_resolution_candidate(hit_as_a.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛更新 status 的 resolution_candidate 讀不到".into(),
        })?;
    if after_status.status != ResolutionStatus::AutoConfirmed
        || after_status.reviewed_at != Some(reviewed_at)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "update_resolution_candidate_status 之後 status 應為 auto_confirmed、\
                 reviewed_at 應為呼叫端傳入的時間，實際是 status={:?} reviewed_at={:?}",
                after_status.status, after_status.reviewed_at
            ),
        });
    }
    if store
        .update_resolution_candidate_status(Uuid::now_v7(), ResolutionStatus::Rejected, reviewed_at)
        .await?
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "update_resolution_candidate_status 對不存在的 id 應回 false".into(),
        });
    }

    // --- §7 merge history -------------------------------------------------
    let history = MergeHistory {
        id: Uuid::now_v7(),
        survivor_id: entity.id,
        merged_id: other_entity.id,
        reason: format!("conformance merge {run}"),
        operator: "conformance@example.invalid".into(),
        timestamp: fixture_ts(),
        repointed_references: vec![
            RepointedReference {
                table: "relationships".into(),
                row_id: Uuid::now_v7(),
                column: "target_object_id".into(),
                previous_value: other_entity.id,
            },
            RepointedReference {
                table: "entity_extractions".into(),
                row_id: Uuid::now_v7(),
                column: "entity_id".into(),
                previous_value: other_entity.id,
            },
        ],
        merged_relationships: Vec::new(),
        undone_at: None,
        auto_approval_audit: None,
    };
    store.put_merge_history(&history).await?;
    // repointed_references 是 undo 的全部依據：少一筆或順序錯掉都會少還原一個參照。
    assert_eq_debug(
        "merge_history",
        &history,
        &store
            .get_merge_history(history.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 merge_history 讀不到".into(),
            })?,
    );

    // 兩端都要查得到：「這個 canonical 吃掉了誰」與「這個 id 被併去哪了」。
    for (label, entity_id) in [
        ("survivor_id", history.survivor_id),
        ("merged_id", history.merged_id),
    ] {
        let rows = store.list_merge_history_by_entity(entity_id, 100).await?;
        if !rows.iter().any(|h| h.id == history.id) {
            return Err(StorageError::NotFound {
                message: format!(
                    "list_merge_history_by_entity 從 {label} 這一端查不到剛寫入的 merge。\
                     只支援單邊等於「舊 entity id 被併去哪了」查不到"
                ),
            });
        }
    }

    // Acceptance C：undo 之後這一列還在，只是多了 undone_at，
    // repointed_references 必須原封不動——否則歷史 evidence 就遺失了。
    let undone = MergeHistory {
        undone_at: Some(fixture_ts()),
        ..history.clone()
    };
    store.put_merge_history(&undone).await?;
    let after_undo =
        store
            .get_merge_history(history.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "標記 undone 之後 merge_history 讀不到了——歷史不可以消失".into(),
            })?;
    assert_eq_debug("merge_history_undone", &undone, &after_undo);
    if !store
        .list_merge_history_by_entity(history.survivor_id, 100)
        .await?
        .iter()
        .any(|h| h.id == history.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "已撤銷的 merge 不該從 list_merge_history_by_entity 消失（Acceptance C）"
                .into(),
        });
    }

    if !store
        .list_merge_history_by_entity(Uuid::now_v7(), 100)
        .await?
        .is_empty()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_merge_history_by_entity 對不存在的 entity_id 回了資料".into(),
        });
    }

    // Phase 1e：merged_into 與 merged_relationships 必須原樣讀回來。
    // 不驗證 merge 業務邏輯——只證明欄位能寫進去讀出來。漏掉的話下一棒
    // 的 merge 實作會把標記寫進去、讀出來卻永遠是 None／空陣列，而且不會報錯。
    let marked = Entity {
        merged_into: Some(entity.id),
        ..other_entity.clone()
    };
    store.put_entity(&marked).await?;
    assert_eq_debug(
        "entity_merged_into",
        &marked,
        &store
            .get_entity(marked.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入 merged_into 的 Entity 讀不到".into(),
            })?,
    );

    let absorbed = Relationship {
        id: Uuid::now_v7(),
        source_object_id: other_entity.id,
        relationship_type: RelationshipType::Affects,
        target_object_id: Uuid::now_v7(),
        confidence: 0.6,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        evidence_count: 1,
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
    };
    let with_merged_rels = MergeHistory {
        merged_relationships: vec![MergedRelationship {
            absorbed_relationship_id: absorbed.id,
            absorber_relationship_id: Some(Uuid::now_v7()),
            absorbed_snapshot: absorbed,
            absorber_pre_merge: Some(AbsorberSnapshot {
                evidence_count: 3,
                confidence: 0.9,
                first_seen: fixture_ts(),
                last_seen: fixture_ts(),
            }),
            moved_evidence_ids: vec![Uuid::now_v7()],
        }],
        ..undone.clone()
    };
    store.put_merge_history(&with_merged_rels).await?;
    assert_eq_debug(
        "merge_history_merged_relationships",
        &with_merged_rels,
        &store
            .get_merge_history(history.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入 merged_relationships 的 merge_history 讀不到".into(),
            })?,
    );

    // ADR-012 Step 0：`auto_approval_audit` 必須能寫進去讀出來。
    // 漏掉的話下一棒自動核准會把稽核 JSON 寫進去、讀出來卻永遠是 None，
    // 而且不會報錯——出事時分不出哪些合併沒有人類看過。
    let with_audit = MergeHistory {
        auto_approval_audit: Some(json!({
            "decision_path": "score",
            "score": 0.95,
            "method": "exact_identifier",
        })),
        ..with_merged_rels.clone()
    };
    store.put_merge_history(&with_audit).await?;
    assert_eq_debug(
        "merge_history_auto_approval_audit",
        &with_audit,
        &store
            .get_merge_history(history.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入 auto_approval_audit 的 merge_history 讀不到".into(),
            })?,
    );

    Ok(())
}

/// V0.2 Phase 3 Step 1：Embedding metadata。
///
/// 驗四件事：put→find 命中、找不到回 None、UNIQUE 撞號回 Conflict、
/// list_embeddings_by_target 多筆依 id 升序。
async fn assert_embedding_queries<S: RelationalStore>(
    store: &S,
    target_id: Uuid,
) -> Result<(), StorageError> {
    let minilm = "huggingface/sentence-transformers/all-MiniLM-L6-v2";
    let e5 = "intfloat/multilingual-e5-small-int8";
    let hash_a = "a".repeat(64);
    let hash_b = "b".repeat(64);

    // id 先產生再排序：UUID v7 同一毫秒內後 74 bit 是隨機的，
    // 不能假設「後寫的一定比較大」。主鍵仍用 now_v7，避免共用 Postgres
    // 重跑 conformance 時撞到上一輪留下的固定 id。
    let mut ids = [Uuid::now_v7(), Uuid::now_v7()];
    ids.sort();
    let first_id = ids[0];
    let second_id = ids[1];
    let first = Embedding {
        id: first_id,
        target_id,
        target_type: EmbeddingTarget::DocumentBody,
        model: minilm.into(),
        model_version: "c".repeat(64),
        dimensions: 384,
        content_hash: hash_a.clone(),
        created_at: fixture_ts(),
    };
    store.put_embedding(&first).await?;
    let found = store
        .find_embedding(target_id, EmbeddingTarget::DocumentBody, minilm, &hash_a)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "find_embedding 查不到剛寫入的 Embedding。\
                 embedding-worker 靠這個方法判斷要不要重算，查不到就會每來一份文件都重跑推論"
                .into(),
        })?;
    assert_eq_debug("find_embedding", &first, &found);

    if store
        .find_embedding(target_id, EmbeddingTarget::DocumentBody, minilm, &hash_b)
        .await?
        .is_some()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_embedding 對不存在的 content_hash 應回 None".into(),
        });
    }
    if store
        .find_embedding(
            Uuid::now_v7(),
            EmbeddingTarget::DocumentBody,
            minilm,
            &hash_a,
        )
        .await?
        .is_some()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "find_embedding 對不存在的 target_id 應回 None".into(),
        });
    }

    let clash = Embedding {
        id: Uuid::now_v7(),
        ..first.clone()
    };
    match store.put_embedding(&clash).await {
        Err(StorageError::Conflict { .. }) => {}
        other => {
            return Err(StorageError::Unknown {
                backend: "conformance",
                message: format!(
                    "(target_id, target_type, model, content_hash) 應為 UNIQUE，重複寫入卻得到 {other:?}。\
                     put_embedding 不是 upsert——同一個 key 出現第二次代表呼叫端沒先 find"
                ),
            });
        }
    }

    // 第二筆用不同模型，同一目標應能並存（英文 MiniLM 與多語 e5 各留一筆）。
    let second = Embedding {
        id: second_id,
        target_id,
        target_type: EmbeddingTarget::DocumentBody,
        model: e5.into(),
        model_version: "d".repeat(64),
        dimensions: 384,
        content_hash: hash_a,
        created_at: fixture_ts(),
    };
    store.put_embedding(&second).await?;

    let listed = store
        .list_embeddings_by_target(target_id, EmbeddingTarget::DocumentBody, 10)
        .await?;
    let ours: Vec<_> = listed
        .iter()
        .filter(|e| e.id == first.id || e.id == second.id)
        .collect();
    if ours.len() != 2 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "list_embeddings_by_target 應回剛寫的 2 筆（不同模型），實際 {} 筆",
                ours.len()
            ),
        });
    }
    if ours[0].id >= ours[1].id {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "list_embeddings_by_target 應依 id 升序，實際 {} 然後 {}",
                ours[0].id, ours[1].id
            ),
        });
    }
    if listed
        .iter()
        .any(|e| e.target_id != target_id || e.target_type != EmbeddingTarget::DocumentBody)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_embeddings_by_target 回了不屬於該目標的列".into(),
        });
    }

    let other_type = store
        .list_embeddings_by_target(target_id, EmbeddingTarget::DocumentTitle, 10)
        .await?;
    if other_type
        .iter()
        .any(|e| e.id == first.id || e.id == second.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_embeddings_by_target 不該把 DocumentBody 的列算進 DocumentTitle".into(),
        });
    }
    Ok(())
}

/// V0.3 Phase 0：Discovery Foundation 的四張新表（seed／candidate／candidate_evidence／
/// ai_run）。
///
/// 驗證的不是「每張表都能 put/get」這種至少在機器上能跑的冒煙測，而是逐條對齊
/// SPEC_V0.3 的語意——Angular/Console 與 Discovery Engine 都靠這些方法回答產品問題，
/// 少了哪一條都不會報錯，只會在整合時靜默得到空清單：
///
/// 1. `put_seed`／`get_seed` 把 `collection_id`／`entity_id` 都填 `Some` 完整往返——空 Option 的欄位最容易被 adapter 靜默丟掉，兩個都填才能抓到。
/// 2. `list_seeds` 的 `status` 過濾在 SQL 端生效——取回一頁再在程式端 filter，「沒有 pending seed」與「最新一頁沒有 pending seed」會變成同一個答案。
/// 3. `list_seeds_by_collection` 只回該 collection 的 seed——range 是 `GET /collections/{id}/discovery` 的邊界，越界資料等於洩漏。
/// 4. `update_seed_status` 真的改到列、不存在的 id 回 `false`——回傳值驅動呼叫端「到底改到沒」的判斷。
/// 5. `put_candidate`／`get_candidate` 往返。
/// 6. `list_candidates` 的 `status` 過濾生效（同 #2，換成封閉列舉 `CandidateStatus`）。
/// 7. `list_candidates_by_collection` 只回該 collection 的 candidate（同 #3）。
/// 8. `update_candidate_status` 真的改到列且 `reviewed_at` 寫入、不存在的 id 回 `false`。
/// 9. `put_candidate_evidence`／`get_candidate_evidence` 往返——`object_id` 填任意 `Uuid`（無 FK）、`entity_id` 填 `Some(entity.id)`、其餘參照留 `None`，證明 Option 欄位能空能填。
/// 10. `list_candidate_evidence_by_candidate` 只回該 candidate 的證據、且**非空**——Acceptance C「能回答 Why was this discovered?」在這裡驗證資料真的讀得回來。
/// 11. `put_ai_run`／`get_ai_run` 往返——`input_reference`／`output` 用非空 JSON，確保不是靜默存成 `{}`。
/// 12. `list_ai_runs` 的 `task_type` 過濾生效（同 #2）。
async fn assert_v0_3_discovery_queries<S: RelationalStore>(
    store: &S,
    collection: &Collection,
    entity: &Entity,
) -> Result<(), StorageError> {
    let run = Uuid::now_v7();

    // --- Seed ---------------------------------------------------------------
    let seed = Seed {
        id: Uuid::now_v7(),
        collection_id: Some(collection.id),
        seed_type: SeedType::Account,
        value: format!("conformance-seed-{run}"),
        entity_id: Some(entity.id),
        priority: 5,
        confidence: 0.9,
        origin: SeedOrigin::Manual,
        status: "pending".into(),
        depth: 1,
        created_at: fixture_ts(),
    };
    store.put_seed(&seed).await?;
    assert_eq_debug(
        "seed",
        &seed,
        &store
            .get_seed(seed.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 seed 讀不到".into(),
            })?,
    );
    // status 過濾必須在 SQL 端生效：pending 該看到、不相干的 status 不該看到。
    let pending = store.list_seeds(Some("pending"), None, 100).await?;
    if !pending.iter().any(|s| s.id == seed.id) {
        return Err(StorageError::NotFound {
            message: "list_seeds 用 status=pending 過濾不到剛寫入的 seed".into(),
        });
    }
    let confirmed = store.list_seeds(Some("confirmed"), None, 100).await?;
    if confirmed.iter().any(|s| s.id == seed.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_seeds 用不相干的 status 過濾卻回傳了這筆 seed".into(),
        });
    }
    // 一筆屬於這個 collection、一筆 `collection_id: None`——by_collection 只該看到前者。
    let orphan_seed = Seed {
        id: Uuid::now_v7(),
        collection_id: None,
        seed_type: SeedType::Keyword,
        value: format!("conformance-orphan-seed-{run}"),
        entity_id: None,
        priority: 1,
        confidence: 0.5,
        origin: SeedOrigin::Connector,
        status: "pending".into(),
        depth: 0,
        created_at: fixture_ts(),
    };
    store.put_seed(&orphan_seed).await?;
    let by_collection = store
        .list_seeds_by_collection(collection.id, None, None, 100)
        .await?;
    if !by_collection.iter().any(|s| s.id == seed.id) {
        return Err(StorageError::NotFound {
            message: "list_seeds_by_collection 查不到屬於該 collection 的 seed".into(),
        });
    }
    if by_collection.iter().any(|s| s.id == orphan_seed.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_seeds_by_collection 把 collection_id=None 的 seed 也算了進來".into(),
        });
    }
    if !store.update_seed_status(seed.id, "confirmed").await? {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "update_seed_status 對剛寫入的 seed 回 false".into(),
        });
    }
    let refreshed = store.get_seed(seed.id).await?.expect("seed");
    if refreshed.status != "confirmed" {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "update_seed_status 之後 status 應為 confirmed，實際是 {}",
                refreshed.status
            ),
        });
    }
    if store.update_seed_status(Uuid::now_v7(), "x").await? {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "update_seed_status 對不存在的 id 回 true".into(),
        });
    }

    // --- Candidate ------------------------------------------------------------
    let candidate = Candidate {
        id: Uuid::now_v7(),
        candidate_type: CandidateType::Account,
        value: format!("conformance-candidate-{run}"),
        normalized_value: format!("conformance-candidate-{run}"),
        collection_id: Some(collection.id),
        discovered_by: format!("seed:{}", seed.id),
        discovery_method: "account_expansion".into(),
        confidence: 0.85,
        score: 0.9,
        status: CandidateStatus::Pending,
        depth: 1,
        created_at: fixture_ts(),
        reviewed_at: None,
    };
    store.put_candidate(&candidate).await?;
    assert_eq_debug(
        "candidate",
        &candidate,
        &store
            .get_candidate(candidate.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 candidate 讀不到".into(),
            })?,
    );
    let pending_candidates = store
        .list_candidates(Some(CandidateStatus::Pending), None, 100)
        .await?;
    if !pending_candidates.iter().any(|c| c.id == candidate.id) {
        return Err(StorageError::NotFound {
            message: "list_candidates 過濾不到剛寫入的 pending candidate".into(),
        });
    }
    let approved_candidates = store
        .list_candidates(Some(CandidateStatus::Approved), None, 100)
        .await?;
    if approved_candidates.iter().any(|c| c.id == candidate.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_candidates 用不相干的 status 過濾卻回傳了這筆 candidate".into(),
        });
    }
    let orphan_candidate = Candidate {
        id: Uuid::now_v7(),
        collection_id: None,
        ..candidate.clone()
    };
    store.put_candidate(&orphan_candidate).await?;
    let by_collection_candidates = store
        .list_candidates_by_collection(collection.id, None, None, 100)
        .await?;
    if !by_collection_candidates
        .iter()
        .any(|c| c.id == candidate.id)
    {
        return Err(StorageError::NotFound {
            message: "list_candidates_by_collection 查不到屬於該 collection 的 candidate".into(),
        });
    }
    if by_collection_candidates
        .iter()
        .any(|c| c.id == orphan_candidate.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_candidates_by_collection 把 collection_id=None 的 candidate 算了進來"
                .into(),
        });
    }
    if !store
        .update_candidate_status(candidate.id, CandidateStatus::Approved, fixture_ts())
        .await?
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "update_candidate_status 對剛寫入的 candidate 回 false".into(),
        });
    }
    let refreshed_candidate = store.get_candidate(candidate.id).await?.expect("candidate");
    if refreshed_candidate.status != CandidateStatus::Approved
        || refreshed_candidate.reviewed_at.is_none()
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "update_candidate_status 之後 status 應為 approved 且 reviewed_at 有值，\
                 實際 status={:?} reviewed_at={:?}",
                refreshed_candidate.status, refreshed_candidate.reviewed_at
            ),
        });
    }
    if store
        .update_candidate_status(Uuid::now_v7(), CandidateStatus::Rejected, fixture_ts())
        .await?
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "update_candidate_status 對不存在的 id 回 true".into(),
        });
    }

    // --- CandidateEvidence（先 put_candidate 成功後才能引用它的 id，順序錯會撞
    // postgres 的外鍵；sqlite 不強制外鍵，但兩邊都照正確順序寫）------------------
    let evidence = CandidateEvidence {
        id: Uuid::now_v7(),
        candidate_id: candidate.id,
        object_id: Some(Uuid::now_v7()),
        entity_id: Some(entity.id),
        relationship_id: None,
        raw_evidence_id: None,
        reason: format!("discovered via seed {run}"),
        weight: 0.8,
        created_at: fixture_ts(),
    };
    store.put_candidate_evidence(&evidence).await?;
    assert_eq_debug(
        "candidate_evidence",
        &evidence,
        &store
            .get_candidate_evidence(evidence.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 candidate_evidence 讀不到".into(),
            })?,
    );
    let by_candidate = store
        .list_candidate_evidence_by_candidate(candidate.id, 100)
        .await?;
    if by_candidate.is_empty() {
        return Err(StorageError::NotFound {
            message: "list_candidate_evidence_by_candidate 對剛寫入證據的 candidate 回空頁".into(),
        });
    }
    if !by_candidate.iter().any(|e| e.id == evidence.id) {
        return Err(StorageError::NotFound {
            message: "list_candidate_evidence_by_candidate 查不到剛寫入的證據".into(),
        });
    }
    if by_candidate.iter().any(|e| e.candidate_id != candidate.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_candidate_evidence_by_candidate 回了不屬於該 candidate 的證據".into(),
        });
    }

    // --- AiRun ---------------------------------------------------------------
    let input_reference = json!({ "seed_id": seed.id.to_string() });
    let output = json!({ "candidate_id": candidate.id.to_string(), "score": 0.87 });
    let ai_run = AiRun {
        id: Uuid::now_v7(),
        task_type: "candidate_scoring".into(),
        provider: "llamacpp".into(),
        model: "Qwen3.8-27B-UD-Q4_K_XL".into(),
        model_version: "1".into(),
        prompt_version: "v1".into(),
        input_reference: input_reference.clone(),
        output: output.clone(),
        confidence: 0.7,
        tokens: 120,
        estimated_cost: 0.0012,
        duration_ms: 850,
        created_at: fixture_ts(),
    };
    store.put_ai_run(&ai_run).await?;
    let got_run = store
        .get_ai_run(ai_run.id)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "剛寫入的 ai_run 讀不到".into(),
        })?;
    // 非空 JSON 才防得住 adapter 靜默把 value 存成 `{}` 後文字對不上。
    assert_eq_debug("ai_run", &ai_run, &got_run);
    let scoring_runs = store
        .list_ai_runs(Some("candidate_scoring"), None, 100)
        .await?;
    if !scoring_runs.iter().any(|r| r.id == ai_run.id) {
        return Err(StorageError::NotFound {
            message: "list_ai_runs 用 task_type 過濾不到剛寫入的 run".into(),
        });
    }
    let summary_runs = store.list_ai_runs(Some("summarization"), None, 100).await?;
    if summary_runs.iter().any(|r| r.id == ai_run.id) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "list_ai_runs 用不相干的 task_type 過濾卻回傳了這筆 run".into(),
        });
    }

    Ok(())
}

/// V0.3 Phase 2：Discovery Budget（SPEC_V0.3 §10／§11）。
///
/// 最關鍵的一條是 #3——併發正確性：這是整個 Phase 2 存在的理由（防止
/// runaway crawling／runaway AI cost），如果併發下會超額或漏算，這個機制
/// 就完全沒有達到目的，而且不會報錯，只會在事後對帳時才發現。
///
/// 1. `put_collection_budget`／`get_collection_budget` 完整往返。
/// 2. `get_daily_usage` 對「從沒用過的一天」回全零，不是 `NotFound`。
/// 3. **併發消耗不超額也不漏算**：budget=5，8 個平行呼叫各消耗 1，
///    必須剛好 5 個成功、3 個拒絕，且 `get_daily_usage` 讀回的值剛好是 5——
///    多了代表超額放行，少了代表 lost update。
/// 4. 拒絕時 `used_after` 回目前的既有用量（不是 0、不是被拒的那個值）。
/// 5. `daily_request_budget` 與 `daily_ai_budget` 是獨立的兩個計數器，
///    消耗其中一個不影響另一個。
/// 6. 單次 `by` 就超過 `budget`（全新一天，目前用量是 0）必須直接拒絕——
///    這是兩段式 SQL 要防的那個邊界情況（見派工說明的「關鍵設計」一節），
///    不能因為是全新一天就無條件放行第一筆。
async fn assert_v0_3_budget_queries<S: RelationalStore>(
    store: &S,
    collection: &Collection,
) -> Result<(), StorageError> {
    // --- CollectionBudget 往返 ---------------------------------------------
    let budget_cfg = CollectionBudget {
        collection_id: collection.id,
        max_candidates_per_run: 200,
        max_requests_per_run: 500,
        max_ai_calls_per_run: 50,
        max_depth: 3,
        daily_request_budget: 5_000,
        daily_ai_budget: 500,
        created_at: fixture_ts(),
        updated_at: fixture_ts(),
    };
    store.put_collection_budget(&budget_cfg).await?;
    assert_eq_debug(
        "collection_budget",
        &budget_cfg,
        &store
            .get_collection_budget(collection.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "剛寫入的 collection_budget 讀不到".into(),
            })?,
    );

    // --- 從沒用過的一天回全零 -------------------------------------------------
    let fresh_date = NaiveDate::from_ymd_opt(2020, 1, 1).expect("合法日期");
    let never_used = store.get_daily_usage(collection.id, fresh_date).await?;
    if never_used.requests_used != 0 || never_used.ai_calls_used != 0 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!("從沒消耗過的一天 get_daily_usage 應該全零，實際 {never_used:?}"),
        });
    }

    // --- 併發消耗：budget=5，8 個平行呼叫各消耗 1 ------------------------------
    // `collection` 本身在 assert_relational_round_trip 裡每次呼叫都是新建的
    // `Uuid::now_v7()`，所以 (collection_id, concurrency_date) 這個自然鍵
    // 只要 collection_id 不同就不會跟其他測試撞到；這裡併發的日期是固定的
    // 也安全（每個 run 都用自己的 collection id）。斷言邏輯才是重點。
    let concurrency_date = NaiveDate::from_ymd_opt(2021, 1, 1).expect("合法日期");
    let daily_budget = 5i64;
    let (r0, r1, r2, r3, r4, r5, r6, r7) = tokio::join!(
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
        store.try_consume_daily_request_budget(collection.id, concurrency_date, 1, daily_budget),
    );
    let results = [r0, r1, r2, r3, r4, r5, r6, r7];
    let mut allowed_count = 0usize;
    let mut rejected_used_after: Option<i64> = None;
    for result in results {
        let consumption = result?;
        if consumption.allowed {
            allowed_count += 1;
        } else {
            rejected_used_after = Some(consumption.used_after);
        }
    }
    if allowed_count != 5 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "budget=5，8 個平行呼叫各消耗 1，應該剛好 5 個成功，實際 {allowed_count} 個\
                 ——代表 try_consume_daily_request_budget 的原子性有問題\
                 （要嘛超額放行，要嘛少算，不管哪一種都是嚴重 bug）"
            ),
        });
    }
    // 拒絕時 used_after 必須是「已經用滿的 5」，不是別的值。
    if rejected_used_after != Some(5) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "被拒絕的呼叫，used_after 應該回既有用量 5，實際 {rejected_used_after:?}"
            ),
        });
    }
    let usage_after = store
        .get_daily_usage(collection.id, concurrency_date)
        .await?;
    if usage_after.requests_used != 5 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "併發消耗後 get_daily_usage 應回 5，實際 {}——最終用量與允許次數不一致",
                usage_after.requests_used
            ),
        });
    }

    // --- daily_request_budget 與 daily_ai_budget 是獨立計數器 -------------------
    let ai_date = NaiveDate::from_ymd_opt(2022, 6, 15).expect("合法日期");
    let ai_result = store
        .try_consume_daily_ai_budget(collection.id, ai_date, 3, 10)
        .await?;
    if !ai_result.allowed || ai_result.used_after != 3 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "try_consume_daily_ai_budget 第一次消耗應該成功且回 3，實際 {ai_result:?}"
            ),
        });
    }
    let ai_usage = store.get_daily_usage(collection.id, ai_date).await?;
    if ai_usage.ai_calls_used != 3 || ai_usage.requests_used != 0 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "消耗 ai budget 不該影響 requests_used（應該是 0），實際 {ai_usage:?}"
            ),
        });
    }

    // --- 全新一天，單次 by 就超過 budget，必須直接拒絕（兩段式 SQL 要防的邊界）---
    let overflow_date = NaiveDate::from_ymd_opt(2023, 3, 3).expect("合法日期");
    let overflow_result = store
        .try_consume_daily_request_budget(collection.id, overflow_date, 10, 5)
        .await?;
    if overflow_result.allowed {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "全新一天單次 by=10 > budget=5，理論上必須被拒絕，實際卻放行了\
                       ——這正是兩段式 SQL 要防的邊界情況，代表退化回單一 ON CONFLICT \
                       DO UPDATE 的漏洞"
                .into(),
        });
    }
    if overflow_result.used_after != 0 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "被拒絕且是全新一天，used_after 應該是 0，實際 {}",
                overflow_result.used_after
            ),
        });
    }

    Ok(())
}

/// ADR-008 的 `failed_events`：自然鍵 upsert、`attempt_count` 累加、重放標記。
///
/// 這裡最關鍵的一條是**回傳列的 `id` 是既有列的 id，不是傳進去那個**。
/// 若 adapter 只回 `()`，呼叫端會拿著自己產生的 id 去查而永遠查不到，
/// 表面上看起來就只是「DLQ 裡沒有這筆」——正是 ADR-008 想避免的靜默損失。
async fn assert_v0_2_failed_events<S: RelationalStore>(store: &S) -> Result<(), StorageError> {
    let run = Uuid::now_v7();
    // topic 帶 run id：共用的 Postgres 上有其他測試的殘留列，
    // (topic, partition, offset) 撞到的話測的就不是自己寫的那一筆。
    let topic = format!("conformance.failed.{run}");
    let first = FailedEvent {
        id: Uuid::now_v7(),
        topic: topic.clone(),
        partition: 2,
        // 超過 i32 的 offset：欄位若是 32 位會在這裡溢位。
        offset: 4_294_967_400,
        consumer_group: "conformance-group".into(),
        failure_reason: "payload 缺少 document_id".into(),
        attempt_count: 1,
        envelope: json!({"id": run.to_string(), "event_type": "object.normalized"}),
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        replayed_at: None,
    };
    let stored = store.put_failed_event(&first).await?;
    assert_eq_debug("failed_event", &first, &stored);

    // 同一則事件再次失敗：換一個 id、換 last_seen、attempt_count 故意傳 1。
    let again = FailedEvent {
        id: Uuid::now_v7(),
        failure_reason: "第二次仍然缺少 document_id".into(),
        attempt_count: 1,
        first_seen: fixture_ts() + chrono::Duration::seconds(60),
        last_seen: fixture_ts() + chrono::Duration::seconds(60),
        ..first.clone()
    };
    let merged = store.put_failed_event(&again).await?;
    if merged.id != first.id {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "put_failed_event 應以 (topic, partition, offset) 為自然鍵沿用既有列，\
                 實際回了新 id {}（原本是 {}）",
                merged.id, first.id
            ),
        });
    }
    if merged.attempt_count != 2 {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "attempt_count 應由資料庫 +1（1 → 2），實際是 {}。\
                 用傳入值覆寫的話「試了幾次」永遠停在 1",
                merged.attempt_count
            ),
        });
    }
    if merged.first_seen != first.first_seen {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "first_seen 應保留既有列的值（第一次失敗的時間），不可被覆寫".into(),
        });
    }
    if merged.last_seen != again.last_seen || merged.failure_reason != again.failure_reason {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "last_seen／failure_reason 應以本次傳入值覆寫".into(),
        });
    }
    // 只有一列，不是兩列。
    let stored_row =
        store
            .get_failed_event(first.id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                message: "get_failed_event 讀不到既有列".into(),
            })?;
    assert_eq_debug("failed_event_merged", &merged, &stored_row);
    if store.get_failed_event(again.id).await?.is_some() {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "同一則事件不該產生第二列——DLQ 的筆數會失去意義".into(),
        });
    }

    let page = store
        .list_failed_events(Some(next_uuid(first.id)), 10)
        .await?;
    assert_cursor_page("list_failed_events", first.id, page.iter().map(|i| i.id))?;

    // 重放標記。
    let replayed_at = fixture_ts() + chrono::Duration::seconds(120);
    if !store.mark_replayed(first.id, replayed_at).await? {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "mark_replayed 對既有列應回 true".into(),
        });
    }
    let after = store
        .get_failed_event(first.id)
        .await?
        .expect("failed_event");
    if after.replayed_at != Some(replayed_at) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "mark_replayed 之後 replayed_at 應為呼叫端傳入的時間，實際是 {:?}",
                after.replayed_at
            ),
        });
    }
    // 已重放的列仍要列得出來——「上次那批補回去了沒」只能靠它回答。
    if !store
        .list_failed_events(Some(next_uuid(first.id)), 10)
        .await?
        .iter()
        .any(|e| e.id == first.id)
    {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "已重放的 failed event 不該從 list_failed_events 消失".into(),
        });
    }
    if store.mark_replayed(Uuid::now_v7(), replayed_at).await? {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: "mark_replayed 對不存在的 id 應回 false".into(),
        });
    }
    Ok(())
}

fn assert_only(ids: &[Uuid], expected: Uuid, label: &str) -> Result<(), StorageError> {
    if ids != [expected] {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!("{label} 應只回 [{expected}]，實際 {ids:?}"),
        });
    }
    Ok(())
}

/// UUID 的「下一個值」（big-endian +1）。
///
/// list 方法的 cursor 是 strictly-less-than，所以拿 `id + 1` 當 cursor 時，
/// 目標列必定是結果的第一筆——共用表裡有多少其他測試的資料都不影響。
/// 直接用 `list_*(None, 10)` 再找自己的 id 是不可靠的：其他 e2e 測試會同時
/// 往同一個 Postgres 寫入更新的 UUID v7，把這筆擠出第一頁。
fn next_uuid(id: Uuid) -> Uuid {
    let mut bytes = id.into_bytes();
    for byte in bytes.iter_mut().rev() {
        if *byte == 0xff {
            *byte = 0;
        } else {
            *byte += 1;
            break;
        }
    }
    Uuid::from_bytes(bytes)
}

/// 驗證一頁 cursor 結果：非空、第一筆是預期的 id、整體依 id 由大到小。
fn assert_cursor_page(
    label: &str,
    expected_first: Uuid,
    ids: impl Iterator<Item = Uuid>,
) -> Result<(), StorageError> {
    let ids: Vec<Uuid> = ids.collect();
    let Some(&first) = ids.first() else {
        return Err(StorageError::NotFound {
            message: format!("{label} 以 cursor=id+1 查詢卻回空頁，剛寫入的那筆不見了"),
        });
    };
    if first != expected_first {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!("{label} 第一筆應為 {expected_first}，實際是 {first}"),
        });
    }
    if ids.windows(2).any(|w| w[0] <= w[1]) {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!("{label} 必須依 id 嚴格遞減（最新在前），實際順序：{ids:?}"),
        });
    }
    Ok(())
}

/// `TransactionalStore` 的契約：commit／rollback／drop 回滾／部分失敗不留半套／隔離性。
///
/// 這五條合起來才是 V0.2 Entity Merge 能成立的前提——merge 橫跨五張表，
/// 只要其中一條不成立，「repoint 到一半」的狀態就會留在 canonical store 裡，
/// 而且沒有任何錯誤訊息會提到它。
///
/// ⚠️ 用的 fixture 都帶 run-specific UUID，不 TRUNCATE 共用表（同 `assert_relational_round_trip`）。
pub async fn assert_transactional_contract<S: TransactionalStore>(
    store: &S,
    backend: &'static str,
) -> Result<(), StorageError> {
    let fail = |message: String| StorageError::Unknown {
        backend: "conformance",
        message,
    };

    // --- 1. commit：三張表一起進去 --------------------------------------
    let committed = tx_entity("commit");
    let alias = tx_alias(committed.id);
    let identifier = tx_identifier(committed.id, "commit");

    let tx = store.begin().await?;
    {
        let db = tx.store();
        db.put_entity(&committed).await?;
        db.put_entity_alias(&alias).await?;
        db.put_entity_identifier(&identifier).await?;

        // --- 5. 隔離性：commit 之前，交易外讀不到 ---------------------
        // 這一條在交易還開著的時候檢查，所以必須在 commit 之前。
        // PostgreSQL 是 READ COMMITTED 預設，SQLite（WAL）讀的是交易開始前的快照，
        // 兩者對「未提交的資料不可見」的結論一致。
        if store.get_entity(committed.id).await?.is_some() {
            return Err(fail(format!(
                "{backend}：交易尚未 commit，交易外卻讀得到 entity。\
                 這代表寫入沒有真的進交易（executor 接錯），\
                 之後 rollback 也不會把它收回去"
            )));
        }
    }
    tx.commit().await?;

    if store.get_entity(committed.id).await?.is_none() {
        return Err(fail(format!("{backend}：commit 之後讀不到 entity")));
    }
    if store
        .list_entity_aliases_by_entity(committed.id, 100)
        .await?
        .iter()
        .all(|a| a.id != alias.id)
    {
        return Err(fail(format!("{backend}：commit 之後讀不到 entity_alias")));
    }
    if store.get_entity_identifier(identifier.id).await?.is_none() {
        return Err(fail(format!(
            "{backend}：commit 之後讀不到 entity_identifier"
        )));
    }

    // --- 2. rollback：兩張表一起不見 ------------------------------------
    let rolled_back = tx_entity("rollback");
    let rolled_back_alias = tx_alias(rolled_back.id);
    let tx = store.begin().await?;
    {
        let db = tx.store();
        db.put_entity(&rolled_back).await?;
        db.put_entity_alias(&rolled_back_alias).await?;
    }
    tx.rollback().await?;
    if store.get_entity(rolled_back.id).await?.is_some() {
        return Err(fail(format!("{backend}：rollback 之後 entity 還在")));
    }
    if store
        .get_entity_alias(rolled_back_alias.id)
        .await?
        .is_some()
    {
        return Err(fail(format!("{backend}：rollback 之後 entity_alias 還在")));
    }

    // --- 3. drop 不 commit：一樣要回滾 ----------------------------------
    //
    // **這條是防「忘了 commit 卻留下資料」與「連線沒歸還」兩件事。**
    // 只斷言資料不在是不夠的：交易還開著的話資料本來就看不到，兩種狀態從外面
    // 長得一模一樣。所以 drop 之後還要再寫一筆——寫得進去才證明那條連線真的
    // 被 ROLLBACK 後歸還了（SQLite 尤其明顯：交易沒結束的話寫入會卡到 busy_timeout）。
    let dropped = tx_entity("drop");
    let tx = store.begin().await?;
    tx.store().put_entity(&dropped).await?;
    drop(tx);
    if store.get_entity(dropped.id).await?.is_some() {
        return Err(fail(format!(
            "{backend}：交易被 drop 卻沒有 commit，entity 仍然存在。\
             sqlx 的 Transaction::drop 應該排一個 ROLLBACK"
        )));
    }
    let after_drop = tx_entity("after-drop");
    store.put_entity(&after_drop).await?;
    if store.get_entity(after_drop.id).await?.is_none() {
        return Err(fail(format!(
            "{backend}：drop 交易之後連線沒有回到可用狀態（後續寫入讀不到）"
        )));
    }

    // --- 4. 交易內部分失敗 → 不留半套 -----------------------------------
    let partial = tx_entity("partial");
    let first = tx_identifier(partial.id, "partial");
    // 同一個 (namespace, normalized_value) 換一個 id：UNIQUE 必須擋下來。
    let clashing = EntityIdentifier {
        id: Uuid::now_v7(),
        ..first.clone()
    };
    let tx = store.begin().await?;
    {
        let db = tx.store();
        db.put_entity(&partial).await?;
        db.put_entity_identifier(&first).await?;
        match db.put_entity_identifier(&clashing).await {
            Err(StorageError::Conflict { .. }) => {}
            other => {
                return Err(fail(format!(
                    "{backend}：交易內違反 (namespace, normalized_value) UNIQUE 應回 Conflict，\
                     實際 {other:?}"
                )));
            }
        }
    }
    // PostgreSQL 在錯誤之後整個交易進入 aborted 狀態，只能 rollback；
    // 這裡本來就要 rollback，不再對同一條交易下任何寫入。
    tx.rollback().await?;
    if store.get_entity(partial.id).await?.is_some() {
        return Err(fail(format!(
            "{backend}：交易內某一步失敗後回滾，先前寫入的 entity 卻留下來了。\
             這就是「合併到一半」的狀態，Entity Merge 不能接受"
        )));
    }
    if store.get_entity_identifier(first.id).await?.is_some() {
        return Err(fail(format!(
            "{backend}：交易內某一步失敗後回滾，先前寫入的 entity_identifier 卻留下來了"
        )));
    }

    Ok(())
}

/// 交易 conformance 用的 Entity fixture。`normalized_name` 帶 run-specific UUID，
/// 理由同 `assert_relational_round_trip`：`(entity_type, normalized_name)` 是 UNIQUE。
fn tx_entity(label: &str) -> Entity {
    let name = format!("conformance-tx-{label}-{}", Uuid::now_v7().simple());
    Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Vulnerability,
        name: name.clone(),
        normalized_name: name.to_ascii_lowercase(),
        description: None,
        confidence: 1.0,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
        merged_into: None,
        attributes: json!({}),
    }
}

fn tx_alias(entity_id: Uuid) -> EntityAlias {
    EntityAlias {
        id: Uuid::now_v7(),
        entity_id,
        alias: format!("conformance-tx-alias-{}", Uuid::now_v7().simple()),
        alias_type: "localized_name".into(),
        source_id: None,
        confidence: 0.75,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
    }
}

fn tx_identifier(entity_id: Uuid, label: &str) -> EntityIdentifier {
    let value = format!("tx-{label}-{}.example.invalid", Uuid::now_v7().simple());
    EntityIdentifier {
        id: Uuid::now_v7(),
        entity_id,
        namespace: format!("conformance-tx-ns-{}", Uuid::now_v7().simple()),
        value: value.to_ascii_uppercase(),
        normalized_value: value,
        confidence: 0.95,
        source_id: None,
        first_seen: fixture_ts(),
        last_seen: fixture_ts(),
    }
}

fn assert_eq_debug<T: PartialEq + std::fmt::Debug>(label: &str, expected: &T, actual: &T) {
    assert_eq!(
        expected, actual,
        "{label} round-trip 不符\nexpected: {expected:?}\nactual: {actual:?}"
    );
}

/// Canonical adapter 必須宣告 backend id，且 health 為 healthy。
pub async fn assert_canonical_health<S: CanonicalStore>(store: &S) -> Result<(), StorageError> {
    if store.canonical_backend_id() != "postgres" {
        return Err(StorageError::Unknown {
            backend: "conformance",
            message: format!(
                "V0.1 CanonicalStore 應為 postgres，實際是 {}",
                store.canonical_backend_id()
            ),
        });
    }
    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "postgres",
            message: health.message,
        });
    }
    Ok(())
}

/// Embedded adapter 必須能指出檔案路徑。
pub async fn assert_embedded_health<S: EmbeddedStore>(store: &S) -> Result<(), StorageError> {
    if store.database_path().as_os_str().is_empty() {
        return Err(StorageError::Configuration {
            message: "EmbeddedStore 的 database_path 是空的".into(),
        });
    }
    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "sqlite",
            message: health.message,
        });
    }
    Ok(())
}

/// SearchStore：index / bulk / query / delete。呼叫前必須已通過 identity check。
pub async fn assert_search_round_trip<S: SearchStore>(
    store: &S,
    index: &str,
) -> Result<(), StorageError> {
    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "opensearch",
            message: health.message,
        });
    }

    let id_a = Uuid::now_v7().to_string();
    let id_b = Uuid::now_v7().to_string();
    let token = format!("osint-core-conformance-{}", &id_a[..8]);
    store
        .index(SearchDocument {
            index: index.to_string(),
            id: id_a.clone(),
            body: json!({"title": token, "kind": "single"}),
        })
        .await?;
    let bulk = store
        .bulk_index(vec![SearchDocument {
            index: index.to_string(),
            id: id_b.clone(),
            body: json!({"title": token, "kind": "bulk"}),
        }])
        .await?;
    if bulk.errors > 0 {
        return Err(StorageError::Unknown {
            backend: "opensearch",
            message: format!("bulk_index 回報 {} 筆錯誤", bulk.errors),
        });
    }

    let mut hits = None;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let result = store
            .query(SearchQuery {
                index: index.to_string(),
                query_string: token.clone(),
                from: 0,
                size: 10,
            })
            .await?;
        if result.hits.iter().any(|h| h.id == id_a) && result.hits.iter().any(|h| h.id == id_b) {
            hits = Some(result);
            break;
        }
    }
    let hits = hits.ok_or_else(|| StorageError::NotFound {
        message: format!(
            "index `{index}` 寫入後 4 秒內查不到 token `{token}`。請確認 refresh 與 query_string 實作"
        ),
    })?;
    if hits.total < 2 {
        return Err(StorageError::Unknown {
            backend: "opensearch",
            message: format!("預期至少 2 筆，total={}", hits.total),
        });
    }

    if !store.delete(index, &id_a).await? {
        return Err(StorageError::Unknown {
            backend: "opensearch",
            message: "刪除剛寫入的文件應回 true".into(),
        });
    }
    Ok(())
}

/// `SearchStore::bulk_upsert_fields` 必須建立新文件、且**不清空呼叫端沒寫進 body 的欄位**。
///
/// 這是 indexer 與 embedding-worker 共用 `osint-documents` 的契約：indexer 重寫
/// 文件本體時，不能把 embedding-worker 事後疊加的向量欄位整份取代掉。
/// `bulk_index`（`"index"` action）會整份覆寫 `_source`，這支驗的就是改用
/// `"update"` + `doc_as_upsert` 之後那個行為真的成立——組錯 NDJSON 在單元測試
/// 就能抓到，但「合併語意」只能對真實後端證明。
///
/// 未來任何新 `SearchStore` adapter 都必須通過這一支。
pub async fn assert_bulk_upsert_preserves_unknown_fields<S: SearchStore>(
    store: &S,
    index: &str,
) -> Result<(), StorageError> {
    let fail = |message: String| StorageError::Unknown {
        backend: "search",
        message,
    };

    let created_id = Uuid::now_v7().to_string();
    let created = store
        .bulk_upsert_fields(vec![SearchDocument {
            index: index.to_string(),
            id: created_id.clone(),
            body: json!({"title": "fresh", "kind": "created"}),
        }])
        .await?;
    if created.errors > 0 || created.indexed != 1 {
        return Err(fail(format!(
            "文件不存在時 bulk_upsert_fields 應建立一份，實際 indexed={} errors={} failures={:?}",
            created.indexed, created.errors, created.failures
        )));
    }

    let overlay_id = Uuid::now_v7().to_string();
    store
        .index(SearchDocument {
            index: index.to_string(),
            id: overlay_id.clone(),
            body: json!({"title": "original", "kind": "keep"}),
        })
        .await?;
    store
        .update_fields(
            index,
            &overlay_id,
            json!({"overlay": "must-survive", "overlay_version": "v1"}),
        )
        .await?;

    let upserted = store
        .bulk_upsert_fields(vec![SearchDocument {
            index: index.to_string(),
            id: overlay_id.clone(),
            body: json!({"title": "rewritten", "kind": "keep"}),
        }])
        .await?;
    if upserted.errors > 0 || upserted.indexed != 1 {
        return Err(fail(format!(
            "對既有文件 bulk_upsert_fields 應成功，實際 indexed={} errors={} failures={:?}",
            upserted.indexed, upserted.errors, upserted.failures
        )));
    }

    let mut source = None;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let hits = store.query(SearchQuery::new(index, "kind:keep")).await?;
        if let Some(hit) = hits.hits.iter().find(|h| h.id == overlay_id) {
            source = Some(hit.source.clone());
            break;
        }
    }
    let source = source.ok_or_else(|| StorageError::NotFound {
        message: format!("bulk_upsert_fields 之後 4 秒內查不到 `{overlay_id}`"),
    })?;

    if source.get("title").and_then(Value::as_str) != Some("rewritten") {
        return Err(fail(format!(
            "indexer 自己的欄位應被更新，實際 title={}",
            source.get("title").unwrap_or(&Value::Null)
        )));
    }
    if source.get("overlay").and_then(Value::as_str) != Some("must-survive") {
        return Err(fail(format!(
            "呼叫端沒寫進 body 的 overlay 被清掉了。這就是 indexer 用 bulk_index \
             整份覆寫時會發生的事；bulk_upsert_fields 必須留下它。實際：{source}"
        )));
    }
    if source.get("overlay_version").and_then(Value::as_str) != Some("v1") {
        return Err(fail(format!("overlay_version 也被清掉了：{source}")));
    }

    let created_hits = store.query(SearchQuery::new(index, "kind:created")).await?;
    if !created_hits.hits.iter().any(|h| h.id == created_id) {
        return Err(fail(
            "文件不存在時 bulk_upsert_fields 宣稱建立成功，但查詢找不到那份文件".into(),
        ));
    }

    let _ = store.delete(index, &created_id).await;
    let _ = store.delete(index, &overlay_id).await;
    Ok(())
}

/// `SearchStore::search`（[`StructuredSearch`]）的後端契約。
///
/// **未來新增任何 SearchStore adapter 都必須通過這一支。** 這裡驗的四件事全部都是
/// 「做錯了不會報錯，只會讓搜尋悄悄變得不對」的那一類：
///
/// 1. 過濾條件真的過濾（不是被忽略）；
/// 2. `Not` 真的排除（只有 `must_not` 的 bool 在某些後端等於不篩選）；
/// 3. hit 帶得回 `sort`（沒有它就翻不了頁）；
/// 4. `search_after` 真的接續（不是每次都從頭回同一頁）。
///
/// # 呼叫端要先準備好 index
///
/// `index` 必須已經存在且宣告了三個欄位：
///
/// | 欄位 | 需要的能力 |
/// |---|---|
/// | `doc_id` | **可排序的精確值**（OpenSearch 是 `keyword`） |
/// | `title` | 全文檢索 |
/// | `kind` | 精確比對過濾 |
///
/// 刻意不由這裡建立：建 index 的語法是後端專屬的，寫進 conformance 就等於
/// 把 OpenSearch 的 mapping DSL 塞進後端中立的介面。
/// 靠 dynamic mapping 也不行——OpenSearch 會把 `doc_id` 猜成 `text`，
/// 排序會直接 400（text 欄位沒有 fielddata）。
pub async fn assert_structured_search<S: SearchStore>(
    store: &S,
    index: &str,
) -> Result<(), StorageError> {
    use crate::traits::{QueryExpr, SearchField, SearchFilter, SortField};

    let token = format!("osintconf{}", Uuid::now_v7().simple());
    let docs: Vec<SearchDocument> = (0..3)
        .map(|i| SearchDocument {
            index: index.to_string(),
            id: format!("{token}-{i}"),
            body: json!({
                "doc_id": format!("{token}-{i}"),
                "title": format!("{token} item {i}"),
                "kind": if i == 0 { "special" } else { "ordinary" },
            }),
        })
        .collect();
    let bulk = store.bulk_index(docs).await?;
    if bulk.errors > 0 {
        return Err(StorageError::Unknown {
            backend: "search",
            message: format!("conformance 資料寫入失敗：{:?}", bulk.failures),
        });
    }

    let base = |expr: Option<QueryExpr>, filters: Vec<SearchFilter>| StructuredSearch {
        index: index.to_string(),
        expression: expr,
        fields: vec![SearchField::new("title", 1.0)],
        filters,
        size: 10,
        search_after: None,
        sort: vec![SortField {
            field: "doc_id".into(),
            ascending: true,
        }],
        highlight_fields: vec!["title".into()],
    };

    // 1) 全文條件命中三筆。
    let all = retry_search(
        store,
        base(Some(QueryExpr::Term(token.clone())), Vec::new()),
        3,
    )
    .await?;
    if all.total != 3 {
        return Err(StorageError::Unknown {
            backend: "search",
            message: format!("預期 3 筆，實際 {}", all.total),
        });
    }

    // 2) term 過濾要真的過濾。
    let filtered = store
        .search(base(
            Some(QueryExpr::Term(token.clone())),
            vec![SearchFilter::Term {
                field: "kind".into(),
                value: "special".into(),
            }],
        ))
        .await?;
    if filtered.total != 1 {
        return Err(StorageError::Unknown {
            backend: "search",
            message: format!(
                "term 過濾後預期 1 筆，實際 {}——過濾條件被忽略了",
                filtered.total
            ),
        });
    }

    // 3) Not 要真的排除。
    let negated = store
        .search(base(
            Some(QueryExpr::And(vec![
                QueryExpr::Term(token.clone()),
                QueryExpr::Not(Box::new(QueryExpr::Term("0".into()))),
            ])),
            Vec::new(),
        ))
        .await?;
    if negated.total != 2 {
        return Err(StorageError::Unknown {
            backend: "search",
            message: format!(
                "NOT 之後預期 2 筆，實際 {}——排除條件沒有生效",
                negated.total
            ),
        });
    }

    // 4) sort 值與 search_after 接續。
    let mut first = base(Some(QueryExpr::Term(token.clone())), Vec::new());
    first.size = 2;
    let page1 = store.search(first.clone()).await?;
    let cursor = page1
        .hits
        .last()
        .map(|hit| hit.sort.clone())
        .filter(|sort| !sort.is_empty())
        .ok_or_else(|| StorageError::Unknown {
            backend: "search",
            message: "hit 沒有帶 sort 值，cursor pagination 無法運作".into(),
        })?;
    let mut second = first;
    second.search_after = Some(cursor);
    let page2 = store.search(second).await?;
    if page2.hits.len() != 1 {
        return Err(StorageError::Unknown {
            backend: "search",
            message: format!("第二頁預期 1 筆，實際 {}", page2.hits.len()),
        });
    }
    let page1_ids: Vec<&str> = page1.hits.iter().map(|h| h.id.as_str()).collect();
    if page1_ids.contains(&page2.hits[0].id.as_str()) {
        return Err(StorageError::Unknown {
            backend: "search",
            message: "search_after 沒有接續，第二頁又回了第一頁的文件".into(),
        });
    }

    for hit in &all.hits {
        store.delete(index, &hit.id).await?;
    }
    Ok(())
}

/// 寫入後可能還沒 refresh，重試幾次再放棄。
async fn retry_search<S: SearchStore>(
    store: &S,
    query: StructuredSearch,
    expected: u64,
) -> Result<crate::traits::SearchHits, StorageError> {
    let mut last = store.search(query.clone()).await?;
    for _ in 0..20 {
        if last.total >= expected {
            return Ok(last);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        last = store.search(query.clone()).await?;
    }
    Ok(last)
}

/// `ProjectionStore` 的契約（V0.2 Phase 0f）。
///
/// **未來的 `storage-neo4j` 也必須通過這一支。** 這裡驗的五件事全部都是
/// 「做錯了不會報錯，只會讓運維看到一個假的數字」的那一類：
///
/// 1. 沒有 checkpoint 時 lag 是 `None` 而不是 `0`——0 會被讀成「沒落後」，
///    一個從來沒跑過的投影在儀表板上就變成健康的。
/// 2. checkpoint 寫完立刻讀得到（OpenSearch 需要明確 refresh）。
///    讀到舊值的話下一批的累加會從舊值開始，計數永遠停在原地。
/// 3. 沒有重建記錄時回 `Idle` 而不是 `Err`。
/// 4. `save_checkpoint` 與 `set_rebuild_status` **不會互相蓋掉**——
///    兩者若存在同一列而用整列覆寫實作，寫 checkpoint 就會清掉上次的重建紀錄。
/// 5. `reset_projection` 之後回到初始狀態，且對不存在的投影也是 `Ok`（冪等）。
///
/// `projection` 必須是**本次測試專屬**的名字：這一列的鍵就是投影名，
/// 寫死的話多個測試會互相覆寫對方的 checkpoint。
pub async fn assert_projection_store_contract<S: ProjectionStore>(
    store: &S,
    projection: &str,
) -> Result<(), StorageError> {
    let fail = |message: String| StorageError::Unknown {
        backend: "conformance",
        message,
    };

    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "projection",
            message: health.message,
        });
    }

    // --- 1. 初始狀態 ------------------------------------------------------
    if store.checkpoint(projection).await?.is_some() {
        return Err(fail(format!(
            "投影 `{projection}` 從沒寫過，checkpoint 卻有值——測試用的投影名撞到了別人"
        )));
    }
    let now = fixture_ts();
    let lag = store.projection_lag(projection, now).await?;
    if lag.lag_seconds.is_some() || lag.checkpoint.is_some() {
        return Err(fail(format!(
            "沒有 checkpoint 時 lag 必須是 None，實際 {:?}。\
             回 0 會被讀成「沒落後」，而實際狀況是這個投影從來沒寫過東西",
            lag.lag_seconds
        )));
    }
    let status = store.rebuild_status(projection).await?;
    if status.state != RebuildState::Idle {
        return Err(fail(format!(
            "沒有重建記錄時 rebuild_status 應為 Idle，實際 {:?}",
            status.state
        )));
    }

    // --- 2. checkpoint round-trip ----------------------------------------
    let source_at = fixture_ts() - chrono::Duration::seconds(300);
    let object_id = Uuid::now_v7();
    let mut checkpoint = ProjectionCheckpoint::empty(projection, now);
    checkpoint.advance(Some(source_at), Some(object_id), 42, now);
    store.save_checkpoint(&checkpoint).await?;

    let got = store
        .checkpoint(projection)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: format!(
                "save_checkpoint 之後立刻 checkpoint() 讀不到 `{projection}`。\
                 OpenSearch 類後端要用 refresh=wait_for 寫入，否則下一批的累加會從舊值開始"
            ),
        })?;
    assert_eq_debug("projection_checkpoint", &checkpoint, &got);

    let lag = store.projection_lag(projection, now).await?;
    if lag.lag_seconds != Some(300) {
        return Err(fail(format!(
            "lag 應為 now - last_source_at = 300 秒，實際 {:?}",
            lag.lag_seconds
        )));
    }

    // --- 3. rebuild 狀態 round-trip --------------------------------------
    let running = RebuildStatus {
        projection: projection.to_string(),
        state: RebuildState::Running,
        started_at: Some(now),
        finished_at: None,
        scanned: 7,
        written: 5,
        failed: 2,
        last_error: Some("conformance 寫入的假錯誤".into()),
    };
    store.set_rebuild_status(&running).await?;
    assert_eq_debug(
        "rebuild_status_running",
        &running,
        &store.rebuild_status(projection).await?,
    );

    // --- 4. 兩者不可互相蓋掉 ----------------------------------------------
    // checkpoint 與重建狀態常常被實作成同一列。整列覆寫的話，一次 flush 的
    // checkpoint 寫入就會把「上次 rebuild 何時、寫了幾筆」清掉——而那正是
    // SPEC_V0.2 §27 要顯示的東西。
    let mut advanced = got.clone();
    advanced.advance(
        Some(source_at + chrono::Duration::seconds(60)),
        Some(Uuid::now_v7()),
        8,
        now,
    );
    store.save_checkpoint(&advanced).await?;
    let status = store.rebuild_status(projection).await?;
    if status.state != RebuildState::Running || status.scanned != 7 {
        return Err(fail(format!(
            "寫 checkpoint 之後重建狀態被蓋掉了（state={:?} scanned={}）。\
             兩者必須能各自更新，否則每次 flush 都會清掉上次 rebuild 的紀錄",
            status.state, status.scanned
        )));
    }
    let completed = RebuildStatus {
        state: RebuildState::Completed,
        finished_at: Some(now + chrono::Duration::seconds(30)),
        last_error: None,
        ..running.clone()
    };
    store.set_rebuild_status(&completed).await?;
    let after = store
        .checkpoint(projection)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: "寫重建狀態之後 checkpoint 不見了——兩者必須能各自更新".into(),
        })?;
    assert_eq_debug("projection_checkpoint_after_status", &advanced, &after);

    // --- 5. reset ---------------------------------------------------------
    store.reset_projection(projection).await?;
    if store.checkpoint(projection).await?.is_some() {
        return Err(fail(
            "reset_projection 之後 checkpoint 應回到 None（`--rebuild --drop` 要重新計數)".into(),
        ));
    }
    let status = store.rebuild_status(projection).await?;
    if status.state != RebuildState::Idle || status.written != 0 {
        return Err(fail(format!(
            "reset_projection 之後 rebuild_status 應回到 Idle 零值，實際 {status:?}"
        )));
    }
    // 冪等：清一個不存在的投影不是錯誤。`--drop` 在第一次重建時就會走到這條路。
    store
        .reset_projection(&format!("{projection}-absent"))
        .await?;
    Ok(())
}

/// GraphStore 契約（V0.2 Phase 2 `storage-neo4j`）。
///
/// 用 per-run UUID 當 entity_id／relationship_id，測完 `delete_node` 清掉。
/// **不刪 constraint**——那是全域 schema，重複 `CREATE CONSTRAINT IF NOT EXISTS` 是安全的。
///
/// 覆蓋：
/// - upsert_node 後用 `shortest_path(id, id)` 讀回一致（trait 沒有 get_node）
/// - upsert_edge 後 neighbors 雙向都找得到（邊當無向）
/// - `relationship_types`／`min_confidence`／`time_range`（重疊語意）過濾生效
/// - shortest_path 找得到／找不到（含節點不存在）
/// - `query` 對空 `starts` 回 `ConstraintViolation`
/// - delete_edge 只刪那條邊
/// - delete_node 連帶刪邊
pub async fn assert_graph_store_contract<S: GraphStore>(store: &S) -> Result<(), StorageError> {
    let fail = |message: String| StorageError::Unknown {
        backend: "conformance",
        message,
    };

    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "neo4j",
            message: health.message,
        });
    }

    let a = Uuid::now_v7();
    let b = Uuid::now_v7();
    let c = Uuid::now_v7();
    let missing = Uuid::now_v7();
    let e_ab = Uuid::now_v7();
    let e_bc = Uuid::now_v7();
    let e_ac = Uuid::now_v7();
    let cleanup = [a, b, c];

    let run = async {
        let node = |id: Uuid, ty: &str, name: &str| GraphNode {
            entity_id: id,
            entity_type: ty.into(),
            display_name: name.into(),
            attributes: json!({"k": name}),
        };
        let t0 = fixture_ts();
        let t100 = t0 + chrono::Duration::seconds(100);
        let t200 = t0 + chrono::Duration::seconds(200);
        let t300 = t0 + chrono::Duration::seconds(300);

        store.upsert_node(&node(a, "person", "Alice")).await?;
        store.upsert_node(&node(a, "person", "Alicia")).await?;
        let me = store
            .shortest_path(&a, &a, &GraphTraversalOptions::one_hop())
            .await?
            .ok_or_else(|| fail("upsert_node 之後 shortest_path(自己, 自己) 讀不到節點".into()))?;
        if me.nodes.len() != 1 || !me.edges.is_empty() {
            return Err(fail(format!(
                "shortest_path(a, a) 應為單節點無邊，實際 nodes={} edges={}",
                me.nodes.len(),
                me.edges.len()
            )));
        }
        if me.nodes[0].display_name != "Alicia" || me.nodes[0].entity_type != "person" {
            return Err(fail(format!(
                "upsert_node 覆寫後讀回不符：{:?}",
                me.nodes[0]
            )));
        }
        if me.nodes[0].attributes.get("k").and_then(Value::as_str) != Some("Alicia") {
            return Err(fail(format!(
                "attributes 沒 round-trip，實際 {:?}",
                me.nodes[0].attributes
            )));
        }

        store
            .upsert_node(&node(b, "organization", "ExampleOrg"))
            .await?;
        store.upsert_node(&node(c, "domain", "example.com")).await?;

        let edge = |id: Uuid, src: Uuid, tgt: Uuid, ty: &str, conf: f64, from, to| GraphEdge {
            relationship_id: id,
            source: src,
            target: tgt,
            relationship_type: ty.into(),
            confidence: conf,
            first_seen: from,
            last_seen: to,
        };
        store
            .upsert_edge(&edge(e_ab, a, b, "mentions", 0.9, t0, t100))
            .await?;
        store
            .upsert_edge(&edge(e_bc, b, c, "associated_with", 0.4, t100, t200))
            .await?;
        store
            .upsert_edge(&edge(e_ac, a, c, "belongs_to", 0.8, t200, t300))
            .await?;

        let mut n = store
            .neighbors(&a, &GraphTraversalOptions::one_hop())
            .await?;
        n.sort_by_key(|x| x.entity_id);
        let got_set: std::collections::BTreeSet<_> = n.iter().map(|x| x.entity_id).collect();
        let expect: std::collections::BTreeSet<_> = [b, c].into_iter().collect();
        if got_set != expect {
            return Err(fail(format!(
                "一跳 neighbors(a) 應為 {{b,c}}（無向），實際 {got_set:?}"
            )));
        }

        let mut only_mentions = GraphTraversalOptions::one_hop();
        only_mentions.relationship_types = Some(vec!["mentions".into()]);
        let n = store.neighbors(&a, &only_mentions).await?;
        if n.len() != 1 || n[0].entity_id != b {
            return Err(fail(format!(
                "relationship_types=[mentions] 應只回 b，實際 {:?}",
                n.iter().map(|x| x.entity_id).collect::<Vec<_>>()
            )));
        }

        let mut high = GraphTraversalOptions::one_hop();
        high.min_confidence = Some(0.85);
        let n = store.neighbors(&a, &high).await?;
        if n.len() != 1 || n[0].entity_id != b {
            return Err(fail(format!(
                "min_confidence=0.85 應只留 mentions(0.9)，實際 {} 筆",
                n.len()
            )));
        }

        let mut window = GraphTraversalOptions::one_hop();
        window.time_range = Some((
            t0 - chrono::Duration::seconds(10),
            t0 + chrono::Duration::seconds(50),
        ));
        let n = store.neighbors(&a, &window).await?;
        if n.len() != 1 || n[0].entity_id != b {
            return Err(fail(
                "time_range 重疊語意失敗：區間只跟 mentions 的 [t0,t100] 重疊，應只回 b".into(),
            ));
        }

        let path = store
            .shortest_path(&a, &c, &{
                let mut o = GraphTraversalOptions::one_hop();
                o.max_hops = 2;
                o
            })
            .await?
            .ok_or_else(|| fail("a 到 c 應找得到路徑（直連 belongs_to 或經 b）".into()))?;
        if path.nodes.is_empty() || path.nodes[0].entity_id != a {
            return Err(fail(format!(
                "shortest_path 起點應為 a，實際 {:?}",
                path.nodes
            )));
        }
        if path.nodes.last().map(|n| n.entity_id) != Some(c) {
            return Err(fail(format!(
                "shortest_path 終點應為 c，實際 {:?}",
                path.nodes.last()
            )));
        }

        let none = store
            .shortest_path(&a, &missing, &GraphTraversalOptions::one_hop())
            .await?;
        if none.is_some() {
            return Err(fail(
                "終點不存在時 shortest_path 應為 None，不是 Err 也不是 Some".into(),
            ));
        }

        let err = store
            .query(&GraphQuery {
                starts: vec![],
                pattern: GraphPattern::Neighbors,
                options: GraphTraversalOptions::one_hop(),
            })
            .await
            .err()
            .ok_or_else(|| fail("空 starts 的 query 應回 Err".into()))?;
        if !matches!(err, StorageError::ConstraintViolation { .. }) {
            return Err(fail(format!(
                "空 starts 應為 ConstraintViolation，實際 {err}"
            )));
        }

        store.delete_edge(&e_ac).await?;
        let n = store
            .neighbors(&a, &GraphTraversalOptions::one_hop())
            .await?;
        if n.iter().any(|x| x.entity_id == c) {
            return Err(fail(
                "delete_edge(belongs_to) 之後 neighbors(a) 仍看得到 c".into(),
            ));
        }
        if !n.iter().any(|x| x.entity_id == b) {
            return Err(fail("delete_edge 不該把另一條 mentions 一起刪掉".into()));
        }

        store.delete_node(&b).await?;
        let to_b = store
            .shortest_path(&a, &b, &GraphTraversalOptions::one_hop())
            .await?;
        if to_b.is_some() {
            return Err(fail(
                "delete_node 之後不該還找得到路徑（邊必須連帶刪）".into(),
            ));
        }
        let n = store
            .neighbors(&a, &GraphTraversalOptions::one_hop())
            .await?;
        if n.iter().any(|x| x.entity_id == b) {
            return Err(fail("delete_node(b) 之後 neighbors(a) 仍有 b".into()));
        }

        // ⚠️ 實測過（2026-09-13）：`wipe()` 刪的是**整個資料庫**的 `:Entity`，
        // 不是只刪這個測試自己建立的節點。對本機共用 Neo4j 跑這個 conformance
        // 測試，會把 `osint-graph-worker --rebuild` 投影出來的真實資料也一起
        // 清空（親身遇到：跑完這段之後 `MATCH (n:Entity) RETURN count(n)` 從
        // 1482 變成 0）。不是資料遺失——PostgreSQL 才是 canonical truth，重跑
        // `osint-graph-worker --rebuild` 就補回來——但這是會讓人嚇一跳的副作用，
        // 跑 `cargo test -p storage-neo4j` 之前要有心理準備，跑完如果需要圖投影
        // 請記得重新 rebuild。
        //
        // wipe 必須在既有 cleanup 之前測：測完還要讓外層 `delete_node` 清掉殘留。
        // 用新的節點／邊，避免跟上面已經刪掉的 a/b/c 狀態糾纏。
        let w1 = Uuid::now_v7();
        let w2 = Uuid::now_v7();
        let e_w = Uuid::now_v7();
        store.upsert_node(&node(w1, "person", "WipeA")).await?;
        store
            .upsert_node(&node(w2, "organization", "WipeB"))
            .await?;
        store
            .upsert_edge(&edge(e_w, w1, w2, "associated_with", 0.5, t0, t100))
            .await?;
        store.wipe().await?;
        let n = store
            .neighbors(&w1, &GraphTraversalOptions::one_hop())
            .await?;
        if !n.is_empty() {
            return Err(fail(format!(
                "wipe 之後 neighbors(w1) 應為空，實際 {} 筆",
                n.len()
            )));
        }
        let path = store
            .shortest_path(&w1, &w2, &GraphTraversalOptions::one_hop())
            .await?;
        if path.is_some() {
            return Err(fail(
                "wipe 之後 shortest_path 仍找得到路徑。wipe 必須連帶刪邊".into(),
            ));
        }
        let health_after = store.health().await?;
        if !health_after.healthy {
            return Err(fail(format!(
                "wipe 不該把連線弄壞，health={}",
                health_after.message
            )));
        }
        // 連線還能寫才算沒壞。寫完立刻刪，避免外層 cleanup 漏掉這顆。
        store.upsert_node(&node(w1, "person", "AfterWipe")).await?;
        store.delete_node(&w1).await?;

        Ok(())
    };

    let result = run.await;
    for id in cleanup {
        let _ = store.delete_node(&id).await;
    }
    result
}

/// KeyValueStore：get/set/del/expire。
pub async fn assert_kv_round_trip<S: KeyValueStore>(store: &S) -> Result<(), StorageError> {
    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "redis",
            message: health.message,
        });
    }
    let key = format!("osint-core:conformance:{}", Uuid::now_v7());
    store.set(&key, b"hello").await?;
    let got = store
        .get(&key)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: format!("剛 SET 的 key `{key}` 讀不到"),
        })?;
    if got != b"hello" {
        return Err(StorageError::Unknown {
            backend: "redis",
            message: format!("GET 內容不符：{got:?}"),
        });
    }
    if !store.del(&key).await? {
        return Err(StorageError::Unknown {
            backend: "redis",
            message: "DEL 既有 key 應回 true".into(),
        });
    }
    if store.get(&key).await?.is_some() {
        return Err(StorageError::Unknown {
            backend: "redis",
            message: "DEL 之後不該還讀得到".into(),
        });
    }

    let ttl_key = format!("osint-core:conformance:ttl:{}", Uuid::now_v7());
    store
        .set_ex(&ttl_key, b"temp", Duration::from_secs(2))
        .await?;
    if store.get(&ttl_key).await?.as_deref() != Some(&b"temp"[..]) {
        return Err(StorageError::Unknown {
            backend: "redis",
            message: "set_ex 後立刻讀不到值".into(),
        });
    }
    // 把 TTL 縮短，再等它過期。
    if !store.expire(&ttl_key, Duration::from_millis(200)).await? {
        return Err(StorageError::Unknown {
            backend: "redis",
            message: "對既有 key 設 expire 應回 true".into(),
        });
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    if store.get(&ttl_key).await?.is_some() {
        return Err(StorageError::Unknown {
            backend: "redis",
            message: "expire 後仍讀得到值".into(),
        });
    }
    Ok(())
}

/// ObjectStore：put/get/delete/exists。呼叫前必須已通過 endpoint 埠號檢查。
pub async fn assert_object_round_trip<S: ObjectStore>(
    store: &S,
    key_prefix: &str,
) -> Result<(), StorageError> {
    let health = store.health().await?;
    if !health.healthy {
        return Err(StorageError::Unavailable {
            backend: "s3",
            message: health.message,
        });
    }
    let key = format!("{key_prefix}/{}", Uuid::now_v7());
    store
        .put(&key, b"raw-evidence-bytes", Some("text/plain"))
        .await?;
    if !store.exists(&key).await? {
        return Err(StorageError::Unknown {
            backend: "s3",
            message: format!("put 之後 exists(`{key}`) 應為 true"),
        });
    }
    let got = store
        .get(&key)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            message: format!("put 之後 get(`{key}`) 是 None"),
        })?;
    if got != b"raw-evidence-bytes" {
        return Err(StorageError::Unknown {
            backend: "s3",
            message: format!("get 內容不符：{} bytes", got.len()),
        });
    }
    if !store.delete(&key).await? {
        return Err(StorageError::Unknown {
            backend: "s3",
            message: "delete 既有物件應回 true".into(),
        });
    }
    if store.exists(&key).await? {
        return Err(StorageError::Unknown {
            backend: "s3",
            message: "delete 之後 exists 應為 false".into(),
        });
    }
    if store.get(&key).await?.is_some() {
        return Err(StorageError::Unknown {
            backend: "s3",
            message: "delete 之後 get 應為 None".into(),
        });
    }
    Ok(())
}

/// SQLite 路徑不可由任意使用者輸入直接組出。
pub fn assert_sqlite_path_safe(path: &Path) -> Result<(), StorageError> {
    if path.as_os_str().is_empty() {
        return Err(StorageError::Configuration {
            message: "SQLite 路徑是空的。請在設定裡指定 storage.sqlite_local.path".into(),
        });
    }
    let raw = path.to_string_lossy();
    if raw.contains('\0') {
        return Err(StorageError::Configuration {
            message: "SQLite 路徑含 NUL，已拒絕".into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// `OSINT_STRICT_PORT_ISOLATION` 是行程環境變數；測試必須序列化，否則會互相覆蓋。
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn with_strict_isolation<T>(f: impl FnOnce() -> T) -> T {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::set_var("OSINT_STRICT_PORT_ISOLATION", "1") };
        let result = f();
        unsafe { env::remove_var("OSINT_STRICT_PORT_ISOLATION") };
        result
    }

    #[test]
    fn rejects_opensearch_canonical_port_when_strict() {
        with_strict_isolation(|| {
            let err = verify_not_opencti_search("http://127.0.0.1:9200").unwrap_err();
            assert!(format!("{err}").contains("19200"));
        });
    }

    #[test]
    fn accepts_opensearch_canonical_port_by_default() {
        // 沒開 OSINT_STRICT_PORT_ISOLATION 時（CI、其他沒有 OpenCTI 衝突的機器），
        // 9200 就是普通的 OpenSearch 埠，不該被拒絕——真正的身分驗證交給
        // assert_opensearch_identity，這裡只驗證 URL 格式。
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::remove_var("OSINT_STRICT_PORT_ISOLATION") };
        verify_not_opencti_search("http://127.0.0.1:9200").unwrap();
    }

    #[test]
    fn accepts_opensearch_dev_port() {
        verify_not_opencti_search("http://127.0.0.1:19200").unwrap();
    }

    #[test]
    fn rejects_minio_canonical_port_when_strict() {
        with_strict_isolation(|| {
            let err = verify_not_opencti_s3("http://127.0.0.1:9000").unwrap_err();
            assert!(format!("{err}").contains("19000"));
        });
    }

    #[test]
    fn accepts_minio_canonical_port_by_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { env::remove_var("OSINT_STRICT_PORT_ISOLATION") };
        verify_not_opencti_s3("http://127.0.0.1:9000").unwrap();
    }

    #[test]
    fn elasticsearch_identity_is_rejected() {
        let json = json!({
            "tagline": "You Know, for Search",
            "version": {"number": "8.19.16"}
        });
        assert!(assert_opensearch_identity(&json).is_err());
    }

    #[test]
    fn opensearch_identity_is_accepted() {
        let json = json!({
            "tagline": "The OpenSearch Project: https://opensearch.org/",
            "version": {"distribution": "opensearch", "number": "2.19.6"}
        });
        assert_opensearch_identity(&json).unwrap();
    }

    #[tokio::test]
    async fn mock_graph_store_satisfies_contract() {
        let g = crate::mock::MockGraphStore::new();
        assert_graph_store_contract(&g).await.unwrap();
    }
}
