//! Capability conformance。adapter crate 的整合測試呼叫這些函式，針對真實後端驗證契約。
//!
//! 關聯式測試用 UUID v7 當資料，不 TRUNCATE 共用表。
//! OpenSearch / S3 測試必須先通過 [`verify_not_opencti_search`] / [`verify_not_opencti_s3`]。

use std::env;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{TimeZone, Utc};
use core_model::{
    Collection, Connector, Document, DocumentType, DuplicateGroup, Entity, EntityExtraction,
    EntityType, Event, Job, JobStatus, NetworkRule, Provenance, RawEvidence, Relationship,
    RelationshipEvidence, RelationshipType, Source, SourceType,
};
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::error::StorageError;
use crate::traits::{
    CanonicalStore, EmbeddedStore, KeyValueStore, ObjectStore, RelationalStore, SearchDocument,
    SearchQuery, SearchStore,
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
    };
    store.put_job(&job).await?;
    assert_eq_debug("job", &job, &store.get_job(job.id).await?.expect("job"));
    let listed = store.list_jobs(None, 10).await?;
    if !listed.iter().any(|item| item.id == job.id) {
        return Err(StorageError::NotFound {
            message: "剛寫入的 job 沒有出現在 list_jobs 結果".into(),
        });
    }

    let dup = DuplicateGroup {
        id: Uuid::now_v7(),
        canonical_object_id: document.id,
        member_object_id: Some(document.id),
        member_raw_evidence_id: None,
        method: "sha256".into(),
        similarity: 1.0,
        first_seen: fixture_ts(),
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
}
