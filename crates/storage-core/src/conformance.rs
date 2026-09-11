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
    store
        .link_collection_object(collection.id, document.id)
        .await?;

    let entity = Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Vulnerability,
        name: "CVE-2026-0001".into(),
        normalized_name: "cve-2026-0001".into(),
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
