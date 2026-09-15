//! embedding-worker e2e：對本機 Docker（Postgres + OpenSearch 19200）真跑。
//!
//! schema／k-NN 路徑**沒有** `#[ignore]`——`opensearch-knn` 是官方映像內建，
//! 不需要 ml-commons 模型。打 `_predict` 的路徑才 `#[ignore]`（要先跑
//! `scripts/opensearch-ml-setup.sh` 與 `opensearch-ml-setup-e5.sh`）。
//!
//! 每個測試用自己的 index（`osint-entities-e2e-<uuid>`／`osint-documents-e2e-<uuid>`），
//! 結尾 `delete_index`。留著會讓叢集慢慢長出幾百個測試 index（CLAUDE.md §15）。
//!
//! Entity 的自然鍵 `(entity_type, normalized_name)` 是**全域**唯一的，
//! 共用 Postgres 上不能用固定名字。

use chrono::{TimeZone, Utc};
use core_model::{Document, DocumentType, EmbeddingTarget, Entity, EntityType};
use core_observability::MetricsRegistry;
use embedding_worker::EmbeddingWorker;
use embedding_worker::schema as entity_schema;
use indexer::schema as doc_schema;
use serde_json::json;
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::mock::MockEmbeddingProvider;
use storage_core::{
    RelationalStore, SearchDocument, SearchStore, VectorSearch, embedding_content_hash,
};
use storage_opensearch::OpenSearchStore;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

struct Stack {
    pg: PostgresCanonicalStore,
    os: OpenSearchStore,
}

async fn connect_stack() -> Stack {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "e2e 只連本機 Postgres"
    );
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    verify_not_opencti_search(&url).expect("OPENSEARCH_URL 埠隔離");

    let pg = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("postgres");
    pg.migrate().await.expect("migrate");

    let os = OpenSearchStore::connect(&url)
        .expect("opensearch client")
        .with_refresh_on_write(true);
    let info = os.cluster_info().await.expect("GET /");
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");

    Stack { pg, os }
}

fn entities_index() -> String {
    format!("osint-entities-e2e-{}", Uuid::now_v7().simple())
}

fn documents_index() -> String {
    format!("osint-documents-e2e-{}", Uuid::now_v7().simple())
}

async fn cleanup(stack: &Stack, documents: &str, entities: &str) {
    let _ = stack.os.delete_index(documents).await;
    let _ = stack.os.delete_index(entities).await;
}

fn ts() -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 9, 15, 10, 0, 0).unwrap()
}

fn document(title: Option<&str>, body: Option<&str>, language: Option<&str>) -> Document {
    Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::Article,
        schema_version: "1".into(),
        title: title.map(str::to_string),
        body: body.map(str::to_string),
        summary: None,
        language: language.map(str::to_string),
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: ts(),
        collected_at: ts(),
        source_url: None,
        canonical_url: None,
        normalized_content_hash: None,
        confidence: 0.9,
        labels: vec![],
        attributes: json!({}),
        external_key: None,
        simhash: None,
        duplicate_of: None,
    }
}

fn entity(name: &str, description: Option<&str>) -> Entity {
    // `(entity_type, normalized_name)` 是 Postgres 全域唯一鍵。固定名字
    // （`acme-e2e`）第二次跑會撞 `idx_entities_natural_key`。
    let unique = format!("{name}-{}", Uuid::now_v7().simple());
    Entity {
        id: Uuid::now_v7(),
        entity_type: EntityType::Organization,
        name: unique.clone(),
        normalized_name: unique.to_ascii_lowercase(),
        description: description.map(str::to_string),
        confidence: 0.9,
        first_seen: ts(),
        last_seen: ts(),
        merged_into: None,
        attributes: json!({}),
    }
}

fn worker(
    stack: &Stack,
    documents: &str,
    entities: &str,
) -> EmbeddingWorker<PostgresCanonicalStore, MockEmbeddingProvider, OpenSearchStore> {
    EmbeddingWorker::new(
        stack.pg.clone(),
        MockEmbeddingProvider::new(),
        stack.os.clone(),
        MetricsRegistry::new(),
        embedding_worker::EmbeddingBounds::new(documents, entities, 32, 4),
    )
    .with_update_fields_backoffs(vec![std::time::Duration::from_millis(0); 3])
}

/// mapping PUT 必須成功，且 k-NN 真的能查出剛寫進去的向量。
///
/// 這條**不**打 ml-commons。向量是手填的 384 維，只證明 schema 的
/// `knn_vector`／`index.knn`／`space_type: cosinesimil` 能被 2.19.6 接受。
#[tokio::test]
async fn entities_index_mapping_accepts_knn_and_finds_neighbor() {
    let stack = connect_stack().await;
    let index = entities_index();
    let created = stack
        .os
        .ensure_index_with(
            &index,
            &entity_schema::index_settings(),
            &entity_schema::index_mappings(),
        )
        .await
        .expect("PUT osint-entities mapping 必須 200；失敗請看 OpenSearch log，不要 ignore 這條");
    assert!(created, "測試 index 應該是新建的");

    let mut vector = vec![0.0f32; 384];
    vector[0] = 1.0;
    let id = Uuid::now_v7();
    stack
        .os
        .index(SearchDocument {
            index: index.clone(),
            id: id.to_string(),
            body: json!({
                entity_schema::F_ENTITY_ID: id,
                entity_schema::F_ENTITY_TYPE: "organization",
                entity_schema::F_NAME: "acme",
                entity_schema::F_NORMALIZED_NAME: "acme",
                entity_schema::F_DESCRIPTION_VECTOR_MULTI: vector,
                entity_schema::F_DESCRIPTION_VECTOR_MULTI_MODEL_VERSION: "test",
            }),
        })
        .await
        .expect("寫 knn_vector 不該被 mapping 拒絕");

    let mut query = vec![0.0f32; 384];
    query[0] = 1.0;
    let hits = stack
        .os
        .vector_search(VectorSearch {
            index: index.clone(),
            field: entity_schema::F_DESCRIPTION_VECTOR_MULTI.into(),
            vector: query,
            k: 1,
            filters: vec![],
        })
        .await
        .expect("k-NN 查詢");
    assert_eq!(hits.hits.len(), 1, "剛寫的向量必須被查到");
    assert_eq!(hits.hits[0].id, id.to_string());

    cleanup(&stack, "unused", &index).await;
}

#[tokio::test]
async fn overlay_document_vector_does_not_wipe_title() {
    let stack = connect_stack().await;
    let docs = documents_index();
    let ents = entities_index();
    stack
        .os
        .ensure_index_with(
            &docs,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("documents mapping");
    stack
        .os
        .ensure_index_with(
            &ents,
            &entity_schema::index_settings(),
            &entity_schema::index_mappings(),
        )
        .await
        .expect("entities mapping");

    let doc = document(Some("Hello title"), Some("Hello body"), Some("en"));
    stack.pg.put_document(&doc).await.expect("put document");
    stack
        .os
        .index(SearchDocument {
            index: docs.clone(),
            id: doc.id.to_string(),
            body: json!({
                doc_schema::F_DOCUMENT_ID: doc.id,
                doc_schema::F_OBJECT_TYPE: "article",
                doc_schema::F_TITLE: "Hello title",
                doc_schema::F_BODY: "Hello body",
                doc_schema::F_LANGUAGE: "en",
            }),
        })
        .await
        .expect("seed document");

    let report = worker(&stack, &docs, &ents)
        .process_extracted(&json!({
            "document_id": doc.id,
            "entity_ids": []
        }))
        .await
        .expect("process");
    assert!(report.title_applied || report.body_applied);

    let got = stack
        .os
        .query(storage_core::SearchQuery {
            index: docs.clone(),
            query_string: format!("document_id:{}", doc.id),
            from: 0,
            size: 1,
        })
        .await
        .expect("get");
    assert_eq!(got.hits.len(), 1);
    let source = &got.hits[0].source;
    assert_eq!(source[doc_schema::F_TITLE], "Hello title");
    assert!(
        source.get(doc_schema::F_EMBEDDING_EN).is_some(),
        "英文文件應寫 embedding_en：{source}"
    );
    assert!(source.get(doc_schema::F_EMBEDDING_MULTI).is_none());

    cleanup(&stack, &docs, &ents).await;
}

#[tokio::test]
async fn entity_description_lands_in_entities_index() {
    let stack = connect_stack().await;
    let docs = documents_index();
    let ents = entities_index();
    stack
        .os
        .ensure_index_with(
            &ents,
            &entity_schema::index_settings(),
            &entity_schema::index_mappings(),
        )
        .await
        .expect("entities mapping");

    let doc = document(None, None, None);
    stack.pg.put_document(&doc).await.expect("put document");
    let ent = entity("acme-e2e", Some("A software company in Taipei"));
    stack.pg.put_entity(&ent).await.expect("put entity");

    let report = worker(&stack, &docs, &ents)
        .process_extracted(&json!({
            "document_id": doc.id,
            "entity_ids": [ent.id]
        }))
        .await
        .expect("process");
    assert_eq!(report.entities_applied, 1);

    let got = stack
        .os
        .query(storage_core::SearchQuery {
            index: ents.clone(),
            query_string: format!("entity_id:{}", ent.id),
            from: 0,
            size: 1,
        })
        .await
        .expect("get entity");
    assert_eq!(got.hits.len(), 1);
    let source = &got.hits[0].source;
    assert!(
        source
            .get(entity_schema::F_DESCRIPTION_VECTOR_MULTI)
            .is_some()
    );
    assert!(source.get(entity_schema::F_DESCRIPTION_VECTOR_EN).is_none());

    let found = stack
        .pg
        .find_embedding(
            ent.id,
            EmbeddingTarget::EntityDescription,
            storage_core::mock::MOCK_E5_MODEL,
            &embedding_content_hash("A software company in Taipei"),
        )
        .await
        .expect("find");
    assert!(found.is_some());

    cleanup(&stack, &docs, &ents).await;
}

#[tokio::test]
async fn rebuild_skips_duplicate_and_merged_on_real_backend() {
    let stack = connect_stack().await;
    let docs = documents_index();
    let ents = entities_index();
    stack
        .os
        .ensure_index_with(
            &docs,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("documents mapping");
    stack
        .os
        .ensure_index_with(
            &ents,
            &entity_schema::index_settings(),
            &entity_schema::index_mappings(),
        )
        .await
        .expect("entities mapping");

    let canonical = document(Some("canon-e2e"), None, Some("en"));
    let mut dup = document(Some("dup-e2e"), None, Some("en"));
    dup.duplicate_of = Some(canonical.id);
    stack.pg.put_document(&canonical).await.expect("canon");
    stack.pg.put_document(&dup).await.expect("dup");
    stack
        .os
        .index(SearchDocument {
            index: docs.clone(),
            id: canonical.id.to_string(),
            body: json!({
                doc_schema::F_DOCUMENT_ID: canonical.id,
                doc_schema::F_OBJECT_TYPE: "article",
                doc_schema::F_TITLE: "canon-e2e",
                doc_schema::F_LANGUAGE: "en",
            }),
        })
        .await
        .expect("seed canon");

    let live = entity("live-e2e", Some("desc"));
    let mut merged = entity("merged-e2e", Some("desc"));
    merged.merged_into = Some(live.id);
    stack.pg.put_entity(&live).await.expect("live");
    stack.pg.put_entity(&merged).await.expect("merged");

    // 不對共用 Postgres 跑全表 rebuild：那會掃到別的測試留下的列。
    // skip 語意的單元測試已覆蓋 rebuild()；這裡只驗證「這則事件」路徑。
    let report = worker(&stack, &docs, &ents)
        .process_extracted(&json!({
            "document_id": dup.id,
            "entity_ids": [merged.id, live.id]
        }))
        .await
        .expect("process");
    assert!(report.document_duplicate);
    assert_eq!(report.entities_skipped_merged, 1);
    assert_eq!(report.entities_applied, 1);

    let merged_hits = stack
        .os
        .query(storage_core::SearchQuery {
            index: ents.clone(),
            query_string: format!("entity_id:{}", merged.id),
            from: 0,
            size: 1,
        })
        .await
        .expect("merged query");
    assert_eq!(
        merged_hits.hits.len(),
        0,
        "merged entity 不該進 osint-entities"
    );

    cleanup(&stack, &docs, &ents).await;
}

/// 真打 ml-commons `_predict`。本機沒部署模型時不要跑——那是設定問題，
/// 不是 schema 問題。CI 預設略過。
#[tokio::test]
#[ignore = "需要本機已部署 MiniLM + e5（scripts/opensearch-ml-setup.sh 與 -e5）"]
async fn ml_commons_embed_then_update_fields() {
    let stack = connect_stack().await;
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let embeddings = storage_opensearch::MlCommonsEmbeddingProvider::connect(&url)
        .await
        .expect("兩個模型都必須 DEPLOYED");

    let docs = documents_index();
    let ents = entities_index();
    stack
        .os
        .ensure_index_with(
            &docs,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("documents mapping");

    let doc = document(
        Some("OpenSearch knn lucene engine"),
        Some("Filtered knn returns the next matching neighbor"),
        Some("en"),
    );
    stack.pg.put_document(&doc).await.expect("put");
    stack
        .os
        .index(SearchDocument {
            index: docs.clone(),
            id: doc.id.to_string(),
            body: json!({
                doc_schema::F_DOCUMENT_ID: doc.id,
                doc_schema::F_OBJECT_TYPE: "article",
                doc_schema::F_TITLE: doc.title,
                doc_schema::F_BODY: doc.body,
                doc_schema::F_LANGUAGE: "en",
            }),
        })
        .await
        .expect("seed");

    let service = EmbeddingWorker::new(
        stack.pg.clone(),
        embeddings,
        stack.os.clone(),
        MetricsRegistry::new(),
        embedding_worker::EmbeddingBounds::new(docs.clone(), ents.clone(), 8, 1),
    );
    let report = service
        .process_extracted(&json!({
            "document_id": doc.id,
            "entity_ids": []
        }))
        .await
        .expect("ml-commons process");
    assert!(report.body_applied || report.title_applied);

    cleanup(&stack, &docs, &ents).await;
}

/// Stage 5 寫進 Redis 的向量，embedding-worker 必須撿到。
///
/// 本測試自己 `embed` 一次當「Stage 5 已寫過」的種子，再把同一份 JSON
/// `set_ex` 進 Redis。`process_extracted` 若 `redis_cache_hits ≥ 1`
/// 且 overlay 成功，就證明讀取路徑與 Stage 5 共用同一把 key。
#[tokio::test]
#[ignore = "需要本機已部署 MiniLM + e5，以及 Redis"]
async fn stage5_cache_is_reused_by_embedding_worker() {
    use std::sync::Arc;
    use std::time::Duration;

    use storage_core::HealthProvider;
    use storage_core::{
        EmbeddingKind, EmbeddingProvider, EmbeddingRequest, KeyValueStore, embedding_cache_key,
    };
    use storage_opensearch::MlCommonsEmbeddingProvider;
    use storage_redis::RedisKeyValueStore;

    let stack = connect_stack().await;
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let redis_url = required_env("REDIS_URL").expect("REDIS_URL");
    let embeddings = MlCommonsEmbeddingProvider::connect(&url)
        .await
        .expect("兩個模型都必須 DEPLOYED");
    let redis = RedisKeyValueStore::connect(&redis_url).expect("redis");
    redis
        .health()
        .await
        .expect("Redis PING 必須成功，否則這條測不到快取路徑");

    let docs = documents_index();
    let ents = entities_index();
    stack
        .os
        .ensure_index_with(
            &docs,
            &indexer::schema::index_settings(),
            &indexer::schema::index_mappings(),
        )
        .await
        .expect("documents mapping");

    let run = Uuid::now_v7();
    let title = format!("Cache reuse title {run}");
    let body = format!("stage five cache reuse body {run}");
    let doc = document(Some(&title), Some(&body), Some("en"));
    stack.pg.put_document(&doc).await.expect("put");
    stack
        .os
        .index(SearchDocument {
            index: docs.clone(),
            id: doc.id.to_string(),
            body: json!({
                doc_schema::F_DOCUMENT_ID: doc.id,
                doc_schema::F_OBJECT_TYPE: "article",
                doc_schema::F_TITLE: doc.title,
                doc_schema::F_BODY: doc.body,
                doc_schema::F_LANGUAGE: "en",
            }),
        })
        .await
        .expect("seed");

    let body_vec = embeddings
        .embed(&EmbeddingRequest {
            text: body.clone(),
            kind: EmbeddingKind::Passage,
            language: Some("en".into()),
        })
        .await
        .expect("seed embed");
    redis
        .set_ex(
            &embedding_cache_key(&body_vec.content_hash),
            &serde_json::to_vec(&body_vec).expect("json"),
            Duration::from_secs(900),
        )
        .await
        .expect("seed redis");

    let service = EmbeddingWorker::new(
        stack.pg.clone(),
        embeddings,
        stack.os.clone(),
        MetricsRegistry::new(),
        embedding_worker::EmbeddingBounds::new(docs.clone(), ents.clone(), 8, 1),
    )
    .with_embedding_cache(Arc::new(redis) as Arc<dyn KeyValueStore>);

    let report = service
        .process_extracted(&json!({
            "document_id": doc.id,
            "entity_ids": []
        }))
        .await
        .expect("process");
    assert!(
        report.redis_cache_hits >= 1,
        "body 應命中 Stage 5 寫的 Redis 快取，實際 hits={}",
        report.redis_cache_hits
    );
    assert!(
        report.body_applied,
        "快取命中仍必須 overlay osint-documents"
    );

    cleanup(&stack, &docs, &ents).await;
}
