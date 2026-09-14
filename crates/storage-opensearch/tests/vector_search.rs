//! k-NN 寫入與查詢（`SearchStore::update_fields`／`vector_search`）。
//!
//! 對真實 OpenSearch 跑，用 per-run uuid index，結尾刪掉——即使斷言失敗也先清。
//!
//! **不要 `#[ignore]`。** 官方映像 `opensearchproject/opensearch:2.19.6` 內建
//! `opensearch-knn` 2.19.6.0，不需要跑 `opensearch-ml-setup.sh`。2026-09-14
//! 本機另起一個乾淨容器（埠 19210、未跑任何 ml-setup）實測：
//! `engine=lucene` 建 index 回 200，knn 查詢回最近鄰。CI 的 base compose
//! 就是同一顆映像。

use serde_json::{Value, json};
use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::{SearchDocument, SearchFilter, SearchQuery, SearchStore, VectorSearch};
use storage_opensearch::OpenSearchStore;

fn knn_settings() -> Value {
    json!({
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "index": { "knn": true },
    })
}

fn knn_mappings() -> Value {
    json!({
        "dynamic": "strict",
        "properties": {
            "doc_id": { "type": "keyword" },
            "title": { "type": "keyword" },
            "kind": { "type": "keyword" },
            "vec": {
                "type": "knn_vector",
                "dimension": 4,
                "method": { "name": "hnsw", "engine": "lucene", "space_type": "l2" }
            },
            "vec_model_version": { "type": "keyword" },
        }
    })
}

fn vec4(first: f32) -> Vec<f32> {
    vec![first, 0.0, 0.0, 0.0]
}

fn document(index: &str, id: &str, title: &str, kind: &str) -> SearchDocument {
    SearchDocument {
        index: index.into(),
        id: id.into(),
        body: json!({
            "doc_id": id,
            "title": title,
            "kind": kind,
        }),
    }
}

async fn source_of(store: &OpenSearchStore, index: &str, id: &str) -> Value {
    let hits = store
        .query(SearchQuery::new(index, format!("doc_id:{id}")))
        .await
        .expect("query");
    assert_eq!(
        hits.hits.len(),
        1,
        "應剛好找到 `{id}`，實際 {}",
        hits.hits.len()
    );
    hits.hits[0].source.clone()
}

#[tokio::test]
async fn update_fields_and_vector_search() {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let _parsed = verify_not_opencti_search(&url).expect("URL 格式或本機嚴格模式檢查失敗");

    let store = OpenSearchStore::connect(&url)
        .expect("建立 client")
        .with_refresh_on_write(true);

    let info = store.cluster_info().await.expect("GET /");
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");

    let index = format!("osint-core-knn-{}", uuid::Uuid::now_v7());
    let created = store
        .ensure_index_with(&index, &knn_settings(), &knn_mappings())
        .await
        .expect("建立 knn 測試 index");
    assert!(created, "per-run index 不該已經存在");

    let result = run_cases(&store, &index).await;

    store
        .delete_index(&index)
        .await
        .expect("刪除 knn 測試 index");
    result.expect("k-NN 寫入與查詢");
}

async fn run_cases(store: &OpenSearchStore, index: &str) -> Result<(), String> {
    // --- update_fields 疊加向量，不覆寫既有欄位 ---
    store
        .index(document(index, "keep-body", "原始標題", "report"))
        .await
        .map_err(|err| format!("index keep-body：{err}"))?;
    store
        .update_fields(
            index,
            "keep-body",
            json!({
                "vec": vec4(10.0),
                "vec_model_version": "test-v1",
            }),
        )
        .await
        .map_err(|err| format!("update_fields keep-body：{err}"))?;
    let source = source_of(store, index, "keep-body").await;
    if source.get("title").and_then(Value::as_str) != Some("原始標題") {
        return Err(format!(
            "update_fields 把 title 清掉或改掉了：{}",
            source.get("title").unwrap_or(&Value::Null)
        ));
    }
    if source.get("kind").and_then(Value::as_str) != Some("report") {
        return Err("update_fields 把 kind 清掉了".into());
    }
    let written = source
        .get("vec")
        .and_then(Value::as_array)
        .ok_or_else(|| "向量欄位沒寫進去".to_string())?;
    if written.len() != 4 {
        return Err(format!("向量長度應為 4，實際 {}", written.len()));
    }
    if source.get("vec_model_version").and_then(Value::as_str) != Some("test-v1") {
        return Err("model_version 沒寫進去".into());
    }

    // --- 不存在的 id 回 NotFound，不可靜默建立 ---
    let missing = store
        .update_fields(index, "does-not-exist", json!({ "vec": vec4(0.5) }))
        .await;
    match missing {
        Err(storage_core::StorageError::NotFound { .. }) => {}
        other => {
            return Err(format!("對不存在的 id 應回 NotFound，實際 {other:?}"));
        }
    }
    let ghost = store
        .query(SearchQuery::new(index, "doc_id:does-not-exist"))
        .await
        .map_err(|err| format!("查 ghost：{err}"))?;
    if !ghost.hits.is_empty() {
        return Err("update_fields 對不存在的 id 靜默建立了一份文件".into());
    }

    // --- 最近鄰排序：接近的排前面 ---
    store
        .index(document(index, "near", "接近", "report"))
        .await
        .map_err(|err| format!("index near：{err}"))?;
    store
        .index(document(index, "far", "遠離", "report"))
        .await
        .map_err(|err| format!("index far：{err}"))?;
    store
        .update_fields(index, "near", json!({ "vec": vec4(1.0) }))
        .await
        .map_err(|err| format!("update near：{err}"))?;
    store
        .update_fields(index, "far", json!({ "vec": vec4(50.0) }))
        .await
        .map_err(|err| format!("update far：{err}"))?;

    let ranked = store
        .vector_search(VectorSearch {
            index: index.into(),
            field: "vec".into(),
            vector: vec4(1.0),
            k: 3,
            filters: Vec::new(),
        })
        .await
        .map_err(|err| format!("vector_search 排序：{err}"))?;
    if ranked.hits.len() < 2 {
        return Err(format!(
            "至少應回 near 與 far，實際 {} 筆",
            ranked.hits.len()
        ));
    }
    if ranked.hits[0].id != "near" {
        return Err(format!(
            "最近鄰應是 near，實際第一名是 {}",
            ranked.hits[0].id
        ));
    }
    let near_pos = ranked
        .hits
        .iter()
        .position(|h| h.id == "near")
        .ok_or_else(|| "結果裡沒有 near".to_string())?;
    let far_pos = ranked
        .hits
        .iter()
        .position(|h| h.id == "far")
        .ok_or_else(|| "結果裡沒有 far".to_string())?;
    if near_pos >= far_pos {
        return Err(format!(
            "near 應排在 far 前面（near={near_pos}, far={far_pos})"
        ));
    }

    // --- filter 真的排除，不是被忽略 ---
    // 三份文件：兩份向量都接近查詢，但只有一份 kind=report。
    // 另放一份更近、kind=note 的，用來抓「post-filter 在 k=1 時回空」的陷阱。
    store
        .index(document(index, "near-note", "近但不符合", "note"))
        .await
        .map_err(|err| format!("index near-note：{err}"))?;
    store
        .index(document(index, "mid-report", "中距符合", "report"))
        .await
        .map_err(|err| format!("index mid-report：{err}"))?;
    store
        .index(document(index, "far-note", "遠且不符合", "note"))
        .await
        .map_err(|err| format!("index far-note：{err}"))?;
    store
        .update_fields(index, "near-note", json!({ "vec": vec4(20.0) }))
        .await
        .map_err(|err| format!("update near-note：{err}"))?;
    store
        .update_fields(index, "mid-report", json!({ "vec": vec4(21.0) }))
        .await
        .map_err(|err| format!("update mid-report：{err}"))?;
    store
        .update_fields(index, "far-note", json!({ "vec": vec4(40.0) }))
        .await
        .map_err(|err| format!("update far-note：{err}"))?;

    // 查詢向量貼近 near-note（note）。k=1 若做成 post-filter 會回空；
    // native filter 應回 mid-report（符合條件裡最近的）。
    let filter_query = vec4(20.0);
    let filtered = store
        .vector_search(VectorSearch {
            index: index.into(),
            field: "vec".into(),
            vector: filter_query.clone(),
            k: 1,
            filters: vec![SearchFilter::Term {
                field: "kind".into(),
                value: "report".into(),
            }],
        })
        .await
        .map_err(|err| format!("vector_search filter：{err}"))?;
    if filtered.hits.is_empty() {
        return Err(
            "k=1 加 filter 回空。這通常代表 filter 被做成 knn 之後的 post-filter：\
             最近鄰是 note，濾掉後沒東西。應把 filter 放進 knn 子句內"
                .into(),
        );
    }
    if filtered.hits.iter().any(|h| {
        h.source
            .get("kind")
            .and_then(Value::as_str)
            .is_some_and(|k| k != "report")
    }) {
        return Err(format!(
            "filter 被忽略：結果含非 report：{:?}",
            filtered.hits.iter().map(|h| &h.id).collect::<Vec<_>>()
        ));
    }
    if filtered.hits[0].id == "near-note" {
        return Err("filter 沒排除 near-note".into());
    }
    if filtered.hits[0].id != "mid-report" {
        return Err(format!(
            "k=1 + filter=report 應回 mid-report（符合條件裡最近的），實際 {}",
            filtered.hits[0].id
        ));
    }

    let filtered_k3 = store
        .vector_search(VectorSearch {
            index: index.into(),
            field: "vec".into(),
            vector: filter_query,
            k: 5,
            filters: vec![SearchFilter::Term {
                field: "kind".into(),
                value: "report".into(),
            }],
        })
        .await
        .map_err(|err| format!("vector_search filter k=5：{err}"))?;
    if filtered_k3
        .hits
        .iter()
        .any(|h| h.id == "near-note" || h.id == "far-note")
    {
        return Err(format!(
            "filter 沒排除 note：{:?}",
            filtered_k3.hits.iter().map(|h| &h.id).collect::<Vec<_>>()
        ));
    }
    let report_ids: Vec<&str> = filtered_k3.hits.iter().map(|h| h.id.as_str()).collect();
    if !report_ids.contains(&"keep-body") && !report_ids.contains(&"mid-report") {
        return Err(format!(
            "filter 後應至少看到一份 report，實際 {report_ids:?}"
        ));
    }

    Ok(())
}
