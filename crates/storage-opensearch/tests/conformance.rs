use storage_core::conformance::{
    assert_bulk_upsert_preserves_unknown_fields, assert_opensearch_identity,
    assert_projection_store_contract, assert_search_round_trip, assert_structured_search,
    load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_opensearch::OpenSearchStore;

#[tokio::test]
async fn opensearch_search_conformance() {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    // 埠號本身不代表身分（本機 9200 是 OpenCTI，但 CI 等其他環境的 9200 就是我們自己的
    // OpenSearch）——只在本機開 OSINT_STRICT_PORT_ISOLATION 時才會擋 9200，實際身分一律
    // 以下面連線後的 assert_opensearch_identity 為準。
    let _parsed = verify_not_opencti_search(&url).expect("URL 格式或本機嚴格模式檢查失敗");

    let store = OpenSearchStore::connect(&url)
        .expect("建立 client")
        .with_refresh_on_write(true);

    let info = store.cluster_info().await.expect("GET /");
    eprintln!(
        "opensearch identity: cluster_name={:?} name={:?} distribution={:?} tagline={:?}",
        info.get("cluster_name"),
        info.get("name"),
        info.pointer("/version/distribution"),
        info.get("tagline")
    );
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");

    let index = format!("osint-core-conformance-{}", uuid::Uuid::now_v7());
    store.ensure_index(&index).await.expect("ensure index");
    assert_search_round_trip(&store, &index)
        .await
        .expect("search round-trip");
    // indexer 與 embedding-worker 共用同一份文件：upsert 只能覆寫自己知道的欄位。
    assert_bulk_upsert_preserves_unknown_fields(&store, &index)
        .await
        .expect("bulk upsert 不清空未知欄位");
    // StructuredSearch 是面向使用者的那條路徑（過濾、NOT、search_after）。
    // 未來新增的 SearchStore adapter 也要通過這一支。
    //
    // 用**明確 mapping** 的另一個 index：dynamic mapping 會把 `doc_id` 猜成 text，
    // 而 text 欄位不能排序（400 illegal_argument_exception），search_after 就驗不到。
    // 這正是 indexer 關掉 dynamic mapping 的理由之一。
    let structured_index = format!("osint-core-conformance-structured-{}", uuid::Uuid::now_v7());
    store
        .ensure_index_with(
            &structured_index,
            &serde_json::json!({ "number_of_shards": 1, "number_of_replicas": 0 }),
            &serde_json::json!({
                "properties": {
                    "doc_id": { "type": "keyword" },
                    "title": { "type": "text" },
                    "kind": { "type": "keyword" },
                }
            }),
        )
        .await
        .expect("建立 structured conformance index");
    assert_structured_search(&store, &structured_index)
        .await
        .expect("structured search");
    store
        .delete_index(&structured_index)
        .await
        .expect("刪除 structured 測試 index");

    // 一次性 index 用完刪掉，免得叢集慢慢長出幾百個 conformance index。
    store.delete_index(&index).await.expect("刪除測試 index");
}

/// `ProjectionStore`（V0.2 Phase 0f）：checkpoint／lag／rebuild 狀態／reset。
///
/// 狀態 index 用 **per-run 名稱**而不是正式的 `osint-projection-state`：
/// 這一支會 `reset_projection`，跑在正式那個 index 上等於把真的 indexer 進度清掉。
/// 用完整個刪除（`CLAUDE.md` §15：測試不可以留下 index）。
#[tokio::test]
async fn opensearch_projection_store_conformance() {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let _parsed = verify_not_opencti_search(&url).expect("URL 格式或本機嚴格模式檢查失敗");

    let state_index = format!("osint-core-conformance-state-{}", uuid::Uuid::now_v7());
    let store = OpenSearchStore::connect(&url)
        .expect("建立 client")
        .with_projection_state_index(state_index.as_str());

    let info = store.cluster_info().await.expect("GET /");
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");
    assert_eq!(store.projection_state_index(), state_index);

    let projection = format!("osint-conformance-projection-{}", uuid::Uuid::now_v7());
    let result = assert_projection_store_contract(&store, &projection).await;

    // 先刪 index 再 unwrap：契約失敗時也不要留下 index。
    store
        .delete_index(&state_index)
        .await
        .expect("刪除投影狀態測試 index");
    result.expect("ProjectionStore 契約");
}
