use storage_core::conformance::{
    assert_opensearch_identity, assert_search_round_trip, load_workspace_dotenv, required_env,
    verify_not_opencti_search,
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
}
