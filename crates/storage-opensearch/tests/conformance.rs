use storage_core::conformance::{
    assert_opensearch_identity, assert_search_round_trip, load_workspace_dotenv, required_env,
    verify_not_opencti_search,
};
use storage_opensearch::OpenSearchStore;

#[tokio::test]
async fn opensearch_search_conformance() {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let parsed = verify_not_opencti_search(&url).expect("拒絕 9200 / OpenCTI");
    assert_eq!(parsed.port(), Some(19200), "必須是 osint-core 的 19200");

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
