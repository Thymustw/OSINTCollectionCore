use storage_core::ProjectionStore;
use storage_core::conformance::{
    assert_graph_store_contract, assert_projection_store_contract, load_workspace_dotenv,
    required_env,
};
use storage_neo4j::Neo4jStore;
use uuid::Uuid;

#[tokio::test]
async fn neo4j_graph_and_projection_conformance() {
    load_workspace_dotenv();
    let uri = required_env("NEO4J_URI").expect("NEO4J_URI");
    let user = required_env("NEO4J_USER").expect("NEO4J_USER");
    let password = required_env("NEO4J_PASSWORD").expect("NEO4J_PASSWORD");
    assert!(
        uri.contains("127.0.0.1") || uri.contains("localhost"),
        "conformance 只連本機 Neo4j，實際 URI host 不像本機：{}",
        storage_core::StorageError::sanitize(&uri)
    );

    let store = Neo4jStore::connect(&uri, &user, &password, 5)
        .await
        .expect("連 Neo4j");

    let graph = assert_graph_store_contract(&store).await;

    let projection = format!("osint-conformance-graph-{}", Uuid::now_v7());
    let proj = assert_projection_store_contract(&store, &projection).await;
    let _ = store.reset_projection(&projection).await;

    graph.expect("GraphStore 契約");
    proj.expect("ProjectionStore 契約");
}
