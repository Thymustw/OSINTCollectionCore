//! 對本機 Postgres 跑 Job CRUD 與狀態轉換。

use core_jobs::JobService;
use core_model::JobStatus;
use storage_core::conformance::{load_workspace_dotenv, required_env};
use storage_postgres::PostgresCanonicalStore;

#[tokio::test]
async fn create_transition_list() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    assert!(
        dsn.contains("127.0.0.1") || dsn.contains("localhost"),
        "只連本機 Postgres"
    );
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres。請確認 osint-core-postgres-1 在跑");
    store.migrate().await.expect("migrate");
    let jobs = JobService::new(store, None);

    let created = jobs
        .create("collect", None, None)
        .await
        .expect("create job");
    assert!(created.parameters.is_none());

    let with_params = jobs
        .create(
            "stix_import",
            None,
            Some(serde_json::json!({"source_id": created.id})),
        )
        .await
        .expect("create job with parameters");
    let loaded = jobs.get(with_params.id).await.expect("reload");
    assert_eq!(loaded.parameters, with_params.parameters);
    assert_eq!(created.status, JobStatus::Queued);

    let listed = jobs.list(None, 20).await.expect("list");
    assert!(listed.iter().any(|j| j.id == created.id));

    let running = jobs
        .transition(created.id, JobStatus::Running, None)
        .await
        .expect("queued → running");
    assert_eq!(running.status, JobStatus::Running);
    assert!(running.started_at.is_some());

    let done = jobs
        .transition(created.id, JobStatus::Completed, None)
        .await
        .expect("running → completed");
    assert_eq!(done.status, JobStatus::Completed);
    assert!(done.completed_at.is_some());

    let err = jobs
        .transition(created.id, JobStatus::Running, None)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("不可從"), "{err}");
}

#[tokio::test]
async fn merge_parameters_preserves_existing_fields() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    let jobs = JobService::new(store, None);

    let created = jobs
        .create(
            "stix_export",
            None,
            Some(serde_json::json!({
                "filter": {"entity_types": ["person"]}
            })),
        )
        .await
        .expect("create");
    let key = format!("stix-exports/{}.json", created.id);
    let merged = jobs
        .merge_parameters(
            created.id,
            serde_json::json!({ "result_object_key": key.clone() }),
        )
        .await
        .expect("merge");
    let params = merged.parameters.expect("parameters");
    assert_eq!(
        params["filter"]["entity_types"],
        serde_json::json!(["person"])
    );
    assert_eq!(params["result_object_key"], serde_json::json!(key));
}

#[tokio::test]
async fn merge_parameters_starts_from_empty_when_none() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    let jobs = JobService::new(store, None);

    let created = jobs
        .create("stix_export", None, None)
        .await
        .expect("create");
    assert!(created.parameters.is_none());
    let merged = jobs
        .merge_parameters(created.id, serde_json::json!({ "result_object_key": "k" }))
        .await
        .expect("merge");
    let params = merged.parameters.expect("parameters");
    assert_eq!(params, serde_json::json!({ "result_object_key": "k" }));
}

#[tokio::test]
async fn merge_parameters_unknown_id_is_not_found() {
    load_workspace_dotenv();
    let dsn = required_env("DATABASE_URL").expect("DATABASE_URL");
    let store = PostgresCanonicalStore::connect(&dsn, 5)
        .await
        .expect("連 Postgres");
    store.migrate().await.expect("migrate");
    let jobs = JobService::new(store, None);

    let err = jobs
        .merge_parameters(uuid::Uuid::now_v7(), serde_json::json!({}))
        .await
        .expect_err("不存在的 id 應 NotFound");
    assert!(err.to_string().contains("找不到 job"), "{err}");
}
