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

    let created = jobs.create("collect", None).await.expect("create job");
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
