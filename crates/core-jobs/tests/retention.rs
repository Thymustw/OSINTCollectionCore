//! JobRetention：用一次性 SQLite 檔驗證規則、dry-run、天數 0、批次上限。
//! 不連 Postgres，避免跟共用開發 DB 的 leftover 糾纏。

use chrono::{TimeZone, Utc};
use core_config::RetentionJobsConfig;
use core_jobs::JobRetention;
use core_model::{Job, JobStatus};
use storage_core::conformance::find_workspace_root;
use storage_core::{JobPurgeFilter, RelationalStore};
use storage_sqlite::SqliteEmbeddedStore;
use uuid::Uuid;

async fn temp_store() -> (SqliteEmbeddedStore, std::path::PathBuf) {
    let root = find_workspace_root().expect("workspace root");
    let path = root.join(format!(
        "var/osint-job-retention-{}.sqlite",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let store = SqliteEmbeddedStore::connect(&path)
        .await
        .expect("開 SQLite");
    store.migrate().await.expect("migrate");
    (store, path)
}

fn cleanup(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn job(
    job_type: &str,
    status: JobStatus,
    completed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Job {
    let created = completed_at.unwrap_or_else(Utc::now);
    Job {
        id: Uuid::now_v7(),
        job_type: job_type.into(),
        status,
        correlation_id: None,
        created_at: created,
        started_at: completed_at,
        completed_at,
        retry_count: 0,
        error: None,
        parameters: None,
    }
}

#[tokio::test]
async fn days_zero_skips_that_rule() {
    let (store, path) = temp_store().await;
    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let failed = job("collect", JobStatus::Failed, Some(old));
    let completed = job("collect", JobStatus::Completed, Some(old));
    store.put_job(&failed).await.expect("put failed");
    store.put_job(&completed).await.expect("put completed");

    let retention = JobRetention::new(RetentionJobsConfig {
        collect_failed_days: 0,
        collect_completed_days: 7,
        collect_cancelled_days: 0,
        batch_pause_ms: 0,
        ..Default::default()
    });
    let rules = retention.rules();
    assert_eq!(rules.len(), 1, "天數 0 的規則不該列入：{rules:?}");
    assert_eq!(rules[0].status, JobStatus::Completed);

    let report = retention.run_once(&store, false).await.expect("run_once");
    assert_eq!(report.deleted, 1);
    assert!(store.get_job(failed.id).await.expect("get").is_some());
    assert!(store.get_job(completed.id).await.expect("get").is_none());
    drop(store);
    cleanup(&path);
}

#[tokio::test]
async fn dry_run_counts_but_does_not_delete() {
    let (store, path) = temp_store().await;
    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let failed = job("collect", JobStatus::Failed, Some(old));
    store.put_job(&failed).await.expect("put");

    let retention = JobRetention::new(RetentionJobsConfig {
        collect_completed_days: 0,
        collect_cancelled_days: 0,
        collect_failed_days: 30,
        batch_pause_ms: 0,
        ..Default::default()
    });
    let report = retention.run_once(&store, true).await.expect("dry_run");
    assert!(report.dry_run);
    assert_eq!(report.deleted, 0);
    assert_eq!(report.counted, 1);
    assert!(store.get_job(failed.id).await.expect("get").is_some());
    drop(store);
    cleanup(&path);
}

#[tokio::test]
async fn other_job_types_are_not_purged_by_config_rules() {
    let (store, path) = temp_store().await;
    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let collect = job("collect", JobStatus::Failed, Some(old));
    let stix = job("stix_export", JobStatus::Failed, Some(old));
    store.put_job(&collect).await.expect("put collect");
    store.put_job(&stix).await.expect("put stix");

    let retention = JobRetention::new(RetentionJobsConfig {
        collect_completed_days: 0,
        collect_cancelled_days: 0,
        collect_failed_days: 30,
        batch_pause_ms: 0,
        ..Default::default()
    });
    let report = retention.run_once(&store, false).await.expect("run_once");
    assert_eq!(report.deleted, 1);
    assert!(store.get_job(collect.id).await.expect("get").is_none());
    assert!(store.get_job(stix.id).await.expect("get").is_some());
    drop(store);
    cleanup(&path);
}

#[tokio::test]
async fn max_batches_per_run_truncates() {
    let (store, path) = temp_store().await;
    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let mut ids = Vec::new();
    for _ in 0..5 {
        let j = job("collect", JobStatus::Failed, Some(old));
        ids.push(j.id);
        store.put_job(&j).await.expect("put");
    }

    let retention = JobRetention::new(RetentionJobsConfig {
        collect_completed_days: 0,
        collect_cancelled_days: 0,
        collect_failed_days: 30,
        batch: 2,
        max_batches_per_run: 1,
        batch_pause_ms: 0,
        ..Default::default()
    });
    let report = retention.run_once(&store, false).await.expect("run_once");
    assert!(report.truncated, "一批 2 列、上限 1 批，應截斷：{report:?}");
    assert_eq!(report.deleted, 2);
    assert_eq!(report.batches, 1);

    let mut remaining = 0u64;
    for id in ids {
        if store.get_job(id).await.expect("get").is_some() {
            remaining += 1;
        }
    }
    assert_eq!(remaining, 3);
    drop(store);
    cleanup(&path);
}

#[tokio::test]
async fn custom_filter_respects_type() {
    let (store, path) = temp_store().await;
    let old = Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap();
    let cutoff = Utc.with_ymd_and_hms(2020, 6, 1, 0, 0, 0).unwrap();
    let a = job("purge_test_custom", JobStatus::Failed, Some(old));
    let b = job("collect", JobStatus::Failed, Some(old));
    store.put_job(&a).await.expect("put a");
    store.put_job(&b).await.expect("put b");

    let retention = JobRetention::new(RetentionJobsConfig {
        batch_pause_ms: 0,
        ..Default::default()
    });
    let filter = JobPurgeFilter {
        job_type: "purge_test_custom".into(),
        statuses: vec![JobStatus::Failed],
        older_than: cutoff,
        limit: 5000,
    };
    let report = retention
        .run_custom(&store, &filter, false, 30)
        .await
        .expect("run_custom");
    assert_eq!(report.deleted, 1);
    assert!(store.get_job(a.id).await.expect("get").is_none());
    assert!(store.get_job(b.id).await.expect("get").is_some());
    drop(store);
    cleanup(&path);
}
