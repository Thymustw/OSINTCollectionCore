//! collect job 保留期限。單次 `run_once` 只刪有限批次，迴圈與暫停都在這裡，
//! adapter 的 `purge_jobs` 不做無界 DELETE。

use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use core_config::RetentionJobsConfig;
use core_model::JobStatus;
use serde::Serialize;
use storage_core::{JobPurgeFilter, RelationalStore, StorageError};

/// 一條設定規則對應一次清除範圍。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PurgeRule {
    pub job_type: String,
    pub status: JobStatus,
    pub days: u32,
}

/// 單條規則的執行結果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PurgeRuleReport {
    pub job_type: String,
    pub statuses: Vec<JobStatus>,
    pub days: u32,
    /// 實際刪除列數。dry-run 永遠是 0。
    pub deleted: u64,
    /// dry-run 時是符合條件的總列數（不受 batch limit 限制）；
    /// 實際刪除時等於 `deleted`。
    pub counted: u64,
}

/// `JobRetention::run_once`／自訂 filter 的彙總。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PurgeReport {
    pub dry_run: bool,
    pub deleted: u64,
    pub counted: u64,
    pub batches: u32,
    /// 達到 `max_batches_per_run` 就停，可能還有列沒清。
    pub truncated: bool,
    pub rules: Vec<PurgeRuleReport>,
}

/// 依 `[retention.jobs]` 分批清除終態 collect job。
pub struct JobRetention {
    config: RetentionJobsConfig,
}

impl JobRetention {
    #[must_use]
    pub fn new(config: RetentionJobsConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn config(&self) -> &RetentionJobsConfig {
        &self.config
    }

    /// 天數 `0` 的規則不列入。順序：completed → failed → cancelled。
    #[must_use]
    pub fn rules(&self) -> Vec<PurgeRule> {
        let mut rules = Vec::new();
        push_collect_rule(
            &mut rules,
            JobStatus::Completed,
            self.config.collect_completed_days,
        );
        push_collect_rule(
            &mut rules,
            JobStatus::Failed,
            self.config.collect_failed_days,
        );
        push_collect_rule(
            &mut rules,
            JobStatus::Cancelled,
            self.config.collect_cancelled_days,
        );
        rules
    }

    /// 套用設定裡的規則。dry-run 只計數、不刪。
    pub async fn run_once(
        &self,
        store: &(dyn RelationalStore + Send + Sync),
        dry_run: bool,
    ) -> Result<PurgeReport, StorageError> {
        let mut report = PurgeReport {
            dry_run,
            deleted: 0,
            counted: 0,
            batches: 0,
            truncated: false,
            rules: Vec::new(),
        };
        let now = Utc::now();
        for rule in self.rules() {
            if report.truncated {
                break;
            }
            let older_than = now - ChronoDuration::days(i64::from(rule.days));
            let filter = JobPurgeFilter {
                job_type: rule.job_type.clone(),
                statuses: vec![rule.status],
                older_than,
                limit: self.config.batch,
            };
            let rule_report = self
                .run_filter(store, &filter, dry_run, Some(rule.days), &mut report)
                .await?;
            report.deleted += rule_report.deleted;
            report.counted += rule_report.counted;
            report.rules.push(rule_report);
        }
        Ok(report)
    }

    /// 自訂範圍。`days` 只寫進報告（API 的 `older_than_days`）；排程路徑傳 `Some`。
    pub async fn run_custom(
        &self,
        store: &(dyn RelationalStore + Send + Sync),
        filter: &JobPurgeFilter,
        dry_run: bool,
        days: u32,
    ) -> Result<PurgeReport, StorageError> {
        let mut report = PurgeReport {
            dry_run,
            deleted: 0,
            counted: 0,
            batches: 0,
            truncated: false,
            rules: Vec::new(),
        };
        let rule_report = self
            .run_filter(store, filter, dry_run, Some(days), &mut report)
            .await?;
        report.deleted = rule_report.deleted;
        report.counted = rule_report.counted;
        report.rules.push(rule_report);
        Ok(report)
    }

    async fn run_filter(
        &self,
        store: &(dyn RelationalStore + Send + Sync),
        filter: &JobPurgeFilter,
        dry_run: bool,
        days: Option<u32>,
        report: &mut PurgeReport,
    ) -> Result<PurgeRuleReport, StorageError> {
        let days = days.unwrap_or(0);
        if dry_run {
            let counted = store.count_purgeable_jobs(filter).await?;
            return Ok(PurgeRuleReport {
                job_type: filter.job_type.clone(),
                statuses: filter.statuses.clone(),
                days,
                deleted: 0,
                counted,
            });
        }

        let mut deleted = 0u64;
        loop {
            if report.batches >= self.config.max_batches_per_run {
                report.truncated = true;
                break;
            }
            let n = store.purge_jobs(filter).await?;
            report.batches += 1;
            deleted += n;
            if n == 0 {
                break;
            }
            if n < u64::from(filter.limit) {
                break;
            }
            self.pause_between_batches().await;
        }
        Ok(PurgeRuleReport {
            job_type: filter.job_type.clone(),
            statuses: filter.statuses.clone(),
            days,
            counted: deleted,
            deleted,
        })
    }

    async fn pause_between_batches(&self) {
        if self.config.batch_pause_ms == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(self.config.batch_pause_ms)).await;
    }
}

fn push_collect_rule(rules: &mut Vec<PurgeRule>, status: JobStatus, days: u32) {
    if days == 0 {
        return;
    }
    rules.push(PurgeRule {
        job_type: "collect".into(),
        status,
        days,
    });
}
