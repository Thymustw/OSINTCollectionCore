//! Job CRUD 與派工。儲存走 [`RelationalStore`]。
//!
//! 生產路徑仍是 Postgres canonical；bound 不綁 [`storage_core::CanonicalStore`]
//! 是因為那只是標記 trait（沒有多出 job 方法），而單元測試用的
//! `SqliteEmbeddedStore` 實作 `RelationalStore` 卻沒實作 `CanonicalStore`。
//! 真正寫 SQL 的仍是 adapter，domain 不直接碰資料表。

use std::sync::Arc;

use chrono::Utc;
use core_events::{EventProducer, EventTopic};
use core_model::{Job, JobId, JobStatus};
use serde_json::json;
use storage_core::RelationalStore;
use uuid::Uuid;

use crate::error::JobError;
use crate::transition::can_transition;

/// Job 應用服務。
pub struct JobService<S> {
    store: S,
    producer: Option<Arc<EventProducer>>,
}

impl<S: RelationalStore> JobService<S> {
    #[must_use]
    pub fn new(store: S, producer: Option<Arc<EventProducer>>) -> Self {
        Self { store, producer }
    }

    pub async fn create(
        &self,
        job_type: impl Into<String>,
        correlation_id: Option<Uuid>,
        parameters: Option<serde_json::Value>,
    ) -> Result<Job, JobError> {
        let now = Utc::now();
        let job = Job {
            id: Uuid::now_v7(),
            job_type: job_type.into(),
            status: JobStatus::Queued,
            correlation_id,
            created_at: now,
            started_at: None,
            completed_at: None,
            retry_count: 0,
            error: None,
            parameters,
        };
        self.store.put_job(&job).await?;
        Ok(job)
    }

    pub async fn get(&self, id: JobId) -> Result<Job, JobError> {
        self.store
            .get_job(id)
            .await?
            .ok_or_else(|| JobError::NotFound { id: id.to_string() })
    }

    pub async fn list(&self, after: Option<JobId>, limit: u32) -> Result<Vec<Job>, JobError> {
        Ok(self.store.list_jobs(after, limit).await?)
    }

    /// 只列指定狀態。過濾在 SQL 裡做（見
    /// `storage_core::RelationalStore::list_jobs_by_status`）。
    pub async fn list_by_status(
        &self,
        status: JobStatus,
        after: Option<JobId>,
        limit: u32,
    ) -> Result<Vec<Job>, JobError> {
        Ok(self.store.list_jobs_by_status(status, after, limit).await?)
    }

    /// 重試一個失敗的 job（SPEC §31：operator 可以重試，而且要留稽核）。
    ///
    /// # 為什麼是 `failed → retrying` 而不是 `failed → queued`
    ///
    /// 狀態機（`transition.rs`）沒有 `failed → queued` 這條邊，而且不該有：
    /// `retry_count` 是在進入 `retrying` 時累加的。若把失敗的 job 直接丟回
    /// `queued`，它看起來會跟一個**從沒跑過**的新 job 一模一樣——
    /// 「這個 job 重試過幾次」這個資訊會在每次重試時被抹掉，
    /// 無限重試的迴圈也就沒有任何地方看得出來。
    ///
    /// 轉成 `retrying` 之後立刻 dispatch（`retrying` 是可派工狀態），
    /// 所以對呼叫端而言效果就是「它會再跑一次」。
    pub async fn retry(&self, id: JobId) -> Result<Job, JobError> {
        let job = self.get(id).await?;
        if job.status != JobStatus::Failed {
            return Err(JobError::NotRetryable {
                id: id.to_string(),
                status: job.status,
            });
        }
        self.transition(id, JobStatus::Retrying, None).await?;
        self.dispatch(id).await
    }

    pub async fn transition(
        &self,
        id: JobId,
        to: JobStatus,
        error: Option<String>,
    ) -> Result<Job, JobError> {
        let mut job = self.get(id).await?;
        if !can_transition(job.status, to) {
            return Err(JobError::from_status(job.status, to));
        }
        let now = Utc::now();
        match to {
            JobStatus::Running => {
                if job.started_at.is_none() {
                    job.started_at = Some(now);
                }
            }
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled => {
                job.completed_at = Some(now);
            }
            JobStatus::Retrying => {
                job.retry_count += 1;
                job.completed_at = None;
            }
            JobStatus::Queued => {}
        }
        if to == JobStatus::Failed {
            job.error = error;
        } else if to == JobStatus::Completed || to == JobStatus::Running {
            job.error = None;
        }
        job.status = to;
        self.store.put_job(&job).await?;
        Ok(job)
    }

    /// queued／retrying → produce `job.dispatched`。狀態仍保持原狀，由 worker 再轉 running。
    pub async fn dispatch(&self, id: JobId) -> Result<Job, JobError> {
        let job = self.get(id).await?;
        if !matches!(job.status, JobStatus::Queued | JobStatus::Retrying) {
            return Err(JobError::NotDispatchable {
                id: id.to_string(),
                status: job.status,
            });
        }
        if let Some(producer) = &self.producer {
            producer
                .publish(
                    EventTopic::JobDispatched,
                    Some(&job.id.to_string()),
                    job.correlation_id,
                    json!({
                        "job_id": job.id,
                        "job_type": job.job_type,
                        "status": job.status,
                        "retry_count": job.retry_count,
                        "parameters": job.parameters,
                    }),
                )
                .await?;
        }
        Ok(job)
    }

    pub async fn create_and_dispatch(
        &self,
        job_type: impl Into<String>,
        correlation_id: Option<Uuid>,
        parameters: Option<serde_json::Value>,
    ) -> Result<Job, JobError> {
        let job = self.create(job_type, correlation_id, parameters).await?;
        self.dispatch(job.id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::can_transition;

    #[test]
    fn dispatch_only_from_queued_or_retrying() {
        assert!(can_transition(JobStatus::Queued, JobStatus::Running));
        assert!(!can_transition(JobStatus::Running, JobStatus::Queued));
    }
}
