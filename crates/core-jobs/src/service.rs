//! Job CRUD 與派工。儲存走 CanonicalStore。

use std::sync::Arc;

use chrono::Utc;
use core_events::{EventProducer, EventTopic};
use core_model::{Job, JobId, JobStatus};
use serde_json::json;
use storage_core::CanonicalStore;
use uuid::Uuid;

use crate::error::JobError;
use crate::transition::can_transition;

/// Job 應用服務。
pub struct JobService<S> {
    store: S,
    producer: Option<Arc<EventProducer>>,
}

impl<S: CanonicalStore> JobService<S> {
    #[must_use]
    pub fn new(store: S, producer: Option<Arc<EventProducer>>) -> Self {
        Self { store, producer }
    }

    pub async fn create(
        &self,
        job_type: impl Into<String>,
        correlation_id: Option<Uuid>,
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
    ) -> Result<Job, JobError> {
        let job = self.create(job_type, correlation_id).await?;
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
