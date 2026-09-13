//! 把 [`ResolutionCandidate`] 寫進 canonical store。
//!
//! `StorageError::Conflict`（同一對同一方法已存在）視為已處理：記一行 log、
//! 回 `None`，不讓整次 resolve 失敗。其他錯誤往上傳播。
//!
//! [`ResolverService`] 與 [`GraphContextResolver`] 共用這條路徑，不要各寫一份。
//!
//! [`ResolutionCandidate`]: core_model::ResolutionCandidate
//! [`ResolverService`]: crate::ResolverService
//! [`GraphContextResolver`]: crate::GraphContextResolver

use core_model::ResolutionCandidate;
use storage_core::{RelationalStore, StorageError};
use tracing::info;

use crate::error::ResolverError;

/// 寫入一筆 resolution candidate。
///
/// * `Ok(())` → `Some(candidate)`（這次新寫入）
/// * `StorageError::Conflict` → log info 後 `None`（已存在，沿用既有列）
/// * 其他 storage 錯誤往上傳播
pub async fn persist_candidate<S: RelationalStore>(
    store: &S,
    candidate: ResolutionCandidate,
) -> Result<Option<ResolutionCandidate>, ResolverError> {
    match store.put_resolution_candidate(&candidate).await {
        Ok(()) => Ok(Some(candidate)),
        Err(StorageError::Conflict { message }) => {
            info!(
                entity_a_id = %candidate.entity_a_id,
                entity_b_id = %candidate.entity_b_id,
                method = %candidate.method,
                %message,
                "resolution candidate 已存在，沿用既有列，不中斷本次 resolve"
            );
            Ok(None)
        }
        Err(err) => Err(ResolverError::Storage(err)),
    }
}
