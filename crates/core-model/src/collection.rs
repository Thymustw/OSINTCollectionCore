use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{CollectionId, WorkspaceId};

/// Collection（SPEC §7）。
///
/// sources / connectors / objects 的多對多關係在關聯表，不內嵌在這個 struct。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Collection {
    pub id: CollectionId,
    pub workspace_id: Option<WorkspaceId>,
    pub name: String,
    pub description: Option<String>,
    pub status: String,
    pub priority: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
