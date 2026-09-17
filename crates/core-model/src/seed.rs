use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::enums::{SeedOrigin, SeedType};
use crate::ids::{CollectionId, EntityId, SeedId};

/// Discovery 的起點（SPEC_V0.3 §2）。
///
/// `status` 刻意維持 `String`：SPEC §2 列了 `seed_type`（15 種）與 `origin`
/// （7 種）的封閉清單，但完全沒有列出 `status` 的可能值——比照
/// `core-model/src/enums.rs` 檔頭的既有原則「規格沒列值的 status 維持字串，
/// 避免發明狀態機」，這裡不要自己猜一組 pending/processed/expired 之類的值。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Seed {
    pub id: SeedId,
    /// `None`：這個 seed 不屬於任何特定 collection（例如手動輸入、還沒分類）。
    pub collection_id: Option<CollectionId>,
    pub seed_type: SeedType,
    pub value: String,
    /// `None`：這個 seed 還沒被解析成一個已知 Entity（例如一個裸的 keyword）。
    pub entity_id: Option<EntityId>,
    pub priority: i32,
    pub confidence: f64,
    pub origin: SeedOrigin,
    pub status: String,
    pub depth: i32,
    pub created_at: DateTime<Utc>,
}
