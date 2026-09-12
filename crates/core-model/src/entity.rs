use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::EntityType;
use crate::ids::EntityId;

/// Entity（SPEC §10）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    pub id: EntityId,
    pub entity_type: EntityType,
    pub name: String,
    pub normalized_name: String,
    pub description: Option<String>,
    pub confidence: f64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    /// 這個 Entity 是否已被 merge 併掉。`Some(survivor_id)` = 已併掉，指向留下來的
    /// canonical Entity；`None` = 仍是獨立實體。
    ///
    /// merge 不刪除被併掉的 Entity 列——`resolution_candidates`／`entity_extractions`
    /// 都有 FK 指向 `entities(id)`（無 `ON DELETE CASCADE`），刪除會違反外鍵；
    /// 且 undo 需要這一列存在才能把參照寫回去。查詢一般 Entity 列表時應該過濾掉
    /// `merged_into IS NOT NULL` 的列（那是之後 API 層的責任，這裡只提供欄位）。
    pub merged_into: Option<EntityId>,
    pub attributes: Value,
}
