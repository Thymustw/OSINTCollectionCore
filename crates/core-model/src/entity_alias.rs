use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{EntityAliasId, EntityId, SourceId};

/// Entity 別名（SPEC_V0.2 §3）。
///
/// 「Microsoft」／「Microsoft Corporation」／「微軟」是同一個 Entity 的三個 alias，
/// 這張表是 SPEC §6「alias」這條 resolution method 的資料來源。
///
/// 規格未列 `id`；資料表需要主鍵，因此補 UUID v7（同 `EntityExtraction` 的處理）。
///
/// ⚠️ `alias_type` 維持 `String`。SPEC §3 沒有列舉可用值，照 crate 既有慣例
/// （見 `lib.rs` 模組註解）不發明狀態機。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityAlias {
    pub id: EntityAliasId,
    pub entity_id: EntityId,
    pub alias: String,
    pub alias_type: String,
    /// 這個 alias 是從哪個 Source 看到的。
    ///
    /// 可為 `None`：resolver 自己推導出來的 alias（正規化變體、merge 時從被併掉的
    /// Entity 搬過來的名字）沒有單一 Source 可指。設成必填只會逼呼叫端塞假值。
    pub source_id: Option<SourceId>,
    pub confidence: f64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}
