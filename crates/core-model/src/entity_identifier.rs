use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{EntityId, EntityIdentifierId, SourceId};

/// Entity 識別碼（SPEC_V0.2 §4）。
///
/// 與 [`crate::entity_alias::EntityAlias`] 的差別是「唯一性」：alias 是人看的名字，
/// 同一個名字可以屬於很多 Entity；identifier 是**在某個命名空間內唯一**的鍵
/// （domain、email、github_username、CPE、CVE、公司登記號……），
/// 這正是 SPEC §6「exact identifier」這條 resolution method 的依據。
///
/// 規格未列 `id`；資料表需要主鍵，因此補 UUID v7。
///
/// # `namespace` 要解決什麼
///
/// V0.1 實作報告 T10 記錄了一個已接受的限制：**40 位 hex 沒辦法區分 SHA-1 與
/// git commit**，entity-worker 只能標 `ambiguous_sha1`（confidence 0.7）。
/// 值本身不帶任何型別資訊，是那個歧義的根源。`namespace` 就是把型別搬到值外面：
/// `("sha1", "<40 hex>")` 與 `("git_commit", "<40 hex>")` 是兩個不同的識別碼，
/// 不會互相誤判成同一個 Entity。
///
/// ⚠️ V0.2 Phase 0 **只把欄位留著**，entity-worker 還沒有寫入者，
/// T10 的歧義也還沒因此消失——要真的解決，抽取端必須知道自己在抽哪一種雜湊。
///
/// # `value` vs `normalized_value`
///
/// `value` 是原樣看到的字串（`Example.COM`、`User@Example.com`），
/// `normalized_value` 是比對用的正規化形式（小寫、去掉 IDN／IPv6 的書寫差異等）。
/// **正規化由呼叫端負責**，理由同 `Entity::normalized_name`：把折疊塞進 SQL
/// 會走不到索引，而且 PG 與 SQLite 的 collation 規則不同，等於有兩種語意。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityIdentifier {
    pub id: EntityIdentifierId,
    pub entity_id: EntityId,
    pub namespace: String,
    pub value: String,
    pub normalized_value: String,
    pub confidence: f64,
    /// 這個識別碼是從哪個 Source 看到的。可為 `None`，理由同
    /// [`crate::entity_alias::EntityAlias::source_id`]。
    pub source_id: Option<SourceId>,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}
