use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::enums::DocumentType;
use crate::ids::DocumentId;

/// Canonical document（SPEC §9）。
///
/// 最後三個欄位（`external_key`／`simhash`／`duplicate_of`）不在 SPEC §9 的欄位清單裡，
/// 是 SPEC §15 去重五階段的落地需求：規格只描述「怎麼判斷重複」，沒有說判斷依據存在哪。
/// 存在 Document 上而不是另開一張表，是因為三者都是「這一份 Document 的屬性」，
/// 且 Stage 1／2／4 的候選查詢要能走索引。全部可為 NULL：deduplicator 還沒處理過的
/// Document 就是三個都空，跟「處理過但不是重複」（`duplicate_of` 為空、另兩個有值）可以區分。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    pub id: DocumentId,
    pub object_type: DocumentType,
    pub schema_version: String,
    pub title: Option<String>,
    pub body: Option<String>,
    pub summary: Option<String>,
    pub language: Option<String>,
    pub author: Option<String>,
    pub published_at: Option<DateTime<Utc>>,
    pub modified_at: Option<DateTime<Utc>>,
    pub observed_at: DateTime<Utc>,
    pub collected_at: DateTime<Utc>,
    pub source_url: Option<String>,
    pub canonical_url: Option<String>,
    pub normalized_content_hash: Option<String>,
    pub confidence: f64,
    pub labels: Vec<String>,
    pub attributes: Value,
    /// Dedup Stage 1 的鍵：`platform|external_id`（兩者都有值時才寫）。由 deduplicator 填。
    #[serde(default)]
    pub external_key: Option<String>,
    /// Dedup Stage 4 的 64-bit SimHash fingerprint。
    ///
    /// 型別是 `i64` 而不是 `u64`：PostgreSQL 的 `BIGINT` 與 SQLite 的 `INTEGER` 都是有號
    /// 64-bit，沒有無號型別。存的是 `u64 as i64` 的位元重解讀，不是數值轉換——
    /// 比較時只做 XOR／popcount，不做大小比較，所以位元保真即可。
    #[serde(default)]
    pub simhash: Option<i64>,
    /// 命中任一 dedup stage 時指向 canonical Document；`None` 代表這份就是 canonical
    /// （或還沒被 deduplicator 處理過）。
    ///
    /// 標記重複用獨立欄位而不是塞進 `labels`：`labels` 是自由字串陣列，沒有外鍵語意，
    /// 也無法直接查「這份的 canonical 是誰」。
    #[serde(default)]
    pub duplicate_of: Option<DocumentId>,
}
