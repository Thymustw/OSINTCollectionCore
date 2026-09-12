use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::FailedEventId;

/// 永久失敗、無法處理的一則 broker 事件（ADR-008 指定的 V0.2 落地形式）。
///
/// # 為什麼是 canonical 表而不是 Redpanda topic
///
/// ADR-008 的結論：DLQ 不是一個 topic，是一整套子系統（紀錄 → 保留 → 重放 →
/// 監控）。只開一個 topic 會得到最危險的形狀——失敗看起來被記下來了，實際上
/// 沒有人重放，過了 retention 就靜靜消失。PostgreSQL 本來就是 canonical truth
/// （`CLAUDE.md` §5），失敗事件放這裡才查得到、留得住、重放得了。
///
/// V0.1 的行為（ADR-008）是：永久失敗只寫 error log，**offset 照常 commit**，
/// 避免一則毒訊息卡死整個 partition。那個 log-only 的缺口就是這張表要補的。
///
/// # 一則事件只有一列
///
/// `(topic, partition, offset)` 是 broker 上一則訊息的完整座標，也是這張表的
/// 自然鍵。同一則事件重試三次失敗不該變成三列——那會讓「有幾則事件壞掉」
/// 與「壞掉的事件被試了幾次」混在一起。重複寫入改成累加 `attempt_count`
/// 並更新 `last_seen`（見 `RelationalStore::put_failed_event`）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FailedEvent {
    pub id: FailedEventId,
    pub topic: String,
    pub partition: i32,
    /// broker offset。Kafka／Redpanda 的 offset 是 64-bit，用 `i32` 會在長期執行的
    /// topic 上溢位。
    pub offset: i64,
    /// 哪個 consumer group 處理失敗的。同一則事件可能被多個 group 讀到，
    /// 但只有失敗的那個會寫進來。
    pub consumer_group: String,
    /// 人看得懂的失敗原因。要能讓運維判斷「這值不值得重放」，
    /// 只寫 `processing failed` 等於讓 ADR-008 的手動復原路徑走不通。
    pub failure_reason: String,
    /// 這則事件被嘗試處理過幾次。第一次寫入通常是 1。
    pub attempt_count: i32,
    /// 原始 `EventEnvelope` 的 JSON。重放靠它，不是靠回頭去 broker 撈——
    /// 撈得到與否取決於 retention，而 retention 本來就是 ADR-008 要避開的坑。
    pub envelope: Value,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
    /// 重放成功的時間。`None` = 還躺在那裡沒被處理。
    pub replayed_at: Option<DateTime<Utc>>,
}
