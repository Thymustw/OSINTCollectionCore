use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ids::{EntityId, MergeHistoryId};

/// merge 時被改寫的一個 canonical reference。
///
/// # 為什麼要逐筆記，而不是「undo 時再掃一次」
///
/// merge 把指向被併掉 Entity 的參照全部改指向 survivor。要 undo，就必須知道
/// **改之前每一筆是什麼**。merge 完成後資料庫上已經沒有那個資訊了：
/// 事後掃描只能看到「這些列現在指向 survivor」，分不出哪些是 merge 改過來的、
/// 哪些本來就指向 survivor。把後者一起改回去會破壞無關的資料，
/// 而且**不會報錯**，只會讓圖悄悄接錯。
///
/// # 為什麼存 `previous_value` 而不是假設等於 `merged_id`
///
/// 連續 merge 會讓同一列被改寫兩次（A→B，之後 B→C）。undo 第二次 merge 時，
/// 正確的還原值是 B；若靠「還原成這次 merge 的 merged_id」去推，兩次 undo
/// 的順序一旦顛倒就會還原成錯的值。記下當時的實際舊值，undo 與順序無關。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepointedReference {
    /// 被改動的資料表名（`relationships`／`entity_extractions`／`entity_aliases`……）。
    pub table: String,
    /// 被改動那一列的主鍵。
    pub row_id: Uuid,
    /// 被改動的欄位名（`target_object_id`／`entity_id`……）。
    pub column: String,
    /// merge **之前**該欄位的值。undo 就是把它寫回去。
    pub previous_value: Uuid,
}

/// Entity merge 紀錄（SPEC_V0.2 §7）。
///
/// SPEC §7 要求 merge 必須 reversible / audited / preserve history /
/// repoint canonical references safely，Acceptance C 更明確要求
/// 「可以 undo/split merge，而不遺失歷史 evidence」。這個型別是那四件事的落地點：
///
/// | SPEC 要求 | 這裡對應什麼 |
/// |---|---|
/// | audited | `operator`／`reason`／`timestamp`（誰、為什麼、什麼時候） |
/// | repoint safely | `repointed_references`（改了哪些列的哪一欄） |
/// | reversible | 同上，逐筆帶 `previous_value` |
/// | preserve history | `undone_at`——undo **不刪這一列** |
///
/// # `undone_at` 是刻意加的欄位（SPEC §7 沒列）
///
/// 沒有它就只剩兩種做法，兩種都違反 Acceptance C：undo 時刪掉這一列 →
/// 「這兩個 Entity 曾經被合併過又被拆開」這件事整個消失；或是留著不標記 →
/// 查詢分不出哪些 merge 還有效，Entity Merge History 畫面會把已撤銷的
/// merge 顯示成生效中。
///
/// 「是誰 undo 的」不放這裡，走 V0.1 既有的 `audit_log`（migration 0006）——
/// 那張表本來就是記「誰對哪個資源做了什麼」的地方。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeHistory {
    pub id: MergeHistoryId,
    /// 留下來的 canonical Entity。
    pub survivor_id: EntityId,
    /// 被併掉的 Entity（SPEC §7 的 source entity）。
    pub merged_id: EntityId,
    pub reason: String,
    /// 執行者身分。自動合併（`auto_confirmed`）時填服務名。
    pub operator: String,
    pub timestamp: DateTime<Utc>,
    /// merge 改寫過的參照。空陣列代表當時沒有任何列需要 repoint，
    /// **不代表沒記錄**——沒記錄是資料遺失，要當成錯誤處理，不是空陣列。
    pub repointed_references: Vec<RepointedReference>,
    /// 這次 merge 被撤銷的時間。`None` = 仍然生效。
    pub undone_at: Option<DateTime<Utc>>,
}
