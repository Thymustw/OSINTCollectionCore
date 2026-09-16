use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::ids::{EntityId, MergeHistoryId, RelationshipEvidenceId, RelationshipId};
use crate::relationship::Relationship;

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
    /// merge 時因 relationship UNIQUE 撞號而被吸收合併或自迴圈刪除的 relationship。
    /// 空陣列代表「這次 merge 沒有任何 relationship 撞號」，不是沒記錄。
    ///
    /// `RepointedReference` 只能表達「單欄位從 A 改成 B」。撞號時整列被刪、
    /// evidence 搬去別的列，那個模型寫不進去；不另記的話 undo 無法重建被刪的
    /// relationship，而且**不會報錯**，圖上只是少幾條邊。
    pub merged_relationships: Vec<MergedRelationship>,
    /// 這次 merge 被撤銷的時間。`None` = 仍然生效。
    pub undone_at: Option<DateTime<Utc>>,
    /// ADR-012 自動核准的完整稽核記錄。`None` = 人工 merge（既有資料全部是
    /// `None`，向下相容）。非 `None` 時的 JSON schema 由 `resolver::auto_approval`
    /// 定義（這個 crate 不 import 那個 crate，避免循環相依，所以型別是
    /// `serde_json::Value` 而不是強型別 struct）。
    pub auto_approval_audit: Option<serde_json::Value>,
}

/// merge 時因 relationship 的 `(source, type, target)` UNIQUE 撞號而被吸收合併時，
/// absorber（留下來那條）在合併前的可變欄位快照。undo 時用來還原。
///
/// 不記的話 undo 只能看到合併後的 `evidence_count`／`confidence`／時間窗，
/// 分不出哪些是吸收進來的、哪些本來就在 absorber 上。把吸收後的值留著不還原，
/// 等於同一批 evidence 被算兩次。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AbsorberSnapshot {
    pub evidence_count: i32,
    pub confidence: f64,
    pub first_seen: DateTime<Utc>,
    pub last_seen: DateTime<Utc>,
}

/// merge 時因撞號被刪除的一條 relationship（`RepointedReference` 的「單欄位還原」
/// 模型無法表達「整列被刪除」，所以用這個型別另外記）。
///
/// `absorber_relationship_id` 為 `None` 代表**自迴圈刪除**（merged 與 survivor
/// 之間原本就有直接關聯，repoint 後兩端會變成同一個 Entity，語意無效，直接刪除，
/// 不吸收進任何一條——這種情況下 `relationship_evidence` 會被 `ON DELETE CASCADE`
/// 一併刪除，undo 只能重建 relationship 本身，evidence 無法復原，這是已知限制）。
/// 為 `Some(id)` 代表**吸收合併**：`absorbed_relationship_id` 這條被刪除，
/// 它的 evidence 全部搬到 `absorber_relationship_id` 這條、`absorber_pre_merge`
/// 記錄 absorber 合併前的欄位供 undo 還原。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergedRelationship {
    pub absorbed_relationship_id: RelationshipId,
    pub absorber_relationship_id: Option<RelationshipId>,
    /// 被刪除前的完整快照，undo 時用它 `put_relationship` 重建。
    pub absorbed_snapshot: Relationship,
    /// `absorber_relationship_id` 為 `Some` 時才有值。
    pub absorber_pre_merge: Option<AbsorberSnapshot>,
    /// 從 `absorbed_relationship_id` 搬到 `absorber_relationship_id` 的 evidence id
    /// 列表。`absorber_relationship_id` 為 `None`（自迴圈）時這裡是空陣列。
    pub moved_evidence_ids: Vec<RelationshipEvidenceId>,
}
