//! `/api/v1/entities/{id}/timeline`（SPEC_V0.2 §10）。Entity 的文件時間軸。
//!
//! # 為什麼可能是空的
//!
//! 這個端點回傳的是一份文件清單，不是計數。回 `entries: []` 有兩種可能：
//!
//! 1. 這個 Entity 沒有抽取紀錄（`entity_extractions` 空）。
//! 2. Entity 有抽取紀錄，但每筆 extraction 指到的物件都已刪除，或是被判成
//!    別份的 duplicate（`duplicate_of` 有值）——canonical store 不保證
//!    `entity_extractions.object_id` 永遠指到一份存在的 Document，我們選擇
//!    跳過而不中止整批，讓「幾筆壞掉的參照」不會讓整個時間軸一起消失。
//!
//! 兩種都是正確行為，不是缺陷。
//!
//! # 為什麼不查 `events` 表
//!
//! SPEC_V0.2 §10 的 Timeline 原本允許以 Event 為時間來源，但這份文件刻意
//! **只用 Document 的時間戳**。原因是 `events` 表（SPEC §13）到目前為止
//! **沒有任何生產寫入者**——見 `docs/developer/api-skeleton.md` 的 Events
//! 一節與 ADR-013。查它只會讓時間軸被一片空白灌爆語意，不如不查。
//!
//! # 時間戳來源：`published_at` fallback `observed_at`，不用 `collected_at`
//!
//! SPEC_V0.2 §10 明文禁止用採集時間當作 timeline 的事件時間：`collected_at`
//! 是「系統什麼時候把資料抓進來」，不是「這件事什麼時候發生」。一份幾個月前
//! 的文章今天才被正常化，用 `collected_at` 會讓它看起來像是今天發生的。
//! 因此時間戳固定優先 `published_at`，缺值才 fallback `observed_at`
//! （系統觀察到這份內容的時間）。`time_source` 欄位讓呼叫端看得出用的是哪個。
//!
//! # 縮小範圍後的交付
//!
//! 這是 ADR-013 決定的**縮小範圍**版本：V0.2 只交付 Entity 維度、只用既有
//! 資料。`GET /timeline`（全域）與 `GET /collections/{id}/timeline` 在 V0.2
//! **連 501 stub 都沒有**——路由沒註冊，打了會是一般的 404 Not Found。
//! 原因（collection 關聯沒被生產寫入、topic 維度在資料模型裡不存在）見
//! `docs/adr/ADR-013-timeline-entity-only-v0.2-scope.md`。

use axum::Json;
use axum::extract::{Path, Query, State};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::resources::{storage_error, store};
use crate::state::AppState;

/// 時間軸 query。只有 `limit`，沒有 cursor。
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TimelineQuery {
    pub limit: Option<u32>,
}

/// `GET /api/v1/entities/{id}/timeline` 的回應。
#[derive(Debug, Clone, Serialize)]
pub struct EntityTimeline {
    pub entity_id: String,
    pub entries: Vec<TimelineEntry>,
    /// 命中 `limit` 就為 `true`：代表「可能還有更多」，不是精確總數。
    pub truncated: bool,
}

/// 時間軸裡的單一一筆。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimelineEntry {
    pub document_id: String,
    pub title: Option<String>,
    /// RFC3339 時間戳。
    pub time: DateTime<Utc>,
    /// 用的時間來源：`published_at` 或 `observed_at`。
    pub time_source: &'static str,
}

/// 從 `extraction` 的一支 Document 挑時間戳。純函式以便單元測試。
///
/// 時間戳固定 `published_at` 優先，缺值才 fallback `observed_at`
/// （SPEC_V0.2 §10 禁止用 `collected_at`）。`time_source` 標記用的是哪個。
fn timeline_time_source(published_at: Option<DateTime<Utc>>) -> &'static str {
    if published_at.is_some() {
        "published_at"
    } else {
        "observed_at"
    }
}

/// 依時間戳降冪排序（最新在前）。純函式以便單元測試。
fn sort_timeline_descending(entries: &mut [TimelineEntry]) {
    entries.sort_by_key(|e| std::cmp::Reverse(e.time));
}

/// `GET /api/v1/entities/{id}/timeline`。viewer 以上。
pub async fn entity_timeline(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
    Query(query): Query<TimelineQuery>,
) -> Result<Json<EntityTimeline>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let limit = query.limit.unwrap_or(20).clamp(1, 100);

    // 先確認 Entity 存在：不存在回 404，與其他 Entity endpoint 一致。
    store
        .get_entity(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            // 404 訊息比照 entities.rs 的慣例。
            ApiError::not_found(format!(
                "找不到 Entity `{id}`。請用 GET /api/v1/entities 確認 id"
            ))
        })?;

    let extractions = store
        .list_entity_extractions_by_entity(id, limit)
        .await
        .map_err(storage_error)?;

    // 逐筆反查 Document（`get_document` 是 async，不能塞進 `filter_map` 的同步
    // 閉包裡，用一般迴圈逐筆 await）。物件不存在／讀取失敗／不是 canonical
    // 都只跳過該筆，不中止整批——見模組說明「為什麼可能是空的」。
    let mut entries: Vec<TimelineEntry> = Vec::with_capacity(extractions.len());
    for extraction in &extractions {
        let Some(doc) = store
            .get_document(extraction.object_id)
            .await
            .map_err(storage_error)?
        else {
            continue;
        };
        // 是別份的重複而不是 canonical → 跳過，比照 list_documents_filtered 慣例。
        if doc.duplicate_of.is_some() {
            continue;
        }
        let time = doc.published_at.unwrap_or(doc.observed_at);
        let time_source = timeline_time_source(doc.published_at);
        entries.push(TimelineEntry {
            document_id: doc.id.to_string(),
            title: doc.title,
            time,
            time_source,
        });
    }
    sort_timeline_descending(&mut entries);

    let truncated = extractions.len() as u32 >= limit;
    Ok(Json(EntityTimeline {
        entity_id: id.to_string(),
        entries,
        truncated,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn entry(time: DateTime<Utc>, time_source: &'static str) -> TimelineEntry {
        TimelineEntry {
            document_id: "doc".into(),
            title: None,
            time,
            time_source,
        }
    }

    #[test]
    fn published_at_wins_over_observed_at() {
        // `published_at` 存在就優先，不回落到 `observed_at`。
        let published = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
        assert_eq!(timeline_time_source(Some(published)), "published_at");
    }

    #[test]
    fn missing_published_at_falls_back_to_observed_at() {
        assert_eq!(timeline_time_source(None), "observed_at");
    }

    #[test]
    fn entries_sort_descending_by_time() {
        let old = entry(
            Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
            "published_at",
        );
        let mid = entry(
            Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap(),
            "observed_at",
        );
        let new = entry(
            Utc.with_ymd_and_hms(2026, 12, 1, 0, 0, 0).unwrap(),
            "published_at",
        );
        let mut entries = vec![old.clone(), new.clone(), mid.clone()];
        sort_timeline_descending(&mut entries);
        assert_eq!(entries, vec![new, mid, old]);
    }
}
