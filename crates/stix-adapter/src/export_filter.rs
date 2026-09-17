//! `POST /api/v1/export/stix` 的過濾條件，core-api 與 stix-worker 共用同一份型別，
//! 避免兩邊各自 parse、行為分岔（core-api 建 Job 時把它塞進 `parameters.filter`；
//! stix-worker 執行 `stix_export` 時原樣解回來）。

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use core_model::enums::EntityType;

/// 全部 optional，缺省代表不限。語意細節（entity_types 只篩最終輸出、
/// depth 只在有 entity_ids 時才有意義、time_range 只篩 Relationship）
/// 由執行端（stix-worker）決定，這裡只是資料形狀。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StixExportFilter {
    #[serde(default)]
    pub entity_types: Option<Vec<EntityType>>,
    #[serde(default)]
    pub entity_ids: Option<Vec<Uuid>>,
    #[serde(default)]
    pub time_range: Option<StixTimeRange>,
    #[serde(default)]
    pub depth: Option<u32>,
}

/// 匯出時間窗。兩個端點都可省略。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StixTimeRange {
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

impl StixTimeRange {
    /// 一段觀測窗（`first_seen..=last_seen`）是否與這個時間窗重疊。
    /// 語意與 `storage_core::GraphTraversalOptions::time_range` 一致：
    /// `first_seen <= to && last_seen >= from`，兩端缺省視為不限制。
    #[must_use]
    pub fn overlaps(&self, first_seen: DateTime<Utc>, last_seen: DateTime<Utc>) -> bool {
        let after_from = self.from.is_none_or(|from| last_seen >= from);
        let before_to = self.to.is_none_or(|to| first_seen <= to);
        after_from && before_to
    }
}

/// Step 4 worker 寫入匯出結果、`GET /jobs/{id}/result` 讀取結果時共用的物件儲存 key。
#[must_use]
pub fn export_result_object_key(job_id: Uuid) -> String {
    format!("stix-exports/{job_id}.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use serde_json::json;

    fn ts(secs: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(secs, 0).unwrap()
    }

    #[test]
    fn overlaps_always_true_when_no_bounds() {
        let range = StixTimeRange {
            from: None,
            to: None,
        };
        assert!(range.overlaps(ts(100), ts(200)));
    }

    #[test]
    fn overlaps_honors_only_from() {
        let range = StixTimeRange {
            from: Some(ts(150)),
            to: None,
        };
        assert!(!range.overlaps(ts(100), ts(140)), "last_seen < from");
        assert!(range.overlaps(ts(100), ts(150)));
        assert!(range.overlaps(ts(200), ts(300)));
    }

    #[test]
    fn overlaps_honors_only_to() {
        let range = StixTimeRange {
            from: None,
            to: Some(ts(150)),
        };
        assert!(!range.overlaps(ts(160), ts(300)), "first_seen > to");
        assert!(range.overlaps(ts(150), ts(300)));
        assert!(range.overlaps(ts(100), ts(140)));
    }

    #[test]
    fn overlaps_with_both_bounds() {
        let range = StixTimeRange {
            from: Some(ts(100)),
            to: Some(ts(200)),
        };
        // 完全落在窗內
        assert!(range.overlaps(ts(120), ts(180)));
        // 部分重疊（左右各一）
        assert!(range.overlaps(ts(50), ts(150)), "左半重疊");
        assert!(range.overlaps(ts(150), ts(250)), "右半重疊");
        // 完全在窗外
        assert!(!range.overlaps(ts(50), ts(90)), "窗之前");
        assert!(!range.overlaps(ts(210), ts(250)), "窗之後");
    }

    #[test]
    fn stix_export_filter_deserializes_empty() {
        let filter: StixExportFilter = serde_json::from_value(json!({})).unwrap();
        assert!(filter.entity_types.is_none());
        assert!(filter.entity_ids.is_none());
        assert!(filter.time_range.is_none());
        assert!(filter.depth.is_none());
    }

    #[test]
    fn stix_export_filter_rejects_unknown_variant_field() {
        let err =
            serde_json::from_value::<StixExportFilter>(json!({"entity_types": ["not-a-type"]}))
                .expect_err("不認識的 EntityType 應失敗");
        let msg = err.to_string();
        assert!(msg.contains("variant"), "serde 應報 unknown variant：{msg}");
    }

    #[test]
    fn stix_export_filter_rejects_unknown_field() {
        let err = serde_json::from_value::<StixExportFilter>(json!({"nope": true}))
            .expect_err("未知欄位應因 deny_unknown_fields 失敗");
        assert!(err.to_string().contains("nope"), "{err}");
    }

    #[test]
    fn export_result_key_is_deterministic() {
        let id = Uuid::from_u128(7);
        assert_eq!(
            export_result_object_key(id),
            format!("stix-exports/{id}.json")
        );
        assert_eq!(export_result_object_key(id), export_result_object_key(id));
    }
}
