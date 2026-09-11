use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{NetworkRuleId, SourceId};

/// 掛在 `Source` 上的私網白名單（CONNECTOR_SECURITY §3a／ADR-001）。
///
/// 不是 SPEC §5 的欄位；獨立型別與資料表，避免把 SSRF 例外塞進 `Source` JSON。
/// 寫入前必須通過 RBAC（operator 以上）與 hard-deny 驗證——驗證邏輯在 `connector-sdk`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetworkRule {
    pub id: NetworkRuleId,
    pub source_id: SourceId,
    /// 精確 CIDR 或 hostname，禁止萬用字元。
    pub cidr_or_host: String,
    /// `None` 表示該 host／CIDR 的所有埠。
    pub ports: Option<Vec<u16>>,
    pub reason: String,
    pub approved_by: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl NetworkRule {
    /// 過期規則視為不存在。
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|exp| exp <= now)
    }
}
