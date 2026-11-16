//! Topic namespace：同一個 Redpanda 上隔開「誰的事件」（KNOWN_GAPS §26 後續）。
//!
//! 程式內部一律用**邏輯名稱**（[`crate::EventTopic::as_str`]，例如 `raw.collected`）：
//! envelope 的 `event_type`、outbox 表的 `topic`、`failed_events.topic`、各 worker 依
//! `event_type` 分流，全都不帶 namespace。只有在**真正碰 broker** 的那一刻
//! （produce、subscribe、查 watermark）才換成 broker 名稱 `<namespace>.<邏輯名稱>`。
//!
//! 空 namespace（[`TopicNamespace::NONE`]，也是預設）時 broker 名稱就是邏輯名稱，
//! 跟加這個型別之前完全一樣——正式與開發服務都走這條，不需要遷移既有 topic／offset。
//!
//! 目前只有測試會用非空 namespace（`TestTopics`，`test-support` feature）。服務設定
//! **刻意沒有**對應欄位：服務不讀的設定欄位只會讓人以為改了就生效。

use std::borrow::Cow;

use crate::error::EventError;

/// broker 名稱前綴。空字串 = 不加前綴。
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TopicNamespace(String);

impl TopicNamespace {
    /// 不加前綴（正式與開發服務）。
    pub const NONE: Self = Self(String::new());

    /// 只接受 `[a-z0-9-]`、1–64 字元。**不可含 `.`**：broker 名稱用第一個 `.` 切開
    /// namespace 與邏輯名稱，而邏輯名稱本身就有 `.`（`raw.collected`）。
    pub fn new(prefix: impl Into<String>) -> Result<Self, EventError> {
        let prefix = prefix.into();
        let valid = !prefix.is_empty()
            && prefix.len() <= 64
            && prefix
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
        if !valid {
            return Err(EventError::Configuration {
                message: format!(
                    "topic namespace `{prefix}` 不合法：只能用小寫英數與 `-`、1–64 字元、不可含 `.`"
                ),
            });
        }
        Ok(Self(prefix))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn is_none(&self) -> bool {
        self.0.is_empty()
    }

    /// 邏輯名稱 → broker 名稱。
    #[must_use]
    pub fn broker_name<'a>(&self, logical: &'a str) -> Cow<'a, str> {
        if self.is_none() {
            Cow::Borrowed(logical)
        } else {
            Cow::Owned(format!("{}.{logical}", self.0))
        }
    }

    /// broker 名稱 → 邏輯名稱。不屬於這個 namespace 的名稱原樣回傳。
    #[must_use]
    pub fn logical_name<'a>(&self, broker: &'a str) -> &'a str {
        if self.is_none() {
            return broker;
        }
        broker
            .strip_prefix(self.0.as_str())
            .and_then(|rest| rest.strip_prefix('.'))
            .unwrap_or(broker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_is_identity() {
        let ns = TopicNamespace::NONE;
        assert_eq!(ns.broker_name("raw.collected"), "raw.collected");
        assert_eq!(ns.logical_name("raw.collected"), "raw.collected");
        assert_eq!(TopicNamespace::default(), TopicNamespace::NONE);
    }

    #[test]
    fn prefix_round_trips() {
        let ns = TopicNamespace::new("osinttest-abc").expect("ns");
        let broker = ns.broker_name("raw.collected");
        assert_eq!(broker, "osinttest-abc.raw.collected");
        assert_eq!(ns.logical_name(&broker), "raw.collected");
        // 別的 namespace 的名稱不剝。
        assert_eq!(
            ns.logical_name("osinttest-abcd.raw.collected"),
            "osinttest-abcd.raw.collected"
        );
        assert_eq!(ns.logical_name("raw.collected"), "raw.collected");
    }

    #[test]
    fn rejects_dots_uppercase_and_empty() {
        for bad in ["", "a.b", "Upper", "has space", &"x".repeat(65)] {
            assert!(TopicNamespace::new(bad).is_err(), "`{bad}` 應該被拒絕");
        }
    }
}
