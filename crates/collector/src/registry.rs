//! 已知 connector_type → 執行策略。未知種類優雅跳過。

/// collector 認得的 connector 種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownConnectorKind {
    /// RSS 2.0 與 Atom 1.0 共用 `RssConnector`。
    RssOrAtom,
}

/// 未知種類不 panic，回 `None` 讓呼叫端記 log 後跳過。
#[must_use]
pub fn classify_connector_type(connector_type: &str) -> Option<KnownConnectorKind> {
    match connector_type.trim().to_ascii_lowercase().as_str() {
        "rss" | "atom" => Some(KnownConnectorKind::RssOrAtom),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_and_atom_are_known() {
        assert_eq!(
            classify_connector_type("rss"),
            Some(KnownConnectorKind::RssOrAtom)
        );
        assert_eq!(
            classify_connector_type("ATOM"),
            Some(KnownConnectorKind::RssOrAtom)
        );
    }

    #[test]
    fn unknown_type_does_not_panic() {
        assert_eq!(classify_connector_type("static_web"), None);
        assert_eq!(classify_connector_type("rest_api"), None);
        assert_eq!(classify_connector_type(""), None);
    }
}
