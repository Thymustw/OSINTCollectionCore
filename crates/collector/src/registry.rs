//! 已知 connector_type → 執行策略。未知種類優雅跳過。

/// collector 認得的 connector 種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnownConnectorKind {
    /// RSS 2.0 與 Atom 1.0 共用 `RssConnector`。
    RssOrAtom,
    /// 靜態 HTML 頁。
    StaticWeb,
    /// REST JSON API。
    RestApi,
}

/// 未知種類不 panic，回 `None` 讓呼叫端記 log 後跳過。
#[must_use]
pub fn classify_connector_type(connector_type: &str) -> Option<KnownConnectorKind> {
    match connector_type.trim().to_ascii_lowercase().as_str() {
        "rss" | "atom" => Some(KnownConnectorKind::RssOrAtom),
        "static_web" | "static-web" | "web" => Some(KnownConnectorKind::StaticWeb),
        "rest_api" | "rest-api" | "rest" => Some(KnownConnectorKind::RestApi),
        _ => None,
    }
}

/// 匯入／上傳專用類型：沒有可抓的 URL，不該被 collector 排程。
///
/// 字串來自 `import-format::ImportKind::connector_type` 與 STIX 匯入路徑
/// （`stix_import`）。`manual` 是部分測試殘留的舊值，與 `manual_upload` 同等處理。
#[must_use]
pub fn is_import_only_type(connector_type: &str) -> bool {
    matches!(
        connector_type.trim().to_ascii_lowercase().as_str(),
        "manual_upload" | "manual" | "json_import" | "csv_import" | "stix_import"
    )
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
    fn static_web_and_rest_api_are_known() {
        assert_eq!(
            classify_connector_type("static_web"),
            Some(KnownConnectorKind::StaticWeb)
        );
        assert_eq!(
            classify_connector_type("REST_API"),
            Some(KnownConnectorKind::RestApi)
        );
    }

    #[test]
    fn unknown_type_does_not_panic() {
        assert_eq!(classify_connector_type("manual_upload"), None);
        assert_eq!(classify_connector_type("json_import"), None);
        assert_eq!(classify_connector_type(""), None);
    }

    #[test]
    fn import_only_types_are_not_scheduled() {
        for ty in [
            "manual_upload",
            "MANUAL",
            "json_import",
            "csv_import",
            "stix_import",
            " stix_import ",
        ] {
            assert!(is_import_only_type(ty), "{ty} 應視為匯入專用");
            assert_eq!(classify_connector_type(ty), None, "{ty} 不是可抓取種類");
        }
        assert!(!is_import_only_type("rss"));
        assert!(!is_import_only_type("unknown_future_type"));
    }
}
