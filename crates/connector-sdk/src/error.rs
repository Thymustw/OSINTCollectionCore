//! Connector 錯誤。訊息會指出下一步，不含密文。

use std::time::Duration;

/// Connector／SSRF／抓取失敗。
#[derive(Debug, thiserror::Error)]
pub enum ConnectorError {
    #[error("URL `{url}` 使用不允許的 scheme `{scheme}`。connector 只接受 http／https")]
    UnsupportedScheme { url: String, scheme: String },
    #[error("URL `{url}` 沒有 host。請提供完整的 http／https URL")]
    MissingHost { url: String },
    #[error("URL `{url}` 無效：{message}")]
    InvalidUrl { url: String, message: String },
    #[error(
        "目標 `{host}` 屬於 cloud metadata／硬拒絕網段（{detail}）。這層永遠不能被 NetworkRule 覆寫。請改連公開來源或內部 API，不要打 IMDS"
    )]
    HardDenied { host: String, detail: String },
    #[error(
        "目標 `{host}`（解析為 {ip}）屬於私網／loopback／link-local，且 Source `{source_id}` 沒有未過期的 NetworkRule 涵蓋它。請由 operator 以上新增規則，或改用公開 URL"
    )]
    SoftDenied {
        host: String,
        ip: String,
        source_id: String,
    },
    #[error(
        "目標 `{host}` 在 domain denylist。請從 SourcePolicy.domain_denylist 移除，或改用允許的 host"
    )]
    DomainDenied { host: String },
    #[error(
        "目標 `{host}` 不在 domain allowlist。請把 host 加進 SourcePolicy.domain_allowlist，或把 allowlist 留空表示不限制公開網域"
    )]
    DomainNotAllowed { host: String },
    #[error("NetworkRule `{cidr_or_host}` 無效：{message}")]
    InvalidNetworkRule {
        cidr_or_host: String,
        message: String,
    },
    #[error("角色 `{role}` 不能建立或編輯 NetworkRule。請改用 operator／admin，或請管理員調整角色")]
    NetworkRuleForbidden { role: String },
    #[error("NetworkRule.reason 不能是空的。請寫為什麼要放行這個私網目標")]
    MissingRuleReason,
    #[error("NetworkRule.approved_by 不能是空的。請填核准者身分")]
    MissingApprover,
    #[error("解析 `{host}` 失敗：{message}。請確認 DNS 可達，或改用 IP／已允許的 host")]
    Dns { host: String, message: String },
    #[error("解析 `{host}` 沒有得到任何 IP。請確認 host 存在")]
    DnsEmpty { host: String },
    #[error(
        "重新導向次數超過上限 {limit}。請檢查來源是否形成迴圈，或把 SourcePolicy.max_redirects 調大"
    )]
    TooManyRedirects { limit: u32 },
    #[error("回應超過 {limit} bytes。請把 SourcePolicy.max_response_bytes 調大，或換較小的來源")]
    ResponseTooLarge { limit: u64 },
    #[error("連線 `{url}` 逾時（{timeout:?}）。請把 timeout 調大或稍後再試")]
    Timeout { url: String, timeout: Duration },
    #[error("HTTP 抓取 `{url}` 失敗：{message}")]
    Fetch { url: String, message: String },
    #[error("解析內容失敗：{message}")]
    Parse { message: String },
    #[error("超過 domain `{domain}` 的速率上限。請降低排程頻率或調高 RateLimit")]
    RateLimited { domain: String },
    #[error("暫時性錯誤，已重試 {attempts} 次仍失敗：{message}")]
    RetryExhausted { attempts: u32, message: String },
    #[error("儲存 RawEvidence 失敗：{message}")]
    Storage { message: String },
    #[error("稽核寫入失敗：{message}")]
    Audit { message: String },
    #[error("{message}")]
    Policy { message: String },
}

impl ConnectorError {
    /// 只有逾時與 rate limit 視為暫時性。SSRF／4xx／parse 失敗立即結束，重試沒有意義。
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout { .. } | Self::RateLimited { .. })
    }
}
