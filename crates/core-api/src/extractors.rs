//! 認證後的 Principal extractor 與呼叫端 IP extractor。

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, FromRequestParts};
use axum::http::request::Parts;

use core_security::Principal;

use crate::error::ApiError;

impl FromRequestParts<crate::state::AppState> for Principal {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &crate::state::AppState,
    ) -> Result<Self, Self::Rejection> {
        parts.extensions.get::<Principal>().cloned().ok_or_else(|| {
            ApiError::unauthorized(
                "沒有通過認證。請在 Authorization 放 Bearer JWT 或 API token（osint_<id>.<secret>）",
            )
        })
    }
}

/// 呼叫端 IP，給稽核用。取不到時是 `None`——**永遠不會讓請求失敗**。
///
/// # 只認 TCP peer
///
/// 與 `middleware::peer_ip` 同一套政策：**刻意不讀 `X-Forwarded-For`**。
/// 那是呼叫端完全可控的字串，在沒有可信任的 reverse proxy 把它覆寫掉之前採信它，
/// 等於讓任何人都能往稽核表裡寫任意 IP——那比沒有 IP 更糟，因為它看起來像證據。
///
/// # 為什麼是 extractor 而不是從 Principal 拿
///
/// `Principal` 是 `core-security` 的型別，代表「你是誰」，不該混進傳輸層的細節。
/// IP 屬於這一次 HTTP 請求，不屬於身分。
///
/// ⚠️ `ConnectInfo` 是由 `into_make_service_with_connect_info::<SocketAddr>()` 插進來的。
/// 少了它（例如測試用 `oneshot` 直接呼叫 router）這裡就是 `None`，
/// 而且不會有任何錯誤——`audit_log.ip` 會整欄是 NULL。
/// 測試要驗 IP 有寫進去時，請自己在 request 的 extensions 裡放一個 `ConnectInfo`。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientIp(pub Option<String>);

impl FromRequestParts<crate::state::AppState> for ClientIp {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        _state: &crate::state::AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(Self(
            parts
                .extensions
                .get::<ConnectInfo<SocketAddr>>()
                .map(|info| info.0.ip().to_string()),
        ))
    }
}
