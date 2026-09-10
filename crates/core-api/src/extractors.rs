//! 認證後的 Principal extractor。

use axum::extract::FromRequestParts;
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
