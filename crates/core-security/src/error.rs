//! 安全模組錯誤。訊息會指出下一步，不含密文。

/// 認證／授權／token 處理失敗。
#[derive(Debug, thiserror::Error)]
pub enum SecurityError {
    #[error(
        "JWT secret 太短（{len} bytes）。請提供至少 32 bytes 的隨機密鑰，例如用 openssl rand -base64 48"
    )]
    JwtSecretTooShort { len: usize },
    #[error("JWT 簽發失敗：{message}")]
    JwtEncode { message: String },
    #[error("JWT 無效或已過期。請重新登入或改用有效的 API token")]
    JwtInvalid { message: String },
    #[error("API token 格式不正確。預期 osint_<id>.<secret>，請確認複製完整")]
    MalformedApiToken,
    #[error("API token 驗證失敗：雜湊運算錯誤。請回報此錯誤，不要重試同一把壞掉的 hash")]
    TokenHash { message: String },
    #[error("找不到 API token `{id}`。請確認 id 或重新發行")]
    TokenNotFound { id: String },
    #[error("API token 已撤銷。請改用新 token")]
    TokenRevoked,
    #[error("API token 已過期。請用 POST /api/v1/tokens 重新發行一把")]
    TokenExpired,
    #[error("沒有通過認證。請在 Authorization 放 Bearer JWT 或 API token")]
    Unauthenticated,
    #[error("角色 `{role}` 沒有 `{permission}` 權限。請改用 operator／admin，或請管理員調整角色")]
    Forbidden { role: String, permission: String },
    #[error("稽核寫入失敗：{message}")]
    Audit { message: String },
}
