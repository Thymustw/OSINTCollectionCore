//! STIX 解析／驗證錯誤。

/// STIX bundle／id 結構錯誤。語意映射的錯誤不在這裡（那是 Step 1）。
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum StixError {
    /// 根物件不是合法 bundle（缺欄位、型別不對、`objects` 不是陣列）。
    #[error("STIX bundle 格式錯誤：{message}")]
    InvalidBundle { message: String },
    /// `type--uuid` 格式不符 STIX 2.1。
    #[error("STIX id `{id}` 格式不對：{message}")]
    InvalidId { id: String, message: String },
    /// `objects` 超過呼叫端設定的上限。把 bundle 拆小或調高上限。
    #[error("STIX bundle 物件數量 {actual} 超過上限 {limit}")]
    TooManyObjects { actual: usize, limit: usize },
    /// `objects[index]` 結構不完整或 id 不合法。
    #[error("STIX bundle 第 {index} 個物件格式錯誤：{message}")]
    InvalidObject { index: usize, message: String },
}
