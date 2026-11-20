use thiserror::Error;

#[derive(Debug, Error)]
pub enum ChunkerError {
    #[error("讀不到 tokenizer 檔 {path}：{source}")]
    TokenizerRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "tokenizer 檔 sha256 不符：預期 {expected}，實際 {actual}。計數規則換了，段落 512 保護會失準；\
         不要改 hash 繞過，先確認檔案來源"
    )]
    TokenizerHashMismatch { expected: String, actual: String },
    #[error("tokenizer 檔解析失敗：{0}")]
    TokenizerParse(String),
    #[error("tokenize 失敗：{0}")]
    Tokenize(String),
    #[error("切段設定不合法：{0}")]
    Config(String),
    /// 程式不變式被打破（不該發生）。回錯誤而不 panic：外部內容不可信，一份畸形文件
    /// 不該炸掉整個 worker（CLAUDE.md §5）。
    #[error("切段不變式失敗：{0}")]
    Invariant(String),
}
