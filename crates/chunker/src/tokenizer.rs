//! e5（XLM-RoBERTa）tokenizer：載入時驗 sha256，只用來**計數**與找硬切點。
//!
//! ADR-016 D7：長度一律用模型自己的斷詞器算，不用字數估。實驗 5-2c 已證實 Rust
//! `tokenizers` 0.23.2 與 Python 版對同一份 `tokenizer.json` 30／30 句 id 完全相同。

use std::path::Path;

use sha2::{Digest, Sha256};
use tokenizers::Tokenizer;

use crate::error::ChunkerError;

/// `intfloat/multilingual-e5-small` 的 `tokenizer.json`。與
/// `scripts/opensearch-ml-setup-e5.sh` 的 `TOKENIZER_SHA256` 是同一顆（ml-commons 部署的就是這份）。
/// 換檔＝換計數規則，所有策略版本都要升。
pub const E5_TOKENIZER_SHA256: &str =
    "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39"; // gitleaks:allow

/// e5 向量化時加在被索引文字前面的前綴（模型卡規定，見 `storage-opensearch` embedding.rs）。
/// 512 上限要把它算進去。
pub const PASSAGE_PREFIX: &str = "passage: ";

/// 只讀、可跨執行緒共用（`Tokenizer` 是 `Send + Sync`）。
pub struct E5Tokenizer {
    inner: Tokenizer,
}

impl std::fmt::Debug for E5Tokenizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("E5Tokenizer").finish_non_exhaustive()
    }
}

impl E5Tokenizer {
    /// 讀檔、比對 sha256、解析。hash 不符直接拒絕：計數規則換了而段落版本沒升，
    /// 512 保護會靜默失準。
    pub fn load(path: &Path, expected_sha256: &str) -> Result<Self, ChunkerError> {
        let bytes = std::fs::read(path).map_err(|source| ChunkerError::TokenizerRead {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_bytes(&bytes, expected_sha256)
    }

    pub fn from_bytes(bytes: &[u8], expected_sha256: &str) -> Result<Self, ChunkerError> {
        let actual = hex::encode(Sha256::digest(bytes));
        if !actual.eq_ignore_ascii_case(expected_sha256) {
            return Err(ChunkerError::TokenizerHashMismatch {
                expected: expected_sha256.to_string(),
                actual,
            });
        }
        let mut inner = Tokenizer::from_bytes(bytes)
            .map_err(|err| ChunkerError::TokenizerParse(err.to_string()))?;
        // tokenizer.json 若帶 truncation，超長段落會被截成剛好 512 而**通過**保護檢查。
        // e5 的檔案實測兩者皆為 null；仍明確關掉，不依賴檔案內容。
        inner
            .with_truncation(None)
            .map_err(|err| ChunkerError::TokenizerParse(err.to_string()))?;
        inner.with_padding(None);
        Ok(Self { inner })
    }

    /// 不含特殊 token（`<s>`／`</s>`）的 token 數。
    pub fn count(&self, text: &str) -> Result<u32, ChunkerError> {
        if text.is_empty() {
            return Ok(0);
        }
        let encoding = self
            .inner
            .encode_fast(text, false)
            .map_err(|err| ChunkerError::Tokenize(err.to_string()))?;
        Ok(u32::try_from(encoding.len()).unwrap_or(u32::MAX))
    }

    /// 模型實際吃到的 token 數：`passage: ` 前綴 + 文字 + 特殊 token。512 保護用這個比。
    pub fn count_model_input(&self, text: &str) -> Result<u32, ChunkerError> {
        let encoding = self
            .inner
            .encode_fast(format!("{PASSAGE_PREFIX}{text}"), true)
            .map_err(|err| ChunkerError::Tokenize(err.to_string()))?;
        Ok(u32::try_from(encoding.len()).unwrap_or(u32::MAX))
    }

    /// 每個 token 的起點（**字元**偏移，相對 `text`），遞增、去重。硬切點只取這些位置，
    /// 不會把一個 token 切成兩半。
    pub fn token_char_starts(&self, text: &str) -> Result<Vec<usize>, ChunkerError> {
        let encoding = self
            .inner
            .encode_char_offsets(text, false)
            .map_err(|err| ChunkerError::Tokenize(err.to_string()))?;
        let mut starts: Vec<usize> = Vec::with_capacity(encoding.len());
        for &(start, _) in encoding.get_offsets() {
            if starts.last().is_none_or(|&last| start > last) {
                starts.push(start);
            }
        }
        Ok(starts)
    }
}
