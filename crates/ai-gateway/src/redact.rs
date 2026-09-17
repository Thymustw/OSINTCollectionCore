//! 記錄／稽核 LLM 呼叫內容前，過濾常見的驗證資訊格式（SPEC_V0.3 §3 redaction policy）。
//!
//! **這不是 PII 偵測**——沒有 email／電話／人名的辨識，只做關鍵字／格式比對，
//! 跟 `storage_core::redact_secrets`（DSN／連線字串）同一種「盡力而為」定位，
//! 但鎖定的是 LLM 請求/回應內容裡可能出現的驗證資訊，不是連線字串。
//!
//! **不遮罩一般業務內容**（entity 名稱、描述、prompt 裡的 OSINT 資料等）——
//! 那些本來就是要送進 LLM／寫進稽核的資料，過濾掉等於讓稽核失去意義。這個
//! 模組只防「LLM 回應意外echo回驗證資訊」這一種風險（例如錯誤訊息裡帶了
//! 原始 request 的 `Authorization` header，或模型輸出剛好包含一段像 API key
//! 的字串）。

/// 遮罩常見的驗證資訊格式：`Bearer <token>`、`sk-...`（OpenAI 風格 API key
/// 前綴，vLLM／llama.cpp 相容 server 常見延用同格式）。
#[must_use]
pub fn redact_credentials(s: &str) -> String {
    let s = s.replace("Bearer ", "Bearer [已省略] ");
    redact_prefixed_tokens(&s, "sk-")
}

/// 把字串裡以 `prefix` 開頭的「詞」（以空白／常見分隔符界定）換成
/// `<prefix>[已省略]`，其餘原樣保留。
fn redact_prefixed_tokens(s: &str, prefix: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.char_indices().peekable();
    let mut last_end = 0;
    while let Some(&(start, _)) = chars.peek() {
        if s[start..].starts_with(prefix) {
            out.push_str(&s[last_end..start]);
            let end = s[start..]
                .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ')' | ']'))
                .map(|n| start + n)
                .unwrap_or(s.len());
            out.push_str(prefix);
            out.push_str("[已省略]");
            for _ in 0..s[start..end].chars().count() {
                chars.next();
            }
            last_end = end;
        } else {
            chars.next();
        }
    }
    out.push_str(&s[last_end..]);
    out
}

/// 截斷過長字串，超過 `limit` 個字元（不是 byte）時加上 `…`。
///
/// 用於 log／錯誤訊息，避免單行 log 塞進整份 LLM 回應。**不要用在寫進
/// `AiRun.output`／`merge_history.auto_approval_audit` 的內容**——那些是
/// 完整證據，截斷會讓稽核少一段資料且不會有任何錯誤提示。
#[must_use]
pub fn truncate(s: &str, limit: usize) -> String {
    if s.chars().count() <= limit {
        return s.to_string();
    }
    let end = s
        .char_indices()
        .map(|(i, _)| i)
        .nth(limit)
        .unwrap_or(s.len());
    format!("{}…", &s[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_bearer_token() {
        let out = redact_credentials("Authorization: Bearer abc123.def456");
        assert_eq!(out, "Authorization: Bearer [已省略] abc123.def456");
    }

    #[test]
    fn redacts_sk_prefixed_key() {
        // 用明顯是 placeholder 的值（不是隨機高熵字串），避免被 gitleaks 的
        // entropy-based 規則誤判成真的外洩金鑰。
        let out = redact_credentials(r#"{"key": "sk-0000000000000000"}"#);
        assert_eq!(out, r#"{"key": "sk-[已省略]"}"#);
    }

    #[test]
    fn leaves_ordinary_content_untouched() {
        let out = redact_credentials("entity description: Acme Corp, founded 2001");
        assert_eq!(out, "entity description: Acme Corp, founded 2001");
    }

    #[test]
    fn truncate_short_string_is_noop() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_long_string_adds_ellipsis() {
        let long = "a".repeat(600);
        let out = truncate(&long, 500);
        assert_eq!(out.chars().count(), 501); // 500 個 'a' + '…'
        assert!(out.ends_with('…'));
    }

    #[test]
    fn truncate_counts_chars_not_bytes() {
        // 中文字元多 byte，確保是照字元數截斷不是 byte 數。
        let s = "中".repeat(10);
        let out = truncate(&s, 5);
        assert_eq!(out.chars().count(), 6); // 5 個「中」+ '…'
    }
}
