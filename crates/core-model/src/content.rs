//! `Document.normalized_content_hash` 的**唯一**定義（SPEC §15 Stage 3）。
//!
//! 放在 core-model 而不是 normalizer，是因為這個 hash 有兩個使用者：normalizer 產生它、
//! deduplicator 拿它比對。兩份實作遲早會分岔，分岔之後 Stage 3 會靜默失效——
//! 不會報錯，只是再也比不中任何東西。

use sha2::{Digest, Sha256};

/// 內容正規化：trim + 把連續空白（含換行、tab、全形空白）壓成單一半形空格。
///
/// **不做大小寫轉換**，這是刻意的：Stage 3 的語意是「同一份內容」，改過大小寫的標題
/// （例如編輯修正）是真的被改過，該由 Stage 4 的 SimHash 以近似重複處理，
/// 不該在 Stage 3 就被判成完全相同。
///
/// 會做空白正規化則是因為原本的定義（直接對原文做 SHA256）在實務上太脆：
/// 同一篇文章經 HTML 重新排版、`\r\n` 換成 `\n`、或多一個縮排空格，hash 就完全不同，
/// Stage 3 幾乎只在「同一次抓取的同一份 bytes」時才命中——那種情況 Stage 1／2 早就攔下了。
#[must_use]
pub fn normalize_content(raw: &str) -> String {
    raw.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `normalized_content_hash` = SHA256(正規化後的 `title|summary|body`)，小寫十六進位。
///
/// 欄位順序與 `|` 分隔子是格式的一部分，改了會讓既有資料全部比不中。
/// 缺欄位以空字串參與，維持分隔子數量固定——否則 `title=""`／`summary="x"`
/// 會跟 `title="x"`／`summary=""` 撞成同一個 hash。
#[must_use]
pub fn content_hash(title: Option<&str>, summary: Option<&str>, body: Option<&str>) -> String {
    let joined = format!(
        "{}|{}|{}",
        normalize_content(title.unwrap_or("")),
        normalize_content(summary.unwrap_or("")),
        normalize_content(body.unwrap_or("")),
    );
    hex::encode(Sha256::digest(joined.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitespace_variants_share_one_hash() {
        let a = content_hash(Some("標題"), Some("摘要"), Some("第一行\n第二行"));
        let b = content_hash(Some("  標題  "), Some("摘要"), Some("第一行\r\n\t 第二行 "));
        assert_eq!(a, b, "只差空白排版的內容必須算出同一個 hash");
    }

    #[test]
    fn case_change_is_a_different_hash() {
        let a = content_hash(Some("Advisory"), None, None);
        let b = content_hash(Some("advisory"), None, None);
        assert_ne!(a, b, "Stage 3 不做大小寫正規化，那是 Stage 4 的職責");
    }

    #[test]
    fn field_shift_does_not_collide() {
        let a = content_hash(Some("x"), None, None);
        let b = content_hash(None, Some("x"), None);
        assert_ne!(a, b, "分隔子數量固定，欄位位移不可撞 hash");
    }

    #[test]
    fn hash_is_64_hex_chars() {
        let h = content_hash(Some("t"), Some("s"), Some("b"));
        assert_eq!(h.len(), 64);
        assert!(
            h.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase())
        );
    }
}
