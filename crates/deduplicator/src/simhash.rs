//! SPEC §15 Stage 4：64-bit SimHash 近似重複指紋。
//!
//! 自己實作而不是拉 crates.io 上的 `simhash`：演算法本體不到一百行，
//! 而現成 crate 的維護狀態不明，為了一百行擴大相依面不划算。
//!
//! # 設計決定
//!
//! **Token 化用 word-level unigram + 出現次數當權重**，不用 shingle（n-gram）。
//! 理由：轉載改寫通常是改動零星幾個詞，unigram 下每改一個詞只影響一個 token 的權重，
//! 距離增量小而可控；3-gram 下改一個詞會同時擾動最多三個 shingle，
//! 短文（標題 + 摘要，數十個詞）會直接把距離推過門檻，Stage 4 形同虛設。
//! 代價是**對詞序不敏感**——同一組詞重新排列會算出相同指紋。
//! 這對「轉載偵測」可以接受，對「抄襲偵測」不行，已知限制寫在
//! `docs/developer/deduplicator.md`。
//!
//! **Token 太少就不給指紋**（回 `None`）。64 bit 要靠足夠多的 token 才會分布均勻；
//! 只有三五個詞的文件算出來的指紋非常容易互撞，會製造假的「近似重複」。
//! 寧可 Stage 4 放棄，也不要產生錯誤的 duplicate group——SPEC §16 不准刪除證據，
//! 但錯誤的 canonical 指向一樣會誤導後續分析。

use sha2::{Digest, Sha256};

/// 預設 Hamming 距離門檻。
///
/// 3/64 是 SimHash 論文與多數實務實作的慣用值（Manku et al. 對網頁近似重複用 k=3）。
/// 往上調到 5 以上，不相關但主題相近的文章會開始互相命中；
/// 調到 1 以下則只剩下「幾乎逐字相同」，那是 Stage 3 已經攔下的範圍。
/// 可由 `[deduplicator].simhash_max_distance` 覆寫。
pub const DEFAULT_MAX_DISTANCE: u32 = 3;

/// 少於這個 token 數就不產生指紋。見模組說明。
pub const MIN_TOKENS: usize = 16;

/// 算出 64-bit SimHash 指紋，以 `i64` 位元保存（DB 沒有無號 64-bit）。
///
/// token 不足 [`MIN_TOKENS`] 時回 `None`——呼叫端應把它當成「Stage 4 不適用」，
/// 不是「距離很大」。
#[must_use]
pub fn fingerprint(text: &str) -> Option<i64> {
    let tokens = tokenize(text);
    if tokens.len() < MIN_TOKENS {
        return None;
    }
    Some(fingerprint_of_tokens(&tokens))
}

/// 把文字切成 token：轉小寫、以非文數字為分隔、丟掉長度 1 的碎片。
///
/// 大小寫在這裡就抹平，所以 Stage 4 天然對大小寫不敏感——
/// 這正是 Stage 3 刻意不做大小寫正規化的補位（見 `core-model::content`）。
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.chars().count() > 1)
        .map(str::to_lowercase)
        .collect()
}

fn fingerprint_of_tokens(tokens: &[String]) -> i64 {
    // 每個 bit 一個帶號累加器：token 的雜湊該位為 1 就 +權重，為 0 就 -權重。
    let mut weights = [0i64; 64];
    for token in tokens {
        let hash = token_hash(token);
        for (bit, weight) in weights.iter_mut().enumerate() {
            if hash & (1u64 << bit) != 0 {
                *weight += 1;
            } else {
                *weight -= 1;
            }
        }
    }
    let mut out = 0u64;
    for (bit, weight) in weights.iter().enumerate() {
        // 恰好打平（weight == 0）時取 0：規則必須是決定性的，隨機或依輸入順序決定
        // 會讓同一段文字算出不同指紋。
        if *weight > 0 {
            out |= 1u64 << bit;
        }
    }
    out as i64
}

/// 單一 token 的 64-bit 雜湊。用 SHA256 前 8 bytes。
///
/// 用密碼學雜湊是為了位元分布均勻，不是為了安全性；換成任何分布良好的 64-bit 雜湊
/// 都可以，但**換掉就等於換掉指紋定義**，既有 `documents.simhash` 會全部失效。
fn token_hash(token: &str) -> u64 {
    let digest = Sha256::digest(token.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_le_bytes(bytes)
}

/// 兩個指紋的 Hamming 距離（0..=64）。
#[must_use]
pub fn hamming_distance(a: i64, b: i64) -> u32 {
    ((a as u64) ^ (b as u64)).count_ones()
}

/// 由 Hamming 距離換算相似度：`1 - d/64`，夾在 0.0..=1.0。
///
/// 這是「指紋位元相同的比例」，**不是**內容相似度的嚴謹估計，
/// 只是給 `DuplicateGroup.similarity` 一個有序可比的數值。
#[must_use]
pub fn similarity(distance: u32) -> f64 {
    1.0 - f64::from(distance.min(64)) / 64.0
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = "\
        The advisory describes a remote code execution flaw affecting the reporting service. \
        Attackers can reach the vulnerable endpoint without authentication whenever the \
        management interface is exposed to untrusted networks. The vendor published a patched \
        release and recommends upgrading immediately. Administrators who cannot upgrade should \
        restrict network access to the management interface and monitor authentication logs \
        for unexpected requests.";

    #[test]
    fn identical_text_has_distance_zero() {
        let a = fingerprint(BASE).expect("指紋");
        let b = fingerprint(BASE).expect("指紋");
        assert_eq!(hamming_distance(a, b), 0);
    }

    #[test]
    fn whitespace_and_case_do_not_change_fingerprint() {
        let a = fingerprint(BASE).expect("指紋");
        let noisy = BASE.to_uppercase().replace(' ', "\n  ");
        let b = fingerprint(&noisy).expect("指紋");
        assert_eq!(
            hamming_distance(a, b),
            0,
            "token 化已抹平大小寫與空白，指紋不該變"
        );
    }

    #[test]
    fn changing_a_few_words_stays_within_default_threshold() {
        let a = fingerprint(BASE).expect("指紋");
        // 模擬轉載改寫：換掉三個詞。
        let reprint = BASE
            .replace("Attackers", "Adversaries")
            .replace("immediately", "promptly")
            .replace("unexpected", "suspicious");
        let b = fingerprint(&reprint).expect("指紋");
        let distance = hamming_distance(a, b);
        assert!(
            distance <= DEFAULT_MAX_DISTANCE,
            "改三個詞的轉載必須落在預設門檻 {DEFAULT_MAX_DISTANCE} 內，實際距離 {distance}"
        );
        assert!(distance > 0, "改過字的內容不該算出完全相同的指紋");
    }

    #[test]
    fn unrelated_text_is_far_beyond_threshold() {
        let a = fingerprint(BASE).expect("指紋");
        let other = "\
            Quarterly shipping volumes for the northern ports increased again this season. \
            Container throughput rose while bulk cargo remained flat, and the harbour authority \
            expects dredging works to finish before winter storms arrive. Local operators asked \
            for longer berth windows to absorb the additional demand from inland rail traffic.";
        let b = fingerprint(other).expect("指紋");
        let distance = hamming_distance(a, b);
        assert!(
            distance > DEFAULT_MAX_DISTANCE * 3,
            "完全不同的內容距離應遠大於門檻，實際 {distance}"
        );
    }

    #[test]
    fn too_few_tokens_gets_no_fingerprint() {
        assert_eq!(fingerprint("short title here"), None);
        assert_eq!(fingerprint(""), None);
    }

    #[test]
    fn similarity_matches_distance() {
        assert!((similarity(0) - 1.0).abs() < f64::EPSILON);
        assert!((similarity(64) - 0.0).abs() < f64::EPSILON);
        assert!((similarity(32) - 0.5).abs() < f64::EPSILON);
        // 距離不可能超過 64，但夾住避免呼叫端傳錯值時算出負相似度。
        assert!((similarity(200) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn fingerprint_survives_i64_round_trip() {
        // 指紋最高位為 1 時 `as i64` 會是負數；DB 存回來必須位元相同。
        let value = fingerprint(BASE).expect("指紋");
        let round = (value as u64) as i64;
        assert_eq!(value, round);
        assert_eq!(hamming_distance(value, round), 0);
    }
}
