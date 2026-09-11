//! SPEC §15 Stage 2：canonical URL 正規化。
//!
//! 規格列了五件事：lowercase hostname、remove fragment、normalize encoding、
//! remove known tracking params、preserve meaningful query params。
//! 這個模組就是那五件事的實作，外加兩項規格沒寫但必要的處理（去 userinfo、排序 query），
//! 每一項都在下面說明為什麼。
//!
//! # 為什麼放在 `core-model`
//!
//! 原本住在 `crates/deduplicator/src/url_norm.rs`。entity-worker 抽出 URL Entity 時，
//! `normalized_name` 必須與 deduplicator 的 `documents.canonical_url` 用**同一套**規則——
//! 兩邊各留一份實作的話，只要有人改了其中一份，同一個 URL 就會產生兩種正規化結果，
//! 而且不會報錯：Entity 只是悄悄多出一個「看起來一樣但字串不同」的重複列。
//!
//! 這與 `content.rs`（SPEC §15 Stage 3 的 content hash）放在 core-model 的理由相同：
//! **跨服務必須一致的定義就放在 core-model**，不要用 crate 相依把 deduplicator 拉進
//! entity-worker（那會讓一支只做抽取的服務背上整個去重模組）。
//!
//! `deduplicator` 仍以 `pub use core_model::url_norm;` 對外提供同名路徑，既有呼叫端不受影響。

use std::borrow::Cow;

use url::Url;

/// 已知追蹤參數（完整比對，小寫）。
///
/// 這份清單是**可維護的資料**，不是演算法的一部分：發現新的追蹤參數就往這裡加，
/// 不需要改任何邏輯。只收「移除後不影響取回的內容」的參數。
const TRACKING_PARAMS: &[&str] = &[
    // Google Analytics / Ads
    "gclid",
    "gclsrc",
    "dclid",
    "gbraid",
    "wbraid",
    // Meta
    "fbclid",
    // Microsoft / Bing
    "msclkid",
    // Yandex
    "yclid",
    "_openstat",
    // Mailchimp
    "mc_cid",
    "mc_eid",
    // HubSpot
    "_hsenc",
    "_hsmi",
    "hsctatracking",
    // Instagram / TikTok / Twitter
    "igshid",
    "ttclid",
    "twclid",
    // 阿里系
    "spm",
    "scm",
    // 泛用 referrer 類
    "ref",
    "ref_src",
    "ref_url",
    "referrer",
    "referer",
    "cmpid",
    "campaignid",
    "s_cid",
    "icid",
    "ncid",
    "sr_share",
    "vero_id",
    "vero_conv",
    "oly_anon_id",
    "oly_enc_id",
    "wickedid",
];

/// 前綴式追蹤參數。`utm_*` 是開放集合（`utm_source`／`utm_id`／`utm_reader`…），
/// 逐一列舉永遠會漏，用前綴比對。
const TRACKING_PREFIXES: &[&str] = &["utm_", "pk_", "mtm_", "matomo_"];

/// 判斷一個 query 參數名是不是追蹤參數。
#[must_use]
pub fn is_tracking_param(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    TRACKING_PARAMS.contains(&lower.as_str())
        || TRACKING_PREFIXES
            .iter()
            .any(|prefix| lower.starts_with(prefix))
}

/// 把一個 URL 正規化成 dedup 用的 canonical 形式。
///
/// 解析不出來（不是合法絕對 URL）時回 `None`——**不要**退而求其次回原字串：
/// 那會讓「正規化過的 URL」與「沒正規化的字串」混在同一個欄位裡比對，
/// Stage 2 會靜默比不中而沒有任何錯誤跡象。
///
/// 處理項目：
/// 1. scheme／hostname 轉小寫（`url` crate 解析時就會做）
/// 2. 移除 fragment（`#section` 不改變取回的內容）
/// 3. 移除預設埠（`https://x:443/` → `https://x/`，`url` crate 自動處理）
/// 4. 移除 userinfo（`https://user:pw@x/`）——那是憑證不是識別身分，
///    而且把密碼留在 canonical_url 等於把它寫進索引
/// 5. 空路徑補成 `/`；其他路徑**維持原樣**（路徑大小寫在多數伺服器是敏感的，
///    擅自轉小寫會把兩份不同文件併成一份）
/// 6. 正規化 percent-encoding：`%2f` → `%2F`（十六進位轉大寫），
///    且把不需要編碼的 unreserved 字元解碼回字面（`%7E` → `~`）
/// 7. 移除追蹤參數；**其餘參數保留**（`?id=123`、`?page=2` 都會改變內容）
/// 8. 保留下來的參數依「名稱、值」排序
///
/// 第 8 項規格沒提。排序是因為同一篇文章的連結常以不同參數順序流通
/// （`?a=1&b=2` vs `?b=2&a=1`），不排序就比不中。代價是**少數把 query 當有序序列的
/// API 會被改寫**；這類 API 幾乎都是機器介面，不是我們 Stage 2 想比對的文章 URL。
#[must_use]
pub fn canonicalize(raw: &str) -> Option<String> {
    let mut url = Url::parse(raw.trim()).ok()?;

    url.set_fragment(None);
    // set_username／set_password 對 cannot-be-a-base 的 URL（例如 mailto:）會回 Err，
    // 那種 URL 本來就沒有 userinfo，忽略即可。
    let _ = url.set_username("");
    let _ = url.set_password(None);

    let kept: Vec<(String, String)> = {
        let mut pairs: Vec<(String, String)> = url
            .query_pairs()
            .filter(|(name, _)| !is_tracking_param(name))
            .map(|(name, value)| (name.into_owned(), value.into_owned()))
            .collect();
        pairs.sort();
        pairs
    };
    if url.query().is_some() {
        if kept.is_empty() {
            // 全部都是追蹤參數：連 `?` 一起拿掉，否則 `a?utm_x=1` 與 `a` 仍是兩個字串。
            url.set_query(None);
        } else {
            let mut serializer = url.query_pairs_mut();
            serializer.clear();
            for (name, value) in &kept {
                serializer.append_pair(name, value);
            }
            drop(serializer);
        }
    }

    if url.path().is_empty() {
        url.set_path("/");
    }

    let normalized_path = normalize_percent_encoding(url.path());
    if normalized_path != url.path() {
        url.set_path(&normalized_path);
    }

    Some(url.to_string())
}

/// percent-encoding 正規化：`%xx` 的十六進位轉大寫；unreserved 字元解碼回字面。
///
/// RFC 3986 §6.2.2.1／§6.2.2.2：`%2F` 與 `%2f` 等價，`%7E` 與 `~` 等價。
/// 不動其他已編碼字元——`%2F` 解碼成 `/` 會改變路徑結構，那不是正規化是破壞。
fn normalize_percent_encoding(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = String::with_capacity(path.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hi, lo) {
                let value = (hi * 16 + lo) as u8;
                if is_unreserved(value) {
                    out.push(value as char);
                } else {
                    out.push('%');
                    out.push_str(&format!("{value:02X}"));
                }
                i += 3;
                continue;
            }
        }
        // 非 ASCII 的 UTF-8 續位元組會走到這裡；直接按 byte 推進會切壞字元，
        // 所以用 char 邊界推進。
        let ch = path[i..].chars().next().unwrap_or('\u{fffd}');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

/// 給 log／錯誤訊息用：URL 太長時截斷。
#[must_use]
pub fn short(url: &str) -> Cow<'_, str> {
    if url.len() <= 120 {
        Cow::Borrowed(url)
    } else {
        Cow::Owned(format!("{}…", &url[..120]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn norm(raw: &str) -> String {
        canonicalize(raw).unwrap_or_else(|| panic!("`{raw}` 應可正規化"))
    }

    #[test]
    fn hostname_is_lowercased_but_path_is_not() {
        assert_eq!(
            norm("HTTPS://Example.INVALID/Path/To/Article"),
            "https://example.invalid/Path/To/Article",
            "hostname 不分大小寫，路徑分——路徑轉小寫會把兩份不同文件併成一份"
        );
    }

    #[test]
    fn fragment_is_removed() {
        assert_eq!(
            norm("https://example.invalid/a#section-3"),
            "https://example.invalid/a"
        );
    }

    #[test]
    fn tracking_params_are_removed_and_meaningful_ones_kept() {
        assert_eq!(
            norm("https://example.invalid/a?utm_source=news&id=42&fbclid=xyz&page=2"),
            "https://example.invalid/a?id=42&page=2",
            "utm_*／fbclid 要拿掉，id／page 會改變內容必須留"
        );
    }

    #[test]
    fn query_made_entirely_of_tracking_params_loses_the_question_mark() {
        assert_eq!(
            norm("https://example.invalid/a?utm_source=news&gclid=1"),
            "https://example.invalid/a",
            "只剩空 query 時連 `?` 都要拿掉，否則跟沒有 query 的同一篇比不中"
        );
    }

    #[test]
    fn utm_prefix_covers_unlisted_variants() {
        assert!(is_tracking_param("utm_reader"));
        assert!(is_tracking_param("UTM_Campaign"));
        assert!(is_tracking_param("pk_kwd"));
        assert!(!is_tracking_param("utmost"), "前綴比對不可誤傷 `utmost`");
    }

    #[test]
    fn meaningful_params_are_sorted_so_order_does_not_matter() {
        assert_eq!(
            norm("https://example.invalid/a?b=2&a=1"),
            norm("https://example.invalid/a?a=1&b=2")
        );
    }

    #[test]
    fn userinfo_is_dropped() {
        assert_eq!(
            norm("https://user:secret@example.invalid/a"),
            "https://example.invalid/a",
            "憑證不該留在 canonical_url，那個欄位有索引也會進 log"
        );
    }

    #[test]
    fn default_port_is_dropped_but_custom_port_is_kept() {
        assert_eq!(
            norm("https://example.invalid:443/a"),
            "https://example.invalid/a"
        );
        assert_eq!(
            norm("http://example.invalid:8080/a"),
            "http://example.invalid:8080/a"
        );
    }

    #[test]
    fn empty_path_becomes_slash() {
        assert_eq!(norm("https://example.invalid"), "https://example.invalid/");
        assert_eq!(
            norm("https://example.invalid"),
            norm("https://example.invalid/")
        );
    }

    #[test]
    fn percent_encoding_is_normalized() {
        // 十六進位轉大寫。
        assert_eq!(
            norm("https://example.invalid/a%2fb"),
            norm("https://example.invalid/a%2Fb")
        );
        // unreserved 字元解碼回字面。
        assert_eq!(
            norm("https://example.invalid/a%7Eb"),
            "https://example.invalid/a~b"
        );
        // 但 %2F 不可解碼成 `/`：那會改變路徑結構。
        assert_ne!(
            norm("https://example.invalid/a%2Fb"),
            "https://example.invalid/a/b"
        );
    }

    #[test]
    fn trailing_slash_is_significant() {
        assert_ne!(
            norm("https://example.invalid/a"),
            norm("https://example.invalid/a/"),
            "`/a` 與 `/a/` 在許多伺服器是不同資源，不擅自合併（已知限制）"
        );
    }

    #[test]
    fn non_url_returns_none() {
        assert_eq!(canonicalize("not a url"), None);
        assert_eq!(canonicalize(""), None);
        assert_eq!(
            canonicalize("/relative/path"),
            None,
            "相對路徑沒有 host，無法當 canonical 身分"
        );
    }

    #[test]
    fn short_truncates_only_long_urls() {
        assert_eq!(
            short("https://example.invalid/a"),
            "https://example.invalid/a"
        );
        let long = format!("https://example.invalid/{}", "x".repeat(300));
        assert!(short(&long).ends_with('…'));
    }
}
