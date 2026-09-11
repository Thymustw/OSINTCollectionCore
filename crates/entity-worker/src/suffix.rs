//! Domain 驗證用的**靜態** public suffix 清單。
//!
//! # 為什麼不用 `psl` / `publicsuffix` crate
//!
//! 實測過的事實（2026-09-12，查 `https://index.crates.io/3/p/psl`）：
//! `psl` 最新版是 `2.1.232`，發布於 **2026-09-09**。它的版號尾數就是 PSL 資料版本，
//! 每隔幾天就發一版。也就是說「用 crate 就不會過時」是錯的——只是把「維護一份清單」
//! 換成「每隔幾天 bump 一次相依版本」，清單照樣會在兩次 bump 之間過時。
//!
//! 動態抓取（啟動時下載 `publicsuffix.org/list/public_suffix_list.dat`）更不可行：
//! CLAUDE.md §5「External content is untrusted」，而且會讓 entity-worker 多一個
//! 開機期外網相依——離線環境與 CI 都跑不起來。
//!
//! # 這份清單實際在擋什麼
//!
//! **要先講清楚一件事：完整 PSL 擋不掉 `foo.bar`。** `bar` 是 2014 年委任的正式 gTLD，
//! 在完整 PSL 裡面，所以用 `psl` crate 一樣會把 `foo.bar` 判成合法 domain。
//! 任務說明裡舉的那個例子，真正需要的不是「更完整的清單」，而是**更保守的清單**。
//!
//! 所以這裡收的是「OSINT 情報文本裡實際會出現的 suffix」，刻意**不收**
//! 2012 年之後那一大批新 gTLD（`.bar`／`.pizza`／`.ninja`…）。取捨：
//!
//! | | 收到誤判（false positive） | 漏抽（false negative） |
//! |---|---|---|
//! | 完整 PSL | 高：`node.js` 不會中，但 `file.zip`／`foo.bar`／`something.sh` 都會被當 domain | 低 |
//! | 這份保守清單 | 低 | 有：冷門新 gTLD 下的 domain 抽不到 |
//!
//! V0.1 選**低誤判**。理由：誤判會污染 Entity 表且難以事後清理（要人工判斷哪些是假的），
//! 漏抽則可以在補上 suffix 後重跑抽取補回來（Document 與 RawEvidence 都還在）。
//! 「可以重跑補救的損失」優先於「需要人工清理的污染」。
//!
//! **已知限制**：新 gTLD 下的 domain 會被漏掉。要加就往 [`TLDS`] 加一行，
//! 不需要改任何邏輯——這份清單是資料，不是演算法。

/// 單標籤 suffix（即 TLD）。全小寫、不含前導點。
///
/// 內容：ISO 3166-1 兩碼 ccTLD 全集 + 傳統 gTLD + 少數在資安/OSINT 文本裡高頻出現的新 gTLD。
pub const TLDS: &[&str] = &[
    // --- 傳統 gTLD 與基礎設施 ---
    "com",
    "org",
    "net",
    "edu",
    "gov",
    "mil",
    "int",
    "arpa",
    "info",
    "biz",
    "name",
    "pro",
    "aero",
    "coop",
    "museum",
    "jobs",
    "mobi",
    "travel",
    "cat",
    "tel",
    "asia",
    "post",
    "xxx",
    // --- 資安／技術文本高頻的新 gTLD ---
    // 只收這些是刻意的。每多收一個就多一類誤判（例如收了 `zip` 之後
    // 文章裡的 `payload.zip` 就會變成一個 Domain Entity）。
    "app",
    "blog",
    "cloud",
    "club",
    "dev",
    "email",
    "link",
    "live",
    "network",
    "news",
    "one",
    "online",
    "page",
    "security",
    "services",
    "shop",
    "site",
    "software",
    "systems",
    "tech",
    "tools",
    "top",
    "xyz",
    "zone",
    // --- ISO 3166-1 alpha-2 ccTLD ---
    // 注意這裡包含 `ac`／`ad`／`ai`／`co`／`io`／`is`／`it`／`me`／`sh`／`so`／`to` 等
    // 同時也是常見英文單字或副檔名的兩碼——它們是貨真價實的 ccTLD，不能拿掉。
    // 由此產生的誤判（例如 `README.md` 的 `md`）改由 extract.rs 的形態規則擋，見該檔。
    "ac",
    "ad",
    "ae",
    "af",
    "ag",
    "ai",
    "al",
    "am",
    "ao",
    "aq",
    "ar",
    "as",
    "at",
    "au",
    "aw",
    "ax",
    "az",
    "ba",
    "bb",
    "bd",
    "be",
    "bf",
    "bg",
    "bh",
    "bi",
    "bj",
    "bm",
    "bn",
    "bo",
    "br",
    "bs",
    "bt",
    "bw",
    "by",
    "bz",
    "ca",
    "cc",
    "cd",
    "cf",
    "cg",
    "ch",
    "ci",
    "ck",
    "cl",
    "cm",
    "cn",
    "co",
    "cr",
    "cu",
    "cv",
    "cw",
    "cx",
    "cy",
    "cz",
    "de",
    "dj",
    "dk",
    "dm",
    "do",
    "dz",
    "ec",
    "ee",
    "eg",
    "er",
    "es",
    "et",
    "eu",
    "fi",
    "fj",
    "fk",
    "fm",
    "fo",
    "fr",
    "ga",
    "gd",
    "ge",
    "gf",
    "gg",
    "gh",
    "gi",
    "gl",
    "gm",
    "gn",
    "gp",
    "gq",
    "gr",
    "gs",
    "gt",
    "gu",
    "gw",
    "gy",
    "hk",
    "hm",
    "hn",
    "hr",
    "ht",
    "hu",
    "id",
    "ie",
    "il",
    "im",
    "in",
    "io",
    "iq",
    "ir",
    "is",
    "it",
    "je",
    "jm",
    "jo",
    "jp",
    "ke",
    "kg",
    "kh",
    "ki",
    "km",
    "kn",
    "kp",
    "kr",
    "kw",
    "ky",
    "kz",
    "la",
    "lb",
    "lc",
    "li",
    "lk",
    "lr",
    "ls",
    "lt",
    "lu",
    "lv",
    "ly",
    "ma",
    "mc",
    "md",
    "me",
    "mg",
    "mh",
    "mk",
    "ml",
    "mm",
    "mn",
    "mo",
    "mp",
    "mq",
    "mr",
    "ms",
    "mt",
    "mu",
    "mv",
    "mw",
    "mx",
    "my",
    "mz",
    "na",
    "nc",
    "ne",
    "nf",
    "ng",
    "ni",
    "nl",
    "no",
    "np",
    "nr",
    "nu",
    "nz",
    "om",
    "pa",
    "pe",
    "pf",
    "pg",
    "ph",
    "pk",
    "pl",
    "pm",
    "pn",
    "pr",
    "ps",
    "pt",
    "pw",
    "py",
    "qa",
    "re",
    "ro",
    "rs",
    "ru",
    "rw",
    "sa",
    "sb",
    "sc",
    "sd",
    "se",
    "sg",
    "sh",
    "si",
    "sk",
    "sl",
    "sm",
    "sn",
    "so",
    "sr",
    "ss",
    "st",
    "su",
    "sv",
    "sx",
    "sy",
    "sz",
    "tc",
    "td",
    "tf",
    "tg",
    "th",
    "tj",
    "tk",
    "tl",
    "tm",
    "tn",
    "to",
    "tr",
    "tt",
    "tv",
    "tw",
    "tz",
    "ua",
    "ug",
    "uk",
    "us",
    "uy",
    "uz",
    "va",
    "vc",
    "ve",
    "vg",
    "vi",
    "vn",
    "vu",
    "wf",
    "ws",
    "ye",
    "yt",
    "za",
    "zm",
    "zw",
    // --- 測試／保留 TLD（RFC 2606 / RFC 6761）---
    // 收進來是刻意的：本專案的測試 fixture 一律用 `.invalid`／`.example`，
    // 抽取器若不認得它們，所有 e2e 都會變成「什麼都沒抽到」而看起來像通過。
    "test",
    "example",
    "invalid",
    "localhost",
];

/// 多標籤 public suffix（不含前導點，全小寫）。
///
/// 用途有兩個，缺一不可：
/// 1. **避免把 suffix 本身當成 domain**。文章寫「註冊 .co.uk 網域」時，
///    裸 domain 的 regex 會抓到 `co.uk`；沒有這份清單就會產生一個叫 `co.uk` 的 Entity。
/// 2. 算出 registrable domain 放進 `attributes.registrable_domain`
///    （`www.example.co.uk` → `example.co.uk`，而不是錯誤的 `co.uk`）。
///
/// **這份清單是完整 PSL 的一個子集**，只收常見的。沒收到的多標籤 suffix 會讓
/// registrable domain 算成 `co.xx` 這種形狀——已知限制，寫在
/// `docs/developer/entity-worker.md`。
pub const MULTI_LABEL_SUFFIXES: &[&str] = &[
    // 英國
    "co.uk",
    "org.uk",
    "ac.uk",
    "gov.uk",
    "net.uk",
    "sch.uk",
    "police.uk",
    "nhs.uk",
    // 台灣
    "com.tw",
    "org.tw",
    "net.tw",
    "edu.tw",
    "gov.tw",
    "idv.tw",
    "mil.tw",
    "game.tw",
    // 日韓中港澳
    "co.jp",
    "or.jp",
    "ne.jp",
    "ac.jp",
    "go.jp",
    "lg.jp",
    "ad.jp",
    "co.kr",
    "or.kr",
    "ne.kr",
    "re.kr",
    "go.kr",
    "pe.kr",
    "com.cn",
    "net.cn",
    "org.cn",
    "gov.cn",
    "edu.cn",
    "ac.cn",
    "com.hk",
    "org.hk",
    "net.hk",
    "edu.hk",
    "gov.hk",
    "idv.hk",
    "com.mo",
    "org.mo",
    // 澳紐
    "com.au",
    "net.au",
    "org.au",
    "edu.au",
    "gov.au",
    "id.au",
    "asn.au",
    "co.nz",
    "org.nz",
    "net.nz",
    "ac.nz",
    "govt.nz",
    "school.nz",
    // 美洲
    "com.br",
    "net.br",
    "org.br",
    "gov.br",
    "edu.br",
    "com.mx",
    "org.mx",
    "gob.mx",
    "com.ar",
    "gob.ar",
    "edu.ar",
    "com.co",
    "gov.co",
    "edu.co",
    "com.pe",
    "com.ve",
    "com.ec",
    // 歐洲其餘
    "co.at",
    "or.at",
    "ac.at",
    "gv.at",
    "com.es",
    "org.es",
    "gob.es",
    "edu.es",
    "com.pl",
    "net.pl",
    "org.pl",
    "gov.pl",
    "com.ua",
    "gov.ua",
    "com.tr",
    "gov.tr",
    "edu.tr",
    "com.ru",
    "org.ru",
    "net.ru",
    "gov.ru",
    "co.il",
    "org.il",
    "ac.il",
    "gov.il",
    "co.rs",
    "co.de",
    // 亞非其餘
    "co.in",
    "net.in",
    "org.in",
    "gov.in",
    "ac.in",
    "edu.in",
    "co.id",
    "or.id",
    "go.id",
    "ac.id",
    "web.id",
    "com.sg",
    "edu.sg",
    "gov.sg",
    "com.my",
    "org.my",
    "gov.my",
    "edu.my",
    "co.th",
    "in.th",
    "ac.th",
    "go.th",
    "com.ph",
    "gov.ph",
    "com.vn",
    "gov.vn",
    "edu.vn",
    "com.pk",
    "gov.pk",
    "com.bd",
    "com.np",
    "com.sa",
    "com.eg",
    "gov.eg",
    "co.za",
    "org.za",
    "gov.za",
    "ac.za",
    "co.ke",
    "go.ke",
    "com.ng",
    "gov.ng",
    "co.tz",
    "co.ug",
    // 常見的「私有 public suffix」——使用者可在其下註冊子網域，所以 `github.io`
    // 本身不是一個有意義的 domain entity，`someuser.github.io` 才是。
    "github.io",
    "gitlab.io",
    "pages.dev",
    "workers.dev",
    "vercel.app",
    "netlify.app",
    "herokuapp.com",
    "azurewebsites.net",
    "cloudfront.net",
    "s3.amazonaws.com",
    "blogspot.com",
    "wordpress.com",
    "readthedocs.io",
    "firebaseapp.com",
    "web.app",
];

/// 這個字串是不是一個已知的 public suffix 本身（例如 `com`、`co.uk`、`github.io`）。
///
/// entity-worker 用它擋掉「把 suffix 本身抽成 Domain Entity」。
#[must_use]
pub fn is_public_suffix(candidate: &str) -> bool {
    let lower = candidate.to_ascii_lowercase();
    let lower = lower.trim_end_matches('.');
    TLDS.contains(&lower) || MULTI_LABEL_SUFFIXES.contains(&lower)
}

/// 取出這個 host 的 public suffix（不含前導點）。找不到回 `None`。
///
/// 優先比對較長的多標籤 suffix：`www.example.co.uk` 要回 `co.uk` 而不是 `uk`。
#[must_use]
pub fn public_suffix_of(host: &str) -> Option<&'static str> {
    let lower = host.to_ascii_lowercase();
    let lower = lower.trim_end_matches('.');

    // 多標籤優先。用 `ends_with` 還要確認前面是 `.`，否則 `notaco.uk` 會被
    // `co.uk` 命中——那是靜默的錯誤分類，不會有任何跡象。
    let mut best: Option<&'static str> = None;
    for suffix in MULTI_LABEL_SUFFIXES {
        let hit = lower == *suffix || lower.ends_with(&format!(".{suffix}"));
        if hit && best.is_none_or(|current| suffix.len() > current.len()) {
            best = Some(suffix);
        }
    }
    if best.is_some() {
        return best;
    }

    let last = lower.rsplit('.').next()?;
    TLDS.iter().find(|tld| **tld == last).copied()
}

/// 可註冊網域（public suffix + 前面一個標籤）。
///
/// `www.example.co.uk` → `example.co.uk`；`example.com` → `example.com`；
/// host 本身就是 suffix（`co.uk`）→ `None`，因為那不是任何人的網域。
#[must_use]
pub fn registrable_domain(host: &str) -> Option<String> {
    let lower = host.to_ascii_lowercase();
    let lower = lower.trim_end_matches('.');
    let suffix = public_suffix_of(lower)?;
    if lower == suffix {
        return None;
    }
    let head = lower.strip_suffix(&format!(".{suffix}"))?;
    let label = head.rsplit('.').next()?;
    if label.is_empty() {
        return None;
    }
    Some(format!("{label}.{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_suffixes_are_recognised() {
        assert!(is_public_suffix("com"));
        assert!(is_public_suffix("COM"), "比對前要折疊大小寫");
        assert!(is_public_suffix("co.uk"));
        assert!(is_public_suffix("github.io"));
        assert!(is_public_suffix("invalid"), "RFC 2606 的測試 TLD 要認得");
    }

    #[test]
    fn ordinary_domains_are_not_suffixes() {
        assert!(!is_public_suffix("example.com"));
        assert!(!is_public_suffix("example.co.uk"));
    }

    #[test]
    fn longest_suffix_wins() {
        assert_eq!(public_suffix_of("www.example.co.uk"), Some("co.uk"));
        assert_eq!(public_suffix_of("example.com"), Some("com"));
        assert_eq!(public_suffix_of("user.github.io"), Some("github.io"));
    }

    #[test]
    fn suffix_match_requires_label_boundary() {
        // `notaco.uk` 的 suffix 是 `uk`，不是 `co.uk`。用裸 ends_with 會弄錯，
        // 而且錯得毫無跡象——registrable_domain 會算成 `notaco.uk` 以外的東西。
        assert_eq!(public_suffix_of("notaco.uk"), Some("uk"));
        assert_eq!(
            registrable_domain("notaco.uk").as_deref(),
            Some("notaco.uk"),
            "suffix 是 `uk`，`notaco` 是可註冊標籤——若誤判成 `co.uk` 就會算出別的結果"
        );
        assert_eq!(
            registrable_domain("x.notaco.uk").as_deref(),
            Some("notaco.uk")
        );
    }

    #[test]
    fn registrable_domain_strips_subdomains() {
        assert_eq!(
            registrable_domain("www.news.example.co.uk").as_deref(),
            Some("example.co.uk")
        );
        assert_eq!(
            registrable_domain("blog.example.com").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            registrable_domain("example.com").as_deref(),
            Some("example.com")
        );
    }

    #[test]
    fn a_bare_suffix_has_no_registrable_domain() {
        assert_eq!(registrable_domain("co.uk"), None);
        assert_eq!(registrable_domain("com"), None);
    }

    #[test]
    fn unknown_tld_is_rejected() {
        // 這是這份清單存在的目的：擋掉看起來像 domain 的非 domain。
        assert_eq!(public_suffix_of("payload.zip"), None);
        assert_eq!(public_suffix_of("node.js"), None);
        // ⚠️ 別拿 `.py`／`.sh`／`.md`／`.pl` 當例子——它們都是真實 ccTLD。
        assert_eq!(public_suffix_of("payload.exe"), None);
        assert_eq!(public_suffix_of("dump.sql"), None);
    }

    #[test]
    fn some_file_extensions_collide_with_real_cctlds() {
        // `readme.md`／`script.sh`／`main.io` 的副檔名**都是**貨真價實的 ccTLD
        // （摩爾多瓦／聖赫勒拿／英屬印度洋領地），suffix 清單擋不掉它們，
        // 拿掉這些 ccTLD 又會漏掉真實網域。這是已知限制，
        // 緩解手段在 extract.rs（`DOMAIN_STOPWORDS` 與形態規則），不在這裡。
        assert_eq!(public_suffix_of("readme.md"), Some("md"));
        assert_eq!(public_suffix_of("script.sh"), Some("sh"));
    }

    #[test]
    fn trailing_dot_is_tolerated() {
        assert!(is_public_suffix("com."));
        assert_eq!(public_suffix_of("example.com."), Some("com"));
    }

    #[test]
    fn lists_have_no_duplicates_and_no_leading_dot() {
        for list in [TLDS, MULTI_LABEL_SUFFIXES] {
            for entry in list {
                assert!(!entry.starts_with('.'), "`{entry}` 不該有前導點");
                assert_eq!(*entry, entry.to_ascii_lowercase(), "`{entry}` 必須全小寫");
                assert_eq!(
                    list.iter().filter(|e| *e == entry).count(),
                    1,
                    "`{entry}` 在清單裡重複了"
                );
            }
        }
    }
}
