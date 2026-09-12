//! SPEC §17 的確定性抽取器。**純函式、無 I/O、無資料庫**。
//!
//! 這個模組整個是 CPU-bound 的：regex 掃描 + 驗證。呼叫端必須把它丟進
//! `spawn_blocking`（CLAUDE.md §6：CPU-heavy work must not block Tokio executor threads），
//! 見 `service.rs` 的 `extract_blocking`。
//!
//! # V0.1 的範圍限制：沒有 NER
//!
//! 這裡**只有規則**，沒有任何模型。Person 與 Organization 不從自由文本抽，
//! 只從 Document 的結構化欄位（`author` / `attributes`）取。
//! 自由文本的 NER 是 V0.3 的工作（`docs/architecture/LOCAL_AI.md`）。
//!
//! 這是刻意的：規則式抽取在自由文本上找人名／組織名的精確度非常低，
//! 而 Entity 一旦寫進 canonical store 就會被 relationship 與（V0.2 的）圖投影引用，
//! 清理成本遠高於「先不抽」。
//!
//! # 為什麼不用 `Document` 型別
//!
//! 抽取器只吃 [`ExtractionInput`]（純文字 + 幾個欄位）。這讓每一條規則都能用
//! 字串常數寫單元測試，不必先建一份 Document。

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::LazyLock;

use core_model::{EntityType, url_norm};
use regex::Regex;

/// 抽取器版本。**寫進 `entity_extractions.extractor_version`。**
///
/// 規則改了就要往上加：那一欄是之後判斷「這筆是舊規則抽的、要不要重抽」的唯一依據。
/// 沿用 crate 版本沒有用——crate 版本跟著整個 workspace 走，規則沒改也會變。
///
/// `"2"`：新增 Account 個人檔案網址抽取（GitHub／Twitter／X／Telegram）。
pub const EXTRACTOR_VERSION: &str = "2";

/// excerpt 取命中位置前後各幾個字元。
///
/// 240 是取捨後的值：足夠看出「這個 IP 是被封鎖的還是攻擊來源的」這種語意，
/// 又不會讓一篇塞滿 hash 的文章把 `relationship_evidence.excerpt` 撐爆
/// （500 筆 × 約 500 字元 ≈ 250 KB／篇，可接受）。
/// 切割一律在 **char 邊界**上，不是 byte——中日韓內容用 byte 切會產生無效 UTF-8。
pub const EXCERPT_RADIUS: usize = 240;

// ---------------------------------------------------------------------------
// regex
// ---------------------------------------------------------------------------
// 全部用 LazyLock 編譯一次。regex 的編譯成本遠高於執行，每份 Document 重編一次
// 會讓 CPU 時間幾乎全花在編譯上。`expect` 在這裡是安全的：pattern 是編譯期常數，
// 能通過一次就永遠能通過（單元測試 `all_patterns_compile` 會先踩到）。
//
// # 為什麼一律用 `(?-u:\b)` 而不是 `\b`
//
// **這是實測出來的，不是風格偏好。** `regex` crate 的 `\b` 預設是 Unicode 感知的，
// 而 CJK 字元屬於 `\w`。於是中文文本裡最常見的寫法——字與英數之間不加空白——
// 會讓兩側**都是** word 字元而不存在邊界：
//
// ```text
// "這是一則公告CVE-2026-0001"   \b → 無命中        (?-u:\b) → CVE-2026-0001
// "公告 CVE-2026-0001 影響"      \b → CVE-2026-0001  (?-u:\b) → CVE-2026-0001
// "XCVE-2026-0001"               \b → 無命中        (?-u:\b) → 無命中（正確拒絕）
// ```
//
// 這個專案的主要語料是繁體中文，用 `\b` 等於在中文文章上**靜默漏抽**——
// 不會報錯，只會回報「這篇沒有任何 entity」。
// `(?-u:\b)` 只把 ASCII `[0-9A-Za-z_]` 當 word 字元，CJK 因此構成邊界，
// 而黏著的 ASCII（`XCVE-`）仍然正確被拒絕。

/// CVE-YYYY-NNNN(N…)。SPEC §10 的 Vulnerability。
///
/// 序號至少 4 碼、沒有上限（CVE 從 2014 起就是變長的，`CVE-2021-1234567` 合法）。
/// 大小寫不敏感；前後用 `\b` 界定，避免 `XCVE-2026-0001` 這種黏著命中。
static CVE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)(?-u:\b)CVE-(\d{4})-(\d{4,})(?-u:\b)").expect("CVE pattern"));

/// IPv4 候選。**這裡只做形狀比對，值域由 `IpAddr::parse` 驗證**——
/// 在 regex 裡寫 `25[0-5]|2[0-4]\d|…` 那種值域判斷既難讀又容易寫錯，
/// 而標準函式庫已經有一份正確的實作。
static IPV4: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?-u:\b)\d{1,3}(?:\.\d{1,3}){3}(?-u:\b)").expect("IPv4 pattern"));

/// IPv6 候選。同樣只比對形狀（hex 群組 + 至少一個 `::` 或 7 個 `:`），
/// 真正的合法性交給 `IpAddr::parse`。
static IPV6: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?-u:\b)(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{0,4}(?-u:\b)").expect("IPv6 pattern")
});

/// http/https URL。刻意不接受其他 scheme：`ftp:`／`mailto:` 不是 SPEC §10 的 URL entity
/// （mailto 走 Email 抽取器）。
///
/// 尾端的標點要排除——「詳見 https://example.com/a。」的句號不是 URL 的一部分。
/// 這裡先寬鬆抓，再由 [`trim_url_tail`] 修剪。
static URL: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(?i)(?-u:\b)https?://[^\s<>"'`\\]+"#).expect("URL pattern"));

/// RFC 5322 的**簡化版**。完整的 RFC 5322 允許引號字串與註解
/// （`"very.(),:;<>[]\".VERY..\"very@\\ \"very\".unusual"@example.com` 是合法的），
/// 用 regex 完整實作它既不可讀也沒有實益——真實情報文本裡不會出現那種位址。
/// 這裡收 local-part 為 dot-atom 的形式，涵蓋實務上幾乎全部。
static EMAIL: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?-u:\b)[a-z0-9._%+\-]+@([a-z0-9](?:[a-z0-9\-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?)+)(?-u:\b)")
        .expect("Email pattern")
});

/// 裸 domain。至少兩個標籤，最後一段是字母（數字結尾的一定是 IP 或版本號）。
/// suffix 的合法性另外由 `suffix::public_suffix_of` 把關。
static BARE_DOMAIN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?-u:\b)[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9\-]*[a-z0-9])?)*\.[a-z]{2,}(?-u:\b)")
        .expect("Domain pattern")
});

/// 32／40／64 個十六進位字元。長度就是 MD5／SHA1／SHA256 的判準。
///
/// `\b` 兩側界定很關鍵：UUID（`8-4-4-4-12`）的每一段都比 32 短，而去掉連字號的
/// UUID 正好 32 位 hex——與 MD5 撞形狀。見 [`extract_hashes`] 的 UUID 排除。
static HEX_RUN: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?-u:\b)(?:[0-9a-f]{64}|[0-9a-f]{40}|[0-9a-f]{32})(?-u:\b)")
        .expect("Hash pattern")
});

/// 標準 UUID 形狀。命中的話那一段就不是 hash。
static UUID_SHAPE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(?-u:\b)[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?-u:\b)")
        .expect("UUID pattern")
});

/// 社交／程式碼平台的個人檔案網址。SPEC §10 的 Account。
///
/// 只抓第一個路徑段當 handle。`github.com/alice/repo` 的 handle 是 `alice`，
/// 後面的 repo 路徑被 `(?:/|…)` 吃掉當結尾，不會把 `alice/repo` 當成一個 handle。
///
/// **已知限制（不假裝解決了）**：
/// * handle 字元集是 `[A-Za-z0-9_]{1,32}`。GitHub 實際允許連字號（`octo-cat`），
///   Telegram 允許更長；那些這次抽不到。放寬字元集會把 `github.com/alice-vs-bob`
///   這種散文片段也抓進來，誤判成本高過漏抽。
/// * `regex` crate 沒有 look-around。`octo-cat` 會在 `-` 處形成 ASCII word
///   boundary，前綴 `octo` 會命中；[`extract_accounts`] 在擷取後丟掉這種前綴。
/// * 平台清單不窮舉：沒有 Instagram／Reddit／LinkedIn／GitLab。要加就擴
///   [`ACCOUNT_PROFILE`] 與 [`platform_from_host`]，不要在這裡發明第二套對照。
static ACCOUNT_PROFILE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(?-u:\b)(?:https?://)?(?:www\.)?(github\.com|twitter\.com|x\.com|t\.me)/([A-Za-z0-9_]{1,32})(?:/|(?-u:\b))",
    )
    .expect("Account profile pattern")
});

/// 明顯不是網域、但形狀符合 `名稱.兩個以上字母` 的常見字串。
///
/// 這是 `suffix.rs` 擋不住的那一類：副檔名與檔名剛好撞上真實 ccTLD
/// （`md` = 摩爾多瓦、`sh` = 聖赫勒拿、`io` = 英屬印度洋領地、`pl` = 波蘭…）。
/// 拿掉那些 ccTLD 會漏掉真網域，所以改成擋**整個字串**。
///
/// 這份清單是資料不是邏輯——發現新的誤判就往這裡加。
/// **已知限制**：只擋得掉列在這裡的，`config.pl` 之類沒列的仍會被當成 domain。
const DOMAIN_STOPWORDS: &[&str] = &[
    "readme.md",
    "changelog.md",
    "license.md",
    "contributing.md",
    "security.md",
    "index.md",
    "setup.py",
    "main.py",
    "app.py",
    "test.py",
    "__init__.py",
    "index.js",
    "app.js",
    "main.js",
    "server.js",
    "config.js",
    "package.json",
    "index.php",
    "config.php",
    "shell.php",
    "cmd.php",
    "main.go",
    "main.rs",
    "lib.rs",
    "mod.rs",
    "build.rs",
    "makefile.am",
    "configure.ac",
    "cargo.toml",
    "package.lock",
    "install.sh",
    "build.sh",
    "run.sh",
    "entrypoint.sh",
    "setup.sh",
    "start.sh",
    "index.html",
    "index.htm",
    "default.aspx",
];

/// 明顯不是個人檔案的第一段路徑。比對時一律小寫。
///
/// 這是**已知誤判來源，清單不窮舉**——同 T10 風格：發現新的就往這裡加，
/// 不假裝已經擋完所有平台保留路徑。沒列到的（例如 GitHub 之後新加的
/// 行銷落地頁）仍會被抽成 Account。
///
/// 三個平台共用一份清單是刻意偏保守：有人真的叫 `settings` 的 GitHub
/// 帳號會被漏掉，那個代價低於把每個平台的設定頁都寫進 Entity 表。
const ACCOUNT_RESERVED_PATHS: &[&str] = &[
    // GitHub
    "orgs",
    "settings",
    "marketplace",
    "sponsors",
    "notifications",
    "login",
    "signup",
    "join",
    "features",
    "topics",
    "collections",
    "explore",
    "about",
    "pricing",
    "enterprise",
    "security",
    "new",
    "search",
    "site",
    "apps",
    "gist",
    "pulls",
    "issues",
    "organizations",
    "users",
    "account",
    "copilot",
    "codespaces",
    "discussions",
    // Twitter / X
    "home",
    "i",
    "messages",
    "compose",
    "intent",
    "hashtag",
    "tos",
    "privacy",
    // Telegram
    "share",
    "joinchat",
    "addstickers",
    "proxy",
    "socks",
    "setlanguage",
    "addlist",
];

// ---------------------------------------------------------------------------
// 輸入／輸出型別
// ---------------------------------------------------------------------------

/// 抽取器的輸入。刻意不是 `Document`，理由見模組說明。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtractionInput {
    /// `title` + `summary` + `body` 串起來的掃描文本（已套用 body 上限）。
    pub text: String,
    /// Document 的 `author` 欄位 → Person。
    pub author: Option<String>,
    /// 從 `attributes` 取出的組織名稱（publisher／organization／…）→ Organization。
    pub organizations: Vec<String>,
}

/// 一次命中。
///
/// `text_offset` 是**字元**偏移量不是 byte 偏移量——`entity_extractions.text_offset`
/// 是給人看的定位資訊，byte 偏移量在中日韓內容上完全無法對應到肉眼看到的位置。
#[derive(Debug, Clone, PartialEq)]
pub struct Extracted {
    pub entity_type: EntityType,
    /// 原文中的樣子（保留大小寫），寫進 `entities.name`。
    pub name: String,
    /// 正規化後的身分，寫進 `entities.normalized_name`。
    pub normalized_name: String,
    /// 抽取器代號，寫進 `entity_extractions.extractor`（例如 `regex-cve`）。
    pub extractor: &'static str,
    pub confidence: f64,
    /// 字元偏移量。結構化欄位（author／organization）沒有文本位置，為 `None`。
    pub text_offset: Option<i32>,
    pub excerpt: Option<String>,
    /// 寫進 `entities.attributes` 的補充資訊（例如 IP 是否私網、hash 演算法）。
    pub attributes: BTreeMap<String, serde_json::Value>,
    /// 由這一筆衍生出來的關聯（例如 URL → 它的 host Domain）。
    /// key 是被關聯實體的 `(entity_type, normalized_name)`。
    pub derived_from: Option<DerivedLink>,
}

/// 「這個 Entity 是從另一個 Entity 衍生出來的」。
///
/// URL `https://a.example.com/x` 會同時產生一個 URL entity 與一個 Domain entity，
/// 兩者之間要有一條 relationship（SPEC §11）。這個欄位記的是**來源那一端**。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedLink {
    pub entity_type: EntityType,
    pub normalized_name: String,
}

/// 一份 Document 的抽取結果。
#[derive(Debug, Clone, PartialEq)]
pub struct ExtractionResult {
    pub items: Vec<Extracted>,
    /// 命中數超過上限而被截斷。截斷是**有界失敗**，必須讓呼叫端知道並記 log，
    /// 不可以悄悄少抽——那正是 CLAUDE.md 說的「不報錯不等於正常」。
    pub truncated: bool,
    /// 截斷前實際命中的總數。
    pub total_candidates: usize,
}

/// 抽取上限。全部有硬性預設值，沒有「不限」這個選項。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExtractionBounds {
    /// 單份 Document 最多留幾筆命中。
    ///
    /// 500 的依據：一篇正常的資安公告大約產生 10～60 筆（CVE + 幾個 IOC + 連結）。
    /// 500 給了約一個數量級的餘裕，同時把「一篇塞滿 IOC 的傾印檔」擋在
    /// 「會拖垮 worker」之外。超過就截斷並記 warn。
    pub max_extractions: usize,
    /// 只掃描文本的前 N 個 **byte**。
    ///
    /// 256 KiB：`documents.body` 沒有長度限制，而 regex 掃描是線性但常數不小的工作。
    /// 這裡用 byte 而不是 char 是因為要限制的是記憶體與 CPU，那跟 byte 成正比。
    /// 截斷一律切在 char 邊界上。
    pub max_scan_bytes: usize,
}

impl Default for ExtractionBounds {
    fn default() -> Self {
        Self {
            max_extractions: 500,
            max_scan_bytes: 256 * 1024,
        }
    }
}

// ---------------------------------------------------------------------------
// 主進入點
// ---------------------------------------------------------------------------

/// 跑完所有抽取器。**CPU-bound，呼叫端負責 `spawn_blocking`。**
#[must_use]
pub fn extract_all(input: &ExtractionInput, bounds: ExtractionBounds) -> ExtractionResult {
    let text = truncate_on_char_boundary(&input.text, bounds.max_scan_bytes);
    // 字元偏移量要靠 byte→char 的對照表。先建一次，所有抽取器共用——
    // 每命中一次就 `text[..start].chars().count()` 會讓整體變成 O(n²)，
    // 一篇 256 KiB、命中 500 次的文章要掃一億個字元。
    let index = CharIndex::new(text);

    let mut items = Vec::new();
    items.extend(extract_cves(text, &index));
    items.extend(extract_ips(text, &index));
    let urls = extract_urls(text, &index);
    items.extend(urls.clone());
    let emails = extract_emails(text, &index);
    items.extend(emails.clone());
    items.extend(extract_domains(text, &index, &urls, &emails));
    items.extend(extract_hashes(text, &index));
    items.extend(extract_accounts(text, &index));
    items.extend(extract_people(input.author.as_deref()));
    items.extend(extract_organizations(&input.organizations));

    let total_candidates = items.len();
    let truncated = total_candidates > bounds.max_extractions;
    if truncated {
        // 截斷前先排序，讓「留下來的是哪些」是決定性的而不是看抽取器的呼叫順序。
        // 沒有這一步的話，同一份 Document 重跑可能留下不同的 500 筆，冪等就破了。
        items.sort_by(|a, b| {
            (a.text_offset, &a.normalized_name).cmp(&(b.text_offset, &b.normalized_name))
        });
        items.truncate(bounds.max_extractions);
    }

    ExtractionResult {
        items,
        truncated,
        total_candidates,
    }
}

// ---------------------------------------------------------------------------
// 各抽取器
// ---------------------------------------------------------------------------

/// CVE。正規化成**大寫**（`cve-2026-0001` 與 `CVE-2026-0001` 是同一個漏洞）。
fn extract_cves(text: &str, index: &CharIndex) -> Vec<Extracted> {
    CVE.find_iter(text)
        .map(|m| Extracted {
            entity_type: EntityType::Vulnerability,
            name: m.as_str().to_string(),
            normalized_name: m.as_str().to_ascii_uppercase(),
            extractor: "regex-cve",
            // 1.0：CVE 的格式獨特到幾乎不可能誤判。
            confidence: 1.0,
            text_offset: index.char_offset(m.start()),
            excerpt: Some(index.excerpt(m.start(), m.end())),
            attributes: BTreeMap::new(),
            derived_from: None,
        })
        .collect()
}

/// IPv4 + IPv6。形狀由 regex 抓，合法性由 `IpAddr::parse` 判。
///
/// 私網／loopback **要抽**（內網 IP 出現在情報裡是有意義的），但在 attributes 標記，
/// 讓下游能分辨。
fn extract_ips(text: &str, index: &CharIndex) -> Vec<Extracted> {
    let mut out = Vec::new();
    for (regex, extractor) in [(&*IPV4, "regex-ipv4"), (&*IPV6, "regex-ipv6")] {
        for m in regex.find_iter(text) {
            let raw = m.as_str();
            // 這一步就是 `999.1.1.1`／`1.2.3.4.5`／`2026:09:12` 被擋下來的地方。
            let Ok(ip) = raw.parse::<IpAddr>() else {
                continue;
            };
            // IPv6 regex 會把時間戳 `12:34` 之類的兩段式抓進來，但那些 parse 不過。
            // 真正需要額外擋的是 IPv4-in-IPv6 以外的單純數字串，已由 parse 處理。
            let mut attributes = BTreeMap::new();
            attributes.insert(
                "ip_version".into(),
                serde_json::json!(if ip.is_ipv4() { 4 } else { 6 }),
            );
            attributes.insert("is_loopback".into(), serde_json::json!(ip.is_loopback()));
            attributes.insert("is_private".into(), serde_json::json!(is_private(&ip)));
            attributes.insert(
                "is_globally_routable".into(),
                serde_json::json!(!is_private(&ip) && !ip.is_loopback() && !ip.is_unspecified()),
            );
            out.push(Extracted {
                entity_type: EntityType::Ip,
                name: raw.to_string(),
                // `IpAddr` 的 Display 就是標準形式：IPv6 會壓縮成 `2001:db8::1`，
                // 而且大寫 hex 會轉小寫。這保證 `2001:0DB8:0000::1` 與 `2001:db8::1`
                // 對到同一個 Entity。
                normalized_name: ip.to_string(),
                extractor,
                // 不是 1.0：IPv4 的形狀與版本號（`1.2.3.4`）完全一樣，
                // regex 分不出來。這是**已知誤判**，見 docs/developer/entity-worker.md。
                confidence: if ip.is_ipv4() { 0.75 } else { 0.95 },
                text_offset: index.char_offset(m.start()),
                excerpt: Some(index.excerpt(m.start(), m.end())),
                attributes,
                derived_from: None,
            });
        }
    }
    out
}

/// http/https URL。正規化走 deduplicator 用的**同一套** canonical 規則
/// （`core_model::url_norm`），所以 URL Entity 的身分與 `documents.canonical_url` 一致。
fn extract_urls(text: &str, index: &CharIndex) -> Vec<Extracted> {
    let mut out = Vec::new();
    for m in URL.find_iter(text) {
        let trimmed = trim_url_tail(m.as_str());
        if trimmed.is_empty() {
            continue;
        }
        let Some(canonical) = url_norm::canonicalize(trimmed) else {
            continue;
        };
        let Ok(parsed) = url::Url::parse(trimmed) else {
            continue;
        };
        let mut attributes = BTreeMap::new();
        attributes.insert("scheme".into(), serde_json::json!(parsed.scheme()));
        let host = parsed.host_str().map(str::to_ascii_lowercase);
        if let Some(host) = &host {
            attributes.insert("host".into(), serde_json::json!(host));
        }
        out.push(Extracted {
            entity_type: EntityType::Url,
            name: trimmed.to_string(),
            normalized_name: canonical,
            extractor: "regex-url",
            confidence: 0.95,
            text_offset: index.char_offset(m.start()),
            excerpt: Some(index.excerpt(m.start(), m.end())),
            attributes,
            derived_from: None,
        });
    }
    out
}

/// Email。正規化成**全小寫**。
///
/// ⚠️ 嚴格來說 RFC 5321 的 local-part 是大小寫敏感的，`Bob@x` 與 `bob@x` 可以是
/// 兩個信箱。但實務上沒有任何主流郵件服務這樣做，而情報文本裡同一個信箱以不同
/// 大小寫出現是常態。**選擇合併**：漏掉一個理論上的區分，換取不會把同一個人
/// 拆成三個 Entity。這個取捨寫在 docs/developer/entity-worker.md。
fn extract_emails(text: &str, index: &CharIndex) -> Vec<Extracted> {
    let mut out = Vec::new();
    for caps in EMAIL.captures_iter(text) {
        let m = caps.get(0).expect("group 0 一定存在");
        let domain = caps.get(1).expect("EMAIL pattern 有 group 1").as_str();
        // domain 部分的 suffix 不合法就整筆丟掉——`admin@localhost.lan` 不是
        // 可用的情報，而且會在 Domain 表留下垃圾。
        if suffix_of(domain).is_none() {
            continue;
        }
        let mut attributes = BTreeMap::new();
        attributes.insert(
            "domain".into(),
            serde_json::json!(domain.to_ascii_lowercase()),
        );
        out.push(Extracted {
            entity_type: EntityType::Email,
            name: m.as_str().to_string(),
            normalized_name: m
                .as_str()
                .to_ascii_lowercase()
                .trim_end_matches('.')
                .to_string(),
            extractor: "regex-email",
            confidence: 0.95,
            text_offset: index.char_offset(m.start()),
            excerpt: Some(index.excerpt(m.start(), m.end())),
            attributes,
            derived_from: None,
        });
    }
    out
}

/// Domain。三個來源合併：URL 的 host、Email 的 domain、文本中的裸 domain。
///
/// 從 URL／Email 衍生出來的會帶 `derived_from`，服務層據此建
/// Domain ↔ URL / Domain ↔ Email 的 relationship。
fn extract_domains(
    text: &str,
    index: &CharIndex,
    urls: &[Extracted],
    emails: &[Extracted],
) -> Vec<Extracted> {
    let mut out = Vec::new();

    // 1. URL 的 host。
    for url in urls {
        let Some(host) = url.attributes.get("host").and_then(|v| v.as_str()) else {
            continue;
        };
        // host 可能是 IP（`http://192.0.2.1/`）——那不是 domain，IP 抽取器會處理。
        if host.parse::<IpAddr>().is_ok() || suffix_of(host).is_none() {
            continue;
        }
        out.push(domain_item(
            host,
            "derived-url-host",
            0.95,
            url.text_offset,
            url.excerpt.clone(),
            Some(DerivedLink {
                entity_type: EntityType::Url,
                normalized_name: url.normalized_name.clone(),
            }),
        ));
    }

    // 2. Email 的 domain。
    for email in emails {
        let Some(domain) = email.attributes.get("domain").and_then(|v| v.as_str()) else {
            continue;
        };
        out.push(domain_item(
            domain,
            "derived-email-domain",
            0.95,
            email.text_offset,
            email.excerpt.clone(),
            Some(DerivedLink {
                entity_type: EntityType::Email,
                normalized_name: email.normalized_name.clone(),
            }),
        ));
    }

    // 3. 文本中的裸 domain。
    for m in BARE_DOMAIN.find_iter(text) {
        let candidate = m.as_str().trim_end_matches('.').to_ascii_lowercase();
        if !is_plausible_bare_domain(&candidate) {
            continue;
        }
        // 落在某個 URL 或 Email 命中範圍內的，已經由上面兩步處理過了，
        // 這裡再抓一次只會產生第二筆 offset 不同的 extraction。
        if inside_any(m.start(), urls, emails, index) {
            continue;
        }
        out.push(domain_item(
            &candidate,
            "regex-domain",
            // 裸 domain 的誤判率高於從 URL 衍生的（沒有 scheme 佐證），給較低的信心。
            0.8,
            index.char_offset(m.start()),
            Some(index.excerpt(m.start(), m.end())),
            None,
        ));
    }

    out
}

fn domain_item(
    host: &str,
    extractor: &'static str,
    confidence: f64,
    text_offset: Option<i32>,
    excerpt: Option<String>,
    derived_from: Option<DerivedLink>,
) -> Extracted {
    let normalized = host.to_ascii_lowercase().trim_end_matches('.').to_string();
    let mut attributes = BTreeMap::new();
    if let Some(suffix) = crate::suffix::public_suffix_of(&normalized) {
        attributes.insert("public_suffix".into(), serde_json::json!(suffix));
    }
    if let Some(registrable) = crate::suffix::registrable_domain(&normalized) {
        attributes.insert(
            "is_registrable_domain".into(),
            serde_json::json!(registrable == normalized),
        );
        attributes.insert("registrable_domain".into(), serde_json::json!(registrable));
    }
    Extracted {
        entity_type: EntityType::Domain,
        name: host.to_string(),
        normalized_name: normalized,
        extractor,
        confidence,
        text_offset,
        excerpt,
        attributes,
        derived_from,
    }
}

/// 裸 domain 的額外形態把關。regex 與 suffix 清單擋不掉的都在這裡。
fn is_plausible_bare_domain(candidate: &str) -> bool {
    if DOMAIN_STOPWORDS.contains(&candidate) {
        return false;
    }
    // suffix 必須在清單裡。
    let Some(suffix) = crate::suffix::public_suffix_of(candidate) else {
        return false;
    };
    // 不可以只是 suffix 本身（文章寫「.co.uk 網域」時 regex 會抓到 `co.uk`）。
    if candidate == suffix {
        return false;
    }
    // 至少要有一個可註冊標籤。
    if crate::suffix::registrable_domain(candidate).is_none() {
        return false;
    }
    // 全數字的標籤 + 兩碼 suffix 幾乎都是版本號或編號，不是網域。
    let labels: Vec<&str> = candidate.split('.').collect();
    if labels
        .iter()
        .take(labels.len() - 1)
        .all(|l| l.chars().all(|c| c.is_ascii_digit()))
    {
        return false;
    }
    true
}

/// MD5／SHA1／SHA256。純 hex + 長度判斷。
///
/// 兩件事必須明確處理：
/// * **UUID**：去掉連字號的 UUID 是 32 位 hex，與 MD5 同形。帶連字號的形狀直接排除。
/// * **git commit**：40 位 hex 與 SHA1 完全同形，**在字串層面無法區分**。
///   所以**不硬判**——照樣抽成 Hash entity，但在 attributes 標 `ambiguous_sha1 = true`，
///   把判斷權留給下游（有語境的一方）。硬判會兩邊都錯：當成 commit 會漏掉真的惡意檔案
///   雜湊，當成 hash 會讓每篇技術文章的 commit id 都變成 IOC。
fn extract_hashes(text: &str, index: &CharIndex) -> Vec<Extracted> {
    let uuid_spans: Vec<(usize, usize)> = UUID_SHAPE
        .find_iter(text)
        .map(|m| (m.start(), m.end()))
        .collect();

    let mut out = Vec::new();
    for m in HEX_RUN.find_iter(text) {
        // 落在 UUID 內的 hex 段不是 hash。
        if uuid_spans
            .iter()
            .any(|(start, end)| m.start() >= *start && m.end() <= *end)
        {
            continue;
        }
        let raw = m.as_str();
        let (algorithm, ambiguous) = match raw.len() {
            32 => ("md5", false),
            40 => ("sha1", true),
            64 => ("sha256", false),
            // HEX_RUN 只會命中這三種長度；留這條讓之後加長度時不會靜默漏掉。
            _ => continue,
        };
        let mut attributes = BTreeMap::new();
        attributes.insert("algorithm".into(), serde_json::json!(algorithm));
        if ambiguous {
            attributes.insert("ambiguous_sha1".into(), serde_json::json!(true));
            attributes.insert(
                "ambiguity_note".into(),
                serde_json::json!("40 位十六進位同時符合 SHA1 與 git commit id，僅憑字串無法區分"),
            );
        }
        out.push(Extracted {
            entity_type: EntityType::Hash,
            name: raw.to_string(),
            // hex 一律小寫。
            normalized_name: raw.to_ascii_lowercase(),
            extractor: "regex-hash",
            confidence: if ambiguous { 0.7 } else { 0.9 },
            text_offset: index.char_offset(m.start()),
            excerpt: Some(index.excerpt(m.start(), m.end())),
            attributes,
            derived_from: None,
        });
    }
    out
}

/// 社交／程式碼平台的個人檔案網址 → Account。
///
/// `name` 用原樣 handle（保留大小寫），`normalized_name` 是
/// `{platform}:{handle_lowercased}`。platform 用固定字串
/// （`github`／`twitter`／`telegram`），不要用網域原文——否則
/// `x.com/alice` 與 `twitter.com/alice` 會變成兩個 Entity。
///
/// confidence 0.75，低於 URL（0.95）：路徑第一段常是行銷頁、組織頁、
/// 設定頁而不是個人檔案，[`ACCOUNT_RESERVED_PATHS`] 擋不完。
/// 寧可抽到再靠低信心標記，也不要為了乾淨把真帳號丟掉。
fn extract_accounts(text: &str, index: &CharIndex) -> Vec<Extracted> {
    let mut out = Vec::new();
    for caps in ACCOUNT_PROFILE.captures_iter(text) {
        let whole = caps.get(0).expect("group 0 一定存在");
        let host = caps.get(1).expect("ACCOUNT_PROFILE 有 host group").as_str();
        let handle = caps.get(2).expect("ACCOUNT_PROFILE 有 handle group");
        // `regex` crate 沒有 look-around。`octo-cat` 會在 `-` 處形成 word
        // boundary，前綴 `octo` 會命中。下一個位元組是 `-` 就整段丟掉。
        if text.as_bytes().get(handle.end()) == Some(&b'-') {
            continue;
        }
        let handle_raw = handle.as_str();
        let handle_lower = handle_raw.to_ascii_lowercase();
        if ACCOUNT_RESERVED_PATHS.contains(&handle_lower.as_str()) {
            continue;
        }
        let Some(platform) = platform_from_host(host) else {
            continue;
        };
        let mut attributes = BTreeMap::new();
        attributes.insert("platform".into(), serde_json::json!(platform));
        attributes.insert("handle".into(), serde_json::json!(handle_lower));
        out.push(Extracted {
            entity_type: EntityType::Account,
            name: handle_raw.to_string(),
            normalized_name: format!("{platform}:{handle_lower}"),
            extractor: "regex-account-profile",
            confidence: 0.75,
            text_offset: index.char_offset(whole.start()),
            excerpt: Some(index.excerpt(whole.start(), whole.end())),
            attributes,
            derived_from: None,
        });
    }
    out
}

/// 網域原文 → 固定 platform 字串。`x.com` 與 `twitter.com` 必須對到同一個。
fn platform_from_host(host: &str) -> Option<&'static str> {
    match host.to_ascii_lowercase().as_str() {
        "github.com" => Some("github"),
        "twitter.com" | "x.com" => Some("twitter"),
        "t.me" => Some("telegram"),
        _ => None,
    }
}

/// Person：**只**從 `Document.author` 抽。自由文本 NER 是 V0.3。
fn extract_people(author: Option<&str>) -> Vec<Extracted> {
    let Some(author) = author.map(str::trim).filter(|a| !a.is_empty()) else {
        return Vec::new();
    };
    // `author` 有時是 email（RSS 的 `<author>` 常是 `bob@x.invalid (Bob)`）。
    // 那種情況取括號內的名字，取不到就整串當名字。
    let name = author_display_name(author);
    let mut attributes = BTreeMap::new();
    attributes.insert("source_field".into(), serde_json::json!("document.author"));
    vec![Extracted {
        entity_type: EntityType::Person,
        name: name.clone(),
        normalized_name: normalize_person_or_org(&name),
        extractor: "field-author",
        // 0.6：`author` 欄位的內容品質完全取決於來源，可能是人名、帳號、
        // 也可能是「編輯部」。抽出來有價值，但不該假裝它很可靠。
        confidence: 0.6,
        text_offset: None,
        excerpt: Some(author.to_string()),
        attributes,
        derived_from: None,
    }]
}

/// Organization：**只**從 `attributes` 的結構化欄位抽。
fn extract_organizations(names: &[String]) -> Vec<Extracted> {
    names
        .iter()
        .map(|raw| raw.trim())
        .filter(|raw| !raw.is_empty())
        .map(|raw| {
            let mut attributes = BTreeMap::new();
            attributes.insert(
                "source_field".into(),
                serde_json::json!("document.attributes"),
            );
            Extracted {
                entity_type: EntityType::Organization,
                name: raw.to_string(),
                normalized_name: normalize_person_or_org(raw),
                extractor: "field-organization",
                confidence: 0.6,
                text_offset: None,
                excerpt: Some(raw.to_string()),
                attributes,
                derived_from: None,
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 輔助
// ---------------------------------------------------------------------------

/// Person／Organization 的正規化：小寫 + 空白壓成單一半形空格。
///
/// 刻意**不**做更多（不去頭銜、不轉拼音）：那些需要語境判斷，屬於 V0.3 的範圍。
fn normalize_person_or_org(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

/// `bob@x.invalid (Bob Smith)` → `Bob Smith`；沒有括號就回原字串。
fn author_display_name(author: &str) -> String {
    parenthesised(author).unwrap_or(author).to_string()
}

/// `a (b)` → `b`。沒有非空的括號內容就回 `None`。
///
/// 抽成獨立函式是為了用 `?` 串接，不用 let-chain——那是 Rust 1.88 才穩定的語法，
/// 而 workspace 宣告的 `rust-version` 是 1.85。
fn parenthesised(raw: &str) -> Option<&str> {
    let open = raw.find('(')?;
    let rest = &raw[open + 1..];
    let close = rest.find(')')?;
    let inner = rest[..close].trim();
    (!inner.is_empty()).then_some(inner)
}

/// 削掉 URL 尾端的句讀。`https://x/a).` → `https://x/a`。
///
/// 括號要配對處理：維基百科式的 `https://x/Foo_(bar)` 尾括號是 URL 的一部分。
fn trim_url_tail(raw: &str) -> &str {
    let mut end = raw.len();
    while end > 0 {
        let tail = raw[..end].chars().next_back().expect("end > 0");
        let strip = match tail {
            '.' | ',' | ';' | ':' | '!' | '?' | '"' | '\'' | '，' | '。' | '、' | '；' | '：' => {
                true
            }
            ')' => {
                // 只有在「右括號比左括號多」時才削掉。
                let opens = raw[..end].matches('(').count();
                let closes = raw[..end].matches(')').count();
                closes > opens
            }
            ']' => {
                let opens = raw[..end].matches('[').count();
                let closes = raw[..end].matches(']').count();
                closes > opens
            }
            _ => false,
        };
        if !strip {
            break;
        }
        end -= tail.len_utf8();
    }
    &raw[..end]
}

/// 這個位置是否落在任何 URL／Email 的命中範圍內。
fn inside_any(
    byte_start: usize,
    urls: &[Extracted],
    emails: &[Extracted],
    index: &CharIndex,
) -> bool {
    let Some(offset) = index.char_offset(byte_start) else {
        return false;
    };
    urls.iter().chain(emails.iter()).any(|item| {
        match (item.text_offset, item.name.chars().count()) {
            (Some(start), len) => {
                offset >= start && offset < start.saturating_add(i32::try_from(len).unwrap_or(0))
            }
            _ => false,
        }
    })
}

/// domain 字串的 suffix（給 email 用）。
fn suffix_of(domain: &str) -> Option<&'static str> {
    crate::suffix::public_suffix_of(domain)
}

/// RFC 1918 / RFC 4193 等私有位址。
///
/// `Ipv4Addr::is_private` 只涵蓋 RFC 1918 三段，不含 CGNAT（100.64/10）與
/// link-local（169.254/16）；`Ipv6Addr::is_unique_local` 在 Rust 1.85 仍是 unstable，
/// 所以自己判斷前綴。
fn is_private(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_link_local()
                // CGNAT 100.64.0.0/10
                || (v4.octets()[0] == 100 && (64..=127).contains(&v4.octets()[1]))
        }
        IpAddr::V6(v6) => {
            let first = v6.segments()[0];
            // fc00::/7（unique local）或 fe80::/10（link-local）
            (first & 0xfe00) == 0xfc00 || (first & 0xffc0) == 0xfe80
        }
    }
}

/// 在 char 邊界上把字串截到最多 `max_bytes` 個 byte。
///
/// `pub(crate)`：`service::extraction_input` 在串接 title/summary/body 時就要套用上限，
/// 而不是串完 50 MB 再丟掉 99%。
pub(crate) fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// byte offset → char offset 的對照，外加 excerpt 切割。
///
/// 建一次共用。理由見 [`extract_all`]。
struct CharIndex<'a> {
    text: &'a str,
    /// `byte_to_char[i]` = 第 i 個 char 的起始 byte offset。
    char_starts: Vec<usize>,
}

impl<'a> CharIndex<'a> {
    fn new(text: &'a str) -> Self {
        Self {
            text,
            char_starts: text.char_indices().map(|(i, _)| i).collect(),
        }
    }

    /// byte offset 對應的 char offset。
    ///
    /// 回 `Option<i32>` 而不是 `i32`：`entity_extractions.text_offset` 是 `INTEGER`，
    /// 超過 `i32::MAX` 的文件存不進去。與其靜默截斷成錯誤的位置，不如回 `None`
    /// （欄位可為空，語意是「位置不明」）。實務上 `max_scan_bytes` 遠小於 2 GiB，
    /// 這條路走不到，但不要留一個會在極端輸入下寫錯資料的縫。
    fn char_offset(&self, byte_offset: usize) -> Option<i32> {
        let idx = self
            .char_starts
            .partition_point(|start| *start < byte_offset);
        i32::try_from(idx).ok()
    }

    /// 命中前後各 [`EXCERPT_RADIUS`] 個字元的上下文。切在 char 邊界上。
    fn excerpt(&self, byte_start: usize, byte_end: usize) -> String {
        let start_char = self.char_starts.partition_point(|s| *s < byte_start);
        let end_char = self.char_starts.partition_point(|s| *s < byte_end);
        let from = start_char.saturating_sub(EXCERPT_RADIUS);
        let to = (end_char + EXCERPT_RADIUS).min(self.char_starts.len());
        let from_byte = self.char_starts.get(from).copied().unwrap_or(0);
        let to_byte = self.char_starts.get(to).copied().unwrap_or(self.text.len());
        self.text[from_byte..to_byte].trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(text: &str) -> Vec<Extracted> {
        extract_all(
            &ExtractionInput {
                text: text.into(),
                author: None,
                organizations: Vec::new(),
            },
            ExtractionBounds::default(),
        )
        .items
    }

    fn names_of(items: &[Extracted], kind: EntityType) -> Vec<String> {
        let mut out: Vec<String> = items
            .iter()
            .filter(|i| i.entity_type == kind)
            .map(|i| i.normalized_name.clone())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    #[test]
    fn all_patterns_compile() {
        // LazyLock 內的 expect 只有在第一次使用時才會炸。先全部碰一次，
        // 讓 pattern 寫錯在這個測試失敗，而不是在跑 e2e 時才炸在 worker 裡。
        for regex in [
            &*CVE,
            &*IPV4,
            &*IPV6,
            &*URL,
            &*EMAIL,
            &*BARE_DOMAIN,
            &*HEX_RUN,
            &*UUID_SHAPE,
            &*ACCOUNT_PROFILE,
        ] {
            let _ = regex.is_match("x");
        }
    }

    // ---- CVE ----

    #[test]
    fn cve_is_uppercased() {
        let items = run("影響 cve-2026-0001 與 CVE-2026-12345。");
        assert_eq!(
            names_of(&items, EntityType::Vulnerability),
            vec!["CVE-2026-0001", "CVE-2026-12345"]
        );
    }

    #[test]
    fn cve_same_id_twice_yields_two_extractions_with_different_offsets() {
        let items: Vec<_> = run("CVE-2026-0001 ... 再提一次 CVE-2026-0001")
            .into_iter()
            .filter(|i| i.entity_type == EntityType::Vulnerability)
            .collect();
        assert_eq!(items.len(), 2, "同一個 CVE 出現兩次要有兩筆 extraction");
        assert_eq!(items[0].normalized_name, items[1].normalized_name);
        assert_ne!(
            items[0].text_offset, items[1].text_offset,
            "兩筆的 offset 必須不同，否則服務層會把它們當成同一筆而只寫一列"
        );
    }

    #[test]
    fn entities_glued_to_cjk_text_are_still_found() {
        // 迴歸測試。用預設的 Unicode `\b` 時這些**全部抽不到**——CJK 屬於 \w，
        // 與後面的 ASCII 之間不存在 word boundary。中文文章不加空白是常態，
        // 所以那個版本等於在主要語料上靜默漏抽。見模組開頭的說明。
        let items = run(
            "資安公告CVE-2026-0001指出，受影響主機203.0.113.5與網域example.com，\
             聯絡信箱soc@example.com，樣本雜湊d41d8cd98f00b204e9800998ecf8427e。",
        );
        assert_eq!(
            names_of(&items, EntityType::Vulnerability),
            vec!["CVE-2026-0001"]
        );
        assert_eq!(names_of(&items, EntityType::Ip), vec!["203.0.113.5"]);
        assert_eq!(names_of(&items, EntityType::Email), vec!["soc@example.com"]);
        assert_eq!(
            names_of(&items, EntityType::Hash),
            vec!["d41d8cd98f00b204e9800998ecf8427e"]
        );
        assert!(
            names_of(&items, EntityType::Domain).contains(&"example.com".to_string()),
            "實際抽到的 Domain：{:?}",
            names_of(&items, EntityType::Domain)
        );
    }

    #[test]
    fn cve_needs_word_boundary_and_four_digit_year() {
        assert!(names_of(&run("XCVE-2026-0001"), EntityType::Vulnerability).is_empty());
        assert!(names_of(&run("CVE-26-0001"), EntityType::Vulnerability).is_empty());
        assert!(names_of(&run("CVE-2026-001"), EntityType::Vulnerability).is_empty());
    }

    // ---- IP ----

    #[test]
    fn valid_ipv4_is_extracted_and_invalid_is_not() {
        let items = run("攻擊來自 203.0.113.5，但 999.1.1.1 與 1.2.3.4.5 不是位址。");
        let ips = names_of(&items, EntityType::Ip);
        assert!(ips.contains(&"203.0.113.5".to_string()));
        assert!(
            !ips.iter().any(|i| i.starts_with("999")),
            "999.1.1.1 的每一段都超過 255，IpAddr::parse 必須擋下來"
        );
    }

    #[test]
    fn private_and_loopback_ips_are_extracted_but_flagged() {
        let items = run("內網 10.1.2.3 與 127.0.0.1 也要記錄。");
        let private = items
            .iter()
            .find(|i| i.normalized_name == "10.1.2.3")
            .expect("私網 IP 也要抽——這是 OSINT，內網位址出現在情報裡是有意義的");
        assert_eq!(private.attributes["is_private"], serde_json::json!(true));
        assert_eq!(
            private.attributes["is_globally_routable"],
            serde_json::json!(false)
        );
        let loopback = items
            .iter()
            .find(|i| i.normalized_name == "127.0.0.1")
            .expect("loopback");
        assert_eq!(loopback.attributes["is_loopback"], serde_json::json!(true));
    }

    #[test]
    fn cgnat_and_link_local_count_as_private() {
        let items = run("100.64.0.1 與 169.254.1.1");
        for name in ["100.64.0.1", "169.254.1.1"] {
            let item = items
                .iter()
                .find(|i| i.normalized_name == name)
                .unwrap_or_else(|| panic!("{name} 應被抽出"));
            assert_eq!(
                item.attributes["is_private"],
                serde_json::json!(true),
                "{name}：Ipv4Addr::is_private 不含 CGNAT 與 link-local，要自己補"
            );
        }
    }

    #[test]
    fn ipv6_is_normalised_to_compressed_form() {
        let items = run("位址 2001:0DB8:0000:0000:0000:0000:0000:0001 出現。");
        assert_eq!(
            names_of(&items, EntityType::Ip),
            vec!["2001:db8::1"],
            "IPv6 要壓縮 + 轉小寫，否則同一個位址的不同寫法會變成兩個 Entity"
        );
    }

    #[test]
    fn a_version_number_is_extracted_as_ip_known_false_positive() {
        // 這是**已知誤判**，不是 bug。用測試釘住現況，免得有人以為它被處理掉了。
        // 緩解手段是 confidence 0.75，不是丟掉——丟掉會漏真實 IP。
        let items = run("升級到 1.2.3.4 版本");
        let ip = items
            .iter()
            .find(|i| i.entity_type == EntityType::Ip)
            .expect("已知誤判：版本號與 IPv4 同形");
        assert!(
            ip.confidence < 0.8,
            "IPv4 的 confidence 必須低於 0.8 以反映這個誤判"
        );
    }

    // ---- URL / Domain ----

    #[test]
    fn url_is_canonicalised_with_the_same_rules_as_dedup() {
        let items = run("詳見 https://Example.COM/a?utm_source=x&id=1#frag 。");
        assert_eq!(
            names_of(&items, EntityType::Url),
            vec!["https://example.com/a?id=1"],
            "必須與 deduplicator 的 canonical_url 一致（同一份 url_norm）"
        );
    }

    #[test]
    fn trailing_punctuation_is_not_part_of_the_url() {
        assert_eq!(
            names_of(&run("見 https://example.com/a。"), EntityType::Url),
            vec!["https://example.com/a"]
        );
        assert_eq!(
            names_of(&run("見 (https://example.com/a)."), EntityType::Url),
            vec!["https://example.com/a"]
        );
    }

    #[test]
    fn balanced_parentheses_stay_in_the_url() {
        assert_eq!(
            names_of(&run("見 https://example.com/Foo_(bar)"), EntityType::Url),
            vec!["https://example.com/Foo_(bar)"]
        );
    }

    #[test]
    fn url_host_produces_a_linked_domain() {
        let items = run("https://news.example.com/a");
        let domain = items
            .iter()
            .find(|i| i.entity_type == EntityType::Domain)
            .expect("URL 的 host 要衍生出 Domain");
        assert_eq!(domain.normalized_name, "news.example.com");
        assert_eq!(
            domain.derived_from.as_ref().map(|d| d.entity_type),
            Some(EntityType::Url),
            "Domain 與 URL 之間要能建 relationship"
        );
        assert_eq!(
            domain.attributes["registrable_domain"],
            serde_json::json!("example.com")
        );
    }

    #[test]
    fn url_with_ip_host_does_not_create_a_domain() {
        let items = run("https://192.0.2.1/a");
        assert!(
            names_of(&items, EntityType::Domain).is_empty(),
            "IP 當 host 時不該產生 Domain entity"
        );
        assert_eq!(names_of(&items, EntityType::Ip), vec!["192.0.2.1"]);
    }

    #[test]
    fn fake_domain_is_rejected_by_the_suffix_list() {
        assert!(
            names_of(&run("檔案 payload.zip 與 node.js"), EntityType::Domain).is_empty(),
            "zip／js 不在 suffix 清單裡"
        );
    }

    #[test]
    fn stopwords_block_filenames_whose_extension_is_a_real_cctld() {
        // `md` 是摩爾多瓦的 ccTLD，suffix 清單擋不掉 readme.md，靠 stopword。
        assert!(names_of(&run("見 README.md"), EntityType::Domain).is_empty());
        assert!(names_of(&run("執行 install.sh"), EntityType::Domain).is_empty());
    }

    #[test]
    fn a_bare_public_suffix_is_not_a_domain() {
        assert!(
            names_of(&run("註冊一個 .co.uk 網域"), EntityType::Domain).is_empty(),
            "`co.uk` 本身是 public suffix，不是任何人的網域"
        );
    }

    #[test]
    fn numeric_labels_are_not_domains() {
        assert!(names_of(&run("版本 1.2.3.co"), EntityType::Domain).is_empty());
    }

    // ---- Email ----

    #[test]
    fn email_is_lowercased_and_links_a_domain() {
        let items = run("回報給 Security@Example.COM 。");
        assert_eq!(
            names_of(&items, EntityType::Email),
            vec!["security@example.com"]
        );
        let domain = items
            .iter()
            .find(|i| i.entity_type == EntityType::Domain)
            .expect("Email 的 domain 要衍生出 Domain entity");
        assert_eq!(domain.normalized_name, "example.com");
        assert_eq!(
            domain.derived_from.as_ref().map(|d| d.entity_type),
            Some(EntityType::Email)
        );
    }

    #[test]
    fn email_with_unknown_suffix_is_dropped() {
        assert!(
            names_of(&run("寄到 root@localhost.lan"), EntityType::Email).is_empty(),
            "`lan` 不在 suffix 清單，整筆丟掉"
        );
    }

    // ---- Hash ----

    #[test]
    fn md5_sha1_sha256_are_recognised_by_length() {
        let md5 = "d41d8cd98f00b204e9800998ecf8427e";
        let sha1 = "da39a3ee5e6b4b0d3255bfef95601890afd80709";
        let sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let items = run(&format!("{md5} {sha1} {sha256}"));
        let mut algorithms: Vec<&str> = items
            .iter()
            .filter(|i| i.entity_type == EntityType::Hash)
            .map(|i| i.attributes["algorithm"].as_str().expect("algorithm"))
            .collect();
        algorithms.sort_unstable();
        assert_eq!(algorithms, vec!["md5", "sha1", "sha256"]);
    }

    #[test]
    fn hash_is_lowercased() {
        let items = run("D41D8CD98F00B204E9800998ECF8427E");
        assert_eq!(
            names_of(&items, EntityType::Hash),
            vec!["d41d8cd98f00b204e9800998ecf8427e"]
        );
    }

    #[test]
    fn a_uuid_is_not_a_hash() {
        let items = run("關聯 id 是 0199f3aa-1b2c-7d3e-8f40-a1b2c3d4e5f6 。");
        assert!(
            names_of(&items, EntityType::Hash).is_empty(),
            "帶連字號的 UUID 不是 hash"
        );
    }

    #[test]
    fn a_git_commit_is_flagged_ambiguous_not_dropped() {
        let items = run("修正於 commit da39a3ee5e6b4b0d3255bfef95601890afd80709");
        let hash = items
            .iter()
            .find(|i| i.entity_type == EntityType::Hash)
            .expect("40 位 hex 仍要抽出來，不硬判成 commit");
        assert_eq!(
            hash.attributes["ambiguous_sha1"],
            serde_json::json!(true),
            "40 位 hex 無法憑字串區分 SHA1 與 git commit，要標記而不是硬判"
        );
        assert!(hash.confidence < 0.8);
    }

    // ---- Person / Organization ----

    #[test]
    fn person_comes_only_from_the_author_field() {
        let result = extract_all(
            &ExtractionInput {
                text: "報導指出 Alice Chen 與 Bob Smith 參與調查。".into(),
                author: Some("  Carol  Wu ".into()),
                organizations: vec!["Example Security Lab".into()],
            },
            ExtractionBounds::default(),
        );
        assert_eq!(
            names_of(&result.items, EntityType::Person),
            vec!["carol wu"],
            "自由文本裡的人名**不抽**（V0.1 無 NER），只抽 author 欄位；空白要壓成單一空格"
        );
        assert_eq!(
            names_of(&result.items, EntityType::Organization),
            vec!["example security lab"]
        );
    }

    #[test]
    fn rss_style_author_takes_the_parenthesised_display_name() {
        let result = extract_all(
            &ExtractionInput {
                text: String::new(),
                author: Some("bob@example.com (Bob Smith)".into()),
                organizations: Vec::new(),
            },
            ExtractionBounds::default(),
        );
        let person = result
            .items
            .iter()
            .find(|i| i.entity_type == EntityType::Person)
            .expect("person");
        assert_eq!(person.name, "Bob Smith");
    }

    #[test]
    fn empty_author_produces_nothing() {
        assert!(extract_people(Some("   ")).is_empty());
        assert!(extract_people(None).is_empty());
    }

    // ---- offset / excerpt ----

    #[test]
    fn offset_is_in_characters_not_bytes() {
        // 前綴是 5 個中文字（15 bytes）。offset 必須是 5。
        let items = run("這是一則公告CVE-2026-0001");
        let cve = items
            .iter()
            .find(|i| i.entity_type == EntityType::Vulnerability)
            .expect("cve");
        assert_eq!(
            cve.text_offset,
            Some(6),
            "offset 要用字元數。用 byte 的話中日韓內容的位置完全對不上肉眼所見"
        );
    }

    #[test]
    fn excerpt_is_valid_utf8_around_multibyte_text() {
        let padding = "中".repeat(600);
        let items = run(&format!("{padding}CVE-2026-0001{padding}"));
        let cve = items
            .iter()
            .find(|i| i.entity_type == EntityType::Vulnerability)
            .expect("cve");
        let excerpt = cve.excerpt.as_ref().expect("excerpt");
        assert!(excerpt.contains("CVE-2026-0001"));
        assert!(
            excerpt.chars().count() <= EXCERPT_RADIUS * 2 + 13,
            "excerpt 長度要受 EXCERPT_RADIUS 控制"
        );
    }

    // ---- bounds ----

    #[test]
    fn extraction_count_is_bounded_and_reports_truncation() {
        let many = (0..50)
            .map(|i| format!("CVE-2026-{:04}", i + 1))
            .collect::<Vec<_>>()
            .join(" ");
        let result = extract_all(
            &ExtractionInput {
                text: many,
                author: None,
                organizations: Vec::new(),
            },
            ExtractionBounds {
                max_extractions: 10,
                max_scan_bytes: 1024 * 1024,
            },
        );
        assert_eq!(result.items.len(), 10, "必須截斷到上限");
        assert!(result.truncated, "截斷必須回報，不可以靜默少抽");
        assert_eq!(result.total_candidates, 50);
    }

    #[test]
    fn truncation_keeps_the_same_items_every_time() {
        // 冪等的前提：同一份輸入截斷後留下的必須是同一批。
        let many = (0..50)
            .map(|i| format!("CVE-2026-{:04}", i + 1))
            .collect::<Vec<_>>()
            .join(" ");
        let bounds = ExtractionBounds {
            max_extractions: 10,
            max_scan_bytes: 1024 * 1024,
        };
        let input = ExtractionInput {
            text: many,
            author: None,
            organizations: Vec::new(),
        };
        let first = extract_all(&input, bounds);
        let second = extract_all(&input, bounds);
        assert_eq!(first.items, second.items);
    }

    #[test]
    fn scan_is_limited_to_max_scan_bytes() {
        let filler = "x".repeat(1000);
        let result = extract_all(
            &ExtractionInput {
                text: format!("{filler} CVE-2026-0001"),
                author: None,
                organizations: Vec::new(),
            },
            ExtractionBounds {
                max_extractions: 500,
                max_scan_bytes: 100,
            },
        );
        assert!(
            result.items.is_empty(),
            "超過 max_scan_bytes 的部分不該被掃到"
        );
    }

    #[test]
    fn truncating_scan_never_splits_a_character() {
        // 切在多位元組字元中間會 panic（字串切片要求 char 邊界）。
        let text = "中".repeat(100);
        for limit in 0..=text.len() {
            let cut = truncate_on_char_boundary(&text, limit);
            assert!(cut.len() <= limit);
        }
    }

    #[test]
    fn default_bounds_are_finite() {
        let bounds = ExtractionBounds::default();
        assert_eq!(bounds.max_extractions, 500);
        assert_eq!(bounds.max_scan_bytes, 256 * 1024);
    }

    // ---- Account ----

    fn account_of<'a>(items: &'a [Extracted], normalized: &str) -> &'a Extracted {
        items
            .iter()
            .find(|i| i.entity_type == EntityType::Account && i.normalized_name == normalized)
            .unwrap_or_else(|| {
                panic!(
                    "應抽出 Account `{normalized}`，實際 {:?}",
                    names_of(items, EntityType::Account)
                )
            })
    }

    #[test]
    fn github_profile_extracts_platform_and_handle() {
        let items = run("見 https://github.com/Alice 的檔案。");
        let account = account_of(&items, "github:alice");
        assert_eq!(account.name, "Alice");
        assert_eq!(account.attributes["platform"], serde_json::json!("github"));
        assert_eq!(account.attributes["handle"], serde_json::json!("alice"));
        assert_eq!(account.extractor, "regex-account-profile");
        assert!(
            (account.confidence - 0.75).abs() < f64::EPSILON,
            "Account 誤判風險高於 URL，confidence 應為 0.75，實際 {}",
            account.confidence
        );
    }

    #[test]
    fn twitter_and_x_collapse_to_the_same_platform() {
        let items = run("https://twitter.com/Bob 與 https://x.com/Bob 是同一個帳號。");
        let accounts: Vec<_> = items
            .iter()
            .filter(|i| i.entity_type == EntityType::Account)
            .collect();
        assert_eq!(
            accounts.len(),
            2,
            "兩種網域寫法各抽一筆 extraction，但必須對到同一個 Entity 自然鍵"
        );
        assert!(
            accounts.iter().all(|a| a.normalized_name == "twitter:bob"
                && a.attributes["platform"] == serde_json::json!("twitter")),
            "x.com 與 twitter.com 不可拆成兩個 platform，實際 {:?}",
            accounts
                .iter()
                .map(|a| (&a.normalized_name, &a.attributes["platform"]))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn telegram_profile_extracts_telegram_platform() {
        let items = run("聯絡 t.me/SocDesk");
        let account = account_of(&items, "telegram:socdesk");
        assert_eq!(account.name, "SocDesk");
        assert_eq!(
            account.attributes["platform"],
            serde_json::json!("telegram")
        );
    }

    #[test]
    fn reserved_account_paths_are_not_handles() {
        for text in [
            "見 https://github.com/orgs/example",
            "https://github.com/settings/profile",
            "https://github.com/marketplace/actions",
            "https://github.com/sponsors/alice",
            "https://github.com/notifications",
            "https://twitter.com/home",
            "https://x.com/explore",
            "https://x.com/settings",
            "https://x.com/i/flow",
            "https://t.me/share/url",
            "https://t.me/joinchat/AAAA",
        ] {
            assert!(
                names_of(&run(text), EntityType::Account).is_empty(),
                "`{text}` 的第一段路徑是保留字，不該抽成 handle"
            );
        }
    }

    #[test]
    fn github_hyphenated_handle_is_a_known_miss() {
        // 已知限制，不是 bug。字元集不含 `-`，否則 `alice-vs-bob` 這種散文會誤判。
        // `regex` crate 沒有 look-around，所以靠擷取後檢查丟掉前綴命中：
        // 沒有那步的話 `octo-cat` 會抽出 `octo`。
        assert!(
            names_of(&run("https://github.com/octo-cat"), EntityType::Account).is_empty(),
            "連字號 handle 這次刻意不抽，也不可把前綴當 handle"
        );
    }

    #[test]
    fn scheme_less_and_www_prefixed_profiles_are_extracted() {
        let items = run("見 github.com/Carol 與 https://www.github.com/Carol");
        let accounts: Vec<_> = names_of(&items, EntityType::Account);
        assert_eq!(accounts, vec!["github:carol"]);
    }

    #[test]
    fn account_glued_to_cjk_is_still_found() {
        let items = run("帳號見https://github.com/alice詳情。");
        assert_eq!(names_of(&items, EntityType::Account), vec!["github:alice"]);
    }

    // ---- Acceptance D 的五種型別 ----

    #[test]
    fn acceptance_d_five_entity_types_from_one_article() {
        let items = run("公告 CVE-2026-0001 影響 example.com，\
             攻擊來源 203.0.113.5，回報信箱 soc@example.com，\
             樣本 SHA256 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855。");
        for kind in [
            EntityType::Vulnerability,
            EntityType::Domain,
            EntityType::Ip,
            EntityType::Email,
            EntityType::Hash,
        ] {
            assert!(
                items.iter().any(|i| i.entity_type == kind),
                "SPEC §26 Acceptance D 要求 {kind:?} 必須被抽出，實際抽到 {:?}",
                items
                    .iter()
                    .map(|i| (i.entity_type, &i.normalized_name))
                    .collect::<Vec<_>>()
            );
        }
    }
}
