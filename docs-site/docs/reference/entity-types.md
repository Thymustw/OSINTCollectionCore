# 實體類型

這一頁說明系統支援的所有實體類型、各類型怎麼產生，以及相關的列舉值。

---

## 實體類型（EntityType）

序列化值是 snake_case（例如 `threat_actor`）。

| 類型值 | 說明 | 怎麼產生 |
|---|---|---|
| `person` | 人員（例如文章作者） | 從文件的 `author` 欄位取得 |
| `organization` | 組織（例如發布單位、廠商） | 從文件的特定結構化欄位取得（見下方） |
| `account` | 社群帳號（GitHub、Twitter/X、Telegram） | 從文字中的個人檔案網址抽取（例如 `github.com/alice`） |
| `domain` | 網域（例如 `example.com`） | 從文字自動抽取，經 public suffix 驗證 |
| `hostname` | 主機名稱（含子網域） | 從文字自動抽取 |
| `ip` | IP 位址（IPv4 或 IPv6） | 從文字自動抽取，經 `IpAddr::parse` 驗證 |
| `url` | 網址（http/https） | 從文字自動抽取（只接受 http/https scheme） |
| `email` | 電子郵件位址 | 從文字自動抽取 |
| `vulnerability` | 漏洞（CVE 編號） | 從文字自動抽取（格式 `CVE-YYYY-NNNN(N…)`，大小寫不敏感） |
| `software` | 軟體 | 目前只能透過 STIX 匯入 |
| `product` | 產品 | 目前只能透過 STIX 匯入 |
| `repository` | 程式碼儲存庫 | 目前只能透過 STIX 匯入 |
| `hash` | 雜湊值（MD5、SHA-1、SHA-256） | 從文字自動抽取（按十六進位長度判斷：32=MD5、40=SHA-1、64=SHA-256） |
| `location` | 地理位置 | 目前只能透過 STIX 匯入 |
| `threat_actor` | 威脅行為者 | 目前只能透過 STIX 匯入 |
| `malware` | 惡意程式 | 目前只能透過 STIX 匯入 |
| `indicator` | 情報指標（STIX Indicator 物件） | 目前只能透過 STIX 匯入 |

---

## 自動抽取規則詳細說明

抽取器（`entity-worker`）在文件的標題、摘要、本文掃描正規表示式。以下是目前實作的抽取規則。

### CVE（vulnerability）

- 格式：`CVE-<4 位年份>-<4 位以上序號>`（大小寫不敏感）
- 序號沒有上限（例如 `CVE-2021-1234567` 是合法的）
- 前後需要詞界（word boundary），避免 `XCVE-2026-0001` 誤命中

### IP 位址（ip）

- IPv4：形狀比對後由 `IpAddr::parse` 驗證值域
- IPv6：形狀比對後由 `IpAddr::parse` 驗證（hex 群組 + `::` 或多個 `:`）

### 網址（url）

- 只接受 `http://` 或 `https://` 開頭
- 尾端標點（句號、括號等）會被修剪

### Email（email）

- local-part 是 dot-atom 形式（不支援引號字串的完整 RFC 5322）

### 網域（domain）

- 至少兩個標籤，最後一段是字母（數字結尾被排除）
- 通過 public suffix 驗證
- 常見副檔名與設定檔名（`readme.md`、`main.py` 等）不抽

### 雜湊值（hash）

- 長度判斷：32 個十六進位字元 = MD5、40 = SHA-1、64 = SHA-256
- 去掉連字號的 UUID（剛好 32 個十六進位）會被排除

### 帳號（account）

從個人檔案網址抽取，支援以下平台：

| 平台 | URL 格式 |
|---|---|
| GitHub | `github.com/<handle>` |
| Twitter | `twitter.com/<handle>` |
| X | `x.com/<handle>` |
| Telegram | `t.me/<handle>` |

已知限制：handle 字元集目前限 `[A-Za-z0-9_]`（1-32 字元）。GitHub 允許連字號的帳號名（例如 `octo-cat`）目前無法抽取。

### 人員（person）

從文件的 `author` 欄位取得（結構化欄位，不從自由文字抽取）。

### 組織（organization）

從文件屬性的結構化欄位取得（不從自由文字抽取），鍵名依序為 `publisher`、
`organization`、`organisation`、`vendor`、`feed_title`、`site_name`。這些鍵由
上游在整理文件時寫入：

| 來源 | 寫入的鍵 | 取自 |
|---|---|---|
| JSON／CSV 匯入 | `publisher` | `publisher`／`vendor`／`organization`／`organisation`／`source_name`（可用 `mapping.publisher` 覆寫） |
| RSS／Atom | `feed_title` | feed 層標題 |
| 靜態網頁 | `site_name` | `og:site_name` |
| STIX 匯入 | （直接建實體） | `identity` 且 `identity_class` 為 `organization`。見 [STIX 匯入匯出](../usage/stix.md) |

抽出後會建立 `published_by` 關聯。空字串與 `null` 不會寫進屬性，因此也不會變成假的組織實體。

!!! warning "REST API 收集不會產生文件，因此也不會產生組織實體"
    REST API connector 只把回應存成原始證據，不拆成文件。沒有文件就沒有屬性可抽，
    組織抽取這條路不會觸發。系統也沒有提供手動建立實體或關聯的 API
    （`POST /api/v1/objects` 固定回 501，沒有 `POST /api/v1/entities`）。
    文章內文裡提到的組織名同樣不會被自動識別，見 [目前的限制](../architecture/limitations.md)。

---

## 關聯類型（RelationshipType）

| 類型值 | 說明 |
|---|---|
| `mentions` | 文件提到某個實體 |
| `references` | 文件引用（超連結到）某個實體 |
| `published_by` | 文件由某個組織發布 |
| `authored_by` | 文件作者是某個人 |
| `links_to` | 網址連到另一個資源 |
| `affects` | 漏洞影響某個軟體或產品 |
| `belongs_to` | 網址屬於某個網域；Email 屬於某個網域 |
| `member_of` | 人員或帳號是某個組織的成員 |
| `owns` | 組織擁有某個帳號或網域 |
| `uses` | 威脅行為者使用某個惡意程式或工具 |
| `located_at` | 實體位於某個地點 |
| `associated_with` | 兩個實體有關聯（廣義） |
| `derived_from` | 一個實體衍生自另一個實體 |
| `indicates` | STIX Indicator 指向某個威脅 |
| `attributed_to` | 活動歸因於某個威脅行為者 |
| `targets` | 攻擊行動的目標 |
| `mitigates` | 緩解措施 |

---

## 來源類型（SourceType）

建立 Source 時 `source_type` 欄位的允許值：

| 值 | 說明 |
|---|---|
| `rss` | RSS feed |
| `atom` | Atom feed |
| `static_web` | 靜態網頁 |
| `rest_api` | REST API |
| `manual_upload` | 手動上傳 |
| `json_import` | JSON 批次匯入 |
| `csv_import` | CSV 批次匯入 |
| `stix_import` | STIX 2.1 Bundle 匯入 |

---

## 種子類型（SeedType）

建立 Seed 時 `seed_type` 欄位的允許值：

`keyword`、`topic`、`person`、`organization`、`account`、`channel`、`url`、
`domain`、`ip`、`email`、`vulnerability`、`software`、`product`、`repository`、
`hashtag`、`location`

---

## 候選類型（CandidateType）

Discovery 產生的 Candidate 的 `candidate_type` 欄位允許值：

`entity`、`relationship`、`account`、`channel`、`source`、`url`、`domain`、
`repository`、`keyword`、`topic`、`seed`
