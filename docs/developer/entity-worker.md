# Entity Worker（V0.1 Phase 4b）

對應內部規格 V0.1 §10（Entity Types）、§11（Relationship）、§12（Relationship Evidence）、§14（Provenance）、§17（Entity Extraction）、§20（topic）、§26 Acceptance D／E（規格文件本身未隨原始碼公開，章節號留著方便查找對應決策）。

管線的第四段：`dedup.completed` → 抽取 Entity → 建 Relationship 與 RelationshipEvidence →
`entity.extracted`。Search indexing（Phase 5）不在這裡。

```text
crates/entity-worker    bin: osint-entity-worker
```

---

## 兩條硬性範圍限制

這兩條是**刻意的設計決定**，不是還沒做完。

### 1. 沒有 AI／NER

V0.1 只有**確定性規則**。沒有模型、沒有推論、不呼叫任何 AI runtime。

具體影響：**Person 與 Organization 只從結構化欄位抽**——
`Document.author` → Person，`Document.attributes` 的 publisher/organization 等欄位 → Organization。
自由文本裡寫「根據記者王小明報導」不會產生任何 Person Entity。

為什麼不順手做：規則式抽取在自由文本上找人名／組織名的精確度非常低，而 Entity 一旦寫進
canonical store 就會被 relationship 與（V0.2 的）圖投影引用，清理成本遠高於「先不抽」。
自由文本 NER 是 **V0.3** 的工作（規劃詳見內部架構文件 LOCAL_AI.md，未隨原始碼公開）。

`crates/entity-worker/tests/e2e.rs` 的 `person_and_organization_come_from_structured_fields_only`
會**反向斷言**：正文裡刻意放了人名與組織名，測試要求它們**不存在**於 Entity 表。

### 2. 只處理 canonical Document

`dedup.completed` 帶 `is_duplicate`。`true` 的一律跳過。

理由：duplicate 已經指向 canonical，對它再抽一次不會產生重複 Entity（自然鍵擋住），
但**會**在 `entity_extractions` 留下一批 `object_id` 指向重複文件的列，
讓「這個 CVE 出現在幾篇文章」這類查詢全部多算。

有**兩道**檢查，不是一道：

1. 事件的 `is_duplicate` 欄位
2. 資料庫上 `documents.duplicate_of` 是否有值

第 2 道是因為事件可能比資料庫舊（在該 Document 被判成重複之前就發出了）。
**DB 才是事實。**

> ⚠️ payload 缺 `is_duplicate` 時**回報錯誤**，不是預設成 `false`。
> 預設成 false 會讓所有重複文件都被抽取，而且不會有任何錯誤跡象。

---

## 抽取器

| Entity type | `extractor` | 方式 | `normalized_name` |
|---|---|---|---|
| Vulnerability | `regex-cve` | `CVE-\d{4}-\d{4,}`，大小寫不敏感 | 轉**大寫** |
| Ip | `regex-ipv4` / `regex-ipv6` | regex 抓形狀 + `IpAddr::parse` 驗值域 | `IpAddr` 的 Display（IPv6 壓縮 + 小寫） |
| Url | `regex-url` | `http`/`https` + `Url::parse` | `core_model::url_norm::canonicalize` |
| Email | `regex-email` | RFC 5322 簡化版（dot-atom local-part） | 轉**小寫** |
| Domain | `derived-url-host` / `derived-email-domain` / `regex-domain` | URL host、Email domain、裸 domain regex | 小寫、去尾端 `.` |
| Hash | `regex-hash` | 純 hex，長度 32/40/64 | 轉**小寫** |
| Person | `field-author` | 只讀 `Document.author` | 小寫 + 空白壓成單一空格 |
| Organization | `field-organization` | 只讀 `Document.attributes` 的指定欄位 | 小寫 + 空白壓成單一空格 |

`extractor` 字串會寫進 `entity_extractions.extractor`，並且是該列 UUID v5 的雜湊輸入之一。
**改字串等於讓既有列無法對照**。

`Organization` 讀的欄位（依序全取）：
`publisher`、`organization`、`organisation`、`vendor`、`feed_title`、`site_name`。

### `extractor_version`

`crates/entity-worker/src/extract.rs` 的 `EXTRACTOR_VERSION`，目前是 `"1"`。

**規則改了就要往上加。** 那一欄（與 provenance metadata 裡的同名欄位）是之後判斷
「哪些 Document 是舊規則抽的、需要重跑」的**唯一**依據。不要沿用 crate 版本——
crate 版本跟著整個 workspace 走，規則沒改也會變。

### `(?-u:\b)`：中文文本的靜默漏抽

**所有 regex 都用 `(?-u:\b)` 而不是 `\b`。這是實測出來的，不是風格偏好。**

`regex` crate 的 `\b` 預設是 Unicode 感知的，而 CJK 字元屬於 `\w`。
於是中文最常見的寫法——字與英數之間不加空白——兩側都是 word 字元，不存在邊界：

| 輸入 | `\b` | `(?-u:\b)` |
|---|---|---|
| `這是一則公告CVE-2026-0001` | **無命中** | `CVE-2026-0001` |
| `公告 CVE-2026-0001 影響` | `CVE-2026-0001` | `CVE-2026-0001` |
| `XCVE-2026-0001` | 無命中 | 無命中（正確拒絕） |

本專案的主要語料是繁體中文，用 `\b` 等於在中文文章上**靜默漏抽**——
不會報錯，只會回報「這篇沒有任何 entity」。
迴歸測試：`extract.rs` 的 `entities_glued_to_cjk_text_are_still_found`。

### `text_offset` 是字元不是 byte

`entity_extractions.text_offset` 存的是**字元**偏移量。byte 偏移量在中日韓內容上
完全無法對應到肉眼看到的位置，那一欄的用途就是給人定位。

`excerpt` 取命中前後各 `EXCERPT_RADIUS`（**240**）個字元，切割一律在 char 邊界上。
240 的取捨：足夠看出「這個 IP 是被封鎖的還是攻擊來源的」這種語意，
又不會讓一篇塞滿 hash 的文章把 `relationship_evidence.excerpt` 撐爆
（500 筆 × 約 500 字元 ≈ 250 KB／篇）。

---

## Public suffix：為什麼是靜態清單

`crates/entity-worker/src/suffix.rs`。

### 先講一件常被搞錯的事

**完整 PSL 擋不掉 `foo.bar`。** `bar` 是 2014 年委任的正式 gTLD，在完整 PSL 裡面，
所以用 `psl` crate 一樣會把 `foo.bar` 判成合法 domain。
要擋掉它需要的不是「更完整的清單」，而是**更保守的清單**。

### 為什麼不用 crate

實測（2026-09-12，查 `https://index.crates.io/3/p/psl`）：
`psl` 最新版是 `2.1.232`，發布於 **2026-09-09**。版號尾數就是 PSL 資料版本，每隔幾天發一版。
「用 crate 就不會過時」是錯的——只是把「維護一份清單」換成「每隔幾天 bump 一次版本」。

動態抓取更不可行：CLAUDE.md §5「External content is untrusted」，
而且會讓 entity-worker 多一個開機期外網相依，離線環境與 CI 都跑不起來。

### 取捨

| | 誤判（false positive） | 漏抽（false negative） |
|---|---|---|
| 完整 PSL | 高：`payload.zip`／`foo.bar`／`script.sh` 都會被當 domain | 低 |
| 本專案的保守清單 | 低 | 有：冷門新 gTLD 下的 domain 抽不到 |

**V0.1 選低誤判。** 誤判會污染 Entity 表且難以事後清理（要人工判斷哪些是假的）；
漏抽可以在補上 suffix 後重跑抽取補回來（Document 與 RawEvidence 都還在）。
可以重跑補救的損失，優先於需要人工清理的污染。

清單內容：ISO 3166-1 兩碼 ccTLD 全集 + 傳統 gTLD + 少數資安文本高頻的新 gTLD
（`dev`／`app`／`cloud`／`xyz`…）+ RFC 2606 的測試 TLD（`invalid`／`example`／`test`）。
**刻意不收** 2012 年後那一大批新 gTLD。

要加就往 `TLDS` 加一行，不需要改任何邏輯——那份清單是資料，不是演算法。

### 多標籤 suffix

`MULTI_LABEL_SUFFIXES` 收約 150 個常見的（`co.uk`／`com.tw`／`github.io`…），用途有二：

1. 避免把 suffix 本身當成 domain。文章寫「註冊 .co.uk 網域」時裸 domain regex 會抓到
   `co.uk`；沒有這份清單就會產生一個叫 `co.uk` 的 Entity。
2. 算 `attributes.registrable_domain`（`www.example.co.uk` → `example.co.uk`）。

比對時要求 **label 邊界**（`ends_with(".{suffix}")` 而非裸 `ends_with`），
否則 `notaco.uk` 會被 `co.uk` 命中——那是靜默的錯誤分類。

**已知限制**：這是完整 PSL 的子集。沒收到的多標籤 suffix 會讓 registrable domain
算成 `co.xx` 這種形狀。

---

## 已知限制與誤判

這一節列的都是**現況**，不是待辦。有處理的說明怎麼處理，沒處理的就說沒處理。

### 版本號被當成 IPv4（未消除，已標記）

`升級到 1.2.3.4 版本` 會產生一個 IP Entity。**regex 與 `IpAddr::parse` 都無法區分**——
`1.2.3.4` 是一個完全合法的 IPv4 位址。

處理方式：**不丟棄，降低信心**。IPv4 的 `confidence` 是 `0.75`（IPv6 是 `0.95`）。
丟棄會漏掉真實 IP，那個代價更高。下游要自行決定信心門檻。

釘住現況的測試：`a_version_number_is_extracted_as_ip_known_false_positive`。

### git commit 與 SHA1 同形（未消除，已標記）

40 位十六進位同時符合 SHA1 與 git commit id，**僅憑字串無法區分**。

處理方式：照樣抽成 Hash Entity，但在 `attributes` 標
`ambiguous_sha1 = true` 與 `ambiguity_note`，`confidence` 給 `0.7`。
**也不寫 `entity_identifiers`**：若用同一個 `namespace="hash"` 寫進去，
exact_identifier 會把檔案雜湊與 commit id 誤判成同一個識別碼
（false merge 比 false negative 危險）。等抽取端能明確分辨雜湊型別再處理。

硬判會兩邊都錯：當成 commit 會漏掉真的惡意檔案雜湊，當成 hash 會讓每篇技術文章的
commit id 都變成 IOC。**把判斷權留給有語境的下游。**

### 副檔名撞真實 ccTLD（部分處理）

`readme.md`、`install.sh`、`main.go`、`setup.py` 的副檔名**都是**真實 ccTLD
（摩爾多瓦／聖赫勒拿／加彭／巴拉圭）。suffix 清單擋不掉，拿掉那些 ccTLD 又會漏掉真網域。

處理方式：`extract.rs` 的 `DOMAIN_STOPWORDS` 擋掉整個字串。

**已知限制**：只擋得掉列在裡面的。`config.pl`、`deploy.sh` 之類沒列的仍會被當成 Domain。
發現新的誤判就往清單加。

### UUID 不會被當成 hash（已處理）

去掉連字號的 UUID 正好 32 位 hex，與 MD5 同形。帶連字號的形狀由 `UUID_SHAPE`
先掃一遍，落在 UUID 範圍內的 hex 段直接排除。

**未處理的殘留**：文中若出現**已去掉連字號**的 UUID，仍會被當成 MD5。

### Email local-part 的大小寫（刻意合併）

嚴格來說 RFC 5321 的 local-part 是大小寫敏感的，`Bob@x` 與 `bob@x` 可以是兩個信箱。
但實務上沒有主流郵件服務這樣做，而情報文本裡同一個信箱以不同大小寫出現是常態。

**選擇合併**（一律轉小寫）：漏掉一個理論上的區分，換取不會把同一個人拆成三個 Entity。

### `evidence_count` 上限 100

`relationships.evidence_count` 是重新數出來的（見下），數的時候用 `list_relationship_evidence`，
而 `RelationalStore` 的 limit 夾在 1..=100。
**超過 100 筆證據的邊，`evidence_count` 會停在 100。**

---

## Relationship 對應表

SPEC §11 只列出可用的 13 種 type，沒有規定哪種 entity 配哪種。以下是本專案的決定。

### Document → Entity

| Entity | relationship | 為什麼 |
|---|---|---|
| Person（來自 `author`） | `authored_by` | 語意精確：這個人**寫了**這份文件 |
| Organization（來自 `attributes`） | `published_by` | 發布者不等於作者 |
| Url | `references` | 文件**引用**了這個連結，比 `mentions` 精確 |
| 其餘（CVE／IP／Domain／Email／Hash） | `mentions` | 只知道「提到了」，不宜過度解讀 |

刻意**不用** `affects`（Document affects CVE 語意不通）與 `links_to`
（那是 URL→URL 的關係，不是 Document→URL）。

### Entity → Entity（衍生）

| 來源 | relationship | 目標 | 為什麼 |
|---|---|---|---|
| Url | `belongs_to` | Domain | 這個 URL **屬於**那個網域，是結構上的從屬關係 |
| Email | `associated_with` | Domain | 說「屬於」過強——`soc@example.com` 不代表該信箱由 example.com 擁有 |

### 每一條邊都有 evidence

**SPEC §12：任何 relationship 必須能回查 evidence。**
建一條邊就必定寫一筆 `RelationshipEvidence`，帶 `object_id`（哪份 Document）、
`raw_evidence_id`（哪筆原始證據）與 `excerpt`（命中的上下文）。

`evidence_count` **重新數**而不是累加。累加的話同一份 Document 重跑時
evidence 是 upsert（不會變多）但計數會每跑一次加一，「邊數」與「證據數」永遠對不上。

---

## 冪等

三道互相獨立的保險。

### 1. provenance claim

`idx_provenance_entity_subject`：同一個 `subject_id` 只能有一列 `action='entity_extracted'`。
重複消費同一事件會在入口就被擋下，回 `AlreadyDone`。

> ⚠️ `entity_extracted` 這個字串同時寫在三個地方：
> `crates/entity-worker/src/service.rs` 的 `ACTION_ENTITY_EXTRACTED`、
> `migrations/*/0005_entity_natural_key.sql` 的部分索引 `WHERE` 子句、
> `crates/osint-cli/src/commands/documents.rs` 的 `ENTITY_EXTRACTED_ACTION`。
> **只改一處不會報錯**，只會讓唯一性保證或 CLI 提示靜默失效。
> 兩個 `*_action_matches_*` 測試把它們釘住了。

### 2. 決定性的 UUID v5（主力）

五種衍生物件的 id 全部由自然鍵推導，所有 `put_*` 都是依主鍵 upsert，
所以重跑寫的是同一列。

| 物件 | v5 雜湊輸入 |
|---|---|
| `Entity` | `entity_type\|normalized_name` |
| `Relationship` | `source\|type\|target` |
| `RelationshipEvidence` | `relationship\|object\|text_offset` |
| `EntityExtraction` | `object\|entity\|extractor\|text_offset` |
| `EntityIdentifier` | `namespace\|entity_id\|normalized_name` |

enum 轉字串走 **serde 的 snake_case 名稱**，不是 `Debug`。
`Debug` 的輸出不是穩定契約，改個 variant 名稱就會讓所有既有 id 算出不同結果。

`RelationshipEvidence` 把 `text_offset` 納入是刻意的：同一份 Document 在**不同位置**
提到同一個 Entity 是兩筆獨立的證據。只用 (relationship, object) 的話十次提及會塌成一列。

`EntityExtraction` 把 `extractor` 納入是因為同一個 Domain 可能同時被 `regex-domain`
與 `derived-url-host` 命中，那是兩筆來自不同規則的紀錄。

**第 2 點才是主力，第 1 點只是省掉重複工。** claim 寫入前 crash 就會重跑，
那時要靠 v5 id 才不會產生重複。e2e 的
`rerunning_without_the_claim_still_produces_no_duplicates` 會**刪掉 claim 再跑一次**
來驗證這件事。

### 3. 資料庫唯一鍵

`idx_entities_natural_key`、`idx_relationships_natural_key`。
擋掉任何繞過第 2 點的寫入路徑。

### 落地順序：write-then-claim

先寫 Entity／Relationship／Extraction，**最後**才 claim。
取捨見 `collector-normalizer.md`「落地原子性」：
claim-first 的 crash window 會造成「宣稱處理過但其實沒寫」的靜默失效；
write-then-claim 的 crash window 只會造成重跑，而重跑因為第 2 點不會產生重複列。

⚠️ **normalizer 已經在 V0.2 Phase 0e 改成交易，entity-worker 還沒。**
`storage-core` 現在有 `TransactionalStore`，這裡也該改，排在 V0.2 Phase 1。
在那之前 crash window 依然存在，靠的是 v5 id 的冪等，不是原子性。

---

## `entity_identifiers`（V0.2 Phase 1c-0-data）

`upsert_entity` 在 `put_entity` 成功後呼叫 `write_identifier_if_applicable`，
對特定 `EntityType` 再寫一筆 `EntityIdentifier`。同一個 Entity 重跑不累積重複列
（UUID v5 主鍵 + `ON CONFLICT (id)`）。

公開函式（`crates/entity-worker/src/service.rs`）：

- `identifier_namespace_for(entity_type)`：五種唯一鍵型別回 namespace 字串，其餘 `None`。
- `identifier_id(namespace, entity_id, normalized_name)`：UUID v5，namespace 常數是
  `IDENTIFIER_NAMESPACE`（`0x0199_5c31_7e20_7a55_8b4c_2d3e_6f70_8095`）。
  seed 含 `entity_id`，這樣兩個 Entity 宣稱同一個識別碼會算出**不同**主鍵，
  UNIQUE `(namespace, normalized_value)` 才會回 `Conflict` 而不是被 `ON CONFLICT (id)` 覆寫。

| EntityType | namespace | value / normalized_value |
|---|---|---|
| Domain | `domain` | `entity.name` / `entity.normalized_name`（抽取時已正規化，不再折一次） |
| Ip | `ip` | 同上 |
| Url | `url` | 同上 |
| Email | `email` | 同上 |
| Vulnerability | `cve` | 同上 |

其餘型別**不寫**：

- **Hash**：T10，見上一節。
- **Person／Organization**：`name` 是人看的名字，不是命名空間內的唯一鍵。這次也不寫 `entity_aliases`。
- **Account／Software／Repository／Location／Hostname**：沒有清楚的 namespace 語意。

`source_id` 從 Document 的 RawEvidence 反查；查不到就留 `None`，不編造。

### 衝突處理（log-only，不讓抽取失敗）

`(namespace, normalized_value)` 是 UNIQUE。另一個 Entity 已佔這個識別碼時，
`put_entity_identifier` 回 `StorageError::Conflict`。這**不是**錯誤邊角，
是 SPEC §6 exact identifier 要偵測的訊號：

1. `find_entity_identifier_owner` 拿既有 owner
2. `resolver::resolution_candidate_from_identifier_conflict` 組候選
3. `put_resolution_candidate`

identifier 衝突與候選建立是 resolution 的旁支，**不讓核心抽取失敗**。
`put_resolution_candidate` 自己再撞 `Conflict`（同一對同一方法已存在）視為正常，
info log。其他錯誤 error log。Entity 已經寫進去了，這裡回 `Err` 只會讓整份
Document 重試、永遠過不了。

同一型別、同一 `normalized_name` 的兩份文件**不會**走到這條路——Entity 自然鍵
會把它們合併成同一個 Entity，identifier 是 upsert 同一列。衝突要靠「另一個
型別的 Entity 先佔了這個識別碼」（例如手動／舊資料把某個 domain 寫在 Person 上）。

e2e：`upsert_writes_identifiers_for_unique_key_types_and_skips_the_rest`、
`rerunning_upsert_does_not_duplicate_identifiers`、
`identifier_conflict_writes_exact_identifier_candidate`。

---

## 有界（CLAUDE.md §6）

沒有「不限」這個選項。

| 設定 | 預設 | 意義 |
|---|---|---|
| `[entity_worker].max_extractions` | `500` | 單份 Document 最多留幾筆命中 |
| `[entity_worker].max_scan_bytes` | `262144`（256 KiB） | 只掃 `title+summary+body` 的前 N byte |

`max_extractions = 500` 的依據：一篇正常的資安公告大約產生 10～60 筆
（CVE + 幾個 IOC + 連結）。500 給了約一個數量級的餘裕，同時把
「一篇塞滿 IOC 的傾印檔」擋在「會拖垮 worker」之外。

**截斷不是靜默的**：會記 `warn`、在 `ExtractOutcome` 回 `truncated = true`、
並寫進 provenance metadata 的 `truncated` 與 `total_candidates`。
之後任何人看那份 Document 都能知道它的 entity 是不完整的。

截斷前會**先排序**（依 `text_offset`、`normalized_name`），讓留下的是決定性的那 500 筆。
沒有這一步的話同一份 Document 重跑可能留下不同的 500 筆，冪等就破了。

`max_scan_bytes` 在**串接時**就套用，不是串完再截——`documents.body` 沒有長度限制，
先把一份 50 MB 的正文複製進記憶體再丟掉 99% 是白花的配置，而且那是外部輸入控制的大小。
截斷一律切在 char 邊界上。

### CPU-bound 放 `spawn_blocking`

regex 掃描是 CPU-bound。一份 256 KiB 的文章要跑八組 regex，在 executor thread 上做
會卡住同一個 runtime 上的所有 I/O（含 health endpoint 與 broker 心跳）。
`service.rs` 用 `tokio::task::spawn_blocking` 包住 `extract::extract_all`。

`extract.rs` 整個模組是**純函式、無 I/O、無資料庫**，所以搬進 blocking pool 沒有副作用。

---

## `url_norm` 搬到 core-model

原本在 `crates/deduplicator/src/url_norm.rs`，本版搬到 `crates/core-model/src/url_norm.rs`。

entity-worker 抽 URL Entity 時，`normalized_name` 必須與 deduplicator 的
`documents.canonical_url` 用**同一套**規則。兩邊各留一份實作的話，只要有人改了其中一份，
同一個 URL 就會產生兩種正規化結果，而且不會報錯：Entity 只是悄悄多出一個
「看起來一樣但字串不同」的重複列。

與 `content.rs`（Stage 3 的 content hash）放在 core-model 的理由相同：
**跨服務必須一致的定義就放 core-model**，不要用 crate 相依把 deduplicator 拉進 entity-worker。

`deduplicator` 仍以 `pub use core_model::url_norm;` 對外提供同名路徑，既有呼叫端不受影響。

---

## Migration 0005

`migrations/{postgres,sqlite}/0005_entity_natural_key.sql`。四件事，**順序不可對調**。

### 1～2. 合併既有的重複 Entity 與 Relationship

原始 schema 的 `idx_entities_normalized_name` 是**非唯一**索引，所以既有資料庫可能
已經有多列共用同一組 `(entity_type, normalized_name)`。

實測：本專案的開發用 Postgres 有 **35 列** `(vulnerability, cve-2026-0001)`，
全部來自 storage-core conformance 的固定字串 fixture（該 fixture 已在本版改成含 UUID）。

直接 `CREATE UNIQUE INDEX` 會失敗；直接 `DELETE` 會撞 `entity_extractions` 的外鍵。
所以做**合併**：每組保留 `id` 最小的當存活者，把 `entity_extractions.entity_id` 與
`relationships` 的兩端改指過去，再刪掉輸家。

合併而不是報錯要求人工處理，是因為這些列**在語意上本來就是同一個實體**——
自然鍵相同就是同一個。

> **會丟失的東西**：輸家那幾列的 `attributes` 與 `description`。
> 抽取紀錄與關聯全部保留。

> ⚠️ `relationship_evidence` 對 `relationships` 是 `ON DELETE CASCADE`，
> 所以**必須先把 evidence 改指存活者再刪**，否則證據會跟著被刪掉——
> migration 會成功，只是證據少了（靜默資料遺失）。

實測結果（2026-09-12）：

| 後端 | 合併前 | 合併後 | 孤兒列 |
|---|---|---|---|
| PostgreSQL（開發 DB） | 35 entities / 35 extractions | 1 entity / 35 extractions | 0 |
| SQLite（合成重複資料） | 3 entities / 3 rels / 3 evidence | 1 entity / 1 rel / 3 evidence | 0 |

SQLite 版不能用 `UPDATE ... FROM`（3.33 之後才有，不假設本機版本），改用相關子查詢。

### 3～4. 索引

* `idx_entities_natural_key`（UNIQUE）— 取代非唯一的 `idx_entities_normalized_name`
* `idx_relationships_natural_key`（UNIQUE）
* `idx_provenance_entity_subject`（UNIQUE，部分索引）
* `idx_relationship_evidence_relationship` — SPEC §12 的反查

`idx_provenance_dedup_subject`（0004）沒有把 action 寫進鍵、只寫在 `WHERE` 裡，
所以它與 `idx_provenance_entity_subject` 是**兩個互不衝突的部分索引**：
同一個 `subject_id` 可以同時有 `deduplicated` 與 `entity_extracted` 各一列。

---

## 新增的 storage-core 能力

五個方法，PostgreSQL 與 SQLite 都實作，`conformance.rs` 都有斷言。

| 方法 | 用途 |
|---|---|
| `find_entity_by_normalized_name(entity_type, normalized_name)` | entity-worker 重用既有 Entity 的**唯一**依據 |
| `list_entities(after, limit)` | `osint-cli entities list` |
| `list_relationships_by_object(object_id, limit)` | source 端**或** target 端命中都算 |
| `list_relationship_evidence(relationship_id, limit)` | SPEC §12 的落地點 |
| `list_entity_extractions_by_object/by_entity(id, limit)` | 冪等檢查與 CLI |

`list_relationships_by_object` 刻意合成一個方法而不是 `by_source` / `by_target`：
Acceptance E 要從 **Entity** 往回走（Entity 在 `mentions` 裡是 target），
CLI 的 `documents show` 要從 **Document** 往下走（Document 是 source）。
拆兩個只會讓每個呼叫端都得各查一次再自己合併去重。

> ⚠️ `list_entities` 依 `id` 遞減，而 Entity 的 id 是 UUID v5，**沒有時間序**。
> 這一點與 `list_connectors` 相同（那裡的 id 是 import 用的 v5）。
> 要按時間看請自己比對 `last_seen`。

`find_entity_by_normalized_name` 是**完全相等**比對，不做大小寫折疊——
折疊放進 SQL 會讓查詢走不到索引，而且兩個 backend 的 collation 規則不同，
等於在 PG 與 SQLite 上有兩種語意。正規化是呼叫端的責任。

---

## 事件

訂閱 `dedup.completed`，發布 `entity.extracted`。

`entity.extracted` 的 payload：

```json
{
  "document_id": "...",
  "entity_ids": ["...", "..."],
  "entity_count": 7,
  "extraction_count": 9,
  "relationship_count": 9,
  "truncated": false,
  "extractor_version": "1"
}
```

`SkippedDuplicate` / `AlreadyDone` / `DocumentMissing` **不發事件**：
下游對同一份 Document 收到兩次 `entity.extracted` 沒有意義，
而且會讓「事件數 == 處理數」失效。

`entity_ids` 來自 `BTreeMap` 而不是 `HashMap`：HashMap 的迭代順序每次執行都不同，
同一份 Document 重跑會發出**內容相同但順序不同**的事件，下游拿 payload 做比對就會誤判成有變動。

---

## 執行

```bash
make run-entity-worker
```

health／ready／metrics 在 `127.0.0.1:18084`（`[entity_worker].bind`）。

metrics（`MetricsRegistry::inc` 的額外計數）：

| 名稱 | 意義 |
|---|---|
| `osint_entity_extracted_total` | 寫入的 extraction 筆數 |
| `osint_entities_total` | 建立或重用的 Entity 數 |
| `osint_relationships_total` | 建立或更新的 Relationship 數 |
| `osint_entity_skipped_duplicate_total` | 因重複而跳過的 Document 數 |
| `osint_entity_truncated_total` | 因超過上限而截斷的 Document 數 |

### CLI

```bash
osint-cli entities list --entity-type vulnerability
osint-cli entities show <entity-id>          # 含關聯與證據
osint-cli documents show <document-id>       # 末段列出抽出的 Entity
```

`entities show` 會把 Acceptance E 的鏈印出來，並提示用
`osint-cli raw show <raw-evidence-id>` 接到 Source 與 Connector。

> `entities list --entity-type` 是在**取回的前 N 筆之內**過濾，不是資料庫層篩選
> （storage 沒有依型別分頁的方法）。要看更多請調高 `--limit`，CLI 會印出這個提示。
> 打錯型別名稱會在查詢**之前**回錯誤並列出可用值——
> 直接回「沒有資料」會讓人以為資料庫是空的。

---

## 測試

```bash
cargo test -p entity-worker                      # 單元 59 + e2e 11
cargo test -p entity-worker --test e2e -- --test-threads=1
```

e2e 對本機 Docker 真跑。涵蓋：

| 測試 | 驗證 |
|---|---|
| `acceptance_d_...` | SPEC §26 D：CVE／domain／IP／email／hash 五種都建立 Entity，各有 Extraction + Relationship + RelationshipEvidence |
| `acceptance_e_...` | SPEC §26 E：**走完整管線**（collect → normalize → dedup → extract），再從 Entity 逐步反查到 RawEvidence／Source／Connector，八個步驟每步 assert |
| `same_cve_in_two_documents_...` | 一個 Entity、兩筆 extraction、`first_seen` 保留、`last_seen` 更新 |
| `processing_the_same_document_twice_...` | 冪等（claim 路徑） |
| `rerunning_without_the_claim_...` | 冪等（v5 id 路徑，**刪掉 claim** 再跑） |
| `duplicate_documents_are_skipped` | 兩道檢查都測 |
| `extraction_limit_truncates_and_reports` | 截斷 + provenance 記錄 |
| `person_and_organization_...` | 只從結構化欄位，**反向斷言**正文裡的名字不被抽出 |
| `domain_is_linked_to_the_url_and_email_...` | 衍生關聯與它們的 evidence |

### 共用 DB 的鐵則（比 deduplicator 更嚴格）

Entity 的自然鍵是**全域**唯一的：兩個測試都抽到 `CVE-2026-0001` 時會共用同一列。所以：

1. 每個測試的 CVE／domain／email／hash 都含 run-specific 的值；
2. 對「數量」的斷言一律**限縮在本次的 document id 範圍內**，
   不要去數「資料庫裡總共有幾個 Entity」——那個數字包含前幾次跑的殘留。

> `relationship_count` 與「掛在 Document 上的關聯數」**不相等**，這是正確的：
> 前者還含 Entity→Entity 的衍生邊，那些邊的兩端都不是 Document。
