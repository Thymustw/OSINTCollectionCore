# Deduplicator（V0.1 Phase 4a）

對應內部規格 V0.1 §15（五階段去重）、§16（Duplicate Group）、§20（topic）、§26 Acceptance B／C（規格文件本身未隨原始碼公開，章節號留著方便查找對應決策）。

這是 V0.1 第一個「情報處理」元件：`object.normalized` → 判斷重複 → `DuplicateGroup` → `dedup.completed`。
Entity extraction 在下一段（`docs/developer/entity-worker.md`），search indexing 不在這裡。

> ⚠️ **`url_norm` 已搬到 `crates/core-model/src/url_norm.rs`**（Phase 4b）。
> entity-worker 抽 URL Entity 時要用同一套正規化規則，兩份實作分岔不會報錯，
> 只會悄悄產生「看起來一樣但字串不同」的重複 Entity。
> `deduplicator::url_norm` 仍以 re-export 保留原路徑，呼叫端不受影響。

```text
crates/deduplicator    bin: osint-deduplicator
```

## 最重要的一條規則

**SPEC §16：不得刪掉 duplicate evidence。**

`crates/deduplicator` 裡沒有任何 `delete_*` 呼叫。判定為重複時只做兩件事：

1. 寫一列 `DuplicateGroup`（canonical ← member）
2. 在那份 Document 上把 `duplicate_of` 指向 canonical

RawEvidence、Document、Source 全部原封不動。`crates/deduplicator/tests/e2e.rs` 的
Acceptance B 會在去重後**逐筆數** 10 筆 RawEvidence 是否都還讀得到——
不是看有沒有報錯，是去數。

## 五個階段

依序跑，**第一個命中就停**。

| Stage | 判準 | 資料來源 | `method` 字串 | similarity |
|---|---|---|---|---|
| 1 | `platform` + `external_id` 完全相同 | `Source.platform` + `Document.attributes.external_id` | `platform_external_id` | 1.0 |
| 2 | 正規化後的 canonical URL 完全相同 | `Document.source_url` → `url_norm::canonicalize` | `canonical_url` | 1.0 |
| 3 | `SHA256(normalized content)` 完全相同 | `Document.normalized_content_hash` | `content_sha256` | 1.0 |
| 4 | 64-bit SimHash 的 Hamming 距離 ≤ 門檻 | `Document.simhash` | `simhash` | `1 - d/64` |
| 5 | 語意重複 | — | `semantic` | 由實作決定 |

`method` 字串會寫進 `duplicate_groups.method`、provenance metadata 與 `dedup.completed`。
**改字串等於讓既有資料無法對照**，`service.rs` 的 `stage_names_are_stable` 測試把它們釘住了。

### Stage 1：platform + external_id

`platform` 在 `Source` 上，`external_id` 在 `Document.attributes`，所以要繞一圈
`Document → attributes.raw_evidence_id → RawEvidence → Source`。
兩者**都有非空值**時才組成 `documents.external_key = "{platform}|{external_id}"`。

只有其中一個時不湊空字串：那會讓所有「沒有 platform 的來源」互相撞在同一個鍵上。

### Stage 2：canonical URL

`url_norm::canonicalize` 做的事（前五項是 SPEC 明列的，後兩項不是，理由見下）：

1. scheme／hostname 轉小寫
2. 移除 fragment
3. 正規化 percent-encoding：`%2f` → `%2F`；unreserved 字元解回字面（`%7E` → `~`）。
   **`%2F` 不會解成 `/`**——那會改變路徑結構
4. 移除追蹤參數（完整清單在 `crates/core-model/src/url_norm.rs` 的 `TRACKING_PARAMS`／`TRACKING_PREFIXES`）
5. 保留其餘參數（`?id=42`、`?page=2` 會改變內容）
6. **移除 userinfo**（`https://user:pw@host/`）：那是憑證不是身分，而且 `canonical_url` 有索引也會進 log
7. **保留下來的參數依「名稱、值」排序**：同一篇文章常以不同參數順序流通，不排序就比不中

追蹤參數清單是**資料不是邏輯**：發現新的往 `TRACKING_PARAMS` 加即可。
`utm_*`／`pk_*`／`mtm_*`／`matomo_*` 走前綴比對，因為那是開放集合，逐一列舉永遠會漏。

正規化結果會**寫回 `documents.canonical_url`**。normalizer 原本只是把 `source_url`
原封不動複製過去，那個值沒有正規化過。`source_url` 保持原始觀測值，不被覆蓋。

解析不出合法絕對 URL 時回 `None`（Stage 2 對這份不適用），**不退回原字串**：
那會讓「正規化過的」與「沒正規化的」混在同一欄比對，Stage 2 會靜默比不中。

### Stage 3：SHA256(normalized content)

定義在 `crates/core-model/src/content.rs`，**normalizer 與 deduplicator 共用同一份實作**
（兩份遲早會分岔，分岔後 Stage 3 會靜默失效）。

```text
SHA256( normalize(title) | normalize(summary) | normalize(body) )
normalize = trim + 把連續空白（含換行、tab）壓成單一半形空格
```

#### 這次改了 normalizer 的舊定義

原本是對**原文**直接 `SHA256("{title}|{summary}|{body}")`，沒有任何正規化。
那個定義太脆：同一篇文章經 HTML 重排版、`\r\n` 換成 `\n`、或多一個縮排空格，
hash 就完全不同——Stage 3 幾乎只在「同一次抓取的同一份 bytes」才會命中，
而那種情況 Stage 1／2 早就攔下了，Stage 3 等於白做。

**不做大小寫正規化**是刻意的：Stage 3 的語意是「同一份內容」，改過大小寫的標題
是真的被改過，該由 Stage 4 以近似重複處理。Stage 4 的 token 化本來就會轉小寫，
剛好補上這個位。

⚠️ **這是既有資料的破壞性變更**：改定義之後，舊資料的 `normalized_content_hash`
與新算出來的不一致，Stage 3 對「新舊混合」的資料會比不中。V0.1 尚未有生產資料，
所以不寫回填腳本；真的要回填就是重跑一次 normalizer。

決策紀錄（ADR-007）：Stage 3 內容雜湊在算 SHA256 前只正規化空白字元（trim + 把連續空白/`\r\n`/tab 壓成一個空格），**刻意不做大小寫正規化**——Stage 3 宣稱的是「內容完全相同」（confidence 1.0），大小寫被改過代表內容被編輯過，不該算完全相同；那類差異留給 Stage 4（SimHash）用 confidence < 1.0 的「近似重複」分類接住。

### Stage 4：SimHash

自己實作（`src/simhash.rs`），不用 crates.io 的 `simhash`：演算法不到一百行，
為了它擴大相依面不划算，而且那個 crate 的維護狀態不明。

| 決定 | 選擇 | 為什麼 |
|---|---|---|
| 位元數 | 64 | 與 `BIGINT`／SQLite `INTEGER` 一致，單欄可存 |
| Token 化 | **word-level unigram + 出現次數當權重** | 見下 |
| Token 正規化 | 轉小寫、以非文數字分隔、丟掉長度 1 的碎片 | 吸收大小寫與標點差異 |
| Token 雜湊 | SHA256 前 8 bytes（little-endian） | 只為了位元分布均勻，不是為了安全性 |
| 平手規則 | 權重恰好 0 的 bit 取 0 | 規則必須是決定性的 |
| 最少 token | 16，不足則不給指紋 | 見下 |
| 距離門檻 | 預設 3（`[deduplicator].simhash_max_distance`） | 見下 |
| 儲存 | `i64`（`u64 as i64` 位元重解讀） | PG／SQLite 都沒有無號 64-bit |

**為什麼是 unigram 不是 shingle（n-gram）**：轉載通常是改動零星幾個詞。
unigram 下改一個詞只影響一個 token 的權重，距離增量小而可控；3-gram 下改一個詞
會同時擾動最多三個 shingle，短文會直接把距離推過門檻，Stage 4 形同虛設。
**代價是對詞序不敏感**——同一組詞重新排列會算出相同指紋。
對「轉載偵測」可接受，對「抄襲／改寫偵測」不行。

**為什麼門檻是 3**：3/64 是 SimHash 論文（Manku et al.）與多數實務實作對網頁近似
重複的慣用值。往上調到 5 以上，主題相近但不相關的文章會開始互相命中；
調到 1 以下則只剩「幾乎逐字相同」，那是 Stage 3 已經攔下的範圍。

**為什麼 token 太少就不給指紋**：64 bit 要靠足夠多的 token 才會分布均勻。
只有三五個詞的文件算出來的指紋非常容易互撞，會製造假的「近似重複」。
寧可 Stage 4 對短文放棄，也不要建出指向錯誤 canonical 的 duplicate group——
SPEC §16 不准刪證據，但錯誤的 canonical 指向一樣會誤導後續所有分析。

#### CPU-bound：指紋計算走 `spawn_blocking`

SimHash 要掃過整篇正文並做數百次 SHA256。CLAUDE.md §6：CPU-heavy 不可 block
Tokio executor thread。`derive_keys` 把它丟進 `tokio::task::spawn_blocking`。

#### 候選查詢：PostgreSQL 與 SQLite 的差別

契約（`storage-core::RelationalStore::find_simhash_candidates`）對兩個 backend 相同：

> 只掃描「`id` 嚴格小於 `before`、最近 `scan_limit` 筆有 fingerprint 的 Document」，
> 在這個範圍內回傳全部距離 ≤ `max_distance` 的候選，依 `id` 升序。

| Backend | 做法 |
|---|---|
| PostgreSQL | 子查詢先夾住掃描範圍（走 `idx_documents_simhash_recent`），外層 `WHERE bit_count((c.simhash # $2)::bit(64)) <= $3` 在 **DB 端**算距離。`#` 是 PG 的位元 XOR；`bit_count` 只吃 `bit`／`bytea`，所以要先 `::bit(64)` |
| SQLite | **沒有 popcount，也沒有整數 XOR 運算子**（`#` 是 PG 專有，SQLite 連 `^` 都沒有）。改成取回同一個掃描範圍的 `(id, simhash)` 兩欄，在程式端算 Hamming 距離。SQLite 是 embedded 角色不是高併發 canonical，多搬 `scan_limit` × 16 bytes 可以接受 |

`scan_limit` 在 adapter 內夾在 1..=5000。這是**有界**的代價：
比 `scan_limit` 更舊的近似文件會漏掉（見「已知限制」）。

### Stage 5：語意重複

**V0.1 只有介面**（`src/semantic.rs`）。規格原文就是「只定義 interface，V0.2 才做」。

```rust
trait SemanticDuplicateDetector {
    fn detector_id(&self) -> &'static str;
    async fn detect(&self, document: &Document) -> Result<SemanticOutcome, DeduplicatorError>;
}
```

`SemanticOutcome` 刻意區分 `Unsupported`（沒實作）與 `NoMatch`（查過但沒有）。
兩者對流程的效果相同，差別在寫進 provenance 的 `semantic_detector` 字串——
那是「這份資料是在有沒有 Stage 5 的年代處理的」唯一的判斷依據。
預設實作 `UnsupportedSemanticDetector` 永遠回 `Unsupported`。

接上真實實作時要記得：**AI 失敗不可阻斷 base ingestion**（CLAUDE.md §5）。
判斷不出來要回 `NoMatch`／`Unsupported`，不要回 `Err`。

## Duplicate 的標記方式

用 **`documents.duplicate_of` 欄位**，不是 `labels`。

`labels` 是自由字串陣列，沒有外鍵語意，也無法回答「這份的 canonical 是誰」。
欄位還能建索引，直接查得出「這份 canonical 底下有哪些重複」。

Document 上總共加了三個欄位（migration `0004_dedup_stage_columns.sql`）：

| 欄位 | 型別（PG / SQLite） | 用途 |
|---|---|---|
| `external_key` | `TEXT` | Stage 1 的鍵 |
| `simhash` | `BIGINT` / `INTEGER` | Stage 4 的指紋 |
| `duplicate_of` | `UUID` / `TEXT`（FK → `documents.id`） | 指向 canonical |

三個都可為 NULL，所以 Document 有三個可辨識的狀態：

```text
三個都空                      → deduplicator 還沒處理過
duplicate_of 空、另兩個有值   → 處理過，是 canonical
duplicate_of 有值             → 處理過，是重複
```

**不管有沒有命中，`external_key`／`canonical_url`／`simhash` 都會寫回去。**
只在命中時才寫的話，canonical 那份永遠沒有鍵，後面每一份都比不中——
典型的「不報錯但靜默失效」。

## 候選只看比自己早的 Document

`find_document_ids_by_*` 的第二個參數是 `before`（**嚴格小於**），不是「排除自己」。

若允許比對到更新的 Document，事件亂序時可能出現 A 指向 B、B 指向 A 的
`duplicate_of` 環，之後任何一次 canonical 解析都會繞不出來。
限制成單向（永遠指向更小的 UUID v7）讓環在結構上不可能存在。

代價：亂序抵達時可能漏判一組重複。**漏判可以重跑補回來，環不行。**

`resolve_canonical` 另有 `MAX_CHAIN_DEPTH = 8` 的保險，觸發時**回報錯誤**
而不是靜默取最後一個——靜默會建出指向錯誤 canonical 的 group。

## 冪等

兩道互相獨立的保險。

### 1. `DuplicateGroup.id` 是 UUID v5，不是 v7

```text
id = UUID_v5(DUPLICATE_GROUP_NAMESPACE, member_document_id)
```

同一份 Document 重複處理多少次都算出同一個 id，`put_duplicate_group` 依主鍵 upsert
→ 永遠只有一列。`DUPLICATE_GROUP_NAMESPACE` 是固定常數，**不可更動**：
改了之後舊列不會被覆蓋而是多出一列，冪等保證直接失效（而且不會報錯）。

另有 unique index `idx_duplicate_groups_member_object` 當第二道保險，
擋掉任何繞過 v5 規則、想幫同一個 member 建第二個 group 的寫入。

### 2. provenance claim

部分 unique index `idx_provenance_dedup_subject`：同一個 `subject_id` 只能有一列
`action='deduplicated'`。並發兩個 consumer 處理同一份 Document 時，
輸家拿到 `Conflict` → 重讀既有列 → 回報 `AlreadyDone`。

claim 的 metadata 記下當初的判斷依據：`is_duplicate`、`stage`、`canonical_object_id`、
`similarity`、`external_key`、`canonical_url`、`content_sha256`、`simhash`、
`semantic_detector`、`simhash_max_distance`。

### 落地順序：write-then-claim

先寫 group／Document，**最後**才 claim。取捨見
`docs/developer/collector-normalizer.md`「落地原子性」：claim-first 的 crash window
會造成「宣稱處理過但其實沒寫」的靜默資料問題，write-then-claim 的 crash window
只會造成重跑——而且因為 group id 是 v5，重跑連重複列都不會產生。

⚠️ **normalizer 已經在 V0.2 Phase 0e 改成交易，deduplicator 還沒。**
`storage-core` 現在有 `TransactionalStore`，這裡也應該改（group + Document +
claim 一個交易），排在 V0.2 Phase 1。在那之前上面描述的 crash window 依然存在，
安全性靠的是 v5 group id 的冪等，不是原子性。

## 事件

訂閱 `object.normalized`（payload 的 `document_ids` 陣列），發 `dedup.completed`。

```jsonc
// 命中
{
  "document_id": "...",
  "is_duplicate": true,
  "stage": "simhash",
  "stage_number": 4,
  "canonical_object_id": "...",
  "similarity": 0.953125,
  "duplicate_group_id": "..."
}
// 未命中
{
  "document_id": "...",
  "is_duplicate": false,
  "stage": null,
  "canonical_object_id": "<自己>"
}
```

partition key = `document_id`。

`AlreadyDone` 與 `DocumentMissing` **不發事件**：下游對同一份 Document 收到兩次
`dedup.completed` 沒有意義，而且會讓「事件數 == 處理數」這個對帳關係失效。

## 有界

| 項目 | 上限 | 設定鍵 |
|---|---|---|
| Stage 1～3 每個鍵的候選數 | 20 | `[deduplicator].candidate_limit`（adapter 再夾 1..=100） |
| Stage 4 掃描筆數 | 500 | `[deduplicator].simhash_scan_limit`（adapter 再夾 1..=5000） |
| `duplicate_of` 追鏈層數 | 8 | 常數 `MAX_CHAIN_DEPTH` |
| consumer 併發 | 循序（單 consumer group） | — |

consumer **刻意循序**：dedup 的正確性依賴「先到先成為 canonical」，
同一份 Document 的處理不可互相交錯。要提高吞吐是增加 partition 與 consumer 實例，
不是對同一則事件無界 spawn。

單一 Document 失敗不會中止整批：一則事件可能帶十幾份 Document，
讓其中一份的錯誤吃掉其他份沒有意義。失敗那份記 error log，
claim 還沒寫，重新消費時會再試。

## 設定

```toml
[deduplicator]
bind = "127.0.0.1:18083"
consumer_group = "osint-deduplicator"
candidate_limit = 20
simhash_scan_limit = 500
simhash_max_distance = 3
```

本機常見情境：8080 可能被其他本機服務占用（例如另一套安全/情資平台），health 不要綁 8080。18081／18082 已被 collector／normalizer 佔用。

## Migration

```text
migrations/postgres/0004_dedup_stage_columns.sql
migrations/sqlite/0004_dedup_stage_columns.sql
```

`RelationalStore` 新增（PostgreSQL 與 SQLite 都實作，conformance 會斷言）：

- `find_document_ids_by_external_key(key, before, limit)`
- `find_document_ids_by_canonical_url(url, before, limit)`
- `find_document_ids_by_content_hash(hash, before, limit)`
- `find_simhash_candidates(fingerprint, max_distance, before, scan_limit)`
- `get_duplicate_group_by_member(member_object_id)`
- `list_duplicate_groups_by_canonical(canonical_object_id, limit)`

## 本機啟動

需要 compose（Postgres、MinIO 19000、Redpanda 9092）與正確 `.env`。不要連 8080／9200／9000。

```bash
make run-collector
make run-normalizer
make run-deduplicator
curl -s http://127.0.0.1:18083/health
curl -s http://127.0.0.1:18083/ready
```

查一份 Document 的去重關係：

```bash
make run-cli ARGS="documents list --limit 10"   # 「去重」欄：重複／canonical／未去重
make run-cli ARGS="documents show <id>"         # 指向的 canonical 或底下的 duplicate 清單
```

## 測試

```bash
cargo test -p deduplicator --lib
cargo test -p deduplicator --test e2e -- --test-threads=1
```

單元測試（不需要 Docker）：URL 正規化的各種案例、SimHash 的距離行為、
stage 名稱穩定性、group id 決定性。

e2e（對本機 Docker，不連外網）：

1. **Acceptance B**：假 RSS 抓 10 次 → 10 筆 RawEvidence、10 份 Document →
   去重後只有 1 份 canonical，其餘 9 份進 9 個 group 全部指向它，
   10 筆 RawEvidence 逐筆確認都還在
2. **Acceptance C**：兩個 Source（不同 URL、不同 platform）提供改了三個詞的同一篇文章
   → Stage 1／2／3 全不命中 → Stage 4 命中 → 同一個 group，兩筆 RawEvidence 與兩個 Source 各自保留
3. Stage 1～5 各自命中（用精確控制的 fixture 隔離，確保命中的是目標 stage）
4. 未命中 → canonical，且 `duplicate_of` 為空、鍵有寫回
5. 同一事件消費兩次 → 全部 `AlreadyDone`，group 仍只有一列，claim 只有一列
6. 並發處理同一份 → unique index 只留一列 claim
7. `dedup.completed` 真的發到 Redpanda，帶 stage 與 canonical

### e2e fixture 的一個陷阱（踩過）

測試共用同一個 Postgres，**前幾次跑留下的 Document 也在 Stage 4 的掃描範圍內**。
最初的版本把文章寫成共用常數，第二次跑就失敗：新的 canonical 被判成「上一次 run 那份的重複」。

而「在文章後面附加幾個亂數詞」**沒有用**：SimHash 是加權投票，200 個共用詞會壓過
20 個獨有詞。實測跨 run 距離只有 2～3，比同 run 的轉載距離（5）還小——
兩次 run 的文章確實是近似重複，演算法沒錯，是 fixture 錯。

現在的做法是把 run 標記**重複 42 次**讓它帶足夠權重。用 60 組隨機 run id 實測：

| 標記重複次數 | 同 run（改 3 個詞）的距離 | 跨 run 的距離 |
|---|---|---|
| 30 | 0..=3 | ≥17 |
| **42** | **0..=2** | **≥22** |
| 46 以上 | 0..=0（標記完全蓋過改動） | ≥24 |

Stage 1／2／3／5 的單點測試改用**短到不會產生指紋**的內容（token < 16），
Stage 4 一定不適用，就不可能被別的 run 干擾。

## 已知限制

1. **Stage 4 只看最近 `scan_limit` 筆。** 比這更舊的近似文件會漏判。
   這是有界查詢的必要代價（SimHash 沒有可走索引的等值條件）。
   要完整比對需要 LSH／banding 索引，那是 V0.2 以後的事。
2. **SimHash 對詞序不敏感。** 同一組詞重新排列會算出相同指紋。
3. **`ref` 被當成追蹤參數移除。** 多數網站的 `?ref=` 確實是來源追蹤，
   但少數 API（例如 GitHub 的 `?ref=<branch>`）的 `ref` 是有意義的。
   目前跟隨 SPEC §15 的「remove known tracking params」把它移除；
   若之後要抓這類 API，需要改成 per-Source 的例外清單。
4. **query 參數會被排序。** 少數把 query 當有序序列的 API 會被改寫。
   那類幾乎都是機器介面，不是 Stage 2 想比對的文章 URL。
5. **`/a` 與 `/a/` 視為不同。** 許多伺服器確實把它們當不同資源，不擅自合併。
6. **事件亂序時可能漏判。** 候選只看比自己早的 Document（見上）。
7. **Stage 3 的 hash 定義改過**，新舊資料不相容（見上）。
8. **跨表寫入仍非原子。** 與 normalizer 同一個限制：`storage-core` 沒有跨表交易能力。
   對 dedup 的實際影響被 v5 group id 抵銷掉大半，但 claim 與 group 寫入之間
   仍有 crash window。正式解法是幫 `storage-core` 補上交易能力，留給後續版本。
