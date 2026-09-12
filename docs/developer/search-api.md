# Search API（`POST /api/v1/search`）

SPEC §19 的 `POST /search`、SPEC §18 的八種搜尋。V0.1 Phase 5。
程式在 `crates/core-api/src/search.rs`（HTTP 層）與 `crates/indexer/src/search.rs`
（請求 → 查詢的翻譯）。

## 為什麼是 POST 而不是 GET

搜尋是唯讀的（viewer 即可），但查詢條件有巢狀結構（`entity` 物件、日期、布林語法），
塞進 query string 需要多層編碼，而且長查詢會撞到 URL 長度上限。SPEC §19 寫的也是
`POST /search`。

## 只有一份查詢翻譯

`core-api` 與 `osint-cli search` 都呼叫 `indexer::search::build()`。
兩邊各寫一份的結果是「CLI 查得到、API 查不到」，而且不會有任何錯誤訊息——只是結果不一樣。

## 認證與授權

| 項目 | 值 |
|---|---|
| 認證 | `Authorization: Bearer <jwt>` 或 `Bearer osint_<id>.<secret>` |
| 權限 | `Permission::Read`（**viewer 以上**） |
| 沒有 token | `401 unauthorized` |
| OpenSearch 沒接上 | `503 unavailable` |

## Request

```jsonc
{
  "query": "ransomware AND \"lockbit gang\" NOT decryptor",
  "source_id": "0199....",          // 可選，UUID
  "connector_id": "0199....",       // 可選，UUID
  "entity": {                        // 可選
    "type": "vulnerability",         //   可省略 → 任何型別
    "name": "CVE-2026-0001"          //   必填，大小寫不敏感
  },
  "date_from": "2026-01-01T00:00:00Z",   // 可選，RFC3339
  "date_to":   "2026-09-10T23:59:59Z",   // 可選，RFC3339
  "date_field": "effective",             // effective（預設）/ published / observed
  "language": "en",                      // 可選
  "object_type": "article",              // 可選
  "include_duplicates": false,           // 預設 false
  "limit": 20,                           // 1..=100，預設 20
  "cursor": "eyJ..."                     // 上一頁的 next_cursor
}
```

**全部欄位皆可省略。** `{}` 是合法請求，代表「列出全部（不含 duplicate），
依時間由新到舊」。

### `deny_unknown_fields`

打錯欄位名（`langauge`）會回 **422**，不會被靜默忽略。
靜默忽略的話使用者以為自己過濾了，其實沒有——那是最難發現的一種錯。

### `date_field` 的選擇

| 值 | 比對欄位 |
|---|---|
| `effective`（預設） | `published_at` 有值時用它，否則 `observed_at` |
| `published` | 只看 `published_at`。**沒有發布時間的文件不會出現** |
| `observed` | 只看 `observed_at`（本系統第一次看到它的時間） |

預設是 `effective` 而不是 `published`：很多來源沒有發布時間，用 `published` 當預設
會讓那些文件在任何日期區間查詢裡消失，使用者看到的是「這個來源沒資料」。
細節見 `docs/developer/indexer.md` 的 `effective_date`。

### `entity` 是 nested 過濾

`type` 與 `name` 必須落在**同一個** entity 上。扁平陣列會命中
「有某個漏洞、也有某個叫 CVE-2026-0001 的東西」的文件。

## Response

```jsonc
{
  "total": 42,                    // 精確值（track_total_hits=true），不是「至少 10000」
  "hits": [
    {
      "document_id": "0199....",
      "score": 3.14,              // 沒有全文條件時可能是 null
      "title": "…",
      "snippet": "…<em>ransomware</em>…",
      "object_type": "article",
      "language": "en",
      "source_id": "0199....",
      "connector_id": "0199....",
      "raw_evidence_id": "0199....",   // Acceptance E 的起點
      "canonical_url": "https://…",
      "published_at": "2026-09-10T12:00:00Z",
      "observed_at": "2026-09-10T12:05:00Z",
      "entities": [
        { "entity_id": "…", "entity_type": "vulnerability",
          "name": "CVE-2026-0001", "normalized_name": "CVE-2026-0001" }
      ]
    }
  ],
  "next_cursor": "eyJ..."         // null 代表已經是最後一頁
}
```

### `raw_evidence_id` 為什麼一定要在

SPEC §26 Acceptance E：search result 必須能一路反查到 RawEvidence／Source／Connector。
少了這一欄，那條鏈在第一步就斷了，而使用者看到的是一個完全正常的搜尋結果。

回溯路徑：

```text
hit.raw_evidence_id
  → GET raw_evidence            → raw.source_id / raw.connector_id
  → GET sources/{source_id}
  → GET connectors/{connector_id}
```

`crates/core-api/tests/search_api_e2e.rs` 逐步 assert 這條鏈，
而且刻意只用 hit 裡的欄位（不拿管線中間的變數當捷徑）。

### `snippet` 的退回機制

優先用 highlight（`title` → `summary` → `body`），沒有 highlight 時退回
摘要開頭 200 字元。**退回是必要的**：只用過濾條件（沒有全文查詢）時 OpenSearch
不會產生任何 highlight，若不退回，那些結果的 snippet 會全部是 null，看起來像資料有問題。

### `entities` 上限 20

一份 IOC 清單型的文件可能有上百個 entity，全部塞進每一筆 hit 會讓 20 筆結果的回應
變成好幾 MB。完整清單請用 `osint-cli documents show`（V0.2 會有 `GET /objects/{id}`）。

## 查詢語法與注入防護

### 語法

| 寫法 | 意義 |
|---|---|
| `ransomware` | 一個詞 |
| `"ransomware gang"` | 片語，詞序必須相符 |
| `a b` | 兩個詞都要有（相鄰預設是 AND） |
| `a AND b` / `a OR b` / `a NOT b` | 布林 |
| `(a OR b) AND c` | 括號分組 |

`AND`／`OR`／`NOT` 必須**全大寫**才算運算子。小寫的 `and` 是普通的詞——
否則使用者查 `crowdstrike and falcon` 會得到一個他沒打算下的布林運算。

### 為什麼自己 parse

使用者的字串**永遠不會**被交給 OpenSearch 的查詢語言。
`indexer::query::parse()` 先把它解析成 `storage_core::QueryExpr`
（Term／Phrase／And／Or／Not），adapter 再把樹翻成 `multi_match` + `bool`。

| 輸入 | `query_string` 的行為 | 這裡的行為 |
|---|---|---|
| `title:*` | 對 title 做萬用比對 | 一個要比對的**詞**：字面上的 `title:*` |
| `*` | 命中全部文件 | 一個要比對的詞 |
| `body:/.*(a\|b)*.*/` | 正規表示式，可做 CPU DoS | 一個要比對的詞 |
| `_id:abc` | 直接指定內部欄位 | 一個要比對的詞 |
| `a~2` | 模糊比對（昂貴） | 一個要比對的詞 |

不用 `simple_query_string` 的理由：它擋掉了欄位名與 regex，但仍保留 `*` 前綴萬用與
`~` 模糊比對，而且會**靜默吞掉**語法錯誤（`AND AND` 不報錯，只是不照你想的做）。

代價是語法比較少，換到的是：任何使用者字串都只可能落在 `Term`／`Phrase` 的值裡，
不可能變成查詢語言的結構。

> ⚠️ `*probe*` 這種輸入**會**命中含 `probe` 的文件——因為 standard analyzer 把 `*`
> 當標點去掉，剩下字面上的 `probe`。那是「當成文字」的正確行為，不是萬用比對。
> e2e 用 `*prob*`（真萬用會命中 `probe`，當成文字則不會）來驗這個區別。

### 上限

| 項目 | 上限 | 錯在哪一層 |
|---|---|---|
| 查詢字串長度 | 1024 字元 | `query::parse` → 400 |
| token 數 | 64 | `query::parse` → 400 |
| 括號巢狀 | 8 層 | `query::parse` → 400 |
| 語法樹深度 | 8 層 | `storage-opensearch` 再夾一次 |
| `limit` | 100 | `SearchRequest::effective_limit()` 夾住，不報錯 |
| adapter 端 `size` | 200 | `storage-opensearch` 再夾一次 |

上限在**兩層都夾**是刻意的：少一個地方忘了夾就是一個 DoS 入口。

### `storage_core::SearchQuery` 不可接使用者輸入

`SearchStore::query()`（原始 `query_string`）只給 conformance 與運維臨時查詢用。
面向使用者的路徑一律走 `SearchStore::search()` + `StructuredSearch`。

## 分頁

`search_after`，不是 `from/size`。

`from/size` 深分頁在每個 shard 上都要取回 `from + size` 筆再丟掉前面的，
翻到第 1000 頁時等於每個 shard 排序 20000 筆。`search_after` 是「從這個排序鍵之後
繼續」，成本與頁碼無關。

### 排序鍵

| 情況 | 排序 |
|---|---|
| 有全文條件 | `_score` desc, `document_id` asc |
| 只有過濾條件 | `effective_date` desc, `document_id` asc |

**一律以 `document_id` 收尾。** 排序值能唯一決定一筆文件，否則同分（或同日期）的
文件在翻頁時會漏掉或重複。e2e 用五份內容完全相同的文件驗這一點
（`cursor_pagination_walks_every_document_exactly_once`）。

只有過濾條件時不用 `_score` 排序：每筆分數都一樣，等於隨機順序。

### cursor 格式

`base64url(JSON 陣列的排序值)`。不攤在 query string 裡是為了讓人不要自己編——
編出來的值會被原樣送進 `search_after`。cursor 解不開時回 400
並提示「請直接使用上一頁回應裡的 next_cursor」。

## 錯誤

| 狀況 | 狀態碼 | error |
|---|---|---|
| 沒有／壞 token | 401 | `unauthorized` |
| 角色不足 | 403 | `forbidden` |
| 查詢語法錯、日期區間顛倒、entity 名稱空、cursor 壞 | 400 | `bad_request` |
| 未知欄位、JSON 型別錯 | 422 | — |
| OpenSearch 沒接上 | 503 | `unavailable` |
| OpenSearch 逾時 | 504 | `timeout` |

**查詢語法錯是 400 不是 500。** 回 500 會讓人去查伺服器 log 找一個不存在的故障。
每一則訊息都寫了怎麼修（例如「引號沒有成對。請補上結尾的 `"`」）。

### index 不存在時回 200 + 0 筆

indexer 從沒跑過時 OpenSearch 會回 404。那不是伺服器錯誤，而是「還沒有任何文件被
索引」，所以 adapter 把它轉成空結果。

## Metrics

| metric | 意義 |
|---|---|
| `osint_search_latency_ms_sum` / `_count` | 搜尋延遲 |
| `osint_search_requests_total` | 搜尋請求數 |

## 相關文件

- `docs/developer/indexer.md`：index mapping、analyzer 取捨、rebuild
- `docs/user/search.md`：使用者導向的語法說明
- `docs/user/cli.md`：`osint-cli search`
- 內部治理文件 API_COMPATIBILITY.md（未隨原始碼公開）
