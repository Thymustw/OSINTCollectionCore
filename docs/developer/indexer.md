# indexer（`osint-indexer`）

`entity.extracted` → OpenSearch `osint-documents` 投影（SPEC §18）。
V0.1 Phase 5。程式在 `crates/indexer/`。

```text
entity-worker ──entity.extracted──▶ indexer ──bulk──▶ OpenSearch osint-documents
                         │                                    ▲
                         └── embedding-worker ──update_fields─┘  （向量 overlay，見 embedding-worker.md）
                                       └── 讀 PostgreSQL ────────┘
                                           （Document + Entity + RawEvidence）
                                       └──search.index.completed──▶
```

## 為什麼訂 `entity.extracted` 而不是 `dedup.completed`

index 裡要有 entity，SPEC §18 的 entity 過濾（「找提到 CVE-2026-0001 的文章」）才成立。
訂 `dedup.completed` 會在 entity 抽出來**之前**就把文件寫進去，那份文件會永遠帶著
空的 `entities`——除非之後有東西再來更新它，而沒有那個東西。
這不會報錯，只會讓 entity 過濾漏掉最近的文件。

副作用是好的：**duplicate 不會被索引變成結構性保證**——entity-worker 對
`is_duplicate = true` 根本不發 `entity.extracted`。

## 事件只說「哪一份要重算」，內容一律重讀 PostgreSQL

`prepare()` 只從 payload 取 `document_id`，其餘全部重新從 PostgreSQL 讀。
直接拿事件 payload 當內容會讓亂序或重放的事件覆蓋掉較新的狀態。

## Index mapping

完整定義在 `crates/indexer/src/schema.rs`，**不要在別處寫欄位字串字面值**——
core-api 與 osint-cli 都引用那裡的常數。

### `dynamic: "strict"`

寫進未宣告的欄位會被 OpenSearch 以 400 `strict_dynamic_mapping_exception` 拒絕。
打開 dynamic mapping 的後果很難察覺：一個本來該是 `date` 的欄位被第一筆資料猜成
`text`，range 過濾不會報錯，只會回錯的結果。

`projection.rs` 有一個單元測試（`body_keys_are_all_declared_in_the_mapping`）
把「投影產生的欄位」與「mapping 宣告的欄位」對照，讓少宣告一個欄位在**單元層級**
就被抓到，而不是等到對真 OpenSearch 跑才發現整批寫入失敗。

### 欄位

| 欄位 | 型別 | 用途 |
|---|---|---|
| `document_id` | keyword | `_id` 的副本；`search_after` 的收尾排序鍵 |
| `object_type` | keyword（lowercase normalizer） | object type 過濾 |
| `title` / `summary` / `body` | text（standard）+ `.cjk` sub-field | 全文檢索 |
| `author` | text + `.keyword` | 顯示 |
| `language` | keyword（lowercase） | language 過濾 |
| `labels` | keyword | 保留 |
| `source_id` / `connector_id` | keyword | source 過濾、溯源 |
| `raw_evidence_id` | keyword | **Acceptance E 的起點** |
| `canonical_url` | keyword | 顯示 |
| `source_url` | keyword，`index: false` | 只顯示，不可查詢 |
| `published_at` / `observed_at` / `collected_at` | date | 顯示與 date range |
| `effective_date` | date | date range 的**預設**欄位 |
| `indexed_at` | date | 投影寫入時間（運維用） |
| `duplicate_of` | keyword | 排除 duplicate 的過濾掛點 |
| `confidence` | float | 顯示 |
| `entities` | **nested** | entity 過濾 |
| `entity_count` | integer | 顯示 |
| `embedding_en` | knn_vector（384，hnsw／lucene／`l2`） | MiniLM 英文向量。與 `embedding_multi` **分欄位**：維度相同但空間不相通 |
| `embedding_en_model_version` | keyword | 寫入 `embedding_en` 時的模型版本（內容雜湊）。模型升級時判斷向量是否過期 |
| `embedding_multi` | knn_vector（384，hnsw／lucene／`cosinesimil`） | e5-small 多語／中文向量 |
| `embedding_multi_model_version` | keyword | 寫入 `embedding_multi` 時的模型版本 |

這四個欄位由 embedding-worker（Phase 3 Step 3）事後用 `SearchStore::update_fields`
疊加，**不是** indexer 投影時寫入。`index.knn` 必須在建立 index 時開啟
（OpenSearch 事後打不開這個 setting），對既有 `osint-documents` 加這些欄位
一定要 `osint-indexer --rebuild --drop`。engine／space_type 的實測依據見
`docs/developer/embedding.md`「已由 Phase 3 Step 2 接住」。

### analyzer 取捨：`standard` + `cjk` multi-field

compose 的 image 是原版 `opensearchproject/opensearch:2.19.6`，**不裝任何 plugin**。

| 選項 | 結論 |
|---|---|
| 只用 `standard` | ❌ 中文退化成**單字**切分。查「勒索軟體」等同查「勒 OR 索 OR 軟 OR 體」，幾乎任何中文文件都命中 |
| `standard` + `cjk` sub-field | ✅ **採用**。`cjk` 是 Lucene 內建、OpenSearch core 自帶（2.19.6 已實測：`勒索軟體攻擊` → `勒索`／`索軟`／`軟體`／`體攻`／`攻擊`） |
| `analysis-icu` / `analysis-smartcn` | ❌ 都要 `opensearch-plugin install` → 自建 image ＋ 每次升級重裝。V0.1 不接受這個維運成本 |

查詢時 `multi_match` 同時掃 `title`／`title.cjk`／`summary`／`summary.cjk`／`body`／`body.cjk`，
權重 title 3、summary 2、body 1，`.cjk` 與母欄位同權重。

**`operator: "and"`** 是關鍵：一個 term 被 analyzer 拆成多個 token 時全部都要命中。
中文靠的就是這個——預設的 `or` 會讓只含「軟體」的文件也命中「勒索軟體」。

**已知代價**：bigram 不是真正的斷詞，跨詞邊界會有假命中
（`防毒軟體攻擊者` 會產生 `體攻` 這個 bigram）。要精確比對請用引號做 phrase 查詢。

`highlight` 設 `require_field_match: false`：命中可能發生在 `title.cjk`，但要 highlight
的是 `title`。設成 `true` 的話中文查詢會回一個**沒有 snippet 的結果**。

### `entities` 用 nested 而不是扁平陣列

查「type=vulnerability 且 name=CVE-2026-0001」時，扁平陣列會命中
「有某個漏洞、也有某個叫 CVE-2026-0001 的東西」的文件——兩個條件不保證落在同一個
entity 上。`nested` 才能把它們綁在同一個元素。代價是 nested 查詢較慢，
且每個 nested 元素在 Lucene 裡是一份獨立文件。

> ⚠️ **`_cat/indices` 的 `docs.count` 會把 nested 元素一起算進去**，所以它比實際的
> 文件數大。實測：624 份 Document 在 `_cat/indices` 顯示 1220。
> 要看真實數量請用 `_count` 或 `_search` 的 `hits.total`
> （兩者都只算 root document，實測均為 624）。
> `Indexer::indexed_count()` 用的是 `_count`，`--rebuild` 印的數字是對的。

### `entities.normalized_name` 用 `lowercase` normalizer

entity-worker 的正規化規則**因型別而異**（CVE 轉大寫、domain／email／hash 轉小寫、
IP 走標準形式），搜尋端沒有辦法重現那套規則。用內建的 `lowercase` normalizer
把兩端都折成小寫之後，使用者打 `cve-2026-0001` 或 `CVE-2026-0001` 都查得到。
`_source` 仍保留原始寫法，所以顯示出來的還是 `CVE-2026-0001`。

### `effective_date` 為什麼是冗餘欄位

`effective_date = published_at.unwrap_or(observed_at)`。兩個來源欄位也都在 index 裡。

存在的理由：很多來源（static web、部分 REST API、手動匯入）根本沒有發布時間。
若 date range 預設比對 `published_at`，那些文件會在**任何**日期區間查詢裡消失——
使用者看到的是「這個來源沒資料」，而不是「這個來源沒有發布時間」。

用 script query 在查詢時算 coalesce 的話，每次查詢都要對每份候選文件跑一次腳本；
寫進 index 是一次性成本。要精確指定請用 `date_field: "published"` / `"observed"`。

## index 名稱

預設 `osint-documents`（`[indexer].index`）。帶前綴是刻意的：`OPENSEARCH_URL`
有可能被指到與別的堆疊共用的叢集，一個叫 `documents` 的 index 名字太通用，
撞名時是直接寫進別人的資料。

**core-api 與 osint-indexer 讀同一個設定鍵。** 不一致的話搜尋會查一個空的
（或別人的）index，而且完全不會報錯——使用者看到的是「都沒有資料」。

## 啟動時的 OpenSearch 身分驗證

`connect_search()` 會 `GET /` 並跑 `storage_core::conformance::assert_opensearch_identity`。
**這不是形式**：本機常見情境是 9200 被其他本機服務占用（例如另一套安全/情資平台），少了這道驗證，設定寫錯一個埠號就會開始往別人的叢集寫資料，而且一路都不會報錯。core-api 與 osint-cli 的搜尋路徑也各做一次同樣的檢查。

## 冪等

**OpenSearch 的 `_id` 就是 `Document.id`。** 寫入走 `SearchStore::bulk_upsert_fields`
（部分更新 + upsert），同一則事件重送一萬次還是一筆 hit。e2e 有對應測試
（`indexing_the_same_document_twice_yields_one_hit`）。

刻意**沒有** provenance claim：投影是可重建的衍生資料，為它寫一列 canonical 的
claim 會讓「重建」變成需要先刪 claim 才能跑。

## duplicate 的處理

| 情況 | 行為 |
|---|---|
| entity-worker 不對 duplicate 發事件 | duplicate 從來不會進 index |
| 文件**先被索引、後來才**被判成重複 | `prepare()` 讀到 `duplicate_of.is_some()`，**主動從 index 刪除** |
| 搜尋 | `include_duplicates: false`（預設）加一條 `duplicate_of` must-not-exists 過濾 |

第二點是必要的：只是「不再送新版本」並不會讓它從 index 消失，
那份重複文件會永遠留在搜尋結果裡。

第三點是防禦縱深——搜尋結果出現同一篇文章的十個轉載，比少一筆結果嚴重得多。

## 批次與 bulk 錯誤處理

設定在 `[indexer]`（`config/default.toml`）。

| 鍵 | 預設 | 說明 |
|---|---|---|
| `batch_size` | 200 | 一次 bulk 最多幾筆 |
| `batch_timeout_ms` | 1000 | 累積不到 `batch_size` 時最久等多久 |
| `max_field_bytes` | 262144 | 單一文字欄位寫進 index 的上限 |
| `bulk_max_retries` | 3 | 暫時性失敗的重試次數 |
| `lag_threshold` | 5000 | 超過就降速 |
| `backpressure_sleep_ms` | 200 | 降速的基礎延遲 |

`batch_size = 200` 的依據：OpenSearch 官方建議 bulk 抓 5–15 MB。本專案的 `_source`
平均 10–40 KB（body 上限 256 KiB），200 筆落在 2–8 MB。

**`batch_timeout_ms` 不可以設成 0。** 沒有這個逾時的話，流量低的時候最後幾筆會
永遠卡在記憶體裡不進 index，而且 offset 也不會提交——看起來像「搜尋少了最新的文件」。

### bulk 的 HTTP 200 不代表成功

**OpenSearch bulk 即使每一筆都失敗，HTTP 狀態碼仍是 200。** 錯誤只在
`items[].{action}.error` 裡逐筆出現。只看狀態碼就會把整批丟失當成成功——
這是 OpenSearch bulk 最典型的靜默失效。

`storage-opensearch` 逐筆解析，回 `BulkIndexResult { indexed, errors, failures }`，
每筆 `BulkFailure` 帶 `id` / `status` / `reason`。另外：送進去 N 筆但只回 M 筆 item 時
直接報錯（差額的狀態未知，不能讓它默默消失）。

### 暫時性 vs 永久性

| status | 分類 | 處理 |
|---|---|---|
| 429 / 502 / 503 / 504 | 暫時性 | 指數退避後**只重送失敗的那幾筆**（整批重送等於在已經過載時再加壓） |
| 400（多半是 mapping 不符） | 永久性 | 不重試。記 `error` log ＋ `osint_indexer_bulk_permanent_total`，offset 照常提交 |

永久性失敗仍提交 offset 是刻意的：重送同一則事件會得到同樣的結果，只會把 partition
卡死。要補救請跑 `make rebuild-index`。

**V0.1 沒有獨立的 DLQ topic**（已知限制）。永久性失敗不會靜默丟掉（有 log + metric），
但也不會自動重放。

## Projection checkpoint（V0.2 Phase 0f）

每次 `flush` 成功後（常駐消費與 `--rebuild` 兩條路徑都一樣）寫一次
`ProjectionCheckpoint`，讓 `projection_lag()` 有東西可算。狀態存在獨立的
`osint-projection-state` index，`_id` 就是投影名（= `[indexer].index`）。
語意與「為什麼狀態不放進被投影的 index」見 `docs/developer/storage-adapters.md`。

| 欄位 | 值 |
|---|---|
| `last_source_at` | 本批**成功寫入者**之中最大的 `collected_at` |
| `last_object_id` | 與 `last_source_at` 成對的那一筆 `Document.id`（不是本批最後一筆） |
| `objects_written` | 累積寫入數（`reset_projection` 歸零） |

### 三個刻意的選擇

- **來源時間戳用 `collected_at`，不用 `published_at` / `effective_date`。**
  lag 要回答「投影落後 canonical 多久」。`published_at` 是外部來源宣稱的發布時間：
  匯入一篇 2019 年的文章時它是 2019 年，lag 會變成七年——那個數字與投影健不健康
  完全無關，只會讓門檻永遠是紅的。`Document` **沒有** `updated_at` 欄位，
  `collected_at` 是最接近「何時進入 pipeline」的那一欄。
  讀不到時回 `None`，checkpoint 就不前進；寧可讓 lag 停在 `None`（看得出來不對）
  也不要塞一個現在的時間戳假裝沒落後。

  > ⚠️ 讀取路徑是 `projection::source_timestamp()`，欄位名常數是
  > `schema::F_COLLECTED_AT`。**改欄位名要同時改這兩處**，否則
  > `source_timestamp` 會靜默回 `None`、lag 永遠是 `None`，而且不會有任何錯誤。
  > `projection.rs` 有一支單元測試（`source_timestamp_reads_back_what_the_projection_wrote`）
  > 把「投影寫進去的」與「讀回來的」對照，讓改名在單元層級就壞掉。
- **永久性失敗的那幾筆會從 checkpoint 排除**（`BatchProgress::from_marks`）。
  checkpoint 的語意是「最後一筆**成功寫入**的來源物件」；把失敗的算進去，
  checkpoint 就宣稱進度已經過了它，而它其實不在 index 裡——lag 看起來正常，
  資料卻少一筆。
- **`last_source_at` 只前進不後退**（`ProjectionCheckpoint::advance`）。
  rebuild 是 `id DESC` 掃的，第二頁比第一頁舊。

### 程式進入點

| 位置 | 職責 |
|---|---|
| `Indexer::source_marks(&batch)` → `Vec<SourceMark>` | 在 flush **之前**取出每筆的 (id, 來源時間戳)。`flush` 會吃掉整個批次，而要排除失敗筆數得等 flush 之後才知道；整批 clone 只為寫 checkpoint 是白花記憶體（`_source` 可含 256 KiB 正文） |
| `BatchProgress::from_marks(&marks, &report)` | 純函式：排除永久性失敗、取最大來源時間戳、算 written |
| `Indexer::record_checkpoint(progress)` | 讀既有 checkpoint → `advance` → 寫回。**失敗只 warn** |

### 寫失敗只 warn，不擋索引

checkpoint 與重建狀態是**可觀測性，不是資料路徑**：文件已經進 OpenSearch 了，
為了一筆進度寫不進去而讓整批重送（或不提交 offset）只會把問題放大。

但 warn 會明講後果：**projection lag 會停在舊值、累積計數少算這一批**——
看起來像投影落後，實際落後的只有那個計數器。沒寫清楚的話下一個人會去查一個
不存在的 backlog。

狀態 index 在 `ensure_index()`（啟動時與 rebuild 前）就建立，不留到第一次 flush：
那個 400 會出現在一個只 warn 的路徑上，等於永遠沒有 checkpoint 而服務看起來完全正常。

### 已知限制

- **累加沒有樂觀鎖**：同一個 index 跑兩個以上 indexer 實例時 `objects_written`
  會少算。V0.2 假設一個投影一個 writer。
- **沒有 endpoint 可查**。`/metrics` 也還沒有 lag gauge；目前只能從程式或
  直接讀 `osint-projection-state` 取得（Operations Center 的曝露是 0f-2）。

## offset 只在 flush 成功之後才提交

先提交再送出的話，flush 失敗或行程中途被殺時，那一批文件會**永遠不進 index**，
而且不會有任何跡象——offset 已經往前走了，broker 不會再送一次。
反過來（先送出再提交）最壞只是重送，而 `_id` 是 `Document.id`，重送是覆寫。

## Backpressure

決策邏輯在 `crates/indexer/src/batch.rs`，做成**純函式**（不碰 Kafka／OpenSearch），
所以「lag 多高要降速」可以在單元層級驗證，不必真的塞滿一個 partition。

每 10 次 flush 量一次 consumer lag（`fetch_watermarks` 是一次網路往返，
每則訊息都量會變成主要成本），然後：

1. 把 lag 寫進 `osint_queue_depth` gauge（**只是曝露出來給人看**，見下）；
2. lag 超過 `lag_threshold` 時在批次之間插入延遲，倍率 = 超出門檻的倍數（上限 8 倍）。

### 這個延遲在做什麼（以及不在做什麼）

它**不會讓 lag 變小**——indexer 本來就已經在全速消費。它的目的是 CLAUDE.md §7 的
「background work must yield under VM/host pressure」：lag 高的時候瓶頸幾乎一定在
OpenSearch（bulk 佇列滿、CPU 吃滿），這時候更用力送只會換到更多 429 與重試，
對這台共用工作站上的其他 VM 也不友善。

### 跨服務的 backpressure 在 V0.1 還沒有

要讓 lag 真的變小，得讓**上游降低採集速率**——indexer 這一端做不到。

⚠️ **`osint_queue_depth` 目前只有人寫、沒有人讀。** indexer（以及 normalizer／
deduplicator／entity-worker）會把自己的 consumer lag 寫進這個 gauge，但
**collector 不會讀它**：`crates/collector/src/` 完全沒有引用 `queue_depth`，
主迴圈是固定 `tick_secs` 的排程 tick。也就是說 gauge 現在的用途只有
「給人／Prometheus 看」，不是一條自動生效的控制迴路。

**自動依 `osint_queue_depth` 降低採集速率是 V0.2 項目**，V0.1 沒有實作。
在那之前要壓住採集量只能手動調 `[collector].tick_secs`／`global_inflight`／
`per_domain_inflight`，或把 connector 停用。`RESOURCE_BUDGET.md` §8／§14 描述的是
**應該達成的目標狀態**，不是 V0.1 已經有的行為。

延遲上限刻意壓在 1.6 秒（8 × 200ms）：`core-events` 的 `session.timeout.ms` 是
10 秒，睡太久會讓 consumer 被 broker 踢出 group，反而讓 lag 更糟。

### `consumer_lag()` 的 runtime flavor 處理

librdkafka 的 `fetch_watermarks` 是同步阻塞呼叫。直接在 Tokio executor thread 上跑
會擋住同一條 thread 上的所有 task（包含 health endpoint）。用 `block_in_place`
把 thread 交還給 runtime——但它在 **current-thread runtime 上會 panic**，
而 `#[tokio::test]` 預設就是 current-thread。所以 `EventConsumer::consumer_lag`
先問 `Handle::runtime_flavor()` 再決定走哪條路。

## Rebuild（`--rebuild`）

CLAUDE.md §5：OpenSearch 是可重建的 projection。

```bash
make rebuild-index          # 補齊：既有文件覆寫成最新內容
make rebuild-index-drop     # 先刪 index 再從零重建
```

### 兩者的差別很容易被誤會

| 模式 | 行為 |
|---|---|
| `--rebuild` | 既有文件的 **indexer 已知欄位**被覆寫成最新內容，但**已經不該存在的文件不會被刪掉**（例如 Document 已從 PostgreSQL 刪除） |
| `--rebuild --drop` | 真正的完整重建 |

### 為什麼 `--rebuild` 不會清掉 embedding-worker 疊加的欄位

V0.1 的 indexer 用 `SearchStore::bulk_index`（OpenSearch `"index"` action）寫入，
語意是**整份取代 `_source`**。那時候沒差：`osint-documents` 只有 indexer 一個寫入者，
投影裡有的欄位就是全部欄位。

V0.2 Phase 3 Step 2 起 mapping 多了 `embedding_en`／`embedding_multi` 與對應的
`_model_version`，由 embedding-worker 事後用 `SearchStore::update_fields` 疊加。
indexer 的 `build_body()` 完全不知道這四個欄位。若 `--rebuild`（不必 `--drop`）
繼續走整份取代，那些向量會被靜默清空——官方文件本來就寫「既有文件會被覆寫成最新
內容」，這是 V0.1 就存在的行為，只是當時沒有第二個寫入者所以沒被發現。

所以 `flush()`（live 消費與 rebuild 的唯一寫入路徑）改成 `bulk_upsert_fields`：
只合併 indexer 自己組出來的欄位，其餘既有欄位維持原樣。文件不存在時仍會建立
（indexer 本來就要負責這件事）；這與 embedding-worker 的 `update_fields`（文件
不存在回 `NotFound`、不憑空補一份殘缺文件）是刻意相反的。

mapping 有**破壞性**變更（欄位改型別、analyzer 換掉）時**必須**用 `--drop`：
OpenSearch 的 `_mapping` 只能新增欄位。不加的話重建會成功，但舊欄位仍用舊型別，
查詢行為與新叢集不同**且不會報錯**。

`--drop` 不能單獨使用（沒有 `--rebuild`）：單獨刪掉 index 會讓搜尋直接空掉，
而且沒有任何東西會把它補回來。argument parser 會擋。

### 重建狀態（V0.2 Phase 0f）

`rebuild()` 把進度寫進 `ProjectionStore`（狀態存在 `osint-projection-state`，
`_id` = index 名）：

| 時機 | 寫入 |
|---|---|
| `--drop` 時，**刪 index 之前** | `reset_projection`（清掉 checkpoint 與舊的重建狀態） |
| 開始 | `state = Running`、`started_at` |
| 每頁結束 | `scanned` / `written` / `failed` |
| 正常結束 | `state = Completed`、`finished_at`、最終計數 |
| 中途 `Err` | `state = Failed`、`finished_at`、`last_error`（已 `sanitize`） |

**部分文件永久性失敗仍然是 `Completed`**，失敗筆數在 `failed` 欄位。
重建確實跑完了；把它當 `Failed` 會讓「跑不完」與「跑完了但有 3 筆 mapping 不符」
變成同一個狀態，而那兩者的處理方式完全不同。

`--drop` 的順序不能反：先寫 `Running` 再 `reset_projection`，那個 `Running` 會被
reset 一起清掉，於是重建過程中 `rebuild_status` 回 `Idle`——看起來像沒有人在重建，
於是有人再開一個。

查詢方式（V0.2 還沒有 endpoint，Operations Center 是 0f-2）：

```rust
store.rebuild_status("osint-documents").await?   // Idle / Running / Completed / Failed
store.projection_lag("osint-documents", Utc::now()).await?
```

### 流程與進度

依 `list_documents` 的 cursor 分頁掃 PostgreSQL（每頁 100 筆，
`RelationalStore` 的硬上限），跳過 `duplicate_of.is_some()` 的，
其餘組成批次 bulk 寫入。**每處理完一頁記一次 info log**（掃過幾筆、寫了幾筆、
跳過幾筆 duplicate、失敗幾筆、已耗時）。

V0.1 沒有可查詢的進度 endpoint；長時間重建請看 log 或 `/metrics`。

結束時印一行總結；有任何永久性失敗會以非 0 exit code 結束並列出 document id。

## `/health` `/ready` `/metrics`

預設綁 `127.0.0.1:18085`（`[indexer].bind`）。

`/ready` 比其他服務多檢查一項 **OpenSearch**：indexer 沒有 OpenSearch 就完全沒事可做，
只檢查 Postgres 會讓它在「搜尋投影整個壞掉」的情況下回報 ready。

| metric | 意義 |
|---|---|
| `osint_queue_depth` | consumer lag。**V0.1 沒有任何程式讀它**（見上），只給人／Prometheus 看 |
| `osint_indexer_indexed_total` | 成功寫進 index 的文件數 |
| `osint_indexer_skipped_duplicate_total` | 因為是 duplicate 而跳過 |
| `osint_indexer_bulk_retry_total` | 暫時性失敗的重試次數 |
| `osint_indexer_bulk_retry_exhausted_total` | 重試用盡 |
| `osint_indexer_bulk_permanent_total` | 永久性失敗的文件數 |
| `osint_indexer_backpressure_total` | 觸發降速的次數 |
| `osint_indexer_entities_truncated_total` | extraction 達到查詢上限（entity 不完整） |

## 已知限制

1. **一份 Document 最多收錄前 100 筆 entity extraction。**
   `RelationalStore::list_entity_extractions_by_object` 把 limit 夾在 1..=100
   且沒有 cursor 版本。超過的部分不會進 index，那份文件的 entity 過濾會不完整
   （會記 warn 並計入 `osint_indexer_entities_truncated_total`）。
   entity-worker 的上限是 500，所以確實可能發生。
2. **沒有 DLQ topic**（見上；V0.1 的設計決策：完整 DLQ subsystem 需要保留策略、重放路徑與 RBAC；只建 topic 而沒有這些等於建了一個靜默過期的失效安全網）。
3. **`--rebuild` 不刪多餘文件**（見上）。
4. **cjk bigram 不是真正的斷詞**（見上）。
5. **單一文字欄位截斷到 256 KiB**。超長正文只有前 256 KiB 可被搜尋到，
   而且是在 byte 層級截（不切破 UTF-8 字元）。
6. **`0` replica**。compose 是單節點；多節點部署要改 `index_settings()`，
   否則叢集健康會停在 yellow。

## 相關文件

- `docs/developer/search-api.md`：`POST /api/v1/search` 的 schema 與注入防護
- `docs/user/search.md`：使用者導向的搜尋語法
- `docs/developer/entity-worker.md`：上游的 `entity.extracted`
- `docs/developer/storage-adapters.md`：`SearchStore` capability
