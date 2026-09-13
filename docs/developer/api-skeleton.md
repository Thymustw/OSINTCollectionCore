# API 參考（V0.1）

Base path：`/api/v1`（SPEC §19）。**這份是 V0.1 的 API 參考文件**
（SPEC §27 Exit Criteria「API docs 可查看」指的就是它）。

SPEC §19 的 endpoint 在 Phase 6b 全部落地，唯一的例外是 `POST /objects`——
它一律回 501（V0.1 的決定：直接 POST 建立的物件沒有 Raw Evidence 祖先，provenance 鏈會靜默地不完整——物件在所有 list 與搜尋回應裡看起來完全正常，只有分析師追查「這則資訊從哪來」時才會發現 `raw_evidence: []`；push 式匯入請改用 `POST /api/v1/import`，讓上傳的 bytes 本身成為 Raw Evidence）。

## 公開端點（無需認證）

| 方法 | 路徑 | 說明 |
|---|---|---|
| GET | `/health` | liveness。行程活著即 `{"status":"ok"}` |
| GET | `/ready` | readiness。Postgres 等檢查全過才 200，否則 503 |
| GET | `/metrics` | Prometheus text（記憶體 registry，非正式 OpenTelemetry exporter） |

⚠️ `/metrics` 維持公開是**刻意的決定**，不是還沒做。它只輸出聚合計數器，
但量體本身仍是情報，所以部署時必須用網路層限制（綁 loopback／內網，或放在只允許
Prometheus 來源 IP 的 reverse proxy 後面）。完整理由見 `docs/developer/security.md`
的「`/metrics` 維持公開」一節。

## 需認證端點

`Authorization: Bearer <jwt>` 或 `Authorization: Bearer osint_<id>.<secret>`。

RBAC（SPEC §23）：**GET 一律 viewer 以上，POST／PATCH 一律 operator 以上，
token 管理是 admin only**。角色是嚴格超集（admin ⊃ operator ⊃ viewer）。

| 方法 | 路徑 | 最低角色 | 成功碼 | 說明 |
|---|---|---|---|---|
| GET | `/api/v1/whoami` | viewer | 200 | 目前身分與認證方式 |
| GET | `/api/v1/sources` | viewer | 200 | cursor 分頁 |
| GET | `/api/v1/sources/{id}` | viewer | 200 | 回應帶 `ETag` |
| POST | `/api/v1/sources` | operator | 201 | 帶 `ETag` |
| PATCH | `/api/v1/sources/{id}` | operator | 200 | **必須帶 `If-Match`** |
| GET | `/api/v1/connectors` | viewer | 200 | `?enabled=true\|false` |
| GET | `/api/v1/connectors/{id}` | viewer | 200 | 回應帶 `ETag` |
| POST | `/api/v1/connectors` | operator | 201 | `source_id` 必須存在 |
| PATCH | `/api/v1/connectors/{id}` | operator | 200 | **必須帶 `If-Match`** |
| GET | `/api/v1/collections` | viewer | 200 | cursor 分頁 |
| GET | `/api/v1/collections/{id}` | viewer | 200 | 含三份關聯 id 清單 |
| POST | `/api/v1/collections` | operator | 201 | 可一併 link source／connector |
| GET | `/api/v1/objects` | viewer | 200 | `?object_type=`、`?include_duplicates=` |
| GET | `/api/v1/objects/{id}` | viewer | 200 | 含 provenance 鏈、去重、entity |
| POST | `/api/v1/objects` | operator | — | **一律 501**（ADR-006） |
| GET | `/api/v1/entities` | viewer | 200 | `?entity_type=` |
| GET | `/api/v1/entities/{id}` | viewer | 200 | 含關聯數與抽取紀錄 |
| GET | `/api/v1/entities/{id}/resolution-candidates` | viewer | 200 | cursor 分頁；`?status=` |
| GET | `/api/v1/entities/{id}/merge-history` | viewer | 200 | 含已撤銷的 merge |
| POST | `/api/v1/entities/{id}/resolve` | operator | 200 | 跑只需要 Postgres 的掃描方法，回這次新寫入的候選 |
| POST | `/api/v1/entities/{id}/resolve/graph-context` | operator | 200 | graph_context；Neo4j 沒接上回 503，不影響上一條 |
| POST | `/api/v1/entities/merge` | operator | 200 | body：`survivor_id`／`merged_id`／`reason` |
| POST | `/api/v1/merge-history/{id}/undo` | operator | 204 | 重複 undo 回 409 |
| GET | `/api/v1/relationships` | viewer | 200 | `?type=` |
| GET | `/api/v1/relationships/{id}` | viewer | 200 | 含 evidence（SPEC §12） |
| GET | `/api/v1/events` | viewer | 200 | V0.1 無寫入者，正常是空的 |
| GET | `/api/v1/events/{id}` | viewer | 200 | 正常是 404（見下） |
| GET | `/api/v1/raw/{id}` | viewer | 200 | `?body=true` 才讀內容 |
| GET | `/api/v1/jobs` | viewer | 200 | `?status=failed` 等 |
| GET | `/api/v1/jobs/{id}` | viewer | 200 | |
| POST | `/api/v1/jobs` | operator | 201 | |
| POST | `/api/v1/jobs/{id}/transition` | operator | 200 | |
| POST | `/api/v1/jobs/{id}/dispatch` | operator | 200 | |
| POST | `/api/v1/jobs/{id}/retry` | operator | 200 | 只對 `failed` |
| GET | `/api/v1/ops/health` | viewer | 200／503 | 六個後端聚合 |
| GET | `/api/v1/ops/metrics` | viewer | 200 | 行程資源用量（RAM／CPU／Disk） |
| GET | `/api/v1/ops/connectors` | viewer | 200 | connector 採集健康。`?unhealthy=`、`?stale_after_secs=`、`?limit=` |
| GET | `/api/v1/ops/queues` | viewer | 200／503 | consumer group lag。沒接 Redpanda 回 503 |
| GET | `/api/v1/ops/dlq` | viewer | 200／503 | 失敗 Job 清單。`?limit=`。沒接 Postgres 回 503 |
| POST | `/api/v1/import` | operator | 201 | multipart |
| POST | `/api/v1/search` | viewer | 200 | |
| GET | `/api/v1/graph/entities/{id}/neighbors` | viewer | 200 | 圖鄰居。沒有這個節點回空陣列，不是 404 |
| GET | `/api/v1/graph/entities/{id}/relationships` | viewer | 200 | 圖邊。語意同上 |
| GET | `/api/v1/graph/path` | viewer | 200 | `from`／`to` 必填。找不到路徑回 `null`，永遠 200 |
| POST | `/api/v1/graph/query` | viewer | 200 | 結構化圖查詢。`starts` 空陣列回 400 |
| POST | `/api/v1/graph/rebuild` | operator | 201 | 建立 `graph_rebuild` Job；**不會 drop** |
| POST | `/api/v1/tokens` | **admin** | 201 | 明文只回一次 |
| GET | `/api/v1/tokens` | **admin** | 200 | |
| DELETE | `/api/v1/tokens/{id}` | **admin** | 204 | |

POST `/api/v1/jobs` body：

```json
{"type": "collect", "correlation_id": null, "dispatch": true}
```

`dispatch` 預設 true：寫入 Postgres 後 produce `job.dispatched` 到 Redpanda。

POST `/api/v1/search`（SPEC §18／§19）是唯讀的（viewer 即可），用 POST 是因為查詢條件
有巢狀結構、長查詢會撞到 URL 長度上限。OpenSearch 沒接上時**只有這條路由**回 503。
request/response schema、八種搜尋的語法、注入防護與分頁見 `docs/developer/search-api.md`。

POST `/api/v1/import`（Manual／JSON／CSV 上傳）是 `multipart/form-data`，
上傳大小上限與其他路由分開（`[import].max_upload_bytes`，預設 10 MiB；
其他路由仍是 `[http].request_body_limit_bytes` 1 MiB）。
完整格式、欄位對映與上限見 `docs/developer/import-api.md`。

## 共用規則（資源類 endpoint）

### 錯誤碼對照

| 狀態碼 | 什麼時候 | 呼叫端的下一步 |
|---|---|---|
| 400 | body 格式錯、欄位值不合法、對不可為空的欄位傳 `null`、憑證欄位是明文 | 改請求 |
| 401 | 沒帶 / 帶了無效的 `Authorization` | 換憑證 |
| 403 | 角色不足（viewer 想寫） | 換一把 operator／admin 憑證 |
| 404 | 資源不存在 | 用對應的 list 確認 id |
| 409 | POST 指定的 id 已存在；job 狀態不允許 retry | 改用 PATCH／省略 id；先確認 job 狀態 |
| 412 | `If-Match` 與目前版本不符（有人先改過） | **重新 GET**，把改動套到新版本再重試 |
| 413 | `GET /raw/{id}?body=true` 的內容超過上限 | 直接從物件儲存讀，或調高 `[import].max_upload_bytes` |
| 422 | 引用的資源不存在（connector 的 `source_id`、collection 的 `source_ids`） | 先建立那個資源 |
| 428 | PATCH 沒帶 `If-Match` | 先 GET 拿 `ETag` |
| 501 | `POST /objects`（ADR-006）；`/ops/metrics` 在非 Linux | 改用 `POST /import` |
| 503 | 後端沒接上（Postgres／MinIO／OpenSearch／Redpanda／Neo4j）；`/ops/health` 有任一後端 down | 看訊息裡指的環境變數 |

錯誤 body 一律是 `{"error": "...", "message": "…下一步建議…"}`。

### PATCH：`If-Match` 樂觀鎖

PATCH **一定要帶 `If-Match`**。少了它，兩個人同時改同一個 Source 時後寫的會
靜默蓋掉先寫的（lost update），而且兩邊都看到 200。

```bash
# 1. 先讀，拿 ETag
ETAG=$(curl -sI http://127.0.0.1:18080/api/v1/sources/$ID \
  -H "Authorization: Bearer $TOKEN" | awk -F': ' '/^etag/{print $2}' | tr -d '\r')

# 2. 帶著它改
curl -s -X PATCH http://127.0.0.1:18080/api/v1/sources/$ID \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H "If-Match: $ETAG" -d '{"enabled": false}'
```

`ETag` 對呼叫端是**不透明字串**，原樣送回即可。實作上有兩種來源：

| 資源 | ETag 內容 | 為什麼 |
|---|---|---|
| Source | `updated_at` 的 RFC3339（微秒） | SPEC §5 有 `updated_at` |
| Connector | `sha256:<16 hex>` 狀態指紋 | **SPEC §6 沒有 `updated_at`**，資料表也沒有這一欄 |

不為了 ETag 去加一個 SPEC 沒定義的 `connectors.updated_at`：那要動 migration、
兩個 adapter 的 mapping 與所有 `Connector { … }` 建構點。狀態指紋語意更嚴格
（任何欄位變了就不符），對呼叫端的用法完全一樣。

也接受 `If-Match: *`（RFC 7232：「只要資源存在就好」）與 `W/"…"`。

### PATCH：merge patch 語意

RFC 7396 的子集：

- **body 裡出現的欄位才會被改**，沒出現的維持原值
- 可為空的欄位傳 `null` 代表**清除**
- 不可為空的欄位（`name`、`enabled`、`type`…）傳 `null` 會回 **400**，
  不會被當成「沒提供」而靜默忽略
- 不認得的欄位回 400（`deny_unknown_fields`）。`id`／`created_at`／`updated_at`
  不可 PATCH；Connector 的 `source_id` 也不可改（搬動來源會讓既有 RawEvidence
  的出處變成假的，要換請新建一個）

### POST：建立語意，不是 upsert

- 省略 `id` → server 產 UUID v7（list 依 id 排序當時間序）
- 指定 `id` 且已存在 → **409**，不會覆寫

> ⚠️ 實作是「先查再寫」，不是單一原子語句。兩個請求同時帶同一個新 id 時，
> 理論上可能都通過檢查，後者覆寫前者。V0.1 接受這個窗口（建立資源不是熱路徑）；
> 真的要擋需要 adapter 提供 insert-only 語意（像 `insert_raw_evidence` 那樣）。

## Sources

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/sources \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"某資安部落格","source_type":"rss","base_url":"https://example.org/feed"}'
```

`source_type`：`rss`／`atom`／`static_web`／`rest_api`／`manual_upload`／
`json_import`／`csv_import`。
預設值：`enabled=true`、`collection_policy={}`。
回應是完整的 `Source`（欄位見 SPEC §5 / `core-model::Source`）。

## Connectors

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/connectors \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"source_id":"'$SOURCE_ID'","name":"blog-rss","type":"rss",
       "credential_reference":"env:BLOG_TOKEN","schedule":"0 */6 * * *"}'
```

- `source_id` 必須存在，否則 **422**（不是 400：JSON 合法，是引用的資源不存在）
- `credential_reference`／`proxy_reference` **只接受 SecretRef**
  （`env:VAR`／`file:/path`／`store:backend/path#key`）。明文密碼或連線字串回
  **400**，而且錯誤訊息不會回顯那個值（SPEC §6：禁止儲存明文 password/token）
- 未給時的預設：`version="0.1.0"`、`status="idle"`、`enabled=true`、
  `error_count=0`、JSON 欄位 `{}`
- `GET /connectors?enabled=false` 只列停用的。**省略 `enabled` 代表全部**——
  「為什麼我的 connector 不見了」通常就是因為它被停用，預設藏起來只會更難查

## Collections

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/collections \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"勒索軟體追蹤","source_ids":["'$SOURCE_ID'"],"priority":3}'
```

`source_ids`／`connector_ids` 一次最多 100 筆，**全部先驗過才寫**：
其中一個不存在就整個 422，不會留下一個「只連到一半」的 collection。

`GET /collections/{id}` 回 Collection 本體（flatten 在頂層）加三份清單：

```json
{
  "id": "...", "name": "勒索軟體追蹤", "status": "active", "priority": 3,
  "source_ids": ["..."], "connector_ids": [], "object_ids": ["..."],
  "sources_truncated": false, "connectors_truncated": false, "objects_truncated": false
}
```

三份清單上限都是 100，`*_truncated` 代表還有更多。**不要用 `len() == 100` 自己推**：
剛好 100 筆與超過 100 筆在這個 API 上長得一樣。

## Objects（Document）

`GET /objects` query：

| 參數 | 預設 | 說明 |
|---|---|---|
| `object_type` | 全部 | `article`／`web_page`／`post`／`message`／`report`／`file`／`advisory` |
| `include_duplicates` | `false` | **預設排除重複**（SPEC §15／§16）：一篇報導被十個站轉載時預設是一筆，不是十一筆 |

兩個條件都在 SQL 裡做。先取一頁再在程式端濾是錯的——「最近 20 筆剛好全是轉載」
會變成一個空頁，使用者只會以為系統沒有資料。

`GET /objects/{id}` 把 SPEC §14 的可追溯鏈串起來：

```json
{
  "id": "...", "object_type": "article", "title": "...",
  "provenance": [{"action": "normalized", "raw_evidence_id": "...", "processor": "osint-normalizer"}],
  "raw_evidence": [{"id": "...", "source_url": "...", "sha256": "..."}],
  "duplicate_group": null,
  "duplicates": [{"method": "content_hash", "similarity": 1.0, "member_object_id": "..."}],
  "duplicates_truncated": false,
  "entities": [{"extraction": {"extractor": "regex", "text_offset": 10}, "entity": {"entity_type": "vulnerability"}}],
  "entities_truncated": false
}
```

- `duplicate_group` 非 null ＝ 這份是**別人的重複**，裡面有 canonical 的 id
- `duplicates` ＝ 這份是 canonical，這些指向它
- `entities[].entity` 是 null 代表 extraction 指到的 Entity 查不到（資料被外力改過）；
  這種情況**不隱藏那一筆**，讓它顯示成異常而不是憑空消失

`POST /objects` 一律 501（ADR-006），訊息指向 `POST /api/v1/import`。

## Entities

`GET /entities?entity_type=vulnerability`。`GET /entities/{id}` 多回：

- `relationship_count`：**在 100 筆範圍內數到的**筆數，不是全表 count。
  `relationships_truncated` 為 true 時代表「至少這麼多」——把它當精確總數顯示，
  熱門 IOC 的關聯數會永遠停在 100
- `recent_extractions`：這個 Entity 是從哪些 object 抽出來的（SPEC §17）

### Resolve／Merge

```bash
# 對一個 Entity 跑只需要 Postgres 的掃描方法，回這次新寫入的候選
curl -s -X POST http://127.0.0.1:18080/api/v1/entities/$ID/resolve \
  -H "Authorization: Bearer $TOKEN"

# graph_context（獨立 endpoint）。Neo4j 沒接上回 503，不影響上一條
curl -s -X POST http://127.0.0.1:18080/api/v1/entities/$ID/resolve/graph-context \
  -H "Authorization: Bearer $TOKEN"

# 列出這個 Entity 參與過的候選（entity_a 或 entity_b 命中都算）
curl -s "http://127.0.0.1:18080/api/v1/entities/$ID/resolution-candidates?status=pending" \
  -H "Authorization: Bearer $TOKEN"

# 把 merged 併進 survivor。reason 不可為空白。
curl -s -X POST http://127.0.0.1:18080/api/v1/entities/merge \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"survivor_id":"'$SURVIVOR'","merged_id":"'$MERGED'","reason":"人工確認是同一個人"}'

# 撤銷一次 merge。成功 204；已經 undo 過再打一次是 409。
curl -s -o /dev/null -w '%{http_code}\n' -X POST \
  http://127.0.0.1:18080/api/v1/merge-history/$HISTORY_ID/undo \
  -H "Authorization: Bearer $TOKEN"
```

`POST /entities/{id}/resolve` 目前注入的是 `MockEmbeddingProvider::unsupported()`：
`semantic_similarity` **誠實回空**，不是假裝已接上。會真的產生候選的方法是
`normalized_name`／`alias`／`domain`／`account_handle`。`graph_context` 走獨立
路由 `POST /entities/{id}/resolve/graph-context`（接 `storage-neo4j`；沒接上
回 503，不影響上一條）。完整理由見 `docs/developer/resolver.md`。
merge 行為見 `docs/developer/merge.md`。

### Graph

```bash
# 一跳鄰居。max_hops 省略 = 1。relationship_types／entity_types 是逗號分隔。
curl -s "http://127.0.0.1:18080/api/v1/graph/entities/$ID/neighbors?max_hops=1" \
  -H "Authorization: Bearer $TOKEN"

curl -s "http://127.0.0.1:18080/api/v1/graph/entities/$ID/relationships" \
  -H "Authorization: Bearer $TOKEN"

# 找不到路徑回 JSON null，HTTP 仍是 200（不是 404）
# ⚠️ GraphPathQuery 不能 serde flatten GraphTraversalQuery：
# serde_urlencoded 會把 max_hops=4 當字串，整條變 400。
curl -s "http://127.0.0.1:18080/api/v1/graph/path?from=$FROM&to=$TO&max_hops=4" \
  -H "Authorization: Bearer $TOKEN"

# 結構化查詢。starts 空陣列回 400。
curl -s -X POST http://127.0.0.1:18080/api/v1/graph/query \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"starts":["'$ID'"],"pattern":{"kind":"neighbors"},"options":{"max_hops":1}}'

# 派一個非破壞性 rebuild job。201、body 是 Job（status=queued）。
# 真正執行的是 osint-graph-worker 消費 job.dispatched。
# ⚠️ 這條路由永遠 drop_graph=false：Job model 沒有參數欄位，
# 全清重建只有 CLI `osint-graph-worker --rebuild --drop`。
curl -s -X POST http://127.0.0.1:18080/api/v1/graph/rebuild \
  -H "Authorization: Bearer $TOKEN"
```

`time_from` 與 `time_to` 必須成對提供，只給一端回 **400**（不會靜默忽略——那樣會讓人以為時間過濾生效了）。
Neo4j 沒接上時這 4 條讀取路由回 503；`POST /graph/rebuild` 只需要 Job 系統（Postgres + 能發 `job.dispatched`），跟圖連線是分開的。

## Relationships

`GET /relationships?type=mentions`。`GET /relationships/{id}` 一定帶 `evidence`
清單——SPEC §12「任何 relationship 必須能回查 evidence」就是靠這一欄落地的。

## Events

SPEC §13：V0.1 只建立基礎 event model，**不做自動 Event Detection**。
`events` 表目前沒有任何寫入者，所以 list 回空清單、單筆回 404 都是**正確行為**。
404 的訊息會講明這件事，免得被當成故障查。

## Raw Evidence

```bash
curl -s "http://127.0.0.1:18080/api/v1/raw/$ID?body=true" -H "Authorization: Bearer $TOKEN"
```

預設只回 metadata（RawEvidence 全欄位）。`?body=true` 才去物件儲存拉內容：

```json
{"...RawEvidence 欄位...", "body": {
  "bytes": 1234, "content_encoding": "utf8", "content": "…", "storage_path": "raw/…"
}}
```

- 非 UTF-8（PDF／圖片）→ `content_encoding: "base64"`
- 超過 `[import].max_upload_bytes` → **413**，metadata 仍可取得（去掉 `?body=true`）。
  **不做截斷**：截斷過的內容拿去算 sha256 對不上，會讓人以為證據被竄改
- 上限與 `/import` 共用同一個設定：收得進來就應該讀得出去，
  兩邊用不同的數字只會製造「存得進去卻讀不出來」的洞

## Jobs

`GET /jobs?status=failed` 的過濾在 SQL 裡做（`list_jobs_by_status`）。

`POST /jobs/{id}/retry`（SPEC §31）只對 `failed` 的 job 有效，其他狀態回 **409**。

> ⚠️ **retry 會把 job 轉成 `retrying`，不是 `queued`。** 狀態機沒有
> `failed → queued` 這條邊，而且不該有：`retry_count` 是在進入 `retrying` 時
> 累加的。若把失敗的 job 直接丟回 `queued`，它看起來會跟一個從沒跑過的新 job
> 一模一樣——「重試過幾次」會在每次重試時被抹掉，無限重試的迴圈也就沒有任何地方
> 看得出來。轉成 `retrying` 之後會立刻 dispatch，所以對呼叫端而言效果就是「它會再跑一次」。

## Operations Center（`/api/v1/ops/*`，SPEC §31）

`GET /ops/health` 聚合六個後端：`postgres`／`object_store`／`redis`／
`opensearch`／`redpanda`／`neo4j`。

`neo4j` 是 V0.2 phase 0b 加的，設定鍵是 `[storage.graph].http_url`。
⚠️ 它**只打 HTTP `GET /`（7474）**，沒有建立 Bolt 連線，因此
**只證明 HTTP 埠活著，不證明 Bolt 可連或資料庫可寫**——Neo4j 在還原中、
或某個 database 處於 `offline`／`failed` 時 7474 仍回 200。
Phase 2a 接上 `storage-neo4j` adapter 後要升級成真的跑一次 Cypher。
選擇 HTTP 而非 Bolt 的理由見 `crates/core-api/src/ops.rs` 的 `GraphCheck`。

```json
{
  "healthy": false,
  "checks": [{"name": "postgres", "healthy": true, "message": "SELECT 1 成功"},
             {"name": "redis", "healthy": false, "message": "連線被拒…"}],
  "unhealthy": ["redis"],
  "not_configured": ["opensearch"]
}
```

- **任一後端 down → 整體 503**（而不是 200 + `healthy:false`）：監控系統預設看的是
  狀態碼，只在 body 裡說「壞了」等於沒有人會發現
- `unhealthy` 是給告警規則盯的欄位，不用叫呼叫端自己 filter `checks`
- `not_configured` 與「壞掉」是兩回事：前者要去看設定檔，後者要去看那個服務。
  少了這一欄，一個「只接了 Postgres」的部署會回 `healthy: true`，
  看起來跟六個後端全綠一模一樣——那是最危險的一種假綠燈

與 `/ready`、`/metrics` 的分工：

| 端點 | 認證 | 回答什麼 |
|---|---|---|
| `/ready` | 公開 | 這個行程現在能不能收流量（給 orchestrator） |
| `/metrics` | 公開 | Prometheus 聚合計數器 |
| `/api/v1/ops/health` | viewer+ | 整套系統哪一塊壞了（給運維的人） |
| `/api/v1/ops/metrics` | viewer+ | **這個行程**的資源用量（RAM／CPU／Disk） |
| `/api/v1/ops/connectors` | viewer+ | 哪個 connector 沒在採集，以及為什麼 |
| `/api/v1/ops/queues` | viewer+ | 哪個 consumer group 落後 |
| `/api/v1/ops/dlq` | viewer+ | 有哪些失敗的 Job（V0.1 沒有 DLQ topic） |

Redis／Redpanda 刻意**不放進 `/ready`**：API 自己不需要它們，
把它們塞進 `/ready` 會讓「Redis 掛了」變成「API 不接受流量」，
而 API 其實還能好好地回答查詢。

`GET /ops/metrics`：

```json
{"rss_bytes": 52428800, "peak_rss_bytes": 60817408, "virtual_bytes": 2147483648,
 "threads": 9, "cpu_user_seconds": 1.23, "cpu_system_seconds": 0.45,
 "clock_ticks_per_sec": 100,
 "disk_path": "/proc/self/cwd",
 "disk_bytes_total": 474449907712, "disk_bytes_available": 105944313856}
```

RAM／CPU 的資料來源是 `/proc/self/status` 與 `/proc/self/stat`，
Disk 是對 `/proc/self/cwd` 做一次 `statvfs`（`libc`，本來就在相依樹裡），
**沒有引進 `sysinfo` crate**：需要的就這幾個數字，為了它們拉一個會掃描全系統行程的
相依不划算。代價是**只在 Linux 有效**，其他平台回 501。
`clock_ticks_per_sec` 是換算假設（Linux `USER_HZ`，主流架構都是 100），
回在 API 裡讓呼叫端看得出我們是用什麼換算的。

Disk 兩欄的語意（SPEC §31 Required views 的 "CPU/RAM/Disk metrics"）：

| 欄位 | 意義 |
|---|---|
| `disk_path` | 量的是哪一條路徑。用**行程自己的工作目錄**而不是 `/`——先被吃光的是這個行程實際在寫的檔案系統（`target/`、`var/`、匯入暫存），容器裡幾乎一定不等於 `/` |
| `disk_bytes_total` | 該檔案系統總容量（`f_frsize × f_blocks`） |
| `disk_bytes_available` | **非特權行程**還能寫入的空間（`f_bavail`，不是 `f_bfree`）。ext4 預設保留 5% 給 root，用 `f_bfree` 會在一般行程早就寫不進去時還顯示「還有空間」 |

> `statvfs` 失敗時兩個數字是 **`null`，不是 `0`**（並且會留一行 warn）。
> `0` 會被讀成「磁碟滿了」——與「量不到」完全相反的結論。同 `/ops/queues` 的 `lag`。

它回答的是**這個行程**看得到的磁碟，不是整台主機的磁碟總覽；
主機層級請用 `make disk` 或作業系統自己的監控。

## `GET /ops/connectors`（connector 採集健康）

SPEC §31 的 "connector health"。viewer 以上。沒接 Postgres 回 503。

| query | 預設 | 意義 |
|---|---|---|
| `unhealthy` | `false` | `true` 只回不健康的 |
| `stale_after_secs` | `86400` | 超過這麼久沒有成功採集就算停滯。`<= 0` 回 400 |
| `limit` | `100` | 回傳筆數上限（夾在 1..=1000） |

```json
{
  "items": [{
    "id": "0199...", "source_id": "0199...", "name": "example-rss", "type": "rss",
    "enabled": true, "status": "error", "error_count": 3,
    "last_run": "2026-09-12T03:00:00Z", "last_success": "2026-09-10T03:00:00Z",
    "since_last_success_secs": 172800,
    "healthy": false,
    "reason": "連續錯誤 3 次（connector.status = `error`）。請看 osint-collector 的記錄檔，或用 GET /api/v1/connectors/{id} 查設定"
  }],
  "scanned": 12, "unhealthy_count": 1, "truncated": false, "stale_after_secs": 86400
}
```

- **`reason` 是這個視圖存在的理由**：只回 `healthy: false` 等於要運維自己猜是
  「錯誤累積」還是「根本沒在跑」，那兩件事的下一步完全不同。判定順序是
  錯誤 → 從來沒成功過 → 太久沒成功，只回最能指出下一步的那一條。
- **停用的 connector 不算不健康**（那是人刻意關掉的），但仍然會列出來：
  「為什麼沒有資料」最常見的答案就是它被關掉了。
- **從來沒跑過**（`last_run` 也是 `null`）也不算故障：新建的 connector
  在第一次排程到之前本來就長這樣。
- **這個視圖不分頁**：`unhealthy=true` 必須對整個 fleet 判斷，先取一頁再過濾會讓
  「沒有不健康的 connector」與「最新一頁裡沒有不健康的」變成同一個答案。
  改以硬上限 1000 筆掃描 + `truncated: true` 誠實標示掃不完的情況。
- `scanned`／`unhealthy_count` 是**過濾前**的數字，所以帶 `unhealthy=true`
  時仍然看得出母體多大。

## `GET /ops/queues`（consumer group lag）

SPEC §31 的 "queue summary"。viewer 以上。

**沒接上 Redpanda 時只有這條路由回 503**（`[broker].brokers` 沒設定），其他路由照常。

```json
{
  "brokers": "127.0.0.1:9092",
  "total_lag": 42, "complete": true,
  "queues": [
    {"service": "normalizer", "group": "osint-normalizer", "topic": "raw.collected",
     "lag": 42, "topic_exists": true,
     "partitions": [{"partition": 0, "committed": 58,
                     "low_watermark": 0, "high_watermark": 100, "lag": 42}],
     "error": null},
    {"service": "indexer", "group": "osint-indexer", "topic": "entity.extracted",
     "lag": null, "topic_exists": null, "partitions": [],
     "error": "…broker 回應逾時…"}
  ]
}
```

- **單一 group 查不到不會讓整個端點失敗**：其他 group 照常回，失敗的那個帶
  `error` 且 `lag: null`。一個 group 的問題不該讓運維連別的 group 都看不到。
- **`lag` 量不到時是 `null`，不是 `0`。** 「沒有落後」與「量不到」在面板上長得
  一樣的話，broker 掛掉會顯示成一切正常。`complete: false` 就是在說
  `total_lag` 不完整。
- `topic_exists: false` 與 `lag: 0` 是兩回事：前者代表 topic 還沒被建出來
  （通常是沒有人 produce 過），後者代表真的追上了。

> ⚠️ 這裡的 lag 是**給人看的**。V0.1 **沒有**任何程式讀它來自動降速——
> collector 不會依 `osint_queue_depth` 調整採集速率（見
> `docs/developer/collector-normalizer.md` 的「已知限制」）。跨服務 backpressure 是 V0.2。

## `GET /ops/dlq`（失敗紀錄）

SPEC §31 的 "basic DLQ view"。viewer 以上。沒接 Postgres 回 503。
`?limit=` 預設與上限都是 100。

```json
{
  "dlq_topic": null,
  "note": "V0.1 沒有 DLQ topic：消費失敗的事件只會留在記錄檔…",
  "failed_jobs": [{"id": "0199...", "job_type": "collect", "status": "failed", "…": "…"}],
  "failed_job_count": 3,
  "truncated": false
}
```

> ⚠️ **`dlq_topic: null` 是一句宣告，不是「目前沒有壞掉的東西」。**
> V0.1 沒有 DLQ topic：事件消費失敗時會記 error log 並**照樣提交 offset**，
> 不會被搬到另一個 topic，也不會落地成可查詢的紀錄。因此
> **「永久失敗的事件」在 V0.1 沒有任何地方查得到**——這個視圖列的是
> `failed` 狀態的 **Job**，那是 V0.1 唯一真的落地的失敗紀錄。
> （設計決策：完整的 DLQ subsystem 需要保留策略、重放路徑與 RBAC；只建 topic 而沒有這些等於建了一個靜默過期的失效安全網，比沒有更危險。）

失敗的 Job 可以用 `POST /api/v1/jobs/{id}/retry`（operator 以上）重試；
會轉成 `retrying` 而不是 `queued`（見上方 Jobs 一節），且成功與被拒都會寫稽核。

## API token 管理（`/api/v1/tokens`，admin-only）

V0.1 第一組真正需要 admin 角色的路由。

```bash
# 發行。回應裡的 token 欄位是唯一一次能看到明文的地方。
curl -s -X POST http://127.0.0.1:18080/api/v1/tokens \
  -H "Authorization: Bearer $ADMIN_JWT" -H 'Content-Type: application/json' \
  -d '{"name":"ci-indexer","role":"operator","expires_in_days":30}'

# 列出（不含明文，也不含 hash）
curl -s http://127.0.0.1:18080/api/v1/tokens -H "Authorization: Bearer $ADMIN_JWT"

# 撤銷（204）。撤銷後該 token 立刻無法認證。
curl -s -X DELETE http://127.0.0.1:18080/api/v1/tokens/$ID -H "Authorization: Bearer $ADMIN_JWT"
```

`POST` 回應：

```json
{
  "id": "0199...", "name": "ci-indexer", "role": "operator",
  "created_by": "alice", "created_at": "...", "expires_at": "...",
  "token": "osint_0199....<secret>",
  "message": "請立刻保存 token：…這個明文不會再出現第二次…"
}
```

`GET` 回應是 `{"items": [...]}`，**沒有** cursor 分頁：store 端已經硬夾在 100 筆內，
而 token 的數量級本來就遠小於這個。欄位、到期規則、撤銷語意與稽核見
`docs/developer/security.md`。

## `AppState` 的通用 store handle（Phase 6a）

`AppState` 有兩個泛用 handle，之後的資源 handler 一律從這裡拿，不要各自持有連線：

| 欄位 | 型別 | `None` 代表 |
|---|---|---|
| `store` | `SharedStore` = `Arc<dyn RelationalStore + Send + Sync>` | 沒接上 Postgres，資源類 handler 回 503 |
| `objects` | `SharedObjects` = `Arc<dyn ObjectStore + Send + Sync>` | 沒接上 MinIO |

Phase 6b 另外加了三個欄位：

| 欄位 | 型別 | 用途 |
|---|---|---|
| `backends` | `ReadyProbe` | `GET /api/v1/ops/health` 要敲的後端檢查清單 |
| `backends_missing` | `Vec<&'static str>` | **沒接上**（因此不在 `backends` 裡）的後端名稱 |
| `object_bucket` | `String` | `GET /raw/{id}?body=true` 用來把 `s3://{bucket}/{key}` 形式的 `storage_path` 還原成物件 key；沒接物件儲存時是空字串 |

`backends` 與 `ready` **刻意分開**：`/ready` 只檢查 API 自己非有不可的依賴
（給 orchestrator 決定要不要送流量），`backends` 是整套 pipeline 的後端
（含 Redis／Redpanda，API 自己不用它們）。混在一起會讓「Redis 掛了」
變成「API 不接受流量」。

`backends_missing` 不是可有可無的裝飾：空的檢查清單在 `ReadyStatus::from_checks`
底下是 `healthy: true`（空集合的「全部通過」是 true），
少了這一欄，一個只接了 Postgres 的部署會回 200 且 `healthy: true`，
看起來跟六個後端全綠一模一樣。

> `core-api` 為此多了一個 `storage-redis` 相依。API 的任何路由都不讀 Redis，
> 它只出現在 `/ops/health` 的檢查清單裡。

用 trait object 而不是 `PostgresCanonicalStore`，是因為用具體型別會讓每個 handler
都硬編在 PostgreSQL 上，違反 `CLAUDE.md` §13
（Domain Service → capability interface → concrete adapter）。
代價是拿不到 `pool()`；需要 pool 的 `PostgresAuditLog` 與 `PostgresApiTokenStore`
在 `main.rs` 連線時就從具體型別建好再放進 `AppState`，不經過這個 handle。

> ⚠️ `+ Send + Sync` 不能省。`RelationalStore` 沒有把它們寫成 supertrait，
> 少了的話 `AppState` 無法當 axum state，而編譯器報的是
> 「`FromFn<…>: Service<…>` 不滿足」——完全指不到真因。
>
> ⚠️ 同理，middleware 裡**不要在 `await` 之前持有 `&Request`**。
> axum 的 `Body` 不是 `Sync`，借用跨過 await 點會讓 future 失去 `Send`，
> 報的是同一種看不懂的錯誤。要先同步把需要的欄位抄出來再進 async。

`ImportState.store` 與 `AppState.store` 是同一個 `Arc`（`main.rs` 只建一次）。
`jobs`／`import`／`search` 維持獨立欄位是因為它們有額外組裝需求
（JobService、EvidenceSink、index 名稱）。

## 錯誤格式

```json
{"error": "unauthorized", "message": "…下一步建議…"}
```

## Cursor pagination

SPEC §19：「所有 list API 以 cursor pagination 為主要策略」。全部 list endpoint
都是同一套：

Query：`?cursor=<uuid>&limit=20`。`limit` 預設 20、上限 100（超過會被夾回去，不報錯）。
回應：`{"items": [...], "next_cursor": "<uuid>|null"}`。
`next_cursor` 是本頁最後一筆的 id，語意是**嚴格小於**；`null` 代表沒有下一頁。

SPEC §19 沒有定義 cursor 編碼。這裡採用 **UUID 標準字串**（也可接受 base64url(UUID)）。

> ⚠️ **排序鍵是 `id`，而 id 不全都有時間序。** Source／Collection／Document／
> RawEvidence／Job 是 UUID v7（前 48 bit 是毫秒時間戳，實務上等同「最新的在前」），
> 但 **Entity 與 Relationship 是 UUID v5**（由自然鍵推導，為了冪等），
> `/import` 建立的 Connector 也是 v5。對這些資源，「id 遞減」**不等於**「最新的在前」，
> 要按時間看請自己比對 `last_seen`。
>
> 這不只是文件上的細節：`tests/resources_api_e2e.rs` 第一版就是因為假設
> 「剛建立的 relationship 會在前幾頁」而紅的——v5 的高位是亂數，
> 絕大多數 v5 id 會排在 v7 之上。

## 本機啟動

```bash
export JWT_SECRET="$(python3 -c 'import secrets; print(secrets.token_urlsafe(48))')"
# 或把 JWT_SECRET 寫進 .env（gitignore）
cargo run -p core-api --bin osint-api
curl -s http://127.0.0.1:18080/health
```

本機常見情境：8080 可能被其他本機服務占用（例如另一套安全/情資平台），osint-api 預設綁 `127.0.0.1:18080`（`config/default.toml` `[http].bind`）。`.env` 的 `OSINT_CONFIG_FILE=` 空字串會被忽略。

JWT 可用測試程式簽發，或暫時在整合測試裡用 `JwtService`。secret 至少 32 bytes。

Rate limit：全域 token bucket（`rate_limit_per_second`，預設 20 req/s）。Axum 的 `Router` 需要 Clone 的 Service，所以不用 `tower::limit::RateLimitLayer`。不是 per-IP。
