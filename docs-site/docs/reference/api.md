# API 端點總覽

這一頁列出所有 `/api/v1/*` 路由，直接依照程式裡實際註冊的路由整理。

---

## 通用約定

### Base URL

```
http://127.0.0.1:18080
```

### 認證

所有 `/api/v1/*` 路由（除非另外標註）都需要 HTTP Bearer 憑證：

```
Authorization: Bearer <token>
```

取得開發用憑證：`export TOKEN=$(python3 scripts/dev-token.py)`（有效期 1 小時）

### 角色說明

| 角色 | 說明 |
|---|---|
| `viewer` | 只能讀取；所有認證使用者的最低角色 |
| `operator` | 可讀可寫；能匯入資料、觸發 Discovery |
| `admin` | 最高權限；能管理 API Token |

### 游標分頁

支援分頁的列表端點接受這些參數：

| 參數 | 說明 |
|---|---|
| `limit` | 每頁筆數（有上限） |
| `cursor` | 下一頁的游標（從上一頁回應的 `next_cursor` 取得） |

回應包含 `next_cursor`；若為 `null` 代表已到最後一頁。

### ETag 與樂觀鎖定（`PATCH` 端點）

`PATCH` 請求需要帶 `If-Match` header，值為 `GET` 時回應中的 `ETag`。

- 缺少 `If-Match`：回 `428 Precondition Required`
- ETag 過期（資源已被其他人修改）：回 `412 Precondition Failed`

### 錯誤格式

所有錯誤回應都是 JSON：

```json
{"error":"<錯誤代碼>","message":"<說明>"}
```

---

## 不需要認證的端點

| 方法 | 路徑 | 說明 |
|---|---|---|
| `GET` | `/health` | 存活探針，固定回 `{"status":"ok"}` |
| `GET` | `/ready` | 就緒探針，檢查 PostgreSQL 等相依服務 |
| `GET` | `/metrics` | Prometheus 格式指標（計數器、佇列深度、延遲等） |

---

## 認證與 Token 管理（admin）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/whoami` | viewer | 確認目前憑證的角色與使用者 |
| `POST` | `/api/v1/tokens` | admin | 簽發新的長期 API Token |
| `GET` | `/api/v1/tokens` | admin | 列出所有已簽發的 Token |
| `DELETE` | `/api/v1/tokens/{id}` | admin | 撤銷指定 Token |

---

## 來源（Source）與連接器（Connector）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/sources` | viewer | 列出所有來源 |
| `GET` | `/api/v1/sources/{id}` | viewer | 取得單一來源 |
| `POST` | `/api/v1/sources` | operator | 建立來源 |
| `PATCH` | `/api/v1/sources/{id}` | operator | 更新來源（需要 `If-Match`） |
| `GET` | `/api/v1/connectors` | viewer | 列出所有連接器 |
| `GET` | `/api/v1/connectors/{id}` | viewer | 取得單一連接器 |
| `POST` | `/api/v1/connectors` | operator | 建立連接器 |
| `PATCH` | `/api/v1/connectors/{id}` | operator | 更新連接器（需要 `If-Match`） |

---

## 匯入

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `POST` | `/api/v1/import` | operator | 上傳 JSON/CSV 檔案匯入（multipart form） |
| `POST` | `/api/v1/import/stix` | operator | 匯入 STIX 2.1 Bundle（JSON body，上限 50 MB） |

---

## 文件（Document）與原始證據（Raw Evidence）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/objects` | viewer | 列出文件（支援 `object_type`、`source_id` 等篩選） |
| `GET` | `/api/v1/objects/{id}` | viewer | 取得單一文件 |
| `GET` | `/api/v1/objects/{id}/similar` | viewer | 列出與指定文件相似的文件 |
| `POST` | `/api/v1/objects` | operator | 固定回 **501 Not Implemented**（請改用 `/api/v1/import`） |
| `GET` | `/api/v1/raw/{id}` | viewer | 取得原始證據（SHA-256、來源 URL、收集時間、`collection_ids`）。清單上限 100，超過時 `collection_ids_truncated` 為 true |

---

## 實體（Entity）與合併

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/entities` | viewer | 列出實體（支援 `entity_type` 等篩選） |
| `GET` | `/api/v1/entities/{id}` | viewer | 取得單一實體（含近期出處摘要） |
| `GET` | `/api/v1/entities/{id}/resolution-candidates` | viewer | 列出可能與此實體相同的候選 |
| `GET` | `/api/v1/entities/{id}/merge-history` | viewer | 列出此實體的合併歷史 |
| `POST` | `/api/v1/entities/{id}/resolve` | operator | 核准或拒絕一個合併候選 |
| `POST` | `/api/v1/entities/{id}/resolve/graph-context` | operator | 取得加上圖上下文的合併候選資訊 |
| `POST` | `/api/v1/entities/merge` | operator | 直接合併兩個實體 |
| `POST` | `/api/v1/merge-history/{id}/undo` | operator | 撤銷合併 |

---

## 關聯（Relationship）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/relationships` | viewer | 列出關聯（支援 `from_id`、`to_id`、`relationship_type` 篩選） |
| `GET` | `/api/v1/relationships/{id}` | viewer | 取得單一關聯 |

---

## 搜尋

搜尋端點雖然用 `POST`，但它是唯讀操作（viewer 以上），查詢條件以 JSON body 傳入。

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `POST` | `/api/v1/search` | viewer | 全文搜尋（BM25） |
| `POST` | `/api/v1/search/semantic` | viewer | 語意搜尋（需要語意搜尋模型已部署） |
| `POST` | `/api/v1/search/hybrid` | viewer | 混合搜尋（BM25 + 語意，RRF 融合） |

!!! warning "搜尋請求的欄位名是 `query`，不是 `q`"
    傳 `{"q":"..."}` 會得到 422 錯誤。正確格式：`{"query":"CVE-2026-31337"}`

---

## 圖查詢

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `POST` | `/api/v1/graph/query` | viewer | 結構化圖查詢（GraphQuery body） |
| `GET` | `/api/v1/graph/entities/{id}/neighbors` | viewer | 取得指定實體的鄰居節點 |
| `GET` | `/api/v1/graph/entities/{id}/relationships` | viewer | 取得指定實體的所有圖上關聯 |
| `GET` | `/api/v1/graph/path` | viewer | 查詢兩個實體之間的最短路徑 |
| `POST` | `/api/v1/graph/rebuild` | operator | 觸發關聯圖從 PostgreSQL 重建 |

---

## 時間軸

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/entities/{id}/timeline` | viewer | 取得實體的事件時間軸 |

---

## Discovery（發現新關聯）

### 調查集合（Collection）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/collections` | viewer | 列出所有調查集合 |
| `GET` | `/api/v1/collections/{id}` | viewer | 取得單一調查集合（含來源／連接器／文件 id 清單） |
| `POST` | `/api/v1/collections` | operator | 建立調查集合（可同時掛上 `source_ids`／`connector_ids`，一次各最多 100 筆）。一個來源／連接器最多屬於 50 個集合，超過回 422 且不建立任何東西 |
| `POST` | `/api/v1/collections/{id}/sources` | operator | 把來源掛上這個集合。**不回填**已經落地的舊資料，只影響之後收進來的。一個來源最多屬於 50 個集合，超過回 422（收集時一次只讀得完 100 個集合，寫入端把來源與連接器各卡在 50）；已掛上的重送仍成功 |
| `DELETE` | `/api/v1/collections/{id}/sources/{source_id}` | operator | 拿掉來源。集合或來源不存在 → 404；本來就沒掛上 → 204（冪等） |
| `POST` | `/api/v1/collections/{id}/connectors` | operator | 把連接器掛上這個集合。同樣不回填舊資料。一個連接器最多屬於 50 個集合，超過回 422；已掛上的重送仍成功 |
| `DELETE` | `/api/v1/collections/{id}/connectors/{connector_id}` | operator | 拿掉連接器。語意同上 |
| `GET` | `/api/v1/collections/{id}/budget` | viewer | 取得 Discovery 配額。沒設過時回保守預設，並標 `is_default: true` |
| `PUT` | `/api/v1/collections/{id}/budget` | operator | 覆寫 Discovery 配額。之後 `is_default` 為 `false` |
| `GET` | `/api/v1/collections/{id}/discovery` | viewer | 列出這個調查集合的候選。集合不存在 → 404（不是 200 空頁） |

### 種子（Seed）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/seeds` | viewer | 列出所有種子 |
| `POST` | `/api/v1/seeds` | operator | 建立種子（Discovery 的起點） |

### Discovery 執行

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `POST` | `/api/v1/discovery/run` | operator | 觸發一次 Discovery 執行 |
| `POST` | `/api/v1/entities/{id}/discover` | operator | 從指定實體出發觸發 Discovery |

### 候選（Candidate）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/candidates` | viewer | 列出所有候選 |
| `GET` | `/api/v1/candidates/{id}` | viewer | 取得單一候選 |
| `POST` | `/api/v1/candidates/{id}/approve` | operator | 核准候選 |
| `POST` | `/api/v1/candidates/{id}/reject` | operator | 拒絕候選 |

### AI Run

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/ai/runs` | viewer | 列出 AI 輔助評分的執行記錄 |
| `GET` | `/api/v1/ai/runs/{id}` | viewer | 取得單一 AI Run 詳細資料 |

---

## STIX 匯出

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `POST` | `/api/v1/export/stix` | operator | 匯出 STIX 2.1 Bundle |

---

## Jobs（非同步作業）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/jobs` | viewer | 列出所有 Job |
| `GET` | `/api/v1/jobs/{id}` | viewer | 取得單一 Job |
| `GET` | `/api/v1/jobs/{id}/result` | viewer | 取得 STIX 匯出 Job 的結果 |
| `POST` | `/api/v1/jobs` | operator | 建立 Job |
| `POST` | `/api/v1/jobs/{id}/transition` | operator | 手動轉換 Job 狀態 |
| `POST` | `/api/v1/jobs/{id}/dispatch` | operator | 派送 Job |
| `POST` | `/api/v1/jobs/{id}/retry` | operator | 重試失敗的 Job |

---

## 事件（Event）

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/events` | viewer | 列出系統事件記錄 |
| `GET` | `/api/v1/events/{id}` | viewer | 取得單一事件 |

---

## 維運（Ops）

需要 viewer 以上，但 replay 需要 operator：

| 方法 | 路徑 | 角色 | 說明 |
|---|---|---|---|
| `GET` | `/api/v1/ops/health` | viewer | 各後端（PostgreSQL、OpenSearch、Neo4j 等）連線狀態 |
| `GET` | `/api/v1/ops/metrics` | viewer | 行程資源用量 |
| `GET` | `/api/v1/ops/connectors` | viewer | Connector 採集狀態 |
| `GET` | `/api/v1/ops/queues` | viewer | 各消費者的 Kafka lag |
| `GET` | `/api/v1/ops/dlq` | viewer | 失敗 Job 列表（目前不設獨立 DLQ topic） |
| `GET` | `/api/v1/ops/failed-events` | viewer | 處理失敗的事件記錄 |
| `POST` | `/api/v1/ops/failed-events/{id}/replay` | operator | 重送指定的失敗事件 |
| `GET` | `/api/v1/ops/graph` | viewer | 圖投影 lag 與最近 rebuild 狀態 |
| `GET` | `/api/v1/ops/discovery` | viewer | Discovery AI 並發設定與 Candidate 各狀態統計 |

---

## 路由數量統計

共 **74 個** HTTP 方法+路徑組合，本頁列出全部，含 3 個公開探針端點（`/health`、`/ready`、`/metrics`）。
