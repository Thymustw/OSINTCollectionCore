# 發現新關聯

Discovery（發現新關聯）讓你從一個已知實體出發，讓系統在現有資料裡找出可能相關的漏洞、程式庫、軟體、產品與情報來源（Source），做為後續調查的線索。

!!! note "Discovery 只查「已收進系統的資料」"
    Discovery 不會主動上網爬取新內容。它只在現有的 Neo4j 圖與 PostgreSQL 資料庫裡尋找關聯。如果相關資料還沒匯入，Discovery 不會找到它。

---

## 基本概念

一次 Discovery 的流程：

```
已知實體
  → POST /entities/{id}/discover（或 POST /discovery/run）
  → 背景 Job 執行
  → 寫入 Candidate（候選）
  → 人工審核：核准或拒絕
```

**候選（Candidate）一律先以 Pending 狀態等待審核**，不會直接成為確認的結論。每個候選都附有「為什麼認為它相關」的證據，由人工決定核准或拒絕。這是為了避免自動化把錯誤的關聯當成事實擴散。

### Collection（調查集合）

每次 Discovery 必須掛在一個 Collection 下。Collection 定義這次調查的範圍，也控制 Discovery 的使用配額。

---

## 準備工作

```bash
export TOKEN=$(python3 scripts/dev-token.py)
export API=http://127.0.0.1:18080/api/v1
```

### 建立 Collection

```bash
export COLLECTION_ID=$(curl -s -X POST $API/collections \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"Acme Router 攻擊事件調查"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo $COLLECTION_ID
```

---

## 觸發 Discovery

### 方法一：從實體出發

```
POST /api/v1/entities/{id}/discover
```

以 `CVE-2026-31337` 的實體 id 為例：

```bash
curl -s -X POST "$API/entities/$CVE_ID/discover" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"collection_id\":\"$COLLECTION_ID\"}"
```

```json
{
  "id": "01a0f104-6621-...",
  "status": "queued",
  ...
}
```

回傳 HTTP **202**，`status` 是 `queued`。真正執行的是背景服務。

Body 欄位：

| 欄位 | 必填 | 說明 |
|---|---|---|
| `collection_id` | 是 | 掛在哪個 Collection 下 |
| `depth` | 否 | Discovery 展開的起始深度，預設 `0` |

### 方法二：直接派工

```
POST /api/v1/discovery/run
```

功能與方法一相同，差別只是 `entity_id` 寫進 body 而不是路徑：

```bash
curl -s -X POST $API/discovery/run \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"collection_id\":\"$COLLECTION_ID\",\"entity_id\":\"$CVE_ID\"}"
```

---

## Discovery 做了什麼

每次 Discovery run 包含兩個步驟，使用同一個候選數量上限計數器，共消耗 1 次每日請求配額：

### 圖擴張（Graph Expansion）

對來源實體做 **1-hop** 的 Neo4j 鄰居查詢。只回傳以下四種型別的鄰居作為候選：

| 型別 | 說明 |
|---|---|
| `vulnerability` | 漏洞 |
| `repository` | 程式庫 / 儲存庫 |
| `software` | 軟體 |
| `product` | 產品 |

這個型別清單寫死在系統內部，目前無法透過 API 調整。

### 來源擴張（Source Expansion）

找出「這個實體出現在哪些情報來源（Source）的原始證據裡」。情報來源不在 Neo4j 圖投影裡，系統透過以下三跳關聯式查詢來找：

```
entity_extractions
  → provenance（derived_from）
  → raw_evidence
  → source
```

找到的情報來源也會成為候選（`candidate_type: source`）。

!!! note "只看 1 層鄰居"
    圖擴張只做 1-hop 查詢，不往更深的鄰居展開。展開深度受 Collection Budget 的 `max_depth` 限制控制。

---

## 預算（Collection Budget）

每個 Collection 都有配額設定，防止 Discovery 無限制展開：

| 欄位 | 說明 | 保守預設值 |
|---|---|---|
| `max_candidates_per_run` | 單次 run 最多產生幾筆候選 | 200 |
| `max_requests_per_run` | 單次 run 最多發出幾次外部請求 | 500 |
| `max_ai_calls_per_run` | 單次 run 最多呼叫幾次 AI | 50 |
| `max_depth` | Discovery 展開的最大深度 | 3 |
| `daily_request_budget` | 這個 Collection 每日最多幾次請求（跨多次 run 累計） | 5,000 |
| `daily_ai_budget` | 每日最多幾次 AI 呼叫 | 500 |

沒有明確設定時，系統退回上面的保守預設值——沒有設定不代表沒有限制。

當日 `daily_request_budget` 已用完，或 `depth + 1 > max_depth` 時，Discovery 會直接拒絕而不是部分執行。

---

## 管理 Seed（種子）

Seed（種子）是你明確告訴系統「從這裡開始找」的起點。

### 建立 Seed

```
POST /api/v1/seeds
```

```bash
curl -s -X POST $API/seeds \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "seed_type": "keyword",
    "value": "Acme Router",
    "origin": "manual",
    "collection_id": "'"$COLLECTION_ID"'"
  }'
```

Body 必填欄位：

| 欄位 | 說明 |
|---|---|
| `seed_type` | 種子型別，例如 `keyword`、`entity`、`url` |
| `value` | 種子內容（最多 2048 字元） |
| `origin` | 來源宣告，例如 `manual`、`connector`、`discovery`、`ai` |

!!! warning "Seed 目前不會自動觸發 Discovery"
    建立 Seed 只是記錄「這是一個調查起點」。要讓系統實際執行 Discovery，仍然要呼叫 `POST /entities/{id}/discover` 或 `POST /discovery/run`。Seed 要先被解析成 Entity 才能成為 Discovery 的起點。

### 列出 Seed

```
GET /api/v1/seeds
```

```bash
curl -s "$API/seeds?collection_id=$COLLECTION_ID" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; items=json.load(sys.stdin)['items']; [print(s['value'], s['status']) for s in items]"
```

---

## 查看候選

### 列出候選

```
GET /api/v1/candidates
```

```bash
curl -s "$API/candidates" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; [print(c['value'], '|', c['status'], '|', c['discovery_method']) for c in json.load(sys.stdin)['items'][:5]]"
```

列出特定 Collection 底下的候選：

```bash
curl -s "$API/collections/$COLLECTION_ID/discovery" \
  -H "Authorization: Bearer $TOKEN"
```

從實際執行結果可以看到：

```text
快速上手範例 | pending | source_expansion
```

### 查看單一候選與證據

```
GET /api/v1/candidates/{id}
```

```bash
curl -s "$API/candidates/$CANDIDATE_ID" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('value:', d['value'])
print('type:', d['candidate_type'])
print('method:', d['discovery_method'])
print('status:', d['status'])
for ev in d['evidence']:
    print('evidence:', ev['reason'][:80])"
```

```text
value: 快速上手範例
type: source
method: source_expansion
status: pending
evidence: Entity `CVE-2026-31337` 透過 raw evidence 關聯到 Source `快速上手範例`，經 entity_extractions
```

`evidence` 欄位直接包含「為什麼認為它相關」的說明，不需要另外呼叫其他端點。

---

## 核准與拒絕候選

### 核准

```
POST /api/v1/candidates/{id}/approve
```

```bash
curl -s -X POST "$API/candidates/$CANDIDATE_ID/approve" \
  -H "Authorization: Bearer $TOKEN"
```

回傳更新後的 Candidate，`status` 變為 `approved`。

!!! warning "核准 URL 型候選前會驗證格式"
    `candidate_type = url` 的候選，核准前系統會先驗證 `value` 是合法的絕對 URL（有 host）。格式不合法時回傳 400，請改用拒絕。

### 拒絕

```
POST /api/v1/candidates/{id}/reject
```

```bash
curl -s -X POST "$API/candidates/$CANDIDATE_ID/reject" \
  -H "Authorization: Bearer $TOKEN"
```

回傳更新後的 Candidate，`status` 變為 `rejected`。

---

## 查看 AI 執行紀錄

```
GET /api/v1/ai/runs
GET /api/v1/ai/runs/{id}
```

Discovery 過程中的每次 AI 呼叫都會留下紀錄：

```bash
curl -s "$API/ai/runs" -H "Authorization: Bearer $TOKEN" | python3 -c "
import sys,json; items=json.load(sys.stdin)['items']
print('AI runs:', len(items))
for r in items[:3]:
    print(' -', r.get('task_type','?'), r.get('status','?'))"
```

---

## 目前的限制

1. **不查重**：同一個實體對同一個 Collection 重複執行 Discovery，會產生重複的候選。
2. **觸發點是實體，不是 Seed**：要執行 Discovery，必須先有 Entity id。Seed 需要先被解析成 Entity 才能當起點。
3. **只找已收進系統的資料**：不主動上網抓取，圖擴張只看 1-hop 鄰居。
4. **目標類型不可調整**：圖擴張只回傳 `vulnerability`、`repository`、`software`、`product` 四種型別，無法透過 API 改變。
