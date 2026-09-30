# 搜尋

這一頁說明如何用 `POST /api/v1/search` 在已匯入的文件裡找資料，以及語意搜尋與混合搜尋的用法。

---

## 基本搜尋

```bash
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query":"CVE-2026-31337"}'
```

```json
{
  "total": 2,
  "hits": [
    {
      "document_id": "01a0f0d3-405a-703d-ba36-25f2f6959184",
      "score": 7.55,
      "title": "威脅情報：針對 Acme Router 的攻擊活動持續擴大",
      "snippet": "研究團隊觀察到攻擊者持續利用 <em>CVE</em>-<em>2026</em>-<em>31337</em>…",
      "object_type": "advisory",
      "source_id": "01a0f0d3-3f16-768c-a919-c4c49cdd5b94",
      "raw_evidence_id": "01a0f0d3-4000-74b2-b58b-dce8c5bcf601",
      "published_at": "2026-09-29T00:00:00Z",
      "entities": [
        {"entity_type": "vulnerability", "name": "CVE-2026-31337"},
        {"entity_type": "ip", "name": "203.0.113.45"}
      ]
    }
  ],
  "next_cursor": null
}
```

!!! warning "欄位名稱是 `query`，不是 `q`"
    寫成 `{"q": "..."}` 會得到 `422` 錯誤，說明哪些欄位名稱是合法的。

搜尋是唯讀操作，`viewer` 以上的角色即可使用。

---

## 查詢語法

### 關鍵字

```text
ransomware
```

找含這個詞的文件。

### 多個詞：預設都要有

```text
lockbit ransomware
```

等同 `lockbit AND ransomware`。多打一個字只會讓結果變少，不會變多。

### 片語（詞序必須一致）

```text
"ransomware gang"
```

只找「ransomware gang」連在一起的文件。`"gang ransomware"` 找不到同一批。

### 布林運算

```text
lockbit AND ransomware
lockbit OR blackcat
ransomware NOT decryptor
(lockbit OR blackcat) AND ransomware
```

!!! warning "`AND` / `OR` / `NOT` 必須全大寫"
    小寫的 `and` 會被當成一般的詞——`crowdstrike and falcon` 是在找這三個詞，
    不是布林運算。

沒有括號時 `AND` 比 `OR` 先算：`a AND b OR c` 等同 `(a AND b) OR c`。

### `*`、`:` 等符號沒有特殊意義

`*`、`?`、`~`、`欄位名:值`、正規表示式——**都只是普通文字**。
搜 `title:*` 是在找字面上的「title:*」這串字，不是「找所有有標題的文件」。

這是刻意的設計：使用者字串**不會**被交給 OpenSearch 的查詢語言直接執行——
所有輸入都先解析成內部結構（`Term` / `Phrase` / 布林樹），再轉成查詢，
所以任意使用者輸入不可能變成資料庫查詢的一部分。
要縮小範圍請用下面的篩選條件。

---

## 篩選條件

所有條件都可以單獨用，也可以和查詢字串一起用：

| 條件 | API 欄位 | 說明 |
|---|---|---|
| 來源 | `"source_id": "..."` | 只看這個 Source 的文件 |
| 連接器 | `"connector_id": "..."` | 只看這個 Connector 的文件 |
| 實體 | `"entity": {"type": "...", "name": "..."}` | 找提到某個實體的文件 |
| 起始時間 | `"date_from": "2026-01-01T00:00:00Z"` | RFC3339 格式 |
| 結束時間 | `"date_to": "2026-12-31T23:59:59Z"` | RFC3339 格式 |
| 時間欄位 | `"date_field": "effective"` | 見下方說明 |
| 語言 | `"language": "zh"` | 語言代碼，大小寫不敏感 |
| 物件型別 | `"object_type": "advisory"` | `advisory` / `article` / `report` 等 |
| 筆數 | `"limit": 20` | 1–100，預設 20 |

### 實體過濾：找提到某個指標的文件

```bash
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"entity":{"type":"vulnerability","name":"CVE-2026-31337"}}'
```

```json
{"total": 2, "hits": [...]}
```

- `type` 可省略（找所有型別裡叫這個名字的）
- 名稱大小寫不敏感：`cve-2026-31337` 與 `CVE-2026-31337` 結果相同
- 型別名稱：`vulnerability` / `ip` / `domain` / `hostname` / `url` / `email` / `hash` /
  `person` / `organization` / `account` / `file` / `malware` / `campaign` / `location` / `event` / `product`

### 時間欄位的選擇

`date_field` 決定 `date_from` / `date_to` 比對哪一個時間：

| 值 | 說明 |
|---|---|
| `effective`（預設） | 有發布時間就用發布時間，沒有就用「本系統第一次看到它的時間」 |
| `published` | 只看發布時間。**沒有發布時間的文件不會出現在結果裡** |
| `observed` | 只看「本系統第一次看到它的時間」 |

預設是 `effective`，因為許多來源（手動匯入、靜態網頁）沒有發布時間，
用 `published` 當預設會讓那些文件在任何日期區間查詢裡消失。

---

## 中文搜尋

中文用**兩字一組**的方式比對（沒有裝中文斷詞套件）：

```bash
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query":"韌體"}'
```

實務上的影響：

- 查「勒索軟體」會正確找到含這個詞的文件，不會把含「軟」或「體」的文件都撈出來
- 偶爾有跨詞邊界的誤中（「防毒軟體。攻擊者…」這種）。要精確可用片語：`"勒索軟體攻擊"`

---

## 分頁

搜尋結果用游標（cursor）翻頁，不是頁碼：

```bash
# 第一頁
RESP=$(curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query":"Acme","limit":1}')

CURSOR=$(echo $RESP | python3 -c "import sys,json; print(json.load(sys.stdin)['next_cursor'])")

# 第二頁：把 next_cursor 放進 cursor 欄位
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query":"Acme","limit":1,"cursor":"'"$CURSOR"'"}'
```

`next_cursor` 是 `null` 就代表沒有下一頁。不要自己組 cursor 的值——用回應裡的。

每筆搜尋結果都帶有 `raw_evidence_id`，可以用 `GET /api/v1/raw/{id}` 追回原始匯入的檔案、來源與時間。

---

## 語意搜尋

語意搜尋（`POST /api/v1/search/semantic`）比的是**語意相似性**，不是關鍵字命中——
把一句話丟進去，找向量空間裡最接近的文件：

```bash
curl -s -X POST $API/search/semantic \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query":"router firmware remote code execution","language":"en","limit":5}'
```

```json
{
  "hits": [
    {
      "document_id": "01a0f0d3-...",
      "score": 0.34,
      "object_type": "article",
      "matched_section": "body",
      "title": "...",
      "source_id": "...",
      "published_at": null
    }
  ],
  "model": "huggingface/sentence-transformers/all-MiniLM-L6-v2",
  "model_version": "89b6737c..."
}
```

幾件重要的事：

- **`language` 欄位必須填**。省略時走多語模型，只能搜到有多語向量的文件；英文文件走英文模型，省略 `language` 就找不到它們
- **`score` 是 k-NN 分數**，不是全文搜尋的 BM25 分數，尺度不同，**不能和全文搜尋的分數比大小**
- 這條路由需要 OpenSearch 的語意搜尋模型已部署。沒有部署的話只有這條路由回 `503`，全文搜尋不受影響

---

## 混合搜尋

混合搜尋（`POST /api/v1/search/hybrid`）同時跑全文搜尋（BM25）與語意搜尋（k-NN），
再用 **RRF（Reciprocal Rank Fusion）** 融合排名：

```bash
curl -s -X POST $API/search/hybrid \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query":"CVE-2026-31337 router","language":"en","limit":5}'
```

```json
{
  "hits": [
    {
      "document_id": "...",
      "title": "...",
      "fused_score": 0.0164,
      "bm25_rank": 1,
      "vector_rank": null
    }
  ]
}
```

- `bm25_rank` 與 `vector_rank` 從 1 開始，沒有出現在該訊號裡就是 `null`（不是給一個很差的排名）
- `fused_score` 是兩個訊號的 RRF 融合結果，**不能和 `POST /search` 的 `score` 比大小**
- 兩條訊號任一沒接上就整條回 `503`，錯誤訊息會說明是哪一個

---

## 相似文件

用 `GET /api/v1/objects/{id}/similar` 找與某份文件向量相似的其他文件，
不需要輸入查詢字串：

```bash
curl -s "$API/objects/$DOC_ID/similar?limit=10" \
  -H "Authorization: Bearer $TOKEN"
```

還沒被語意索引處理的文件回 `200`、`hits: []`，不是錯誤。

---

## 空請求也合法

全部欄位都可省略。`{}` 是合法的請求，代表「列出全部文件（預設不含重複的），依時間由新到舊」：

```bash
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{}'
```

---

## 查不到東西的時候

依序確認：

1. **搜尋索引有沒有建立。** 搜尋查的是 OpenSearch，不是資料庫本身。匯入後 `osint-indexer` 還沒跑完之前，新文件不會出現在搜尋結果裡（資料本身還在，沒有遺失）。等幾秒再試
2. **服務是否正常。** `GET /health` 應回 `{"status":"ok"}`
3. **條件是否太窄。** 先把所有篩選條件拿掉只留關鍵字，再一個一個加回去。常見原因是 `date_field: "published"` 把沒有發布時間的文件全部排除了
4. **是否被當成重複文件。** 搜尋預設不顯示重複文件（`include_duplicates` 預設 `false`）

---

## 常見錯誤

| 錯誤碼 | 意思 |
|---|---|
| `422` | 請求裡有未知欄位（例如 `q`）或型別錯誤 |
| `400` | 查詢語法錯（例如引號沒有成對）、日期區間顛倒、`limit` 超過 100 但仍被夾回 100 |
| `503` | OpenSearch 沒接上（全文搜尋）；或 ml-commons 模型沒部署（語意搜尋 / 混合搜尋） |
| `504` | OpenSearch 查詢逾時 |
