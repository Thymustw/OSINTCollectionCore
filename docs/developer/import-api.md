# Import API — Manual upload / JSON import / CSV import（V0.1 Phase 3d）

SPEC §3 的最後三種來源。前四種（RSS／Atom／Static Web／REST API）是 **pull**：collector 依
cron 主動去抓。這三種是 **push**：資料被推進來，所以

- 沒有 cron／`schedule`
- 沒有 `GuardedFetcher`、沒有 SSRF 檢查（不對外連線）
- 不經過 collector

進入點是 `POST /api/v1/import`。

## 為什麼不是 `POST /objects`

SPEC §19 列了 `POST /objects`，但那是另一件事：

| | `POST /objects`（尚未實作） | `POST /api/v1/import` |
|---|---|---|
| 收什麼 | 已經整理好的 canonical Document | 原始位元組（檔案） |
| 產生什麼 | Document | RawEvidence（immutable，body 在 MinIO） |
| 之後 | 直接進 canonical store | 發 `raw.collected` → normalizer 拆 Document |
| provenance | 呼叫者即作者，無 RawEvidence 可回溯 | 有 RawEvidence，可回到原始檔案 |

把檔案上傳做進 `POST /objects` 會讓「這份 Document 的原始資料是什麼」永久遺失。
所以是兩個 endpoint，不是同一個。

**API handler 不做正規化。** 它只負責收成 RawEvidence 並發事件，拆 Document 一律由
normalizer 做——與 pull 路徑同一條線。

## 請求格式

`multipart/form-data`，兩個欄位：

| 欄位 | 內容 |
|---|---|
| `request` | JSON 文字（上限 64 KiB）。描述來源、種類、欄位對映 |
| `file` | 檔案內容。串流讀取，累積超過 `[import].max_upload_bytes` 立刻 413 |

選 multipart 而不是 JSON body：JSON 只能塞 base64，體積膨脹 33%，而且**必須整包讀完才能
解碼**，沒辦法在超過上限的當下中止。

`request` 欄位（`deny_unknown_fields`，鍵名打錯會 400 而不是被忽略）：

| 鍵 | 必填 | 說明 |
|---|---|---|
| `source_id` | 是 | UUID。`source_type` 必須與 `kind` 相符 |
| `kind` | 是 | `manual`／`json`／`csv` |
| `connector_id` | 否 | 指定既有 connector；未填則自動配置 |
| `title`／`description` | 否 | 寫進 `metadata.upload`（給人看的說明） |
| `source_url` | 否 | 未填時合成 `import://{kind}/{filename}` |
| `external_id` | 否 | 寫進 RawEvidence `external_id` |
| `content_type` | 否 | **只有 `kind=manual` 會用**。未填則依內容 sniff |
| `object_type` | 否 | 產出的 Document 型別，預設 `report` |
| `mapping` | 否 | 欄位對映，見下 |

範例：

```bash
curl -sS -X POST http://127.0.0.1:18080/api/v1/import \
  -H "Authorization: Bearer $JWT" \
  -F 'request={"source_id":"<uuid>","kind":"csv","mapping":{"title":"headline"}};type=application/json' \
  -F 'file=@advisories.csv'
```

回應 201：

```json
{
  "raw_evidence_id": "…", "source_id": "…", "connector_id": "…",
  "kind": "csv", "content_type": "text/csv", "bytes": 812,
  "sha256": "…", "record_count": 2, "skipped_empty": 0,
  "published": true, "message": "已收下並發出 raw.collected，normalizer 會接手正規化"
}
```

`published: false` 代表證據已落地但 `raw.collected` 沒發出去（Redpanda 掛了）：
**這筆不會被自動正規化**，需要用 `raw_evidence_id` 手動重送。刻意不回 5xx——
證據已經寫進去了，讓客戶端重試只會產生重複的 RawEvidence。

## 掛在哪個 Source／Connector 底下

- **Source 必填，而且 `source_type` 必須對應 `kind`**（`manual_upload`／`json_import`／
  `csv_import`）。不相符回 400。理由：RSS 的 Source 底下混進 CSV 匯入之後，
  「這個來源的資料怎麼來的」會得到互相矛盾的答案。要匯入就開匯入專用的 Source。
- **Connector 自動配置**：id 是 `UUIDv5(固定命名空間, "{source_id}:{connector_type}")`，
  所以同一個 Source 的匯入永遠對到同一列 connector，不會每上傳一次長出一列。
- 自動配置的 connector 一律 `enabled = false`、`schedule = null`。collector 只跑
  enabled 的 connector，這樣它永遠不會試圖去「抓」一個根本沒有 URL 的匯入來源。

## 欄位對映

`mapping` 把邏輯欄位對到來源鍵。未指定的欄位用慣用名稱（大小寫不敏感，依序嘗試）：

| 邏輯欄位 | 預設嘗試的鍵 |
|---|---|
| `title` | title, headline, name, subject |
| `body` | body, content, text, description_full |
| `summary` | summary, description, abstract, excerpt |
| `url` | url, link, source_url, permalink |
| `published_at` | published_at, published, date, pubdate, timestamp |
| `external_id` | external_id, id, guid, uuid |
| `language` | language, lang |
| `author` | author, creator, byline |

- **CSV**：值是 header 名稱。指定的 header 不存在 → 422 並列出可用 header
  （不靜默忽略，否則會得到「匯入成功但每筆都空白」）。
- **JSON**：值是頂層鍵；以 `/` 開頭時當 JSON Pointer，例如 `"body": "/attributes/full_text"`。
- 只取純量（字串／數字／布林）。物件或陣列當作沒填——把 `{"a":1}` 的字面 JSON 塞進 title
  只會產生看不懂的 Document。
- `published_at` 接受 RFC3339／RFC2822／`YYYY-MM-DD[ HH:MM:SS]`／10 位 epoch 秒。
  解析不出來**不算失敗**（那是選填欄位），原字串留在 `attributes.published_at_raw`。
- title／body／summary 全空的紀錄會被跳過並計入 `skipped_empty`；全部都空才回 422。

## JSON 形狀

由**內容**決定，不看副檔名也不看 `Content-Type`：第一個非空白字元是 `[` 就當物件陣列，
否則當 NDJSON（每行一個物件）。兩種都支援的理由：陣列是手工整理最常見的形狀，
NDJSON 是匯出工具最常見的形狀、而且可以逐行套單筆上限。純量陣列（`[1,2,3]`）沒有欄位可對映，直接拒絕。

## 上限（全部可在 `config/default.toml` 的 `[import]` 調整）

| 鍵 | 預設 | 擋什麼 |
|---|---|---|
| `max_upload_bytes` | 10 MiB | 上傳大小。超過 413，串流當下就中止 |
| `max_records` | 10000 | 筆數 |
| `max_record_bytes` | 256 KiB | 單筆（NDJSON 一行／陣列一元素／CSV 一列） |
| `max_field_bytes` | 64 KiB | 單一對映欄位 |
| `max_depth` | 32 | JSON 巢狀深度 |
| `max_columns` | 512 | CSV 欄位數 |

`max_upload_bytes` 與 `[http].request_body_limit_bytes`（1 MiB）是**兩條獨立界線**：
只有 import 路由掛較寬的上限，其他路由不受影響。

深度檢查是**解析前**掃 bytes 做的（字串內的括號不算），所以超深巢狀不會先進 serde_json。
JSON／CSV 沒有 XML 的 entity expansion，`billion laughs` 的對應攻擊面是超深巢狀與超大
單筆／單欄位，上面三項就是針對它的。

**上限會連同 mapping 一起寫進 `RawEvidence.metadata["import"]`**，normalizer 用的是
上傳當下那份，不是目前的 config。理由：已經被接受的證據不該因為之後有人調小上限
就永遠正規化不了。

## 安全性

- RBAC：operator 以上（`require_write`）。viewer 在 RBAC 就被擋，碰不到 body。
- 每次上傳寫一筆稽核（`action=import.upload`），成功與被拒都寫。
- **解析方式只由 `request.kind` 決定**，不採信 multipart part 自帶的 `Content-Type`
  （那是上傳者完全可控的字串）。`kind=manual` 才會用使用者宣告的 `content_type`，
  而且要通過 MIME 字集檢查（擋 header injection）；未宣告時看 magic number。
- 檔名只留最後一段（`../../etc/passwd` → `passwd`）、去控制字元、截到 128 字元。
- 錯誤訊息不含 storage path、bucket 名、SQL：儲存層錯誤只寫 log，對外是通用訊息。

## normalizer 端

`metadata.import` 存在且 `kind` 是 `json`／`csv` → 用那份 `ImportSpec` 拆 Document，
每筆紀錄一份，`object_type` 由上傳者指定（預設 `report`）。

`kind=manual` → `SkippedUnsupported`（PDF 等任意檔案 V0.1 不處理）。

**沒有 `metadata.import` 的 JSON／CSV 仍然是 `SkippedUnsupported`**——例如 REST API
connector 抓回來的任意 JSON。那不是少做了什麼：沒有欄位對映就不知道哪個鍵是 title，
猜一組會產生看起來正常、內容其實錯位的 Document。

冪等保證與 pull 路徑共用同一段程式（`persist_documents`，V0.2 Phase 0e 起是**單一交易**，
見 `docs/developer/collector-normalizer.md`「落地原子性」）：
同一筆 RawEvidence 重跑只留同一組 Document，中途失敗則一份都不留。

## 已知限制

- `record_count` 上限 10000 意味著單筆 RawEvidence 最多產生 10000 份 Document，
  每份各一次 `put_document` + `put_provenance`。V0.1 沒有批次寫入，大檔會慢。
- manual 上傳的 HTML 會被 normalizer 當成網頁正規化（`object_type=webpage`），
  這是預期行為；PDF／圖片／zip 則跳過。
- 沒有 dedup：同一份檔案上傳兩次會得到兩筆 RawEvidence 與兩組 Document（Phase 4 處理）。
