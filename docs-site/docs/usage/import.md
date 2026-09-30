# 匯入資料

這一頁說明如何把資料匯入系統——包括手動上傳的 JSON 與 CSV 檔案，
以及透過 Connector 自動抓取的設定方式。

---

## 為什麼沒有「直接建立文件」的 API

打 `POST /api/v1/objects` 會得到 `501 Not Implemented`：

```bash
curl -s -X POST $API/objects \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"title":"測試","body":"內容"}'
```

```json
{
  "error": "not_implemented",
  "message": "V0.1 不支援直接建立 object：那會產生沒有 RawEvidence 祖先的孤兒，
  違反最低可追溯鏈。請改用 POST /api/v1/import 上傳原始內容..."
}
```

這是刻意的設計：**每一筆資料都要能追回「誰、在什麼時候、從哪裡帶進來的」**。
直接建立文件會產生一份沒有原始依據的資料——在列表和搜尋結果裡看起來完全正常，
但當你問「這份情報是哪裡來的」時，鏈在第一步就斷了。

推資料一律走 `POST /api/v1/import`：你上傳的那份檔案本身就是原始證據，
連同上傳者與時間一起被永久記下來。

---

## 步驟一：建立 Source（資料來源）

每一批匯入都要掛在一個 Source 底下。**Source 的 `source_type` 必須與你要匯入的種類相符**：

| 匯入種類（`kind`） | Source 的 `source_type` |
|---|---|
| `manual` | `manual_upload` |
| `json` | `json_import` |
| `csv` | `csv_import` |

```bash
export SOURCE_ID=$(curl -s -X POST $API/sources \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"資安公告匯入","source_type":"json_import","language":"zh"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo $SOURCE_ID
```

種類不符時，`POST /api/v1/import` 會回 `400`：

```json
{
  "error": "bad_request",
  "message": "Source 的 source_type 與 kind=`csv` 不符（需要 `csv_import`）。
  請改用對應的匯入 Source，或新建一個 source_type=`csv_import` 的 Source"
}
```

這樣做的原因是：RSS Source 底下混進 CSV 匯入，「這個來源的資料怎麼來的」
就會有互相矛盾的答案。要匯入就開匯入專用的 Source。

---

## 步驟二：呼叫 POST /api/v1/import

上傳格式是 `multipart/form-data`，固定兩個欄位：

| 欄位 | 說明 |
|---|---|
| `request` | JSON 文字，描述這次匯入的設定。**Content-Type 一定要帶 `application/json`** |
| `file` | 實際的檔案內容 |

### JSON 匯入

```bash
curl -s -X POST $API/import \
  -H "Authorization: Bearer $TOKEN" \
  -F 'request={"source_id":"'"$SOURCE_ID"'","kind":"json","object_type":"advisory"};type=application/json' \
  -F 'file=@advisories.json'
```

### CSV 匯入

```bash
curl -s -X POST $API/import \
  -H "Authorization: Bearer $TOKEN" \
  -F 'request={"source_id":"'"$SOURCE_ID"'","kind":"csv"};type=application/json' \
  -F 'file=@iocs.csv'
```

### 手動上傳（任意格式原始檔案）

```bash
curl -s -X POST $API/import \
  -H "Authorization: Bearer $TOKEN" \
  -F 'request={"source_id":"'"$SOURCE_ID"'","kind":"manual"};type=application/json' \
  -F 'file=@report.html'
```

!!! note "request 欄位的 Content-Type"
    curl 的 `-F` 預設不帶 `type=application/json`，API 會拒絕。
    範例裡的 `;type=application/json` 是必要的。

### request 欄位的所有選項

| 鍵 | 必填 | 說明 |
|---|---|---|
| `source_id` | 是 | Source 的 UUID，`source_type` 必須與 `kind` 相符 |
| `kind` | 是 | `manual` / `json` / `csv` |
| `object_type` | 否 | 產出文件的型別，預設 `report`。常用：`advisory`、`article`、`report` |
| `mapping` | 否 | 欄位對映，見下方說明 |
| `title` / `description` | 否 | 這次匯入批次的說明（會出現在 Raw Evidence 的 metadata）|
| `source_url` | 否 | 未填時自動合成 `import://{kind}/{filename}` |
| `external_id` | 否 | 填入 Raw Evidence 的 `external_id` |
| `content_type` | 否 | 只有 `kind=manual` 會用。未填時依內容自動偵測 |

欄位名打錯（例如 `objecttype`）會回 `400`，不會被靜默忽略。

---

## 回應格式

成功回 `201 Created`：

```json
{
  "raw_evidence_id": "01a0f0ea-630d-74ce-9712-4f23dd74fb0c",
  "source_id": "01a0f0ea-0af8-718e-b46a-696269266aed",
  "connector_id": "467e6a5b-7a00-5070-9c25-68f928249b45",
  "kind": "json",
  "content_type": "application/json",
  "bytes": 1041,
  "sha256": "5bbd3ce1af19516c330a89afa297f1c05bc028048bb351145a1a1d9722d023a9",
  "record_count": 2,
  "skipped_empty": 0,
  "published": true,
  "message": "已收下並發出 raw.collected，normalizer 會接手正規化"
}
```

| 欄位 | 意義 |
|---|---|
| `raw_evidence_id` | 這份原始證據的 ID，可用 `GET /api/v1/raw/{id}` 查回來 |
| `record_count` | 偵測到的記錄數。`kind=manual` 時為 `null`（不解析內容） |
| `skipped_empty` | 因為 title／body／summary 全空而被跳過的記錄數 |
| `published` | `true` 代表已發出事件，後面的流程（整理、抽取、搜尋索引）會自動進行 |
| `sha256` | 上傳內容的 SHA-256 雜湊，可用來驗證原件沒被動過 |

!!! warning "`published: false` 代表什麼"
    `false` 代表原始證據已落地，但後端事件佇列（Redpanda）沒有收到訊息——
    **這筆資料不會被自動整理**，也不會出現在搜尋結果裡。

    這種情況不回 5xx，因為資料已經寫進去了，讓你重試只會產生第二份原始證據。
    請用回應的 `raw_evidence_id` 聯絡系統管理者手動重送，或等佇列恢復後重新觸發。

---

## JSON 檔案的兩種格式

系統依**第一個非空白字元**判斷格式，不看副檔名或 Content-Type：

**物件陣列**（第一個字元是 `[`）：

```json
[
  {"title": "公告一", "body": "內容一"},
  {"title": "公告二", "body": "內容二"}
]
```

**NDJSON**（每行一個 JSON 物件，常見於資料匯出工具）：

```text
{"title": "公告一", "body": "內容一"}
{"title": "公告二", "body": "內容二"}
```

CSV 第一列必須是 header，之後每列一筆記錄。

---

## 欄位對映（mapping）

系統把 JSON 或 CSV 裡的鍵對到文件的標準欄位。未指定時依下列預設鍵名（大小寫不敏感，依序嘗試）：

| 文件欄位 | 預設嘗試的鍵 |
|---|---|
| `title` | `title`, `headline`, `name`, `subject` |
| `body` | `body`, `content`, `text`, `description_full` |
| `summary` | `summary`, `description`, `abstract`, `excerpt` |
| `url` | `url`, `link`, `source_url`, `permalink` |
| `published_at` | `published_at`, `published`, `date`, `pubdate`, `timestamp` |
| `external_id` | `external_id`, `id`, `guid`, `uuid` |
| `language` | `language`, `lang` |
| `author` | `author`, `creator`, `byline` |

如果你的資料用不同的鍵名，用 `mapping` 告訴系統：

```bash
curl -s -X POST $API/import \
  -H "Authorization: Bearer $TOKEN" \
  -F 'request={
    "source_id":"'"$SOURCE_ID"'",
    "kind":"csv",
    "mapping": {"title": "headline", "body": "full_text", "published_at": "date_issued"}
  };type=application/json' \
  -F 'file=@news.csv'
```

JSON 的值可以用 JSON Pointer（以 `/` 開頭）指定深層路徑：

```json
"mapping": {"body": "/attributes/full_text"}
```

!!! note "CSV 欄位名稱不存在時"
    CSV 的 mapping 值是 header 欄名。指定的欄名不存在時，API 回 `422` 並列出可用的欄名——
    不會靜默忽略（否則你會得到「成功」但每筆都空白的結果）。

---

## 上限

這些上限都可以在 `config/default.toml` 的 `[import]` 區段調整：

| 項目 | 預設值 | 說明 |
|---|---|---|
| 單次上傳大小 | 10 MiB | 串流超過即中止，回 `413` |
| 最多記錄數 | 10,000 筆 | JSON 或 CSV 的最大行數 |
| 單筆大小 | 256 KiB | NDJSON 一行 / 陣列一個元素 / CSV 一列 |
| 單一欄位大小 | 64 KiB | 對映後的單一欄位值 |
| JSON 巢狀深度 | 32 層 | 超過直接拒絕，不進入解析 |
| CSV 欄位數 | 512 個 | 超過即 `413` |

---

## 自動抓取來源（Connector）

RSS feed、靜態網頁、REST API 這類「系統主動去拿」的來源，是透過 **Connector（連接器）** 設定的，不走 `POST /api/v1/import`。

建立方式：

```bash
curl -s -X POST $API/connectors \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "source_id": "'"$SOURCE_ID"'",
    "name": "acme-rss",
    "type": "rss",
    "schedule": "0 */6 * * *",
    "credential_reference": "env:FEED_TOKEN"
  }'
```

!!! note "密鑰只放 credential_reference"
    `credential_reference` 只接受密鑰的**位置**，不接受密鑰本身：
    `env:FEED_TOKEN`（環境變數）、`file:/run/secrets/token`（檔案）。
    直接填明文會回 `400`。

支援的自動抓取類型：`rss`、`atom`、`static_web`、`rest_api`。
詳細設定選項見 [API 參考](../reference/api.md)。
