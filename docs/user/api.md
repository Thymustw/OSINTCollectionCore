# Core API 使用指南（V0.1）

這份是給**使用 API 的人**看的：怎麼拿到 token、怎麼建立來源與連接器、
怎麼把資料推進來、怎麼查出去。

完整的欄位、狀態碼與設計理由在 `docs/developer/api-skeleton.md`；
這裡只走一條從零到查得到資料的路。

---

## 先讀這段

| | |
|---|---|
| **Base URL** | `http://127.0.0.1:18080/api/v1`（預設綁 loopback） |
| **認證** | `Authorization: Bearer <token>`，每一條 `/api/v1/*` 都要 |
| **角色** | `viewer` 只能讀、`operator` 可以寫、`admin` 可以管 token |
| **寫入的方式** | 上傳原始資料走 `POST /import`；**不能**直接 POST 一份文件（見下方「為什麼沒有 POST /objects」） |
| **每一個寫入動作** | 都會寫進稽核紀錄（誰、從哪個 IP、做了什麼、成功或被拒） |

本機 8080 可能是別的系統（這台機器上是 OpenCTI），所以 Core API 預設是
**18080**。埠號在 `config/default.toml` 的 `[http].bind`。

---

## 1. 拿一把 token

有兩種憑證，都放在同一個 header 裡：

| | JWT | API token |
|---|---|---|
| 長相 | `eyJhbGci…` | `osint_<uuid>.<secret>` |
| 誰發的 | 你自己的簽發流程（密鑰是 `JWT_SECRET`） | `POST /api/v1/tokens`（admin） |
| 存活 | 短期（預設 1 小時） | 長期，可撤銷 |
| 適合 | 人 | 服務、CI、腳本 |

用 admin 身分發一把給服務用的 token：

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/tokens \
  -H "Authorization: Bearer $ADMIN_JWT" \
  -H 'Content-Type: application/json' \
  -d '{"name":"ci-importer","role":"operator","expires_in_days":30}'
```

回應裡的 `token` 欄位是**唯一一次**能看到明文的地方——伺服器只留 argon2 雜湊，
所以「再顯示一次」在結構上不可能。弄丟了就撤銷重發：

```bash
curl -s -X DELETE http://127.0.0.1:18080/api/v1/tokens/$ID \
  -H "Authorization: Bearer $ADMIN_JWT"      # 204，該 token 立刻失效
```

省略 `expires_in_days` 代表**不會自動到期**（只能撤銷）。那會被單獨記進稽核——
「躺在 CI 設定檔裡三年的 operator token」正是要避免的東西。

之後的例子都假設：

```bash
export TOKEN=...        # operator 以上
export API=http://127.0.0.1:18080/api/v1
```

確認憑證有效：

```bash
curl -s $API/whoami -H "Authorization: Bearer $TOKEN"
# {"subject":"token:ci-importer","role":"operator","auth_method":"ApiToken"}
```

---

## 2. 建一個 Source

Source 是「資料從哪裡來」。所有證據都掛在它底下。

```bash
curl -s -X POST $API/sources \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "name": "某資安部落格",
    "source_type": "rss",
    "base_url": "https://example.org/feed.xml",
    "language": "zh"
  }'
```

`source_type` 必須是這幾個之一：`rss`、`atom`、`static_web`、`rest_api`、
`manual_upload`、`json_import`、`csv_import`。

**要手動上傳檔案的話**，Source 的型別必須與上傳種類相符
（`manual_upload` / `json_import` / `csv_import`）。混用會被擋下來，
理由是：RSS 的 Source 底下混進 CSV 匯入之後，「這個來源的資料怎麼來的」
會得到互相矛盾的答案。

---

## 3. 建一個 Connector

Connector 是「怎麼去拿」。它必須掛在一個已存在的 Source 底下
（`source_id` 不存在會回 422）。

```bash
curl -s -X POST $API/connectors \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "source_id": "'"$SOURCE_ID"'",
    "name": "blog-rss",
    "type": "rss",
    "schedule": "0 */6 * * *",
    "credential_reference": "env:BLOG_TOKEN"
  }'
```

> **密碼不要寫在這裡。** `credential_reference` 與 `proxy_reference` 只接受
> **密鑰的位置**，不接受密鑰本身：
>
> - `env:BLOG_TOKEN` — 從環境變數讀
> - `file:/run/secrets/blog-token` — 從檔案讀
> - `store:vault/osint#blog` — 外部 secret store
>
> 直接填 `hunter2` 或 `https://user:pw@…` 會回 400。這不是龜毛：明文一旦落進
> 資料表，它就會出現在備份、log 與每一次 `GET /connectors` 的回應裡，事後清不乾淨。

看哪些 connector 被停用了：

```bash
curl -s "$API/connectors?enabled=false" -H "Authorization: Bearer $TOKEN"
```

---

## 4. 修改：先讀，再改

`PATCH` 一定要帶 `If-Match`，內容是你剛剛讀到的 `ETag`。

```bash
# 讀，順便拿 ETag
ETAG=$(curl -sI $API/sources/$SOURCE_ID -H "Authorization: Bearer $TOKEN" \
       | awk -F': ' '/^etag/{print $2}' | tr -d '\r')

# 改。只會動到你寫出來的欄位
curl -s -X PATCH $API/sources/$SOURCE_ID \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -H "If-Match: $ETAG" \
  -d '{"enabled": false, "description": null}'
```

- 沒帶 `If-Match` → **428**
- 帶的版本過期（有人在你之後改過）→ **412**，請重新讀一次再改
- body 裡**沒出現的欄位不會被動到**；寫 `null` 代表**清除**那個欄位
- `name` 這類不能為空的欄位寫 `null` 會回 400（不會被默默忽略）

為什麼這麼麻煩：沒有這道檢查的話，兩個人同時修改同一筆時，
後送出的會靜默蓋掉先送出的，而且兩邊都看到「成功」。

---

## 5. 把資料推進來

**上傳檔案**（Manual／JSON／CSV）：

```bash
curl -s -X POST $API/import \
  -H "Authorization: Bearer $TOKEN" \
  -F 'request={"source_id":"'"$SOURCE_ID"'","kind":"csv"};type=application/json' \
  -F 'file=@iocs.csv'
```

流程是：內容先原樣落地成**不可變的 Raw Evidence**（body 存物件儲存），
接著發出 `raw.collected`，由 normalizer 產生 Document。
完整的欄位對映與上限見 `docs/developer/import-api.md`。

### 為什麼沒有「直接建立一份文件」的 API

`POST /objects` 存在，但**一律回 501**。

直接塞一份文件進來，會產生一份**沒有來源證據**的資料：它在列表與搜尋結果裡看起來
完全正常，但當你之後想問「這句話是哪裡來的」時，鏈在第一步就斷了——
而且在那之前不會有任何錯誤訊息。

所以推資料一律走 `/import`：你上傳的那份檔案**本身就是**證據，
連同上傳者與時間一起被記下來。理由的完整版在
`docs/adr/ADR-006-no-direct-object-post.md`。

---

## 6. 查資料

```bash
# 文件列表（預設不含重複轉載）
curl -s "$API/objects?limit=20" -H "Authorization: Bearer $TOKEN"

# 只看資安公告
curl -s "$API/objects?object_type=advisory" -H "Authorization: Bearer $TOKEN"

# 單篇：含來源證據、去重關係、抽出的 IOC
curl -s $API/objects/$DOC_ID -H "Authorization: Bearer $TOKEN"

# 抽出的實體（IP／網域／CVE…）
curl -s "$API/entities?entity_type=vulnerability" -H "Authorization: Bearer $TOKEN"

# 某個實體牽涉到哪些關聯，以及支持這些關聯的證據
curl -s $API/entities/$ENTITY_ID -H "Authorization: Bearer $TOKEN"
curl -s $API/relationships/$REL_ID -H "Authorization: Bearer $TOKEN"

# 原始證據：預設只回 metadata
curl -s $API/raw/$RAW_ID -H "Authorization: Bearer $TOKEN"
curl -s "$API/raw/$RAW_ID?body=true" -H "Authorization: Bearer $TOKEN"   # 連內容一起
```

**全文搜尋**用 `POST /search`（條件有巢狀結構，所以用 POST 而不是 query string），
語法見 `docs/user/search.md`。

### 翻頁

所有列表都長這樣：

```json
{"items": [ ... ], "next_cursor": "0199c0de-..."}
```

把 `next_cursor` 放回 `?cursor=` 就是下一頁；`null` 代表沒有了。
`limit` 預設 20、最大 100（填更大不會報錯，會被夾回 100）。

> **不要用「筆數」判斷有沒有資料。** 明細回應裡的關聯清單（collection 的成員、
> relationship 的 evidence、entity 的關聯數）上限都是 100，
> 旁邊有一個 `*_truncated` 欄位告訴你「還有更多」。
> `relationship_count` 在 truncated 時代表「至少這麼多」，不是總數。

### 為什麼 `/events` 是空的

這是**正常的**。V0.1 只定義了事件模型，沒有做自動事件偵測，
所以那張表目前沒有任何寫入者。它回空清單或 404 不代表故障。

---

## 7. 出事的時候

```bash
# 後端服務活著沒有（任一掛掉整體回 503，並指出是哪一個；含 neo4j HTTP 與 neo4j_bolt）
curl -s -o /dev/null -w '%{http_code}\n' $API/ops/health -H "Authorization: Bearer $TOKEN"
curl -s $API/ops/health -H "Authorization: Bearer $TOKEN"

# 圖投影 lag／rebuild（沒接 Neo4j 回 503）
curl -s $API/ops/graph -H "Authorization: Bearer $TOKEN"

# 這個 API 行程用了多少記憶體與 CPU
curl -s $API/ops/metrics -H "Authorization: Bearer $TOKEN"

# 哪些工作失敗了
curl -s "$API/jobs?status=failed" -H "Authorization: Bearer $TOKEN"

# 重試一個失敗的工作（只有 failed 的可以，其他狀態回 409）
curl -s -X POST $API/jobs/$JOB_ID/retry -H "Authorization: Bearer $TOKEN"
```

`/ops/health` 的回應裡有兩組名單，意思不一樣：

- `unhealthy` — 接上了但**壞掉**：去看那個服務
- `not_configured` — **根本沒接**：去看設定檔

重試會把工作轉成 `retrying`（不是 `queued`），然後立刻重新派工。
用 `retrying` 是為了讓 `retry_count` 累加得起來——否則「這個工作重試過幾次」
會在每次重試時被抹掉。

---

## 常見錯誤碼

| 碼 | 意思 | 你要做什麼 |
|---|---|---|
| 401 | 沒帶 token，或 token 無效／過期／已撤銷 | 換一把 |
| 403 | 角色不夠（viewer 想寫） | 用 operator 以上的憑證 |
| 409 | 你指定的 id 已經存在；或工作狀態不允許重試 | 改用 PATCH、或省略 id |
| 412 | 有人在你讀取之後改過這筆 | 重新讀一次，再套用你的改動 |
| 413 | 要拿的原始內容太大 | 去掉 `?body=true`，或請管理者調高上限 |
| 422 | 你引用的資源不存在（例如 connector 的 source） | 先建立它 |
| 428 | PATCH 沒帶 `If-Match` | 先 GET 拿 `ETag` |
| 503 | 後端沒接上 | 看訊息裡提到的環境變數 |

錯誤回應一律是這個形狀，`message` 會講下一步：

```json
{"error": "precondition_failed", "message": "If-Match 與目前的版本不符：…請重新 GET 一次…"}
```

---

## 相關文件

- 完整 API 參考（欄位、所有狀態碼、設計理由）：`docs/developer/api-skeleton.md`
- 上傳格式與欄位對映：`docs/developer/import-api.md`
- 搜尋語法：`docs/user/search.md`
- 本機唯讀查詢工具：`docs/user/cli.md`
- 為什麼沒有 `POST /objects`：`docs/adr/ADR-006-no-direct-object-post.md`
