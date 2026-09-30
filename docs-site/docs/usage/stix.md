# STIX 匯入匯出

STIX（Structured Threat Information eXpression）2.1 是資安情報界通用的交換格式。你可以把其他工具產生的 STIX bundle 匯入本系統，或把系統內的實體匯出成 STIX bundle 分享給其他平台。

---

## 支援的物件型別

### 匯入

匯入時認識以下 STIX 物件型別，並對應到系統內的實體型別：

| STIX 型別 | 條件 | 對應實體型別 |
|---|---|---|
| `identity` | `identity_class = individual` | `person` |
| `identity` | `identity_class = organization` | `organization` |
| `threat-actor` | — | `threat_actor` |
| `malware` | — | `malware` |
| `vulnerability` | — | `vulnerability` |
| `indicator` | — | `indicator` |
| `relationship` | — | Relationship（關聯） |
| `domain-name` | — | `domain` |
| `ipv4-addr` | — | `ip` |
| `url` | — | `url` |
| `email-addr` | — | `email` |

`identity` 的其他 `identity_class`（`group`、`class`、`system` 等）以及 `x-` 開頭的自訂物件會被略過，不會回傳錯誤。

### 匯出

匯出時，有原生 STIX 對應的實體型別（上表中的 10 種）會產生對應的標準 STIX 物件。沒有原生對應的 7 種型別：

| 沒有原生對應的型別 |
|---|
| `account`、`hostname`、`repository`、`hash`、`software`、`product`、`location` |

這 7 種會產生 `x-osint-core-entity` 自訂物件，把 Core 型別與 id 放在 `extra` 欄位裡。

!!! warning "`x-osint-core-entity` 是單向的"
    含有 `x-osint-core-entity` 的 bundle 可以匯出，但再匯回本系統時這些物件會被略過——反向映射刻意不做。如果需要保留這些實體，不要刪除系統內的原始記錄。

---

## 匯入 STIX Bundle

### 前置作業：建立 Source

每次匯入都要掛在一個 `source_type = stix_import` 的 Source 下：

```bash
export TOKEN=$(python3 scripts/dev-token.py)
export API=http://127.0.0.1:18080/api/v1

export SOURCE_ID=$(curl -s -X POST $API/sources \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"我的 STIX 來源","source_type":"stix_import"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo $SOURCE_ID
```

### 發起匯入

```
POST /api/v1/import/stix
```

```bash
curl -s -X POST $API/import/stix \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d "{
    \"source_id\": \"$SOURCE_ID\",
    \"bundle\": {
      \"type\": \"bundle\",
      \"id\": \"bundle--a1b2c3d4-0000-0000-0000-000000000001\",
      \"objects\": [
        {
          \"type\": \"identity\",
          \"id\": \"identity--a1b2c3d4-0000-0000-0000-000000000002\",
          \"name\": \"APT 範例組織\",
          \"identity_class\": \"organization\"
        },
        {
          \"type\": \"threat-actor\",
          \"id\": \"threat-actor--a1b2c3d4-0000-0000-0000-000000000003\",
          \"name\": \"APT-Example\"
        }
      ]
    }
  }"
```

成功回傳 **HTTP 202**，body 是 `stix_import` Job：

```json
{
  "id": "01a0f107-3611-76ff-a351-569a93966c04",
  "job_type": "stix_import",
  "status": "queued",
  ...
}
```

把 `id` 存起來，用 `GET /api/v1/jobs/{id}` 追蹤進度。

### 常見 400 原因

| 情況 | 說明 |
|---|---|
| `source_id` 指到的 Source 不是 `stix_import` 類型 | 換一個正確類型的 Source |
| Bundle 超過 50 MiB | 拆成多個小 bundle 分批匯入 |
| Bundle 裡的物件數超過 10,000 | 拆批 |
| Bundle 格式不對（缺 `type`、`id` 不合法等） | 先用 STIX validator 確認格式 |

---

## 匯出 STIX Bundle

### 發起匯出

```
POST /api/v1/export/stix
```

```bash
export JOB_ID=$(curl -s -X POST $API/export/stix \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "filter": {
      "entity_types": ["vulnerability"],
      "time_range": {
        "from": "2026-01-01T00:00:00Z",
        "to": "2026-12-31T23:59:59Z"
      }
    }
  }' | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo $JOB_ID
```

成功回傳 **HTTP 202**，body 是 `stix_export` Job。

`filter` 所有欄位都可以省略（省略等於「不限」，整表匯出）：

| 欄位 | 說明 |
|---|---|
| `entity_types` | 只匯出這些型別，值用 snake_case，例如 `["threat_actor","ip"]`。省略代表全部 |
| `entity_ids` | 從這些特定實體 id 出發，搭配 `depth` 可展開鄰居 |
| `time_range` | 只匯出 `first_seen`/`last_seen` 在此區間有交集的實體與關係 |
| `depth` | 從 `entity_ids` 出發 BFS 展開幾層（需同時提供 `entity_ids`；省略不展開） |

`entity_type` 使用 snake_case：`person`、`organization`、`threat_actor`、`malware`、`vulnerability`、`indicator`、`domain`、`ip`、`url`、`email`。

### 追蹤進度

```bash
curl -s "$API/jobs/$JOB_ID" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('status:', d['status'])"
```

| `status` 值 | 意思 |
|---|---|
| `queued` | 等待執行 |
| `running` | 正在執行 |
| `completed` | 完成，可以取結果 |
| `failed` | 失敗 |

### 取得匯出結果

Job 狀態變成 `completed` 後：

```
GET /api/v1/jobs/{id}/result
```

```bash
curl -s "$API/jobs/$JOB_ID/result" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('type:', d['type'])
print('objects:', len(d.get('objects',[])))
print('first object:', d['objects'][0].get('name', d['objects'][0].get('value','?')) if d.get('objects') else 'none')"
```

```text
type: bundle
objects: 3343
first object: CVE-2026-0973396
```

回傳標準 STIX 2.1 bundle JSON，可以直接存檔或傳給其他工具。

`GET /jobs/{id}/result` 的狀態碼：

| 碼 | 意思 |
|---|---|
| 200 | 成功，body 是 STIX bundle JSON |
| 404 | Job 不存在，或這個 Job 的 `job_type` 不是 `stix_export` |
| 409 | Job 尚未完成，訊息裡帶目前狀態（例如 `"status": "running"`） |
| 500 | Job 標記為 completed 但結果檔案遺失或格式損毀 |

---

## 關於 STIX id 的一件事

**匯出的 STIX id 跟你當初匯入時的不一樣，這是正常的。**

原因：多個 STIX bundle 匯入的相同實體（例如同一個 IP），在本系統裡會自動收斂成同一筆記錄。匯出時，這筆記錄會被指定一個由本系統決定的 canonical STIX id，而不是沿用任何一個原始的 STIX id。這確保匯出的 bundle 不會有「同一個實體有兩個 STIX id」的情況。

系統內部保留了原始 STIX id 的對照（存成 `namespace="stix"` 的 EntityIdentifier），但目前沒有 API 可以直接查詢。

---

## 已知限制

1. **`x-osint-core-entity` 無法反向匯入**：匯出的 `account`、`hostname`、`repository`、`hash`、`software`、`product`、`location` 使用自訂格式，再匯回本系統時這些物件會被略過。
2. **大量關聯的實體可能有遺漏**：單一實體若參與超過 100 條關係，匯出時可能只包含其中一部分。
3. **匯出 Entity 數量上限 10,000**：超過上限時整批 Failed，需要縮小 `filter` 範圍或拆批匯出。
4. **Document 與 Collection 不在匯出範圍內**：STIX 匯出只包含實體（Entity）和關聯（Relationship），不包含 Document 或 Collection。
