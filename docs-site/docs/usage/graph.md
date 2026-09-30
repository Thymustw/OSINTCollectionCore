# 在圖上看關聯

這一頁說明如何用 Neo4j Browser 網頁介面或 Graph API，查詢實體（Entity）之間的關係。

!!! note "圖上只有「實體和實體之間」的關係"
    文件（Document）本身不是圖上的節點。「哪篇文章提到了這個 IP」這類文件與實體之間的關係不會出現在圖上。要查一個實體出現在哪些文件，用 `GET /api/v1/entities/{id}` 的 `recent_extractions` 欄位。

---

## 一、用 Neo4j Browser 瀏覽

打開瀏覽器到 **<http://127.0.0.1:7474>**，登入：

| 欄位 | 填入 |
|---|---|
| Connect URL | `neo4j://127.0.0.1:7687`（預設值，不用改） |
| Username | `neo4j` |
| Password | `osint_dev_neo4j` |

### 圖裡的節點與關聯

圖上每個節點的標籤（Label）是 `:Entity`，常用屬性：

| 屬性 | 說明 |
|---|---|
| `entity_id` | 對應到 API 的實體 UUID |
| `entity_type` | 型別，例如 `url`、`domain`、`email`、`vulnerability` |
| `display_name` | 顯示名稱（等同 API 回傳的 `name`） |
| `attributes_json` | 結構化屬性（JSON 字串） |

關聯類型（Relationship Type）使用大寫，常見的有：

| 類型 | 語意 |
|---|---|
| `BELONGS_TO` | 網址屬於某個網域；子網域屬於父網域 |
| `ASSOCIATED_WITH` | Email 關聯到某個網域 |
| `DERIVED_FROM` | 衍生關係 |

### 實用 Cypher 查詢範例

**查詢某個實體的 1-hop 鄰居**（以 `psirt@acme-example.com` 為例）：

```cypher
MATCH (a:Entity {display_name: "psirt@acme-example.com"})-[r]-(b:Entity)
RETURN a, r, b
```

**同時查詢多個實體**：

```cypher
MATCH (a:Entity)-[r]->(b:Entity)
WHERE a.display_name IN [
  "https://advisories.example.com/acme-2026-0928",
  "psirt@acme-example.com"
]
RETURN a, r, b
```

執行後你會看到兩組關係：

- 網址 `https://advisories.example.com/acme-2026-0928` —**BELONGS_TO**→ `advisories.example.com`
- Email `psirt@acme-example.com` —**ASSOCIATED_WITH**→ `acme-example.com`

**查兩個實體之間是否有路徑**：

```cypher
MATCH path = shortestPath(
  (a:Entity {display_name: "psirt@acme-example.com"})-[*..5]-(b:Entity {display_name: "acme-example.com"})
)
RETURN path
```

**看某種型別的所有實體**：

```cypher
MATCH (n:Entity)
WHERE n.entity_type = "vulnerability"
RETURN n
LIMIT 20
```

**瀏覽圖上全部關係**（資料多時會很擠，建議加 LIMIT）：

```cypher
MATCH (a:Entity)-[r]->(b:Entity)
RETURN a, r, b
LIMIT 100
```

---

## 二、用 Graph API 查詢

Graph API 適合程式整合。所有端點都需要認證（viewer 以上）：

```bash
export TOKEN=$(python3 scripts/dev-token.py)
export API=http://127.0.0.1:18080/api/v1
```

### 取得某實體的鄰居

```
GET /api/v1/graph/entities/{id}/neighbors
```

以 URL 實體（`https://advisories.example.com/acme-2026-0928`）為例：

```bash
curl -s "$API/graph/entities/7766959c-666d-5759-b620-9f584990f6b5/neighbors" \
  -H "Authorization: Bearer $TOKEN"
```

```json
[
  {
    "entity_id": "6220fc0d-2993-5346-90d3-f48413d125aa",
    "entity_type": "domain",
    "display_name": "advisories.example.com",
    "attributes": {
      "is_registrable_domain": false,
      "public_suffix": "com",
      "registrable_domain": "example.com"
    }
  }
]
```

圖上找不到這個節點時回傳空陣列，不是 404。

### 取得某實體的關聯（含關係屬性）

```
GET /api/v1/graph/entities/{id}/relationships
```

```bash
curl -s "$API/graph/entities/7766959c-666d-5759-b620-9f584990f6b5/relationships" \
  -H "Authorization: Bearer $TOKEN"
```

```json
[
  {
    "relationship_id": "879c6d21-2841-524d-bfe8-57ccf0f60479",
    "source": "7766959c-666d-5759-b620-9f584990f6b5",
    "target": "6220fc0d-2993-5346-90d3-f48413d125aa",
    "relationship_type": "belongs_to",
    "confidence": 0.95,
    "first_seen": "2026-09-30T05:39:31.687Z",
    "last_seen": "2026-09-30T05:39:31.687Z"
  }
]
```

!!! note "API 與 Neo4j Browser 的關聯類型格式不同"
    API 回傳的 `relationship_type` 是小寫底線格式（`belongs_to`），而 Neo4j Browser 顯示的是大寫格式（`BELONGS_TO`）。兩者代表同一條關係，只是格式不同。

### 查詢兩個實體之間的最短路徑

```
GET /api/v1/graph/path?from={id}&to={id}
```

```bash
URL_ID=7766959c-666d-5759-b620-9f584990f6b5
DOMAIN_ID=6220fc0d-2993-5346-90d3-f48413d125aa

curl -s "$API/graph/path?from=$URL_ID&to=$DOMAIN_ID" \
  -H "Authorization: Bearer $TOKEN"
```

```json
{
  "nodes": [
    {
      "entity_id": "7766959c-666d-5759-b620-9f584990f6b5",
      "entity_type": "url",
      "display_name": "https://advisories.example.com/acme-2026-0928",
      "attributes": {"host": "advisories.example.com", "scheme": "https"}
    },
    {
      "entity_id": "6220fc0d-2993-5346-90d3-f48413d125aa",
      "entity_type": "domain",
      "display_name": "advisories.example.com",
      "attributes": {}
    }
  ],
  "edges": [
    {
      "relationship_id": "879c6d21-2841-524d-bfe8-57ccf0f60479",
      "source": "7766959c-...",
      "target": "6220fc0d-...",
      "relationship_type": "belongs_to",
      "confidence": 0.95,
      "first_seen": "2026-09-30T05:39:31.687Z",
      "last_seen": "2026-09-30T05:39:31.687Z"
    }
  ]
}
```

找不到路徑時回傳 `null`（HTTP 200），不是 404——「這兩點沒連上」是查詢結果，不是錯誤。

### 過濾選項

以上端點都支援相同的 query string 參數：

| 參數 | 說明 | 預設 |
|---|---|---|
| `max_hops` | 最多往外走幾跳 | `1` |
| `relationship_types` | 逗號分隔，例如 `belongs_to,associated_with` | 全部 |
| `entity_types` | 逗號分隔，例如 `domain,url` | 全部 |
| `min_confidence` | 最低信心分數（0.0–1.0） | 無限制 |
| `time_from` | RFC3339，例如 `2026-01-01T00:00:00Z` | 無限制 |
| `time_to` | RFC3339 | 無限制 |

!!! warning "`time_from` 與 `time_to` 必須成對"
    只給其中一個會回傳 400 錯誤。兩個都不給等同「不限時間範圍」。

範例——只看 `domain` 型別的 1-hop 鄰居：

```bash
curl -s "$API/graph/entities/$ENTITY_ID/neighbors?entity_types=domain" \
  -H "Authorization: Bearer $TOKEN"
```

### 進階查詢（POST /graph/query）

```
POST /api/v1/graph/query
```

接受結構化的 `GraphQuery` 物件，適合多起點、複雜條件的查詢。回傳多條路徑組成的陣列。

---

## 三、觸發圖重建

```
POST /api/v1/graph/rebuild
```

需要 operator 以上權限。這個端點只建立一個 `graph_rebuild` 工作（Job），真正的重建由背景服務執行，建立後立刻回傳 HTTP 201：

```bash
curl -s -X POST "$API/graph/rebuild" -H "Authorization: Bearer $TOKEN"
```

```json
{
  "id": "01a0f106-...",
  "status": "queued",
  ...
}
```

用 `GET /api/v1/jobs/{id}` 追蹤進度。

!!! note "重建不會刪除既有節點"
    `POST /graph/rebuild` 執行的是增量更新，已經不存在於 PostgreSQL 的舊節點不會自動刪除。完整重建（先清空再重建）需要在伺服器端執行 CLI 指令，無法透過 API 觸發。

---

## 圖是什麼、不是什麼

| 是 | 不是 |
|---|---|
| 實體與實體之間的關係投影 | 文件與實體之間的關係（mentions、authored_by） |
| 可隨時整個刪掉重建 | 真實資料（真實資料在 PostgreSQL） |
| 適合查關聯鏈 | 適合查「這個漏洞出現在幾篇文章」（用 `GET /entities/{id}`） |

圖的資料來源是 PostgreSQL。如果 API 查詢結果與 Neo4j Browser 不一致，以 PostgreSQL 為準，觸發一次重建即可同步。
