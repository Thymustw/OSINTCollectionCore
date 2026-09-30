# 服務與埠號

這一頁列出系統所有服務的角色、埠號與啟動指令。

---

## 基礎設施服務（6 個）

這 6 個服務是資料庫、訊息佇列與物件儲存，預設就會啟動（不需要 `--profile app`）。

| 服務 | 宿主埠（開發環境） | 容器內位址 | 角色 |
|---|---|---|---|
| PostgreSQL | 5432 | `postgres:5432` | **唯一的真實資料來源**。所有資料以這裡為準 |
| OpenSearch | **19200**（開發環境；canonical 為 9200） | `opensearch:9200` | 全文搜尋與語意向量搜尋索引，可從 PostgreSQL 重建 |
| Redpanda | 9092（宿主）、29092（容器內部） | `redpanda:29092` | 服務間的訊息佇列（Kafka 相容） |
| Redis | 6379 | `redis:6379` | 節流快取、dedup embedding 暫存 |
| SeaweedFS | 8333 | `seaweedfs:8333` | S3 相容物件儲存，儲存原始證據內容 |
| Neo4j | 7474（HTTP）、7687（Bolt） | `neo4j:7474`／`neo4j:7687` | 關聯圖投影（Browser 介面），可從 PostgreSQL 重建 |

!!! note "OpenSearch 為什麼用 19200"
    這台開發機的 9200 埠被另一套服務佔用，`docker-compose.dev.yml` 把 OpenSearch 改掛到 19200。
    容器**內部**仍然用 `opensearch:9200`，只有從宿主連進去時才用 19200。

!!! warning "Redpanda 容器內要用 29092，不是 9092"
    Redpanda 設了兩個 Kafka listener：
    - `9092`：對宿主（`cargo run`、本機工具）開放，宣告位址是 `127.0.0.1:9092`
    - `29092`：只在 compose 網路內，宣告位址是 `redpanda:29092`

    容器內的服務連 `9092` 時，broker 回傳的 metadata 會說「我在 `127.0.0.1:9092`」，
    導致服務去連自己的 loopback，最終表現為「匯入成功但文件永遠不出現在搜尋結果」。
    這個錯誤不會讓健康檢查變紅。

---

## 應用服務（10 個）

這 10 個服務是用 Rust 寫的核心業務邏輯，放在 Docker profile `app` 底下，用 `make compose-up-full` 啟動。

### 服務一覽

| 服務 | 宿主埠 | 角色 | 在資料流中的位置 |
|---|---|---|---|
| `osint-api` | 18080 | HTTP API 入口，也負責匯入解析 | 收 HTTP 請求 → 寫入 PostgreSQL → 發布事件 |
| `osint-collector` | 18081 | 定期抓取已設定的資料來源 | 定時觸發 → 從外部 URL 抓取 → 寫 Raw Evidence |
| `osint-normalizer` | 18082 | 把原始證據整理成統一格式的文件 | 訂閱 `raw.collected` → 正規化 → 發布 `object.normalized` |
| `osint-deduplicator` | 18083 | 偵測並標記重複文件 | 訂閱 `object.normalized` → 比對 → 發布 `dedup.completed` |
| `osint-entity-worker` | 18084 | 從文件抽取實體與關聯 | 訂閱 `dedup.completed` → 抽取 → 發布 `entity.extracted` |
| `osint-indexer` | 18085 | 把文件投影進 OpenSearch 搜尋索引 | 訂閱 `entity.extracted` → 批次寫入 OpenSearch |
| `osint-graph-worker` | 18086 | 把實體關聯投影進 Neo4j 圖 | 訂閱 `relationship.changed` → 寫 Neo4j |
| `osint-embedding-worker` | 18087 | 產生語意向量，支援語意搜尋 | 訂閱 `entity.extracted` → 呼叫 OpenSearch ml-commons → 寫向量 index |
| `osint-stix-worker` | 18088 | 處理 STIX 2.1 匯入作業 | 訂閱 `job.dispatched`（job_type=stix_import）→ 對映寫 PostgreSQL |
| `osint-discovery-worker` | 18089 | 執行 Discovery（從已知實體擴展發現新關聯） | 訂閱 `job.dispatched`（job_type=discovery_run）→ 查圖 → 寫 Candidate |

### 所有應用服務共有的端點

每個應用服務都有三個端點（以 18080 為例，其餘換埠號）：

```bash
GET http://127.0.0.1:18080/health   # 存活探針（不需要認證）
GET http://127.0.0.1:18080/ready    # 就緒探針（不需要認證）
GET http://127.0.0.1:18080/metrics  # Prometheus 格式指標（不需要認證）
```

---

## 啟動與停止

### 只啟動基礎設施

```bash
make compose-up
```

等 6 個基礎設施都是 `(healthy)` 之後，才能執行其他步驟（部署模型、啟動應用服務）。

查看狀態：

```bash
make compose-ps
```

### 啟動全部（含 10 個應用服務）

需要先部署語意搜尋模型，否則 `embedding-worker` 會啟動失敗。完整順序見 [快速上手](../getting-started/quickstart.md)。

```bash
make compose-up-full
```

### 查看所有服務狀態

```bash
make compose-ps-full
```

### 停止全部

```bash
make compose-down-full    # 停止，資料保留
```

要**連資料一起清掉**（完全重置）：

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml \
  --profile app down -v
```

!!! danger "`-v` 會刪掉所有 volume"
    包括 PostgreSQL 資料庫、OpenSearch 索引、Neo4j 圖、SeaweedFS 原始證據，以及已下載的語意搜尋模型。執行前請確認。
