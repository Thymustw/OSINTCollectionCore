# 設定檔

這一頁說明 `config/default.toml` 的主要設定區段，以及怎麼覆寫它們。

---

## 覆寫方式

### 環境變數

所有設定都可以用環境變數覆寫，格式是 `OSINT__<區段>__<鍵>`（大寫、巢狀用雙底線）：

```bash
# 覆寫 [storage.search].url
OSINT__STORAGE__SEARCH__URL=http://opensearch:9200

# 覆寫 [http].bind
OSINT__HTTP__BIND=0.0.0.0:18080

# 覆寫 [broker].brokers
OSINT__BROKER__BROKERS=redpanda:29092
```

### 密鑰（SecretRef）

密鑰不能直接寫在設定檔，要用 `SecretRef` 格式：

| 格式 | 說明 | 例子 |
|---|---|---|
| `env:<變數名>` | 從環境變數讀取 | `"env:DATABASE_URL"` |
| `file:<路徑>` | 從檔案讀取 | `"file:/run/secrets/jwt_secret"` |
| `store:<名稱>` | 從外部密鑰管理系統讀取 | 視部署環境而定 |

---

## 主要設定區段

### `[app]`

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `environment` | `"dev"` | 執行環境，影響 log 格式與部分行為 |

---

### `[http]`（僅 osint-api）

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `bind` | `"127.0.0.1:18080"` | 監聽位址與埠。容器內需要覆寫成 `0.0.0.0:18080` |
| `rate_limit_per_second` | `20` | 每秒請求上限（按 IP） |
| `request_body_limit_bytes` | `1048576`（1 MiB） | 一般 API 請求的 body 上限（匯入端點另外有更寬的上限） |

---

### `[auth]`（僅 osint-api）

| 鍵 | 說明 |
|---|---|
| `jwt_secret_ref` | JWT 簽名密鑰，用 SecretRef 格式（例如 `"env:JWT_SECRET"`）。必須至少 32 個 ASCII byte |
| `jwt_issuer` | JWT 的 `iss` 欄位，預設 `"osint-core"` |
| `jwt_ttl_secs` | 簽發的 token 有效期秒數，預設 3600（1 小時） |

---

### `[storage.canonical]`

PostgreSQL 連線設定（這是唯一的真實資料來源）：

| 鍵 | 說明 |
|---|---|
| `adapter` | 固定 `"postgres"` |
| `dsn_secret_ref` | 連線字串，用 SecretRef 格式（例如 `"env:DATABASE_URL"`） |
| `pool_max` | 連線池上限，預設 10 |

---

### `[storage.search]`

OpenSearch 連線設定（搜尋投影，可重建）：

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `adapter` | `"opensearch"` | |
| `url` | `"http://127.0.0.1:9200"` | 容器內覆寫為 `http://opensearch:9200`；本機 dev 覆寫為 `http://127.0.0.1:19200` |

---

### `[storage.object]`

S3 相容物件儲存（SeaweedFS）連線設定：

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `adapter` | `"s3"` | |
| `endpoint` | `"http://127.0.0.1:8333"` | 容器內覆寫為 `http://seaweedfs:8333` |
| `bucket` | `"raw-evidence"` | |
| `access_key_ref` | | 用 SecretRef（例如 `"env:S3_ACCESS_KEY"`） |
| `secret_key_ref` | | 用 SecretRef（例如 `"env:S3_SECRET_KEY"`） |

---

### `[storage.cache]`

Redis 連線設定：

| 鍵 | 說明 |
|---|---|
| `adapter` | 固定 `"redis"` |
| `url_secret_ref` | Redis URL，用 SecretRef（例如 `"env:REDIS_URL"`）。容器內為 `redis://redis:6379/0` |

---

### `[storage.graph]`

Neo4j 連線設定（圖投影，可重建）：

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `adapter` | `"neo4j"` | |
| `http_url` | `"http://127.0.0.1:7474"` | 給健康探針用。容器內覆寫為 `http://neo4j:7474` |
| `bolt_uri` | `"bolt://127.0.0.1:7687"` | 給資料讀寫用。容器內覆寫為 `bolt://neo4j:7687` |
| `username` | `"neo4j"` | Neo4j 帳號（Community 版初始使用者只能是 `neo4j`） |
| `password_secret_ref` | | 用 SecretRef（例如 `"env:NEO4J_PASSWORD"`） |
| `pool_max` | `5` | Bolt 連線池上限 |

---

### `[broker]`

Redpanda（Kafka 相容）連線設定：

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `brokers` | `"127.0.0.1:9092"` | 容器內必須覆寫為 `redpanda:29092`（不能用 9092，原因見 [服務與埠號](../operations/services.md)） |

---

### `[import]`

`POST /api/v1/import` 的上限：

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `max_upload_bytes` | `10485760`（10 MiB） | 單次上傳檔案大小上限 |
| `max_records` | `10000` | 單次匯入最多幾筆記錄 |
| `max_record_bytes` | `262144`（256 KiB） | 單筆記錄大小上限 |

---

### `[auto_approval]`

AI 輔助自動合併設定（預設完全關閉）：

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `enabled` | `false` | 開啟後才會觸發 AI 評分 |
| `auto_confirm_score` | `0.95` | 高於此分數自動確認合併 |
| `llm_review_score` | `0.70` | 高於此分數才送給 LLM 審查 |
| `max_auto_merges_per_resolve` | `3` | 一次 resolve 最多自動合併幾筆 |

!!! warning "門檻數字尚未校準"
    預設值是以少量資料推論出的暫定值，沒有用真實 OSINT 語料驗證。生產環境啟用前建議先校準。

### `[auto_approval.llm]`

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `enabled` | `false` | 是否啟用 LLM 輔助評分 |
| `base_url` | `"http://ai-inference:8000/v1"` | OpenAI 相容 API 端點 |
| `model` | `"qwen-primary"` | 模型別名（由推論服務解析） |
| `timeout_secs` | `30` | 單次 LLM 呼叫逾時秒數 |
| `max_concurrent` | `24` | 最大同時進行中的 LLM 請求數（bounded semaphore） |

---

### `[discovery_worker]`

| 鍵 | 預設值 | 說明 |
|---|---|---|
| `bind` | `"127.0.0.1:18089"` | 監聽位址。容器內覆寫為 `0.0.0.0:18089` |
| `consumer_group` | `"osint-discovery-worker"` | Kafka consumer group 名稱 |

---

## 容器與本機的設定差異

容器化部署（`make compose-up-full`）與本機 `cargo run` 的設定**完全不同**，不能混用：

| 設定項 | 本機 `cargo run` | 容器內 |
|---|---|---|
| OpenSearch URL | `http://127.0.0.1:19200` | `http://opensearch:9200` |
| Redpanda brokers | `127.0.0.1:9092` | `redpanda:29092` |
| Neo4j bolt | `bolt://127.0.0.1:7687` | `bolt://neo4j:7687` |
| `OSINT_STRICT_PORT_ISOLATION` | 設 `1` | **絕對不設** |
| bind 位址 | `127.0.0.1:1808x` | `0.0.0.0:1808x` |

!!! danger "容器服務絕對不要套用宿主的 .env"
    `docker-compose.yml` 裡的應用服務不設 `env_file: ../.env`——宿主的 `.env` 包含本機專屬的連線位址（如 `19200`）與 `OSINT_STRICT_PORT_ISOLATION=1`，套進容器會導致連線失敗並產生誤導性的錯誤訊息。
