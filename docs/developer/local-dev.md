# Local development environment（V0.1 Phase 3 collector／normalizer／Static Web／REST）

## 啟動基礎建設

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml up -d
```

服務與預設埠：

| 服務 | Image | 主機埠（canonical） | 本機 dev override |
|---|---|---|---|
| postgres | postgres:17-bookworm | 5432 | 5432 |
| opensearch | opensearchproject/opensearch:2.19.6 | 9200 | 19200 |
| redpanda | redpandadata/redpanda:v26.2.2 | 9092, 8081, 8082, 9644 | 同左 |
| redis | redis:7.4-bookworm | 6379 | 6379 |
| minio | quay.io/minio/minio:RELEASE.2025-09-07T16-13-09Z | 9000, 9001 | 19000, 19001 |
| neo4j | neo4j:5.26.30-community | 7474, 7687 | 同左 |

`docker-compose.dev.yml` 的記憶體上限：postgres 1GB、**opensearch 2GB（heap 1g）**、
redpanda 512MB、redis 256MB、minio 512MB、**neo4j 1GB（heap 512m + pagecache 256m）**，
合計 5.25GB / 4.5 CPU。明細見內部架構文件 RESOURCE_BUDGET.md §5.1（未隨原始碼公開）。

⚠️ OpenSearch 在 V0.2 Phase 0a 從 1GB／heap 512m 升到 2GB／heap 1g。
**這不是預留餘裕**：低於這個值 embedding 模型一定 deploy 不起來，
而且容器不會掛、healthcheck 全綠（`docs/developer/embedding.md` §4）。

OpenSearch / MinIO 改掛 19200 / 19000 是因為本機 9200、9000 已被其他堆疊佔用。canonical 埠仍寫在 `docker-compose.yml` 與 `config/default.toml`。

## 本機 9200 / 9000 可能被其他服務佔用

本機常見情境：9200/9000 被另一套本機服務（例如安全/情資平台）佔用。本機開發改用偏移埠 19200/19000 以避免衝突：

| 主機埠 | 本專案角色 |
|---|---|
| 9200 | **本機 dev 不用**（可能被其他本機服務佔用） |
| 9000 | **本機 dev 不用**（可能被其他本機服務佔用） |
| **19200** | osint-core OpenSearch（`osint-core-opensearch-1`） |
| **19000** | osint-core MinIO（`osint-core-minio-1`） |

用 `config/default.toml` 的 9200 / 9000 去連，可能會誤寫進宿主上的其他服務。

實務：

1. 從 `.env.example` 複製出 **真實** `.env`（已 gitignore）。
2. 設 `OPENSEARCH_URL=http://127.0.0.1:19200`、`S3_ENDPOINT=http://127.0.0.1:19000`。
3. 建議同時設 `OSINT__STORAGE__SEARCH__URL` 與 `OSINT__STORAGE__OBJECT__ENDPOINT`，覆蓋 default.toml。
4. 寫入前可用 `docker ps` 確認容器名。OpenSearch GET `/` 應出現 `version.distribution=opensearch`，**不可**出現 Elasticsearch tagline `You Know, for Search`。

storage conformance 對非 19200 / 19000 的 URL **硬失敗**。

## 全容器化：`make compose-up-full`

上面那套是「基礎建設在容器裡、七個服務在本機 `cargo run`」。Phase 7a 之後也可以
把七個服務一起放進容器：

```bash
make compose-up-full      # 含 --build --wait，全部 healthy 才返回
make compose-ps-full      # 看狀態
make compose-down-full    # 停掉
```

七個應用服務在 compose 裡標了 `profiles: ["app"]`，**預設不啟動**。
`make compose-up`／`make compose-down`／CI 的 integration-test 行為完全不變
（它們不帶 `--profile app`，所以不會被迫先 build image）。

| 服務 | image | 主機埠 | health |
|---|---|---|---|
| osint-api | `osint-core/osint-api:0.1.0` | 18080 | `/health`、`/ready`、`/metrics` |
| osint-collector | `osint-core/osint-collector:0.1.0` | 18081 | 同上 |
| osint-normalizer | `osint-core/osint-normalizer:0.1.0` | 18082 | 同上 |
| osint-deduplicator | `osint-core/osint-deduplicator:0.1.0` | 18083 | 同上 |
| osint-entity-worker | `osint-core/osint-entity-worker:0.1.0` | 18084 | 同上 |
| osint-indexer | `osint-core/osint-indexer:0.1.0` | 18085 | 同上 |
| osint-graph-worker | `osint-core/osint-graph-worker:0.1.0` | 18086 | 同上 |

⚠️ 容器版與 `cargo run` 版**不能同時跑**，兩者搶同一組 18080–18086。

### 本機跑 vs 容器內跑：設定完全不一樣

這是最容易踩的一格。同一個邏輯後端，兩邊的位址不同：

| 後端 | 本機 `cargo run`（讀 `.env`） | 容器內（compose `environment:`） |
|---|---|---|
| PostgreSQL | `127.0.0.1:5432` | `postgres:5432` |
| OpenSearch | `127.0.0.1:**19200**` | `opensearch:**9200**` |
| MinIO | `127.0.0.1:**19000**` | `minio:**9000**` |
| Redis | `127.0.0.1:6379` | `redis:6379` |
| Redpanda | `127.0.0.1:**9092**` | `redpanda:**29092**` |
| Neo4j | `127.0.0.1:7474`／`7687` | `neo4j:7474`／`7687` |
| `OSINT_STRICT_PORT_ISOLATION` | `1` | **不設** |
| bind | `127.0.0.1:1808x` | `0.0.0.0:1808x` |

三個一定要理解的原因：

1. **19200/19000 是宿主端的規避手段**。本機常見情境是 9200/9000 被其他本機服務占用（例如另一套安全/情資平台），所以本機開發改用 19200/19000。compose network 內沒有這個衝突，`opensearch:9200` 就是本專案自己的服務。
2. **`OSINT_STRICT_PORT_ISOLATION` 在容器裡絕對不能設**。它會讓 `verify_not_opencti_search`（函式名稱裡的 `opencti` 反映了原始撰寫時的本機衝突對象，函式名稱本身沒有改）擋下任何指向 9200 的 URL，而且錯誤訊息會叫你「改成 http://127.0.0.1:19200」——那在容器內是完全錯誤的建議。同理，**不要對應用服務用 `env_file: ../.env`**。
3. **Redpanda 在容器內是 29092，不是 9092**。見下一節。

### Redpanda 為什麼容器內要用 29092

`redpanda` 服務開了兩個 Kafka listener：

```text
external → 0.0.0.0:9092，宣告 127.0.0.1:9092   → 宿主（cargo run、CI）
internal → 0.0.0.0:29092，宣告 redpanda:29092  → compose network 內的容器
```

2026-09-12 實測：原本只有單一 listener 時，broker 宣告的位址是
`127.0.0.1:9092`。容器內的服務拿到這份 metadata 後會去連**自己**的 127.0.0.1，
結果是 `AllBrokersDown` → `MessageTimedOut`。

**這個故障不會讓健康檢查變紅**，這是它真正危險的地方：bootstrap 位址
`redpanda:9092` 連得上，metadata 也查得到，`/api/v1/ops/health` 會回
「redpanda：metadata 可取得，1 個 broker、57 個 topic」一切正常。
只有實際 produce/consume 會逾時。對使用者的表現是
「`POST /import` 回 201，但文件永遠不會出現在搜尋結果裡」。

external 維持 9092 是刻意的：`.env`、`config/default.toml`、CI 的
`REDPANDA_BROKERS` 全指著 9092，換掉會波及整個測試套件。

### Neo4j（V0.2 圖投影）

```bash
make compose-up          # neo4j 沒有 profile，跟其他五個一起起
curl -s http://127.0.0.1:7474/     # 200 + discovery JSON
```

| 項目 | 值 |
|---|---|
| Image | `neo4j:5.26.30-community`（釘版，2026-09-12 於 Docker Hub 確認存在） |
| HTTP | 7474（探活 + Browser `http://127.0.0.1:7474/browser/`） |
| Bolt | 7687 |
| Volume | `osint-core_neo4j-data`（`make disk` 會列出來） |
| 空資料庫的磁碟成本 | **516 MB**，見下方 |
| Plugins | **無**。`NEO4J_PLUGINS: "[]"`，V0.2 用不到 APOC/GDS |
| 授權 | GPLv3（Community）。決策（ADR-010）：Neo4j Community 作為獨立容器以網路協議連接，Rust 程式碼不連結也不修改 Neo4j，GPLv3 的 copyleft 不延伸到本專案的 Rust crate |

`.env` 需要：

```bash
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<至少 8 個字元>
OSINT__STORAGE__GRAPH__HTTP_URL=http://127.0.0.1:7474
```

⚠️ **`NEO4J_USER` 只能是 `neo4j`。** Community 版的 `NEO4J_AUTH` 只能初始化
內建的 `neo4j` 使用者；寫成別的名字**容器照樣啟動、照樣 healthy**，
只是那個帳號從來沒被建立，你會在登入時拿到「密碼錯誤」而不是任何
關於使用者名稱的提示。要別的帳號得先用 `neo4j` 登入再以 Cypher 建立。

⚠️ 密碼**至少 8 個字元**，太短容器會啟動失敗。

#### ⚠️ 一個**完全空的** Neo4j 就吃掉 516 MB 磁碟

2026-09-12 實測，`osint-core_neo4j-data` 在沒有任何資料時的拆解：

| 路徑 | 大小 |
|---|---|
| `transactions/neo4j/neostore.transaction.db.0` | 256 MB |
| `transactions/system/neostore.transaction.db.0` | 256 MB |
| `databases/` | 2.1 MB |
| 合計 | **516 MB** |

Neo4j 預設會**預先配置**交易記錄檔（rotation size 256 MB × 兩個資料庫：
`neo4j` 與內建的 `system`）。所以「volume 有 516 MB」**不代表裡面有資料**，
也不是洩漏——是預配置。第一次看 `make disk` 會覺得數字不合理，這就是原因。

CLAUDE.md §15 的磁碟紀律：這 516 MB 是常駐的，要算進共用工作站的預算。

驗證資料庫真的可用（不只是 HTTP 埠活著）：

```bash
docker exec osint-core-neo4j-1 cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "RETURN 1 AS ok;"
```

#### `/ops/health` 同時有 HTTP 探活與 Bolt Cypher

`GET /api/v1/ops/health` 的 `neo4j` 那一項只打 HTTP `GET /`（7474），
**沒有建立 Bolt 連線**。7474 的 discovery 端點不需要認證。

回應長這樣（實測）：

```json
{"name":"neo4j","healthy":true,"message":"HTTP 200 OK，Neo4j 5.26.30 community"}
```

⚠️ **這只證明 HTTP 埠活著，不證明 Bolt 可連或資料庫可寫。**
Neo4j 在還原中、或某個 database 處於 `offline`／`failed` 時，7474 仍會回 200。

同一份 `checks` 裡另有 `neo4j_bolt`：用 `storage-neo4j` 的 `Neo4jStore`
跑一次 `RETURN 1`。需要 `[storage.graph]` 的帳密。兩個檢查各自獨立。

圖投影 lag／rebuild 看 `GET /api/v1/ops/graph`，不看這兩筆探活。

`OSINT__STORAGE__GRAPH__HTTP_URL` 留空字串 = 刻意未設定，
`/ops/health` 會把 `neo4j` 列進 `not_configured` 而不是 `unhealthy`
（沿用 V0.1 的慣例：「沒設定」與「壞了」下一步不同）。
Bolt 連不上時 `neo4j_bolt` 也會列進 `not_configured`。

### Embedding 模型（V0.2 semantic search 的前提）

```bash
bash scripts/opensearch-ml-setup.sh     # 冪等；已部署時 0.2 秒結束
```

在 OpenSearch ml-commons 上註冊並部署 `all-MiniLM-L6-v2`（ONNX，384 維）。

⚠️ 首次執行會讓 **OpenSearch 容器**往外抓約 600 MB（模型 92 MB +
DJL 的 PyTorch native libs 507 MB），OpenSearch volume 會從 2 MB 長到約 730 MB。
這是常駐的，不是磁碟洩漏。

⚠️ 需要 OpenSearch 容器 2 GB / heap 1 GB（dev override 已設）。
不足時 deploy 失敗在「Memory Circuit Breaker is open」，
**容器不會掛、healthcheck 全綠**，只有模型永遠起不來。

實測數據（記憶體、延遲、磁碟拆解、中文語意品質的已知不足）全部在
`docs/developer/embedding.md`。

### 最小驗證流程（實測通過）

```bash
make compose-up-full

# 七個 health 埠
for p in 18080 18081 18082 18083 18084 18085 18086; do curl -s localhost:$p/health; echo; done

# 後端連通性（需要 JWT；role 至少 viewer）
curl -s -H "Authorization: Bearer $JWT" localhost:18080/api/v1/ops/health

# import → normalizer → deduplicator → entity-worker → indexer → search
curl -s -X POST localhost:18080/api/v1/sources -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke","source_type":"json_import","enabled":true,"collection_policy":{}}'
curl -s -X POST localhost:18080/api/v1/import -H "Authorization: Bearer $JWT" \
  -F 'request={"source_id":"<id>","kind":"json"};type=application/json' \
  -F 'file=@smoke.json'
curl -s -X POST localhost:18080/api/v1/search -H "Authorization: Bearer $JWT" \
  -H 'Content-Type: application/json' -d '{"query":"smoke test"}'
```

⚠️ `POST /api/v1/search` 的欄位是 **`query`**，不是 `q`。給 `q` 會拿到
400「unknown field」。用 `{"total": 0}` 當成功判準時要先確認 HTTP 狀態碼，
否則會把一個打錯的欄位名誤判成「管線沒有把文件送進 index」。

## Migration

```bash
make migrate-postgres
make migrate-sqlite
```

SQLite URL 必須帶 `mode=rwc`（已寫在 Makefile），否則空路徑會回 `unable to open database file`。`sqlx` 裝在 `~/.cargo/bin`；Makefile 會把該目錄加進 `PATH`。

PostgreSQL DSN 預設：

```text
postgres://osint:osint_dev@127.0.0.1:5432/osint_core
```

SQLite 不是 Core canonical store（詳見內部架構文件 STORAGE_ARCHITECTURE.md，未隨原始碼公開）。

## Storage conformance

需要已啟動的 compose 服務與正確的 `.env`：

```bash
cargo test --workspace --all-targets -- --nocapture
```

Postgres 測試會插入 UUID v7 列，不會 TRUNCATE。OpenSearch 用一次性 index 名；S3 用 `conformance/<uuid>/` prefix。

Adapter 細節見 `docs/developer/storage-adapters.md`。Connector SDK／RSS 見 `docs/developer/connector-sdk.md`。`source_network_rules` 在 `migrations/*/0002_source_network_rules.sql`。

## API skeleton

```bash
export JWT_SECRET=change-me-to-a-random-32-byte-minimum-secret
cargo run -p core-api --bin osint-api
curl -s http://127.0.0.1:18080/health
curl -s http://127.0.0.1:18080/ready
curl -s http://127.0.0.1:18080/metrics
```

JWT secret 至少 32 bytes，來自 `env:JWT_SECRET`。`.env` 裡 `OSINT_CONFIG_FILE=` 空字串會被忽略，不會當成必填檔。細節：`docs/developer/api-skeleton.md`。

Redpanda round-trip：`cargo test -p core-events --test redpanda_roundtrip`。

## Collector／Normalizer

```bash
make run-collector
make run-normalizer
curl -s http://127.0.0.1:18081/health
curl -s http://127.0.0.1:18082/health
```

設定在 `config/default.toml` 的 `[collector]`／`[normalizer]`。health 埠 18081／18082，刻意避開可能被其他本機服務占用的 8080。垂直切片測試：

```bash
cargo test -p normalizer --test e2e -- --nocapture --test-threads=1
```

push 路徑（`POST /api/v1/import`：Manual／JSON／CSV）的上限設定在 `config/default.toml`
的 `[import]`，端到端測試：

```bash
cargo test -p core-api --test import_e2e
```

細節：`docs/developer/collector-normalizer.md`、`docs/developer/import-api.md`。

## Deduplicator／Entity Worker

```bash
make run-deduplicator     # 訂閱 object.normalized → dedup.completed
make run-entity-worker    # 訂閱 dedup.completed  → entity.extracted
curl -s http://127.0.0.1:18083/health
curl -s http://127.0.0.1:18084/health
```

設定在 `config/default.toml` 的 `[deduplicator]`／`[entity_worker]`，health 埠 18083／18084。
兩者的上限（`candidate_limit`／`simhash_scan_limit`／`max_extractions`／`max_scan_bytes`）
都是硬上限，沒有「不限」這個選項。

端到端測試（對本機 Docker 真跑，共用同一顆 Postgres，所以用單執行緒）：

```bash
cargo test -p deduplicator  --test e2e -- --test-threads=1
cargo test -p entity-worker --test e2e -- --test-threads=1
```

## Indexer／Search API

```bash
make run-indexer          # 訂閱 entity.extracted → OpenSearch osint-documents
curl -s http://127.0.0.1:18085/health
make rebuild-index        # 從 PostgreSQL 補齊投影（PostgreSQL 是 truth）
make rebuild-index-drop   # 先刪 index 再從零重建（mapping 有破壞性變更時）
make run-cli ARGS="search 'ransomware' --json"
```

設定在 `config/default.toml` 的 `[indexer]`，health 埠 18085。
`batch_size`／`batch_timeout_ms`／`max_field_bytes`／`lag_threshold` 都是硬上限。
**`batch_timeout_ms` 不可設 0**——低流量時最後幾筆會永遠不進 index。

`core-api` 與 `osint-indexer` 都讀 `[indexer].index`。兩邊不一致的話搜尋會查到空的
index，而且不會報錯。

端到端測試（對本機 Docker 真跑；每個測試用自己的一次性 index，跑完會刪掉）：

```bash
cargo test -p indexer  --test e2e            -- --test-threads=1
cargo test -p core-api --test search_api_e2e -- --test-threads=1
cargo test -p storage-opensearch --test conformance
cargo test -p storage-opensearch --test embedding_conformance
```

⚠️ **測試失敗（panic）時 index 不會被刪**——清理寫在測試結尾，panic 會跳過它。
一次一個、每個約 15 KB，累積起來就是磁碟洩漏（CLAUDE.md §15）。
測試失敗後順手清一下：

```bash
curl -s 'http://127.0.0.1:19200/_cat/indices?h=index' \
  | grep -E '^(osint-core-conformance-|osint-documents-e2e-|osint-documents-api-e2e-|osint-documents-failrec-)' \
  | xargs -r -I{} curl -s -XDELETE 'http://127.0.0.1:19200/{}' > /dev/null
```

（前綴刻意與正式的 `osint-documents` 區隔，上面的 grep 不會命中它。）

### ⚠️ 磁碟滿時 OpenSearch 會擋掉建立 index

主機磁碟使用率超過 OpenSearch 的 **high watermark（預設 90%）** 時，叢集會自動設
`cluster.blocks.create_index=true`，於是建立 index 一律回
**403 `index_create_block_exception`**（訊息：`cluster create-index blocked (api)`）。

**這不是程式的問題**，但錯誤訊息完全不提磁碟，很容易被當成 mapping 或權限問題。
先確認：

```bash
curl -s 'http://127.0.0.1:19200/_cluster/settings?flat_settings=true&include_defaults=true' \
  | grep -o '"cluster.blocks.create_index":[^,]*'
curl -s 'http://127.0.0.1:19200/_cat/allocation?v'     # 看 disk.percent
```

**正解是清出磁碟空間**：`auto_release=true`，降到門檻以下後會自動解除
（約一個 `cluster.info.update.interval`，預設 30 秒）。

真的要先跑測試的話，可以暫時放寬 watermark（**transient，容器重啟即失效**；
這只影響我們自己的 `osint-core-opensearch-1`，與宿主上其他 Elasticsearch 服務無關）：

```bash
# 放寬
curl -s -XPUT 'http://127.0.0.1:19200/_cluster/settings' -H 'Content-Type: application/json' \
  -d '{"transient":{"cluster.routing.allocation.disk.watermark.low":"92%",
       "cluster.routing.allocation.disk.watermark.high":"95%",
       "cluster.routing.allocation.disk.watermark.flood_stage":"97%"}}'

# 還原成預設
curl -s -XPUT 'http://127.0.0.1:19200/_cluster/settings' -H 'Content-Type: application/json' \
  -d '{"transient":{"cluster.routing.allocation.disk.watermark.low":null,
       "cluster.routing.allocation.disk.watermark.high":null,
       "cluster.routing.allocation.disk.watermark.flood_stage":null}}'
```

放寬只是把「寫不進去」往後推，磁碟真的滿了還是會在 flood_stage 變成唯讀。

細節：`docs/developer/indexer.md`、`docs/developer/search-api.md`、`docs/user/search.md`。

## Graph worker

```bash
cargo run -p graph-worker --bin osint-graph-worker
cargo run -p graph-worker --bin osint-graph-worker -- --rebuild
cargo run -p graph-worker --bin osint-graph-worker -- --rebuild --drop
curl -s http://127.0.0.1:18086/health
```

設定在 `config/default.toml` 的 `[graph_worker]`，health 埠 18086。
**只有 Entity→Entity 的邊才進 Neo4j**；Document→Entity（例如 `mentions`）會被跳過。
尚未進 compose；Operations Center 整合（Step 6）之前用本機 `cargo run`。

```bash
cargo test -p graph-worker --all-targets
cargo test -p storage-neo4j --all-targets
```

細節：`docs/developer/graph-worker.md`。

細節：`docs/developer/deduplicator.md`、`docs/developer/entity-worker.md`。

## Failure／recovery 測試（會真的停掉 Docker 服務）

`TEST_STRATEGY.md` §6 的四項：broker redelivery、DB 暫時失效、search unavailable、
object storage unavailable。檔案是 `crates/acceptance/tests/failure_recovery.rs`。

```bash
cargo test -p acceptance --test failure_recovery -- --ignored --test-threads=1
```

⚠️ **跑的當下不要同時跑其他測試。** 這四個測試會 `docker compose stop`
postgres／opensearch／minio，跑 workspace 測試的話會被連帶弄壞，
而失敗訊息完全指不到真正的原因。

* 全部標 `#[ignore]`，所以 `cargo test --workspace` **不會**跑到它們，要加 `--ignored`。
* 每個測試都用 `ServiceGuard`（`Drop` 會 `docker compose start` 並輪詢到 healthy），
  **panic 也會把服務起回來**——實測過：故意在停掉 postgres 之後 panic，
  測試失敗後 `docker compose ps` 顯示 postgres `Up 5 seconds (healthy)`。
* 用的是 `stop`／`start`，不是 `down`：`down` 會清掉其他測試累積的資料。
* 實測數據：PG 停掉時 `GET /api/v1/sources` 在 **5.00 秒**回 **503**
  （5 秒＝`PostgresCanonicalStore::connect` 的 `acquire_timeout`；
  資源類 handler 把非客戶端錯誤一律收斂成 503）。整組四個測試約 58 秒。

萬一測試被強制中斷（Ctrl-C）導致服務沒起回來：

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml \
  start postgres opensearch minio
```
