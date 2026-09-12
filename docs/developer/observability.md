# Observability（V0.1 Phase 2）

- tracing：`tracing-subscriber` 0.3.23 JSON。`RUST_LOG` 可覆寫，預設 `info`。
- metrics：行程內原子計數，`GET /metrics` 輸出 Prometheus text。**沒有** OpenTelemetry exporter。
- 計數名稱對齊 SPEC §24：`collected_total`、`raw_bytes`、`duplicate`、`processing_latency`、`queue_depth`、`failed_jobs`、`connector_errors`、`search_latency`。
- health 埠：`osint-api` `127.0.0.1:18080`、`osint-collector` `18081`、`osint-normalizer` `18082`。本機常見情境：8080 可能被其他本機服務占用（例如另一套安全/情資平台），因此 health 埠改用 18080 以上。

## `/metrics` 的存取控制（Phase 6a 決定：維持公開）

`GET /metrics` **不需要認證**，這是刻意的決定，不是還沒做：

1. Prometheus 的預設 scrape 設定不帶憑證。要求 JWT 等於讓每個佈署都得先處理
   「誰來簽一把永不過期的 scraper token」——那會生出一把比 `/metrics` 本身更值得保護的
   長期憑證。
2. 輸出的是**聚合計數器**，沒有 source 名稱、URL、實體內容或任何識別碼
   （欄位見 `crates/core-observability/src/metrics.rs` 的 `render_prometheus`）。
3. 預設 `[http].bind` 是 `127.0.0.1:18080`，不對外。

### 部署時必須做的事

**量體本身仍是情報**——「今天收集量突然掉到零」「queue_depth 一直在漲」對觀察者有意義。
所以 `/metrics` 要靠網路層限制，不是靠「反正沒人知道路徑」：

- 綁 loopback 或內網介面（預設就是 loopback），或
- 放在只允許 Prometheus 來源 IP 的 reverse proxy 後面。

之後若要加認證，Prometheus 支援 `authorization.credentials_file`，
屆時應做成設定開關而不是硬性要求——不要讓本機開發也得先簽 token。

## 稽核（audit）

Phase 6a 起 `AuditLog` 落地到 Postgres 的 `audit_log` 表。哪些動作會寫、
`ip` 欄位為什麼不讀 `X-Forwarded-For`、以及「沒有保留策略」這個已知限制，
見 `docs/developer/security.md` 的 AuditLog 一節。

## `/api/v1/ops/*`（Phase 6b）

四個健康／指標端點的分工——**它們不可以互相取代**：

| 端點 | 認證 | 回答的問題 | 誰在看 |
|---|---|---|---|
| `GET /health` | 公開 | 行程活著嗎 | liveness probe |
| `GET /ready` | 公開 | 這個行程現在能收流量嗎 | readiness probe、LB |
| `GET /metrics` | 公開 | 聚合計數器（收了幾筆、重複率、佇列深度…） | Prometheus |
| `GET /api/v1/ops/health` | viewer+ | **整套系統**哪一塊壞了 | 運維的人 |
| `GET /api/v1/ops/metrics` | viewer+ | **這個行程**吃了多少記憶體／CPU | 運維的人 |

### 為什麼 Redis／Redpanda 不放進 `/ready`

`/ready` 決定的是「要不要把流量送進來」。API 自己不需要 Redis 與 Redpanda
（它們是 collector／worker 的節流快取與事件匯流排），把它們塞進 `/ready`
會讓「Redis 掛了」變成「API 不接受任何查詢」——而那時 API 其實還能好好地
回答 `GET /objects`。

反過來說，運維**必須**看得到它們，因為 pipeline 會因為它們掛掉而變慢卻不報錯。
所以它們出現在 `/api/v1/ops/health`。

### 為什麼 ops 端點需要認證，`/metrics` 卻公開

`/metrics` 輸出的是沒有識別碼的聚合計數器（理由見上方一節）。
`/api/v1/ops/health` 輸出的是**後端拓樸與故障點**：哪些後端存在、哪一個現在是壞的。
那對外部觀察者是有價值的情報（「他們的搜尋投影掛了」＝「現在推進去的東西不會被索引」），
所以它要 viewer 以上。

### 新增一個後端要改哪裡

`main.rs` 裡多包一個 `BackendCheck::new("<名字>", Arc::new(<adapter>))` 進
`health_checks`，連不上時 `missing.push("<名字>")`。
**`ops.rs` 不需要改**——它只認 `HealthProvider`，不知道後面是什麼
（CLAUDE.md §13：Domain Service → capability interface → concrete adapter）。

漏掉 `missing.push` 的後果是：那個後端在 `/ops/health` 上**完全不存在**，
既不是綠的也不是紅的，回應看起來一切正常。
