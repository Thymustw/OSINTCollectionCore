# 健康檢查與監控

這一頁說明怎麼確認系統正常運作，以及出了問題時從哪裡看。

---

## 每個服務的三個端點

所有 10 個應用服務（埠 18080–18089）各自提供三個不需要認證的端點：

| 端點 | 用途 | 典型回應 |
|---|---|---|
| `GET /health` | **存活探針**：行程活著嗎 | `{"status":"ok"}` |
| `GET /ready` | **就緒探針**：這個行程能收流量嗎（依賴的後端就緒了嗎） | `{"ready":true,"checks":[...]}` |
| `GET /metrics` | **Prometheus 格式指標**：計數器、佇列深度、延遲等 | Prometheus text 格式 |

一次確認 10 個服務是否存活：

```bash
for p in 18080 18081 18082 18083 18084 18085 18086 18087 18088 18089; do
  printf ":%s  " $p; curl -s localhost:$p/health
  echo
done
```

每一行都應該是 `{"status":"ok"}`。

就緒探針的回應範例（以 osint-api 為例）：

```bash
curl -s http://127.0.0.1:18080/ready
```

```json
{"ready":true,"checks":[{"name":"postgres","healthy":true,"message":"SELECT 1 成功"}]}
```

!!! note "/metrics 不需要認證"
    這是刻意的設計：輸出的是聚合計數器（沒有實體內容或識別碼），且預設綁在 loopback，
    不對外暴露。若部署到多機器環境，需要靠網路層限制存取（reverse proxy 或來源 IP 限制），
    不要依賴「沒人知道路徑」。

---

## 整套系統健康狀態（維運 API）

公開的 `/health` 只告訴你這個行程活著，不告訴你後端出了什麼問題。
整套系統的狀態由 `osint-api` 的維運 API 提供，**需要 viewer 以上的 API 憑證**。

```bash
export TOKEN=$(python3 scripts/dev-token.py)
curl -s http://127.0.0.1:18080/api/v1/ops/health \
  -H "Authorization: Bearer $TOKEN"
```

回應範例：

```json
{
  "healthy": true,
  "checks": [
    {"name":"postgres","healthy":true,"message":"SELECT 1 成功"},
    {"name":"object_store","healthy":true,"message":"bucket 存在"},
    {"name":"neo4j_bolt","healthy":true,"message":"RETURN 1 成功"},
    {"name":"opensearch","healthy":true,"message":"cluster status=green"},
    {"name":"redis","healthy":true,"message":"PING PONG"},
    {"name":"redpanda","healthy":true,"message":"metadata 可取得：1 個 broker、128 個 topic"},
    {"name":"neo4j","healthy":true,"message":"HTTP 200 OK"}
  ],
  "unhealthy": [],
  "not_configured": []
}
```

---

## 維運 API 總覽

以下端點都需要 viewer 以上的 API 憑證（`Authorization: Bearer <token>`）：

| 端點 | 說明 |
|---|---|
| `GET /api/v1/ops/health` | 各後端（PostgreSQL、OpenSearch、Neo4j、Redis、Redpanda、物件儲存）的連線狀態 |
| `GET /api/v1/ops/metrics` | 這個行程的資源用量（記憶體、CPU 等） |
| `GET /api/v1/ops/connectors` | 各 Connector 的採集狀態與最近一次執行結果 |
| `GET /api/v1/ops/queues` | 各消費者的 Kafka 消費進度（lag） |
| `GET /api/v1/ops/dlq` | 失敗 Job 列表（目前不設獨立 DLQ topic，失敗 Job 可用 `POST /api/v1/jobs/{id}/retry` 重試） |
| `GET /api/v1/ops/failed-events` | 處理失敗的事件記錄 |
| `GET /api/v1/ops/graph` | 圖投影（Neo4j）的 lag 與最近一次 rebuild 狀態 |
| `GET /api/v1/ops/discovery` | Discovery 的 AI 並發上限、Candidate 各狀態數量、最近 AI Run 歷史 |

重送失敗事件需要 operator 以上：

```
POST /api/v1/ops/failed-events/{id}/replay
```

---

## 佇列健康（消費進度）

```bash
curl -s http://127.0.0.1:18080/api/v1/ops/queues \
  -H "Authorization: Bearer $TOKEN"
```

回應包含每個消費者的 `lag`（積壓未處理的訊息數）。
`lag` 持續上升代表消費者跟不上生產速度；持平在 0 代表正常。

---

## 重建投影

搜尋索引（OpenSearch）與關聯圖（Neo4j）都是從 PostgreSQL 推導出來的投影，可以完整重建。

!!! warning "重建指令需要 Rust 與本機 .env"
    以下指令使用 `make`，背後執行 `cargo run`，需要本機有 Rust toolchain 並設好 `.env`。
    純容器化環境目前不直接支援 rebuild 觸發；可以透過 API 的 `POST /api/v1/graph/rebuild`
    觸發圖的重建。

### 搜尋索引

```bash
make rebuild-index         # 增量重建（不刪舊資料）
make rebuild-index-drop    # 刪掉舊 index 後從零重建（mapping 有破壞性變更時使用）
```

### 關聯圖

```bash
make rebuild-graph         # 增量重建
make rebuild-graph-drop    # 清空所有節點與邊後從零重建
```

也可以用 API 觸發（operator 以上）：

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/graph/rebuild \
  -H "Authorization: Bearer $TOKEN"
```

### 語意向量

```bash
make rebuild-embeddings         # 增量重建
make rebuild-embeddings-drop    # 刪掉 osint-entities index 後從零重建
```

---

## 稽核紀錄

所有寫入操作（建立來源、匯入、合併實體…）都會寫進 PostgreSQL 的 `audit_log` 表，記錄動作、操作者與時間。目前沒有透過 API 查詢 audit log 的端點，需要直接查資料庫。
