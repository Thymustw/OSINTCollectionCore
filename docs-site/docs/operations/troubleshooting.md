# 疑難排解

這一頁整理常見的問題症狀、原因與解法。

---

## 怎麼看 log

所有應用服務的 log 都是 JSON 格式，可以用 `docker logs` 查看：

```bash
docker logs osint-core-osint-api-1
docker logs osint-core-osint-embedding-worker-1
docker logs osint-core-osint-indexer-1
# 依此類推，其他服務把名稱換掉
```

加 `-f` 可以持續追蹤；加 `--tail 50` 只看最後 50 行。

若要只看錯誤：

```bash
docker logs osint-core-osint-normalizer-1 2>&1 | grep '"level":"ERROR"'
```

---

## 常見問題

### `embedding-worker` 一直重啟

**症狀**：`make compose-ps-full` 看到 `osint-embedding-worker` 不是 `(healthy)` 或一直重啟，
log 有 `Memory Circuit Breaker is open` 或模型相關錯誤。

**原因**：語意搜尋模型沒有部署，或 OpenSearch 重啟後模型的「已部署」狀態遺失。

**解法**：重新執行兩支部署腳本（冪等，已部署的情況下會直接跳過）：

```bash
bash scripts/opensearch-ml-setup.sh
bash scripts/opensearch-ml-setup-e5.sh
```

等兩支腳本都以 `== 完成。model_id=...` 結束後，`embedding-worker` 應該在幾十秒內恢復。

---

### `opensearch-ml-setup-e5.sh` 失敗：「臨時 HTTP 服務起不來」

**症狀**：執行 `bash scripts/opensearch-ml-setup-e5.sh` 失敗，錯誤訊息提到臨時 HTTP 服務無法啟動。

**原因**：腳本預設用 18099 埠提供臨時 HTTP 服務讓 OpenSearch 容器下載模型。若該埠已被佔用（例如同時有其他程式在跑），腳本會失敗。

**解法**：換一個空閒的埠：

```bash
E5_SERVE_PORT=18199 bash scripts/opensearch-ml-setup-e5.sh
```

---

### 部署模型時出現「推論被斷路器擋下（HTTP 429）」

**症狀**：執行模型部署腳本時，冒煙測試階段出現：

```text
   推論暫時被斷路器擋下（第 1 次），10 秒後重試…
```

**這通常是正常的，不用處理。** 模型剛部署完，OpenSearch 的記憶體用量會短暫衝高，
推論請求被記憶體保護機制（斷路器）暫時擋下。腳本會自動等待並重試，通常一兩次後就會通過。
OpenSearch 剛重新啟動、而且裡面已經有大量資料時特別容易出現。

**只有這種情況才需要處理**：重試 6 次（約 1 分鐘）後仍然失敗，出現
`重試 6 次（約 1 分鐘）仍然如此`。這代表記憶體持續不足，請確認 OpenSearch 的記憶體設定沒有被改小：

```bash
docker inspect osint-core-opensearch-1 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep JAVA_OPTS
```

應該看到 `-Xms1536m -Xmx1536m`（兩個模型並存需要的 heap）。如果是 `-Xmx1g` 或更小，兩個模型
並存時推論會一直被擋。

---

### API 回 401

**症狀**：所有 API 請求都回傳 `401 Unauthorized`。

**原因**：開發憑證的有效期是 1 小時，過期後需要重新產生。

**解法**：

```bash
export TOKEN=$(python3 scripts/dev-token.py)
```

---

### 搜尋回 422 `unknown field`

**症狀**：`POST /api/v1/search` 回傳 422，錯誤訊息提到 `unknown field`。

**原因**：搜尋請求的欄位名寫錯了。查詢關鍵字的欄位名是 `query`，不是 `q`。

**解法**：

```bash
# 錯誤寫法（會得到 400）
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"q":"CVE-2026-31337"}'

# 正確寫法
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"query":"CVE-2026-31337"}'
```

---

### 匯入後查不到文件

**症狀**：`POST /api/v1/import` 成功回 `record_count: 2`，但之後搜尋或列出文件找不到。

**原因**：匯入後有一條處理流程（正規化 → 去重 → 抽取實體 → 建搜尋索引），這條流程是非同步的，第一次啟動時可能需要 30 秒以上。

**解法**：等待後再試。若等了 1 分鐘仍然找不到，檢查各 worker 的 log：

```bash
docker logs osint-core-osint-normalizer-1 --tail 20
docker logs osint-core-osint-indexer-1 --tail 20
```

也可以查佇列積壓狀態：

```bash
curl -s $API/ops/queues -H "Authorization: Bearer $TOKEN"
```

若 `lag` 不是 0，代表消費者還在處理中；若 `lag` 是 0 但仍找不到，代表處理過程可能出錯。

---

### 埠號衝突

**症狀**：`make compose-up` 或 `make compose-up-full` 失敗，錯誤訊息包含 `address already in use`。

**原因**：系統用到以下宿主埠，其中有些可能與本機其他服務衝突：

| 埠號 | 服務 |
|---|---|
| 5432 | PostgreSQL |
| 7474 | Neo4j Browser |
| 7687 | Neo4j Bolt |
| 8081、8082 | Redpanda admin UI / proxy |
| 8333 | SeaweedFS S3 |
| 9092 | Redpanda Kafka（宿主） |
| **19200** | OpenSearch（開發環境改掛此埠） |
| 6379 | Redis |
| 18080–18089 | 10 個應用服務 |

**解法**：找出佔用的行程後停掉，或修改 `docker-compose.dev.yml` 改掛其他埠（需要同步修改 `.env` 的對應設定）。

---

### OpenSearch 磁碟滿，搜尋索引變唯讀

**症狀**：新匯入的資料搜尋不到，`indexer` 的 log 出現 `cluster_block_exception` 或 `index write blocked`。

**原因**：OpenSearch 預設在磁碟使用率超過 95% 時觸發 flood-stage watermark，禁止新的索引寫入（`cluster.blocks.create_index`），但不影響查詢。

**解法**：

1. 釋放磁碟空間（清理 Docker 映像、`target/` 等）
2. 解除 OpenSearch 的寫入封鎖：

```bash
curl -s -X PUT "http://127.0.0.1:19200/_cluster/settings" \
  -H 'Content-Type: application/json' \
  -d '{"persistent":{"cluster.routing.allocation.disk.watermark.flood_stage":"99%"}}'
```

3. 解除後重跑 `make rebuild-index` 補回遺失的文件。

---

### 容器內服務連不上 Redpanda

**症狀**：容器內的 worker 出現 `AllBrokersDown` 或 `MessageTimedOut`，但 `/api/v1/ops/health` 回報 broker 正常。

**原因**：容器內的服務必須用 `redpanda:29092`（容器內部 listener），不能用 `9092`（給宿主用的 listener，宣告位址是 `127.0.0.1:9092`，容器連上去會指向自己）。

**解法**：確認應用服務的環境變數 `OSINT__BROKER__BROKERS` 設的是 `redpanda:29092`，
且沒有套用宿主的 `.env` 檔（容器不應該設 `env_file: ../.env`）。

---

### 容器啟動失敗，錯誤提到 `verify_not_opencti_search`

**症狀**：應用服務啟動失敗，log 有 `verify_not_opencti_search` 相關錯誤，並建議把 URL 改成 `http://127.0.0.1:19200`。

**原因**：容器裡套用了宿主的 `.env` 檔，裡面設了 `OSINT_STRICT_PORT_ISOLATION=1`。這個設定是給宿主環境用的（避免誤連到宿主上其他服務的 Elasticsearch），容器內不應該設。

**解法**：移除應用服務的 `env_file: ../.env` 設定；容器內的連線設定由 `docker-compose.yml` 的 `environment:` 區段提供。
