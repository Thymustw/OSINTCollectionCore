# Collector 與 Normalizer（V0.1 Phase 3 後半）

對應內部規格 V0.1 §4／§6／§8／§9／§14／§20／§21（規格文件本身未隨原始碼公開，章節號留著方便查找對應決策）。安全規範對應內部架構文件 CONNECTOR_SECURITY.md 與 ADR-001（未隨原始碼公開）。

這一層完成垂直切片：**排程觸發 → RawEvidence → Document**。Search／dedup／entity extraction 是 Phase 4，不要在這裡實作。

## 為什麼在 `crates/` 而不是 `services/`

本 repo 沒有 `services/`。`core-api` 已經是 crate + `[[bin]]`。collector／normalizer 沿用同一慣例：

```text
crates/collector      bin: osint-collector
crates/normalizer     bin: osint-normalizer
```

## Collector

讀 `enabled=true` 的 connector，依 `schedule` cron 觸發。測試與手動觸發走 `CollectorRunner::run_connector`，不必等 cron（目前沒有公開的 HTTP 手動觸發 API）。

| 項目 | 行為 |
|---|---|
| 已知種類 | `rss`／`atom` → `RssConnector`；`static_web` → `StaticWebConnector`；`rest_api` → `RestApiConnector` |
| 匯入專用類型 | `manual_upload`／`manual`／`json_import`／`csv_import`／`stix_import` 在 tick 直接略過、不打 warn。這些沒有可抓的 URL，不該被排程 |
| 未知 `connector_type` | 非匯入的未知種類記 log 後 `SkippedUnknownType`，**不 panic**。同一 connector 同一原因只第一次 warn |
| cron | `croner` 4.0.0（`default-features=false` + `chrono`）。從未跑過（`last_run` 空）視為到期。**空／空白 schedule = 未排程**，靜默略過，不是錯誤；有值但解析失敗才是錯誤 |
| log | 真正的錯誤（schedule 解析失敗、收集失敗、未知種類）只在狀態改變時 warn／error：`connector.status=error` 且 `checkpoint.last_error` 相同 → debug。恢復成功打 info。每 tick 最多一行摘要（計數變化立刻印，穩定殘留每 5 分鐘一筆）。**log 量不可隨 connector 數無界放大** |
| 併發 | 全域 semaphore + per-domain inflight。`tick` 用 `JoinSet`，spawn 數夾在 `global_inflight`，不可依外部輸入無界 `tokio::spawn` |
| 速率 | SDK `DomainRateLimiter` 另外管 RPS；這層只管「同時幾個在跑」 |
| 成功 | SDK sink 寫 RawEvidence（Postgres metadata + 物件儲存 body），更新 checkpoint／`last_run`／`last_success`，publish `raw.collected`（partition key = `source_id`） |
| 失敗 | `error_count` +1、publish `raw.failed`、單一 connector 失敗不中斷迴圈。之後依 `error_count` 指數退避，本輪到期但仍在窗內 → `BackingOff`（不派、不改 `last_run`） |
| 公平性 | 到期清單依 `(error_count 升冪, last_success 新到舊且 NULL 最後, id)` 排序；每 tick 對 `error_count>0` 最多派 `max_retry_per_tick` 個，超出 → `RetryDeferred` |
| Job | 每次收集建 `job_type=collect`、`correlation_id=connector.id`。Job 轉態失敗只 warn，不中止收集。退避中與延後的本輪不建 job |

預設 `[collector]`（`config/default.toml`）：

```toml
bind = "127.0.0.1:18081"
tick_secs = 5
global_inflight = 4
per_domain_inflight = 1
backoff_base_secs = 300
backoff_max_secs = 43200
max_retry_per_tick = 8
shutdown_drain_secs = 20

[collector.backpressure]
enabled = true
throttle_lag = 5000
pause_lag = 50000
lag_check_interval_secs = 30
probe_timeout_ms = 3000
```

`backoff_base_secs = 0` 關閉退避（公平性上限仍生效）。`max` 必須 ≥ `base`，
`max_retry_per_tick` 必須 ≥ 1，否則設定載入失敗。算法在
`connector_sdk::retry_backoff`：`min(base × 2^(n−1), max)`，再套由
`connector_id` 雜湊決定的 ±10% jitter（可重現，不用亂數）。
下次可執行時間是 cron 下一個時點與 `last_run + retry_backoff` 的較晚者。

立刻結束退避：`POST /api/v1/ops/connectors/{id}/reset-backoff`（operator），
只把 `error_count` 設 0；`status` 若是 `error` 改 `idle`。不清 `checkpoint.last_error`。
PATCH connector body **不能**改 `error_count`。

**`connector.timeout` 目前不生效。** fetch 走 `SourcePolicy` 的固定 connect 5s／
request 30s，欄位會存進 DB 但 collector 沒讀。

`shutdown_drain_secs` 是收到停止訊號後等待 in-flight 自然結束的最長秒數，必須 ≥ 1。
compose `stop_grace_period` 必須比這個值再多 5 秒以上（目前 30s）。逾時 abort，
job 標成 cancelled（`error`＝「collector 停止時中斷」）、connector 回到 idle，
`error_count` 不加。drain 期間 `/health` 維持 200、`/ready` 回 503。啟動時
`reclaim_orphan_collect_runs` 回收上次 SIGKILL 留下的 running collect job
（標 failed，才能進 `/ops/dlq`）與 running connector（改 idle）。單一副本前提。
細節見 `docs/KNOWN_GAPS.md` §18 步驟 4 與 `docs/operations/OPERATIONS.md` §4。

`[collector.backpressure]`：查 `[normalizer].consumer_group` 在 `raw.collected`
上的 lag。`throttle_lag < pause_lag`、間隔與逾時必須 > 0，否則設定載入失敗。
`enabled=false` 完全不查 broker。pause 時 `/ready` **不**變 503（暫停派工不是
不就緒）。⚠️ `throttle_lag`／`pause_lag` 預設是推測值，未做容量測試。
細節見 `docs/KNOWN_GAPS.md` §18 步驟 5。

殘留：大量 connector **第一次**同時失敗時（`error_count` 都還是 0），退避與
`max_retry_per_tick` 在第一輪都幫不上忙。jobs 保留期限已做（步驟 3）、優雅停止
已做（步驟 4）、下游 lag 背壓已做（步驟 5，見 `docs/KNOWN_GAPS.md` §18）。
主機 CPU／RAM／disk 壓力歸 V0.4 Resource Guard，目前不會讓 collector 因主機壓力降速。

本機常見情境：8080 可能被其他本機服務占用（例如另一套安全/情資平台），health 不要綁 8080。

## Normalizer

Redpanda consumer 訂閱 `raw.collected`（`enable.auto.commit=false`，處理完 `commit_last`）。載入 RawEvidence metadata（Postgres）+ body（物件儲存）。RSS／Atom 走 `connector-rss::parse_feed`（`object_type=Article`）；Static Web 走 `connector-static-web::parse_html`（`object_type=WebPage`）。publish `object.normalized` 只在有寫 Document 時。

**匯入（push）路徑**：`metadata["import"]` 帶有 `ImportSpec`（由 `POST /api/v1/import` 寫入）時，改走 `import-format` 解析，每筆紀錄一份 Document（`object_type` 由上傳者指定，預設 `report`）。`kind=manual` 一律 `SkippedUnsupported`。

**沒有 `ImportSpec` 的 JSON／CSV 仍然 `SkippedUnsupported`**（例如 REST API connector 抓回來的任意 JSON）：沒有欄位對映就不知道哪個鍵是 title，猜一組會產生看起來正常、內容其實錯位的 Document。這不是「還沒做」，是刻意的邊界。細節見 `docs/developer/import-api.md`。

兩條路徑共用同一段落地程式（`persist_documents`）：冪等保證只能有一份實作。

| 項目 | 行為 |
|---|---|
| 冪等 | 部分 unique index `idx_provenance_normalized_raw`：同一 `raw_evidence_id` 只能有一列 `action='normalized'` |
| 落地原子性 | **Document + `derived_from` + `normalized` claim 在同一個交易裡**（V0.2 Phase 0e 起）。中途失敗整批回滾，見下方「落地原子性」 |
| 空 feed | 仍寫一列 `normalized`（`subject_id=raw_evidence_id`、`document_ids=[]`） |
| `derived_from` | 每份 Document 一列；不在 unique 範圍 |
| 未知 content type | 記 log 後 `SkippedUnsupported`，繼續消費下一則 |
| 無法解析 | `SkippedUnparseable`，繼續消費 |

預設 `[normalizer]`：

```toml
bind = "127.0.0.1:18082"
consumer_group = "osint-normalizer"
```

### 落地原子性：交易（V0.2 Phase 0e 起）

`persist_documents` 走 `storage_core::TransactionalStore`：

```text
begin
  ├── put_document      × N
  ├── put_provenance(derived_from) × N
  └── put_provenance(normalized)   ← unique claim
commit
```

**只有兩種結果：全都在，或全都不在。** 中途任何一步失敗就 `rollback`；
連「函式提早 return 忘了 commit」也會回滾（sqlx 的 `Transaction::drop` 會排一個 ROLLBACK，
`storage-core::conformance::assert_transactional_contract` 有斷言）。

`object.normalized` 事件在 **commit 之後**才發，不會出現「事件發了但資料沒落地」。

併發雙寫（兩個 consumer 同時通過「尚未正規化」檢查）由 claim 的 unique index 決勝：
輸的那一邊拿到 `Conflict` → 整個交易回滾 → 回報 `AlreadyDone`。
它這一輪寫的 Document **不會留下來**，所以不再需要 dedup 去收。

#### 這之前是 write-then-claim，代價是什麼

V0.2 Phase 0e 之前 `storage-core` 沒有跨表交易能力，「寫 Document」跟「佔 unique index」
無法原子化，只能在兩個都有 crash window 的順序裡挑代價小的：

- **write-then-claim（當時的選擇）**：Document 寫完、claim 前 crash → 重跑產生一組重複
  Document，留給 dedup pipeline 收。可回收。
- **claim-first（曾經改成這樣，已改回）**：claim 成功、Document 還沒寫就 crash →
  之後永遠回報 `AlreadyDone` 但 Document 不存在——**靜默資料遺失，無法偵測也無法回收**。

交易版把兩個 window 都消掉了，上面這段留著是因為 **deduplicator 與 entity-worker
目前仍是 write-then-claim**（`docs/developer/deduplicator.md`、`docs/developer/entity-worker.md`
指向這一節就是指這個取捨）。它們的交易化留到 V0.2 Phase 1——一次改一個，
每一個都要有自己的 crash 重送測試，一次全改會分不出是哪一個改壞的。

> ⚠️ 交易化**不會**讓那兩個 worker 自動變安全。它們的 crash window 還在，
> 靠的仍然是決定性的 UUID v5 id（重跑算出同一個 id → upsert 同一列）。

驗證：`crates/acceptance/tests/acceptance_f.rs` 的
`acceptance_f_normalizer_uncommitted_replay_leaves_exactly_one_document`——
真的開一個交易寫入後**不 commit 就 drop**，再重送同一則 `raw.collected`，
斷言只會有一份 Document。

## EventConsumer

`core-events::EventConsumer` 關掉 auto-commit。`next_envelope` 記住 offset+1（Kafka 約定），`commit_last` 走 Sync commit。生產 consumer 必須在處理後呼叫；測試可以直接 `normalize_raw`。

## Migration

```text
migrations/postgres/0003_provenance_normalized_unique.sql
migrations/sqlite/0003_provenance_normalized_unique.sql
```

`RelationalStore` 新增：

- `list_enabled_connectors()`
- `list_provenance_by_raw_evidence(raw_evidence_id)`

Postgres 與 SQLite adapter 都有實作；conformance 會斷言。

## 本機啟動

需要 compose（Postgres、SeaweedFS 8333、Redpanda 9092）與正確 `.env`。不要連 8080／9200。

```bash
make run-collector
make run-normalizer
curl -s http://127.0.0.1:18081/health
curl -s http://127.0.0.1:18082/health
```

## 測試

```bash
cargo test -p collector --lib
cargo test -p normalizer --lib
cargo test -p normalizer --test e2e -- --nocapture --test-threads=1
```

e2e（對本機 Docker，不連外網）：

1. 假 RSS → `run_connector` → RawEvidence（Postgres+物件儲存）→ 真的 `raw.collected` → `handle_payload` → Document 讀回
2. 同一 `raw_evidence_id` 連續正規化兩次 → 第二次 `AlreadyDone`，`normalized`／`derived_from` 各一列
3. 並發兩次 → 一次 `Created`、一次 `AlreadyDone`，Document 仍只有一組
4. 未知 `connector_type`（例如 `manual_upload`）→ `SkippedUnknownType`
5. `application/pdf` body → `SkippedUnsupported`，不寫 provenance
6. 假 HTML → `static_web` → Document（`object_type=webpage`）
7. 假 JSON API → `rest_api` → RawEvidence，normalizer `SkippedUnsupported`

匯入（push）路徑的 e2e 在 `crates/core-api/tests/import_e2e.rs`：

```bash
cargo test -p core-api --test import_e2e
```

## 已知限制

1. **下游 lag → collector 已接上（2026-10-29 步驟 5）；看的不是 `osint_queue_depth`。**
   每 tick 開始查 `[normalizer].consumer_group` 在 `raw.collected` 上的 lag
   （`LagSource`／`BrokerLagSource`，`GroupLagProbe` 包在 `spawn_blocking`）。
   `throttle_lag` 時本輪併發降為 1，`pause_lag` 時本輪不派、不改
   `last_run`／`error_count`／`status`。查詢失敗或逾時 fail-open（照常收集，
   記 `osint_collector_lag_probe_failures_total`，只在狀態變化時寫 log）。
   `enabled=false` 完全不查。

   ⚠️ `throttle_lag=5000`、`pause_lag=50000` **是推測值，未做容量測試。**
   調整前看 collector `/metrics` 的 `osint_collector_downstream_lag`。

   indexer／normalizer／deduplicator／entity-worker 仍把自己的 lag 寫進
   `osint_queue_depth`，那個 gauge **仍然只有人寫、沒有人讀**。indexer 自己的
   批次延遲是服務內部的事，不會讓 collector 降速。

   **還沒做的**：主機 CPU／RAM／disk 壓力（V0.4 Resource Guard）。
   `RESOURCE_BUDGET.md` §8／§14 描述的目標狀態，主機壓力那一段仍未達成。
