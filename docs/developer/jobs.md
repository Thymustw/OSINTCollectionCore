# Job 系統（V0.1 Phase 2）

欄位對齊 SPEC §21。CRUD 經 `RelationalStore`（生產仍是 Postgres canonical），不在 domain 寫 SQL。

`JobService` 的 generic bound 是 `RelationalStore` 不是 `CanonicalStore`：`CanonicalStore` 是標記 trait、沒有額外的 job 方法；SQLite 的 `SqliteEmbeddedStore` 實作 job CRUD 卻沒實作 `CanonicalStore`。graph-worker 的 unit test 需要在 SQLite 上跑 `process_dispatched_job`，所以 bound 放寬。生產路徑不變。

## 狀態機

SPEC §21 列了狀態、**沒寫轉換表**。實作：

```text
queued ──► running ──► completed
   │          │
   │          ├──► failed ──► retrying ──► running
   │          ├──► retrying
   │          └──► cancelled
   └──► cancelled
```

終態：`completed`、`cancelled`。`failed` 可經 `retrying` 再進 `running`。

`list_jobs(after, limit)` 加在 `RelationalStore`：UUID v7 由新到舊，`id < after`。

## 重試（Phase 6b，SPEC §31）

`JobService::retry(id)` ＝ 檢查狀態是 `failed` → 轉 `retrying` → `dispatch`。
對外是 `POST /api/v1/jobs/{id}/retry`（operator 以上，寫稽核 `job.retry`）。
不是 `failed` 的一律回 `JobError::NotRetryable` → HTTP **409**。

> ⚠️ **是 `failed → retrying`，不是 `failed → queued`。** 狀態機沒有那條邊，
> 而且不該有：`retry_count` 是在進入 `retrying` 時累加的。把失敗的 job 直接丟回
> `queued`，它看起來會跟一個從沒跑過的新 job 一模一樣——「重試過幾次」會在每次
> 重試時被抹掉，無限重試的迴圈也就沒有任何地方看得出來。
> `retrying` 本身是可派工狀態，所以對呼叫端而言效果仍是「它會再跑一次」。

`list_by_status(status, after, limit)` 對應 `GET /api/v1/jobs?status=failed`，
過濾在 SQL 裡做（理由見 `docs/developer/storage-adapters.md`）。

## 派工

`JobService::dispatch` 對 Redpanda produce `job.dispatched`（envelope v1）。

目前消費這個 topic 去執行工作的 worker 有兩個：

- **`osint-graph-worker`**：只認 `job_type=graph_rebuild`（`POST /api/v1/graph/rebuild` 建立）。
- **`osint-stix-worker`**：只認 `job_type=stix_import`（`POST /api/v1/import/stix` 建立）與 `job_type=stix_export`（`POST /api/v1/export/stix` 建立）。

兩個 worker 各自是獨立的 consumer group，各自只認識自己的 job type，其他 type 會被忽略並 commit，不是錯誤。collector 的 `collect` job **不**走這條路徑。

`stix_export` 完成後，stix-worker 透過 `JobService::merge_parameters` 把 `result_object_key` 回填到 `Job.parameters`。這是**部分合併**（既有欄位保留，只有同名 key 才被 patch 覆寫），用途是讓 worker 回寫結果而不動到 API 建立 Job 時就寫入的 `filter`。完整實作見 `crates/core-jobs/src/service.rs`。

collector 每次收集會建立 `job_type=collect`（`correlation_id=connector.id`），自己把狀態轉 `running`／`completed`／`failed`，**不**走 `job.dispatched`。Job 轉態失敗只記 warn，不中止收集。`graph_rebuild` 才是第一個由 worker 消費 `job.dispatched` 執行的 job type。

## 驗證

```bash
cargo test -p core-jobs --test postgres_jobs -- --nocapture
```
