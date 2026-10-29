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

優雅停止（2026-10-28 步驟 4）時，spawn 前就建 job 並轉 Running；drain 逾時 abort 後外層把剩餘 job 轉 `cancelled`，並寫入 `error`（「collector 停止時中斷」）。`JobService::transition` 對 Failed **與** Cancelled 都寫 `error`、設 `completed_at`——Cancelled 是終態，沒有 `completed_at` 的話保留期限清不到。啟動回收把卡住的 running collect job 標成 **failed**（不是 cancelled），才能出現在 `/ops/dlq`。

`list_by_status_and_type(status, job_type, after, limit)` 對應 `GET /api/v1/ops/dlq?type=`，過濾在 SQL 裡做。

## 保留期限（`[retention.jobs]`）

只清 **collect** 類終態。failed 預設 30 天（`/ops/dlq` 只列 failed）、completed／cancelled 各 7 天。其他 job type 不在設定規則裡，排程不會刪。時間欄位是 `completed_at`（結束後保留 N 天），不是 `created_at`。`completed_at IS NULL` 的終態列不會被選中。

`core_jobs::JobRetention` 吃 `RetentionJobsConfig`。`run_once` 依規則分批呼叫 `RelationalStore::purge_jobs`（單次只刪一批，迴圈與 `batch_pause_ms` 在這裡）。dry-run 走 `count_purgeable_jobs`（不受 batch limit 限制）。天數 `0` 的規則不列入。達到 `max_batches_per_run` 就停，`PurgeReport.truncated = true`。

osint-api 背景任務第一次延後一個 interval，之後週期跑。有刪才寫稽核 `jobs.purge`（actor=`system`、`metadata.source=scheduler`）並累加 `osint_jobs_purged_total`。錯誤只 warn。手動端點 `POST /api/v1/ops/jobs/purge`（admin）見 `docs/developer/api-skeleton.md`。

非終態（queued／running／retrying）在 `JobPurgeFilter::validate` 回 `ConstraintViolation`；postgres／sqlite 都先呼叫，不能只靠呼叫端自律。

## 驗證

```bash
cargo test -p core-jobs --test postgres_jobs -- --nocapture
```
