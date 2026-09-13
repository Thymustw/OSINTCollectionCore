# graph-worker（`osint-graph-worker`）

`relationship.changed` → Neo4j 圖投影（SPEC_V0.2 §8），並消費 `job.dispatched` 執行 `graph_rebuild`。
V0.2 Phase 2 Step 5。程式在 `crates/graph-worker/`。

```text
entity-worker／merge ──relationship.changed──▶ graph-worker ──逐筆──▶ Neo4j
                                                    │                   ▲
core-api POST /graph/rebuild ──job.dispatched──┘                   │
                                                    └── 讀 PostgreSQL ──┘
                                                        （Relationship + Entity）
```

目前**不發** `graph.sync.completed`：這條 topic 還沒有消費者，硬發一則沒人訂閱的事件不算完成。漏發靠 `--rebuild` 補齊。

## 常駐模式訂兩個 topic

同一個 consumer、同一個 group，靠 `EventEnvelope.event_type`（就是 topic 字串）分流：

| topic | 行為 |
|---|---|
| `relationship.changed` | 既有：把 Entity→Entity 的邊投影進 Neo4j |
| `job.dispatched` | 只處理 `job_type=graph_rebuild`。其他 type（例如之後的 `collect`）**忽略並 commit**，debug log，不是錯誤 |

不認識的 `event_type` 也 commit（防禦性；訂閱清單就這兩個）。

### `graph_rebuild` job

1. `JobService::transition(job_id, Running)` 標記開始。graph-worker 自己不 `create`／`dispatch`，`JobService::new(store, None)` 不帶 `EventProducer`。
2. 跑 `rebuild(RebuildOptions { drop_graph: false, page_size })`。**`drop_graph` 永遠是 `false`**：Job model 沒有參數欄位，沒有安全的方式讓 API 傳「要不要 drop」。全清重建只留給 CLI `--rebuild --drop`。
3. 成功 → `Completed`；`rebuild()` 回 `Err` → `Failed`（錯誤訊息寫進 `job.error`）。
4. **執行完（不管成敗）就 commit offset**。這裡的「失敗」是 job 本身跑失敗，不是消費事件失敗。重送只會讓同一個 job_id 再走一次 `Running→Completed/Failed`，`can_transition` 擋下後變成處理失敗、offset 又不 commit……迴圈。already-failed 的 job 重送也跑不起來。
5. `transition` 本身失敗（例如 `job_id` 在 Postgres 查不到）也 commit：重送不會讓那個 id 出現。

對應 metric：`osint_graph_worker_job_completed_total`／`osint_graph_worker_job_failed_total`／`osint_graph_worker_job_ignored_total`。

觸發方式是 `POST /api/v1/graph/rebuild`（見 `docs/developer/api-skeleton.md` 的 Graph 小節），不是 collector 排程。

## 為什麼訂 `relationship.changed` 而不是 `object.updated`

`relationship.changed` 是「這條**邊**變了」。`object.updated` 是「某個 canonical object 改了」，會混進一堆與圖無關的文件更新。partition key 是 `relationship_id`（UUID v5），同一條邊的多次更新落在同一個 partition，才不會亂序。

## ⚠️ 最容易搞錯的一點：只有 Entity → Entity 才進圖

`relationship.changed` 混了兩種邊：

| 種類 | 例子 | `source_object_id` | 進圖？ |
|---|---|---|---|
| Document → Entity | `mentions`、`authored_by` | Document id，**不是** Entity | **否** |
| Entity → Entity | `belongs_to`、`associated_with`、`derived_from` | Entity id | **是** |

`storage-neo4j` 的 `Neo4jStore` 只認 `:Entity` 節點（`entity_id` 是主鍵）。Document 根本不是圖節點，把 Document→Entity 的邊投影進去會讓 `upsert_edge` 因為找不到端點而失敗，或更糟——之後有人把 Document id 當成 Entity 查，shortest path 會 silently 算錯。

判斷方式：對兩端各呼叫一次 `RelationalStore::get_entity`。**任一端回 `None` 就整條跳過**（debug log，講清楚這是預期行為，**不是錯誤**，不要用 warn/error）。兩端都 `Some` 才 `upsert_node`（兩端各一次）再 `upsert_edge`。

merge 發的 `relationship.changed` 全部是 Entity-to-Entity（merge 只處理 Entity 之間的 relationship），走同一套「兩端都查一次」自然正確，不需要特別分支。

rebuild 掃 `list_relationships` 時也走同一條路，所以 Document→Entity 的列會出現在 `skipped_non_entity`，不會進圖。

## 事件只說「哪條邊變了」，內容一律重讀 PostgreSQL

`process_change()` 從 payload 取 `relationship_id`／`change_kind`（以及兩端 id，只給 log 用）。`confidence`／`first_seen`／`last_seen`／`relationship_type`／真正的兩端 **一律重新從 PostgreSQL 讀**。

payload 裡的 `relationship_type` **刻意不用**。直接拿事件當內容會讓亂序或重放的事件覆蓋掉較新的狀態。

| `change_kind` | 行為 |
|---|---|
| `upserted` | `get_relationship(id)`。查到就投影；查不到（發出後又被刪，race）視同 `deleted`，log info 說明是 race 不是錯誤 |
| `deleted` | 直接 `graph.delete_edge(id)`，不查 Postgres。`delete_edge` 本來就是冪等的 |

`change_kind` 不是這兩個值是錯誤（不提交 offset）。缺欄位也是錯誤。

## 冪等

`upsert_node`／`upsert_edge`／`delete_edge` 都是冪等的。同一則事件重送一萬次還是同一條邊。

刻意**沒有** provenance claim：投影是可重建的衍生資料，為它寫一列 canonical 的 claim 會讓「重建」變成需要先刪 claim 才能跑。

## 為什麼沒有批次、沒有 backpressure

`GraphStore` 沒有 bulk API。indexer 那套 `BatchController`／`FlushReason` 在這裡沒有意義——一則事件處理完就可以決定要不要 commit offset。

consumer lag 仍寫進 `osint_queue_depth` gauge（給人／Prometheus 看）。graph-worker **自己不降速**：逐筆寫 Neo4j，沒有「一次送太大」的問題。跨服務的 backpressure 仍是 V0.2 後續項目。

## offset 只在成功或「優雅跳過」之後才提交

| 結果 | commit？ | 理由 |
|---|---|---|
| `relationship.changed`：`Applied`／`Deleted` | 是 | 已經寫進圖 |
| `relationship.changed`：`SkippedNonEntity`／`SkippedRace` | 是 | 預期行為，重送結果一樣，卡住 partition 沒意義 |
| `relationship.changed`：`StorageError`／Neo4j 連不上 | **否** | 這則邊還沒進圖；先提交會讓它永遠消失且沒有跡象 |
| `job.dispatched`：任何結果（含 rebuild 失敗、不認識的 job_type、transition 失敗） | **是** | job 已經跑完（或確定不該跑）；重送不會讓 already-failed 的 job 重跑 |

關機（SIGINT）時**不再**多 commit 一次：上一則成功時已經 commit 過；上一則失敗時必須留給 broker 重送。

## Rebuild（`--rebuild`）

CLAUDE.md §5：Neo4j 是可重建的 projection。

```bash
cargo run -p graph-worker --bin osint-graph-worker -- --rebuild
cargo run -p graph-worker --bin osint-graph-worker -- --rebuild --drop
```

Operations Center 已可看投影狀態：`GET /api/v1/ops/graph`（viewer 以上）回
`[graph_worker].projection` 的 lag 與 rebuild。這支服務目前仍**不**跑在 compose 裡，
本機驗證用上面的 `cargo run`。

### 兩者的差別很容易被誤會

| 模式 | 行為 |
|---|---|
| `--rebuild` | 既有邊被覆寫成最新內容，但**已經不該存在的節點／邊不會被刪掉**（例如 Entity 已從 PostgreSQL 刪除） |
| `--rebuild --drop` | 真正的完整重建：先 `reset_projection`（清 checkpoint／rebuild 狀態），再 `GraphStore::wipe()`（`MATCH (n:Entity) DETACH DELETE n`，**不碰** `:ProjectionState`），然後從 `list_relationships` 掃回來 |

`--drop` 不能單獨使用。argument parser 會擋。

`--drop` 的順序不能反：狀態要在 wipe **之前**清掉。先寫 `Running` 再 `reset_projection`，那個 `Running` 會被 reset 一起清掉，於是重建過程中 `rebuild_status` 回 `Idle`——看起來像沒有人在重建，於是有人再開一個。

分頁來源是 `RelationalStore::list_relationships(cursor, page_size)`（**不是** `list_relationships_by_object`——那個是「這個物件牽涉到什麼」，這個才是「系統裡有哪些邊」）。每一筆呼叫跟 `process_change` 共用的 `apply_relationship`，不要複製一份。

Neo4j 沒有 OpenSearch 那種 `refresh`；重建結束不需要額外一步。

### 重建狀態

寫進 `ProjectionStore`（Neo4j 的 `:ProjectionState` 節點，`projection` = `[graph_worker].projection`，預設 `osint-graph`）：

| 時機 | 寫入 |
|---|---|
| `--drop` 時，**wipe 之前** | `reset_projection` |
| 開始 | `state = Running`、`started_at` |
| 每頁結束 | `scanned`／`written`／`failed` |
| 正常結束 | `state = Completed`、`finished_at`、最終計數 |
| 中途 `Err` | `state = Failed`、`finished_at`、`last_error`（已 `sanitize`） |

單筆寫入失敗**仍然是 `Completed`**，失敗筆數在 `failed` 欄位。把它當 `Failed` 會讓「跑不完」與「跑完了但有 3 筆寫入失敗」變成同一個狀態。

checkpoint 與 rebuild 狀態寫失敗只 warn，不擋投影本身。理由同 indexer：那是可觀測性，不是資料路徑。warn 會明講後果——lag 會停在舊值。

來源時間戳用 Relationship 的 `last_seen`：lag 要回答「投影落後 canonical 多久」，`last_seen` 是邊最後一次被觀察到的時間。

## `/health` `/ready` `/metrics`

預設綁 `127.0.0.1:18086`（`[graph_worker].bind`）。18080–18085 已分別是 api／collector／normalizer／deduplicator／entity-worker／indexer。

`/ready` 檢查 **Postgres + Neo4j**。graph-worker 沒有圖後端就完全沒事可做，只檢查 Postgres 會讓它在「圖投影整個壞掉」的情況下回報 ready。

| metric | 意義 |
|---|---|
| `osint_queue_depth` | consumer lag。給人／Prometheus 看 |
| `osint_graph_worker_applied_total` | 成功 upsert 的 Entity→Entity 邊 |
| `osint_graph_worker_skipped_non_entity_total` | 因一端不是 Entity 而跳過 |
| `osint_graph_worker_deleted_total` | `change_kind=deleted` |
| `osint_graph_worker_race_total` | upserted 但 Postgres 已查不到（當刪除） |
| `osint_graph_worker_errors_total` | 真正的失敗（不提交 offset） |
| `osint_graph_worker_job_completed_total` | `graph_rebuild` job 跑完且成功 |
| `osint_graph_worker_job_failed_total` | rebuild 回錯，或 job 狀態轉移失敗 |
| `osint_graph_worker_job_ignored_total` | `job.dispatched` 的 `job_type` 不是 `graph_rebuild` |

每個結果分支都有計數器。少一個的話那個分支悄悄一直失敗不會被發現。

設定在 `[graph_worker]`（`config/default.toml`）：

| 鍵 | 預設 | 說明 |
|---|---|---|
| `bind` | `127.0.0.1:18086` | health 埠 |
| `consumer_group` | `osint-graph-worker` | Kafka group |
| `projection` | `osint-graph` | ProjectionStore 用的投影名稱。改名等於換一個空投影，要跑 rebuild |
| `page_size` | 100 | rebuild 每頁筆數。硬上限，沒有「不限」 |

## 設定與連線

Neo4j 連線用 `[storage.graph]` 的 `bolt_uri`／`username`／`password_secret_ref`／`pool_max`，呼叫方式與 `osint-api` 的 `connect_graph` 相同，但是**獨立的一份連線**——不要想辦法共用 core-api 那份。

## 已知限制

1. **Document→Entity 的邊不會進圖**（見上；這是設計，不是還沒做）。
2. **`--rebuild` 不刪多餘節點／邊**（見上）。要清幽靈節點必須 `--drop`。
3. **沒有 `graph.sync.completed` 生產者**（見上）。
4. **沒有 DLQ topic**。真正的失敗不提交 offset、靠 broker 重送；永久性錯誤（payload 缺欄位）目前也走同一條——缺欄位的事件會一直被重送。V0.1 同樣沒有 DLQ。
5. **累加沒有樂觀鎖**。同一個投影跑兩個以上 writer 時 `objects_written` 會少算。V0.2 假設一個投影一個 writer。
6. **不在 compose 的 `app` profile 裡**。Operations Center 整合（Step 6）之前用本機 `cargo run`。

## 相關文件

- `docs/developer/events.md`：`relationship.changed` payload 與 partition key
- `docs/developer/entity-worker.md`：上游的抽取與 Document→Entity 邊
- `docs/developer/merge.md`：merge／undo 發的 Entity→Entity 事件
- `docs/developer/storage-adapters.md`：`GraphStore`／`wipe`／`:ProjectionState`
