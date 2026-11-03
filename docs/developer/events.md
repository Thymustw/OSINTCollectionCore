# Event envelope 與 Redpanda（V0.1 Phase 2）

## Envelope v1

```json
{
  "id": "<uuid-v7>",
  "event_type": "job.dispatched",
  "schema_version": "1",
  "source_service": "core-api",
  "timestamp": "2026-09-10T12:00:00Z",
  "correlation_id": "<uuid or null>",
  "payload": {}
}
```

欄位對齊任務要求。SPEC §20 只要求 envelope versioned，沒有規定欄位名。

## Topics（SPEC §20）

```text
raw.collected
raw.failed
object.normalized
object.created
object.updated
dedup.completed
entity.extracted
search.index.requested
search.index.completed
```

另外 **`job.dispatched`** 供 Job 派工。SPEC §20 沒列這個 topic。生產者是
`JobService::dispatch`（`POST /api/v1/jobs`／`POST /api/v1/graph/rebuild`
帶 `dispatch=true`）；**第一個真的消費它的服務是 `osint-graph-worker`**
（Phase 2 Step 5）——過濾 `job_type == "graph_rebuild"`，其餘 job type
忽略但仍提交 offset。這個 topic 在那之前只有生產者，Job 系統的狀態機
沒有任何服務真的去執行工作，只是記錄；細節見
`docs/developer/graph-worker.md`。

### V0.2 新增（`SPEC_V0.2.md` §20，Phase 0d）

```text
entity.resolution.requested
entity.resolution.completed
entity.merged
relationship.changed
graph.sync.requested
graph.sync.completed
embedding.requested
embedding.completed
timeline.updated
```

與 V0.1 的十個 topic **沒有任何名稱重疊**（`EventTopic` 的
`topic_names_are_unique_and_all_is_complete` 測試會擋住重複）。

> ⚠️ V0.2 九個 topic 裡，目前**有生產者**的是 `relationship.changed`
> （entity-worker 抽取、merge／undo）。**消費者**是 `osint-graph-worker`
> （訂 `relationship.changed`，把 Entity→Entity 的邊寫進 Neo4j）。
> `embedding.requested`／`embedding.completed` **仍沒有生產者與消費者**：
> embedding-worker 直接訂 V0.1 的 `entity.extracted`（與 indexer 同一 topic、
> 獨立 consumer group），重新產生走 `osint-embedding-worker --rebuild`。
> 其餘仍只有定義，同 V0.1 的 `search.index.requested`。

`relationship.changed` 與 V0.1 的 `object.updated` 語意不同，容易訂錯：
前者是「relationship 這條**邊**變了」，後者是「某個 canonical object 改了」。
graph-worker（`SPEC_V0.2.md` §8）要訂的是 `relationship.changed`——
訂 `object.updated` 會收到一堆與圖無關的文件更新。

### `relationship.changed`

payload：

```json
{
  "relationship_id": "<uuid>",
  "source_object_id": "<uuid>",
  "target_object_id": "<uuid>",
  "relationship_type": "mentions",
  "change_kind": "upserted"
}
```

`change_kind` 是 `"upserted"` 或 `"deleted"`。`relationship_type` 走 serde
`snake_case`（與寫進資料庫的字串同一套）。

**partition key 用 `relationship_id`，不是 `document_id`／`entity_id`。**
這是刻意偏離其他事件的慣例：`relationship_id` 是 UUID v5（由
source+type+target 推導），同一條邊的多次更新必須落在同一個 partition，
graph-worker 才不會收到亂序更新。envelope 的 `correlation_id`（`publish` 的
第三個參數）也帶同一個 `relationship_id`。

生產者：

| 來源 | 何時發 | `change_kind` |
|---|---|---|
| `osint-entity-worker` | 抽取完成、且 outcome 是 `Extracted` | 永遠 `"upserted"`（沒有刪除場景） |
| `crates/merge` `execute_merge` | `tx.commit()` **成功之後** | 自迴圈／被吸收 → `"deleted"`；absorber 合併後與 safe_repoint → `"upserted"` |
| `crates/merge` `undo_merge` | `tx.commit()` **成功之後** | 把 execute 時刪掉的邊發 `"upserted"`（恢復）；repoint／absorber 還原後發 `"upserted"` |

跳過／已處理／找不到的抽取**不發**（理由同 `entity.extracted`：重跑同一份
文件不該重發）。`MergeService` 的 `producer` 為 `None` 時整段跳過——SQLite
本機開發沒接 Kafka 仍能 merge。

merge 的事件在 **commit 之後**才送：commit 前發送、之後 commit 失敗會讓
graph-worker 寫入不存在的邊。發送失敗**不讓** `execute_merge`／`undo_merge`
回 Err——canonical store 已經改完，重跑 execute 會被 `AlreadyMerged` 擋住，
沒有乾淨的自動重試路徑；漏發靠 graph rebuild 補齊。entity-worker 則用 `?`
往上拋，因為 offset 還沒 commit、失敗了下次會重跑。

`EventTopic::ALL` 列出全部 29 個 variant（V0.3 起；V0.2 時是 19 個），供兩個測試使用：
`as_str()` 必須與 serde 的 `rename` 是同一個字串（兩邊各寫一次名字，
**寫錯一邊不會編譯失敗**——produce 用 `as_str()` 決定 topic、envelope 的
`event_type` 走 serde，結果是 consumer 訂到一個空 topic 而且看起來只是
「目前沒有事件」），以及 topic 名稱不可重複。新增 variant 時
`ALL` 的長度常數與那個 `assert_eq!(total, 29)` 要一起改。

Partition key：job 派工使用 `job_id`（TECH_STACK 預設表沒有 job；這是實作補充）。收集事件 `raw.collected`／`raw.failed` 使用 `source_id`（TECH_STACK 預設）。

生產 consumer 關掉 auto-commit：處理完呼叫 `EventConsumer::commit_last()`（提交上一則的 offset+1）。`osint-normalizer` 訂閱 `raw.collected`，寫完 Document 後 produce `object.normalized`。`osint-deduplicator` 訂閱 `object.normalized`，跑完 SPEC §15 五階段後 produce `dedup.completed`（partition key = `document_id`；payload 與 `AlreadyDone` 不發事件的理由見 `docs/developer/deduplicator.md`）。 `osint-entity-worker` 訂閱 `dedup.completed`，**只處理 `is_duplicate=false` 的 Document**，抽完 Entity 後 produce `entity.extracted`（partition key = `document_id`）以及每條邊一則 `relationship.changed`（partition key = `relationship_id`，見上節）。payload 欄位與「跳過／已處理不發事件」的理由見 `docs/developer/entity-worker.md`。 `osint-indexer` 訂閱 **`entity.extracted`**（不是 `dedup.completed`——要等 entity 抽完才索引，SPEC §18 的 entity 過濾才成立），bulk 寫進 OpenSearch 後 produce **`search.index.completed`**（partition key = 該批第一筆 `document_id`；payload 含 `index`／`document_ids`／`indexed`／`failed`／`failed_document_ids`／`retries`）。`osint-embedding-worker` 同樣訂閱 **`entity.extracted`**（獨立 consumer group `osint-embedding-worker`），把 Document 向量 overlay 進 `osint-documents`、Entity 向量寫進 `osint-entities`。**不**訂 `embedding.requested`，也**不**發 `embedding.completed`。`update_fields` 碰到 NotFound（indexer lag）重試後仍提交 offset；真失敗不提交。細節見 `docs/developer/embedding-worker.md`。

`search.index.requested` 在 V0.1 **沒有生產者也沒有消費者**：indexer 直接訂 `entity.extracted`，重新索引走 `osint-indexer --rebuild`（CLI）而不是事件。topic 名稱保留在 `EventTopic`，留給 V0.2 的 on-demand 重新索引。

⚠️ indexer 是批次消費者：**offset 只在 bulk flush 成功之後才提交**。先提交再送出的話，flush 失敗或行程被殺時那一批會永遠不進 index 且沒有任何跡象。理由與批次／backpressure 設計見 `docs/developer/indexer.md`。

`osint-graph-worker` 同一個 consumer group 訂閱**兩個** topic：`relationship.changed` 與 `job.dispatched`（靠 `EventEnvelope.event_type` 分流）。`relationship.changed` 逐筆寫進 Neo4j（沒有 bulk API，不累積批次）。**只有兩端都是 Entity 的邊才進圖**——Document→Entity（例如 `mentions`）會被跳過，這是預期行為不是錯誤。事件內容不可信，`confidence`／時間戳／型別一律重讀 PostgreSQL。offset 在成功或優雅跳過（非 Entity 端點／race 刪除）之後才提交；Neo4j 寫入失敗不提交。`job.dispatched` 只處理 `job_type == "graph_rebuild"`（其餘忽略但仍提交 offset），跑非破壞性 rebuild 並把 Job 狀態從 `Running` 轉到 `Completed`／`Failed`——執行完不管成敗都提交 offset，跟 `relationship.changed` 的規則不同（job 跑失敗重送只會撞狀態機合法性檢查造成迴圈，不是消費事件失敗）。`--rebuild`／`--rebuild --drop`（CLI）與 `POST /api/v1/graph/rebuild`（API，只會非破壞性，`--drop` 不透過 API 開放）都是從 PostgreSQL 全量重建。細節見 `docs/developer/graph-worker.md`。

`graph.sync.requested`／`graph.sync.completed` 目前**沒有生產者與消費者**：重建走 `osint-graph-worker --rebuild`（CLI）而不是事件。topic 名稱保留在 `EventTopic`。

`embedding.requested`／`embedding.completed` 同樣**沒有生產者與消費者**：embedding-worker 直接訂 `entity.extracted`，重新產生走 `osint-embedding-worker --rebuild`。topic 名稱保留在 `EventTopic`。

### V0.3 新增（`SPEC_V0.3.md` §18）——transactional outbox

SPEC 列了十個 topic。實際 **produce** 的是八個；另外兩個只保留名稱：

```text
seed.created
candidate.created
candidate.approved
candidate.rejected
discovery.completed
ai.requested
ai.completed
ai.failed
```

**不發** `seed.updated`：沒有更新 seed 的 API。
**不發** `discovery.requested`：派工已經由 `job.dispatched` 覆蓋；兩邊都發會讓未來的消費者跑兩次。

這八個走 **transactional outbox**，不是 handler 裡直接 `produce`：

1. 業務寫入與 `event_outbox` 列同一個 DB 交易（`tx.store()` + `tx.outbox()`）。
2. `osint-api` 背景郵差 claim → 送到 Redpanda → 標記已送／失敗退避。
3. broker 掛掉時列留在表裡，API 不會卡住 10 秒 Kafka timeout。

新 topic 第一次 produce 時由 broker **自動建立、1 個 partition**（dev-container 預設）。正式 V0.3 topic 可以留著；測試用的 `osint.conformance.*` 測完一定要刪（見下方「本機驗證」）。

| topic | 發送點 | `source_service` | 何時不發 |
|---|---|---|---|
| `seed.created` | `POST /api/v1/seeds` | `core-api` | — |
| `candidate.created` | discovery-worker `run_graph_expansion` | `discovery-worker` | `record_candidate_discovery` 沿用既有候選（`created == false`），含被拒絕後再發現 |
| `candidate.approved`／`rejected` | `POST /api/v1/candidates/{id}/approve`／`reject` | `core-api` | 狀態已經是目標值（重複核准／拒絕） |
| `discovery.completed` | discovery-worker job 終態（成功與失敗都走這個 topic，用 payload `status` 區分） | `discovery-worker` | Running 轉換不發 |
| `ai.requested` | resolver 打 LLM **之前**（獨立短寫，與 completed 不能同一交易：中間有遠端 HTTP） | `resolver` | 高信心路徑不打 LLM。`ai.requested` 先獨立 commit；若後續 `put_ai_run` 交易失敗，會留下沒有 `ai.completed`／`ai.failed` 的孤兒 `ai.requested` |
| `ai.completed` | resolver 寫 `AiRun` 的同一交易 | `resolver` | HTTP 成功但 JSON 解不出 `same_entity` 仍發 completed（`parsed_same_entity: null`），不改走 failed |
| `ai.failed` | 同上，LLM 呼叫失敗 | `resolver` | `error_kind`：`admission`／`http`／`parse`／`quota_exceeded`／`quota_unavailable`／`unsupported`（沒有 `other`） |

Partition key：有 `collection_id` 用它（同一調查進同一 partition），否則用 seed／candidate／job／ai_run id。見 `TECH_STACK.md` §6。

Typed payload 在 `crates/core-events/src/payloads.rs`。envelope `id` 等於 outbox 列 `id`——重試同一列必須送出同一個 envelope id（at-least-once：crash 在 produce 與 mark published 之間會重送）。

欄位摘要（snake_case JSON）：

| topic | payload |
|---|---|
| `seed.created` | `seed_id`、`collection_id`、`seed_type`、`value`、`entity_id`、`origin`、`status`、`priority`、`confidence`、`depth`、`created_at` |
| `candidate.created`／`approved`／`rejected` | 共用 `CandidateLifecycle`：`candidate_id`、`collection_id`、`candidate_type`、`value`、`normalized_value`、`status`、`discovery_method`、`discovered_by`、`confidence`、`score`、`depth`、`created_at`、`reviewed_at` |
| `discovery.completed` | `job_id`、`status`（`completed`／`failed`）、`collection_id`、`entity_id`、`error`、`completed_at`。`collection_id`／`entity_id` 從 job `parameters` 讀 |
| `ai.requested` | `ai_run_id`、`task_type`、`model`、`collection_id`、`resolution_candidate_id`、`requested_at` |
| `ai.completed` | `ai_run_id`、`task_type`、`model`、`tokens`、`duration_ms`、`confidence`、`parsed_same_entity`（解不出為 `null`）、`collection_id` |
| `ai.failed` | `ai_run_id`、`task_type`、`model`、`error_kind`、`error_message`、`duration_ms`、`collection_id` |

resolver 的 AI 事件目前 `collection_id` 是 `null`（自動核准路徑沒有 collection 脈絡）。

郵差設定 `[event_outbox]`（`config/default.toml`）：`enabled=true`、`poll_interval_ms=500`、`batch_size=50`、退避 `min(base * 2^(attempts-1), max)`（base 1000ms、max 60s）、租約 30s、已送列保留 24 小時後 purge（`0` = 不刪）。producer 連不上時仍寫 outbox、不啟動郵差（log 一次）；列會一直待送，請確認 `[broker].brokers` 與 `osint-core-redpanda-1` 後重啟 osint-api。`enabled=false` 時業務路徑仍寫列，只是沒有郵差。

Postgres claim 用 `FOR UPDATE SKIP LOCKED`：租約內兩個郵差不會拿到同一列。租約到期後同一列可能被再送一次（at-least-once），envelope `id` 仍是同一列的 id，下游用 id 去重。SQLite 沒有 SKIP LOCKED，前提是單一寫入者。

關閉 osint-api 時郵差會等當輪 batch 跑完再離開（不會在 produce 中途丟列）。單則 produce 逾時 10s，所以最久約 `batch_size × 10s`。

指標（`osint-api` `/metrics`）：

| 名稱 | 類型 |
|---|---|
| `osint_event_outbox_pending` | gauge，含尚未到期的退避列 |
| `osint_event_outbox_published_total` | counter |
| `osint_event_outbox_publish_failures_total` | counter |
| `osint_event_outbox_oldest_pending_age_secs` | gauge |

`GET /api/v1/ops/discovery` 的 `event_outbox_pending` 是同一份待送列數。

表結構：migration `0020_v0_3_event_outbox.sql`（postgres + sqlite）。capability 是獨立的 `EventOutboxStore`，**沒有**塞進 `RelationalStore`。見 `docs/developer/storage-adapters.md`。

## Broker health check

`EventProducer::cluster_metadata(timeout) -> Result<(broker 數, topic 數), EventError>`
抓一次 cluster metadata，給 health check 用（`osint-cli health` 的 Redpanda 那一列）。

librdkafka 的 `fetch_metadata` 是**同步阻塞**呼叫：連不上時會卡滿整個 `timeout`。
所以這個方法內部把它丟進 `tokio::task::spawn_blocking`，不會佔住 Tokio executor thread。
呼叫端仍應自己加一層 `tokio::time::timeout`，因為 `spawn_blocking` 的工作無法取消。

## 本機驗證

Redpanda 在 `127.0.0.1:9092`（容器 `osint-core-redpanda-1`）。

```bash
cargo test -p core-events --test redpanda_roundtrip -- --nocapture
```

會 produce 一則、consume 回來比對 id／payload。測試用一次性 topic `osint.conformance.<uuid>`，不寫進共用的 `job.dispatched`（unique group + `auto.offset.reset=earliest` 會先吃到歷史訊息）。

**一次性 topic 測完一定要刪。** 測試開頭會先刪掉所有殘留的 `osint.conformance.*`，結尾再刪自己這次的 topic（刪除函式只接受這個前綴）。原因：本機 Redpanda 以 `--memory 256M --smp 1` 啟動，partition 總數受記憶體限制（`topic_memory_per_partition`）。2026-10-15 實測累積 119 個未刪的測試 topic（144 個 partition）後，**任何新 topic 都建不出來**——`rpk topic create` 回 `INVALID_PARTITIONS: Can not increase partition count due to memory limit`，但 producer 端只看得到 `MessageTimedOut`，完全看不出原因。正式服務的 topic 受同一個上限。producer 一直 `MessageTimedOut` 而 Redpanda 又是 healthy 時，先用 `docker exec osint-core-redpanda-1 rpk topic create <名稱>` 看是不是這個錯誤。

`EventConsumer::next_envelope` 會在逾時內重試 `UnknownTopicOrPartition`：consumer 先 subscribe、topic 由第一次 produce 自動建立時，librdkafka 可能先回這個暫時性錯誤。

`rdkafka` 以 mklove `./configure` + `libz` 編譯，**不開 `cmake-build` / `ssl-vendored`**。

試過 `cmake-build` + `ssl-vendored`：vendored OpenSSL 本身能編，但 librdkafka cmake 用 `#cmakedefine01 WITH_OAUTHBEARER_OIDC`，關閉時仍是 `#define ... 0`，C 端 `#ifdef` 為真，接著 `#include <curl/curl.h>`。本機沒有 `libcurl-dev`，且沒有 passwordless sudo。本機 Redpanda 是明文 9092，不需要 Kafka TLS。
