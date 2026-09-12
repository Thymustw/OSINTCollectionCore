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

另外 **`job.dispatched`** 供 Job 派工。SPEC §20 沒列這個 topic。

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

> ⚠️ **這九個目前只有定義，沒有生產者也沒有消費者**，同 V0.1 的
> `search.index.requested`。Phase 0d 只加 `EventTopic` 的 variant 與 `as_str`，
> 沒有動任何 producer/consumer。

`relationship.changed` 與 V0.1 的 `object.updated` 語意不同，容易訂錯：
前者是「relationship 這條**邊**變了」，後者是「某個 canonical object 改了」。
graph-worker（`SPEC_V0.2.md` §8）要訂的是 `relationship.changed`——
訂 `object.updated` 會收到一堆與圖無關的文件更新。

`EventTopic::ALL` 列出全部 19 個 variant，供兩個測試使用：
`as_str()` 必須與 serde 的 `rename` 是同一個字串（兩邊各寫一次名字，
**寫錯一邊不會編譯失敗**——produce 用 `as_str()` 決定 topic、envelope 的
`event_type` 走 serde，結果是 consumer 訂到一個空 topic 而且看起來只是
「目前沒有事件」），以及 topic 名稱不可重複。新增 variant 時
`ALL` 的長度常數與那個 `assert_eq!(total, 19)` 要一起改。

Partition key：job 派工使用 `job_id`（TECH_STACK 預設表沒有 job；這是實作補充）。收集事件 `raw.collected`／`raw.failed` 使用 `source_id`（TECH_STACK 預設）。

生產 consumer 關掉 auto-commit：處理完呼叫 `EventConsumer::commit_last()`（提交上一則的 offset+1）。`osint-normalizer` 訂閱 `raw.collected`，寫完 Document 後 produce `object.normalized`。`osint-deduplicator` 訂閱 `object.normalized`，跑完 SPEC §15 五階段後 produce `dedup.completed`（partition key = `document_id`；payload 與 `AlreadyDone` 不發事件的理由見 `docs/developer/deduplicator.md`）。 `osint-entity-worker` 訂閱 `dedup.completed`，**只處理 `is_duplicate=false` 的 Document**，抽完 Entity 後 produce `entity.extracted`（partition key = `document_id`；payload 欄位與「跳過／已處理不發事件」的理由見 `docs/developer/entity-worker.md`）。 `osint-indexer` 訂閱 **`entity.extracted`**（不是 `dedup.completed`——要等 entity 抽完才索引，SPEC §18 的 entity 過濾才成立），bulk 寫進 OpenSearch 後 produce **`search.index.completed`**（partition key = 該批第一筆 `document_id`；payload 含 `index`／`document_ids`／`indexed`／`failed`／`failed_document_ids`／`retries`）。

`search.index.requested` 在 V0.1 **沒有生產者也沒有消費者**：indexer 直接訂 `entity.extracted`，重新索引走 `osint-indexer --rebuild`（CLI）而不是事件。topic 名稱保留在 `EventTopic`，留給 V0.2 的 on-demand 重新索引。

⚠️ indexer 是批次消費者：**offset 只在 bulk flush 成功之後才提交**。先提交再送出的話，flush 失敗或行程被殺時那一批會永遠不進 index 且沒有任何跡象。理由與批次／backpressure 設計見 `docs/developer/indexer.md`。

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

`EventConsumer::next_envelope` 會在逾時內重試 `UnknownTopicOrPartition`：consumer 先 subscribe、topic 由第一次 produce 自動建立時，librdkafka 可能先回這個暫時性錯誤。

`rdkafka` 以 mklove `./configure` + `libz` 編譯，**不開 `cmake-build` / `ssl-vendored`**。

試過 `cmake-build` + `ssl-vendored`：vendored OpenSSL 本身能編，但 librdkafka cmake 用 `#cmakedefine01 WITH_OAUTHBEARER_OIDC`，關閉時仍是 `#define ... 0`，C 端 `#ifdef` 為真，接著 `#include <curl/curl.h>`。本機沒有 `libcurl-dev`，且沒有 passwordless sudo。本機 Redpanda 是明文 9092，不需要 Kafka TLS。
