# embedding-worker（`osint-embedding-worker`）

`entity.extracted` → Document／Entity 向量投影（SPEC_V0.2 §11–§14）。
V0.2 Phase 3 Step 3。程式在 `crates/embedding-worker/`。

```text
entity-worker ──entity.extracted──▶ indexer ──bulk──▶ OpenSearch osint-documents
                         │                                    ▲
                         └── embedding-worker ──update_fields─┘  （Document 向量 overlay）
                         └── embedding-worker ──index────────▶ OpenSearch osint-entities
                         └── 讀 PostgreSQL（Document + Entity）
                         └── 推論：OpenSearch ml-commons（本行程不載模型）
```

目前**不發** `embedding.completed`：這條 topic 還沒有消費者，硬發一則沒人訂閱的事件不算完成。漏寫靠 `--rebuild` 補齊。

## 為什麼訂 `entity.extracted` 而不是 `embedding.requested`

`embedding.requested` 是 SPEC §20 的保留名，目前零生產者／零消費者，與 V0.1 的 `search.index.requested` 同一狀態。硬去訂一則沒人發的事件不算完成。

indexer 與本服務是**同一個 topic 上的獨立 consumer group**（`osint-indexer`／`osint-embedding-worker`），沒有順序保證。`update_fields` 碰到 NotFound 當 indexer lag，重試後仍提交 offset。

不訂 `job.dispatched`：沒有 `embedding_rebuild` job type，全量重建只留給 CLI。

## 兩個目標、兩個 index

| 目標 | 來源文字 | 寫入 | API |
|---|---|---|---|
| Document title／body | `Document.title`／`Document.body` | 既有 `osint-documents` 的 `embedding_en`／`embedding_multi` | **`update_fields`**，永不 `index()` |
| Entity description | `Entity.description` | 新 index `osint-entities` 的 `description_vector_multi` | `index()` 整份覆寫 |

獨立 index，不 nested 進 documents：一份 Entity 會活在上百份文件的 nested 陣列裡，違反「PostgreSQL 是 canonical，OpenSearch 是 rebuildable projection」。

Document 欄位名常數來自 `indexer::schema`（`F_EMBEDDING_EN` 等），不要在本 crate 再寫一份字面值。

### Document overlay last-write-wins

`osint-documents` 每個語言空間只有一個向量欄位。title 先寫、body 後寫同一個 `embedding_en`／`embedding_multi`。body 比較能代表整份文件；沒有 body 才留下 title 的向量。

`index()` 會取代整個 `_source`，把 indexer 寫進去的 title／body／entities 清掉。缺文件時 `update_fields` 回 `NotFound`（`doc_as_upsert=false`），不憑空 upsert。

### Entity 語言永遠未知

`Entity` 沒有 `language` 欄位。`EmbeddingRequest.language = None` 代表未知，路由到多語 e5，**不是** MiniLM。V0.2 **只寫** `description_vector_multi`；`description_vector_en` 在 mapping 裡佔位子但永遠是空的（等 NER 為 Entity 加上語言）。兩個 knn 欄位的 `space_type` 都是 `cosinesimil`，與 documents 的 `embedding_en=l2` 不同——這裡沒有 MiniLM 資料。

### 不處理的目標

`EmbeddingTarget::EventDescription` 在 trait／schema 裡存在，但 V0.2 沒有 Event 抽取管線（`core_model::Event` 只有 CRUD）。本服務**不會**掃 Event、也不會假裝處理後靜默跳過一則 event payload——根本沒有那種事件。

## 事件只說「哪一份要重算」，內容一律重讀 PostgreSQL

`process_extracted()` 從 payload 取 `document_id` 與 `entity_ids`。文字、語言、`merged_into`／`duplicate_of` **一律重新從 PostgreSQL 讀**。直接拿 payload 當內容會讓亂序或重放的事件覆蓋掉較新的狀態。

payload 的 `entity_ids` 可能含之後被 merge 的 id。`get_entity`；若 `merged_into.is_some()` 則 skip。duplicate Document 同樣 skip（indexer 也不索引它）。

## 冪等與 re-generate gate

re-generate 的唯一 gate 是 `RelationalStore::find_embedding`（同一目標、同一模型、同一內容雜湊）。`put_embedding` **不是** upsert；撞 UNIQUE 回 `StorageError::Conflict`，當成功吞掉——兩個 consumer 同時算到同一段文字時會發生，不是錯誤。

`content_hash` = SHA-256 of raw UTF-8 text，**不含** `query:`／`passage:` 前綴。定義在 `storage_core::embedding_content_hash`。

Postgres `embeddings` 表只存 metadata，沒有向量本體。`--rebuild` 因此 **force=true** 略過 cache：indexer `--rebuild --drop` 之後 `osint-documents` 是空的，cache hit 會讓向量永遠回不去 OpenSearch。force 時 `put_embedding` 撞 UNIQUE 仍當 Conflict=ok。

## Redis 向量暫存（與 Stage 5 共用）

這是 Redis 在本專案的**第一個應用資料用途**（之前只做 health／conformance）。
key 定義在 `storage_core::embedding_cache_key`：

```text
embedding-cache:v1:{content_hash}
```

value 是整份 `EmbeddingVector` JSON（含 `model`／`model_version`／`vector`）。
TTL 讀 `[embedding].dedup_cache_ttl_secs`（預設 900，夾 300..=3600）。

寫入端是 `osint-deduplicator` Stage 5：它在 `object.normalized` 上當場 embed，
結果不寫 `embeddings` 表、不 overlay `osint-documents`，只 best-effort
`set_ex` 進 Redis。讀取端是本服務：`embed_document_fields` 在打
`embed_bounded` **之前**先 `lookup_embedding_cache`。命中條件：

1. key 存在且 JSON 解得開
2. `content_hash` 對得上這段文字
3. `model` 對得上這次 `model_for(language)` 會用的那個

任一失敗當 miss，再打 ml-commons。model mismatch 不可默默用錯向量——
MiniLM 與 e5 空間不相通。

連線是 `Option`：`REDIS_URL` 解不出或 PING 失敗時 `osint-embedding-worker`
**照常啟動**，永遠 miss。這與 deduplicator 不同——那邊 embeddings／search／redis
任一連不上就整份停用 Stage 5。本服務沒有 Redis 只是多打推論，正確性不變。

單元測試：`redis_cache_hit_skips_embed`、`redis_cache_miss_embeds_as_before`、
`redis_model_mismatch_is_treated_as_miss`。真 Redis + ml-commons 的路徑在
`tests/e2e.rs` 的 `stage5_cache_is_reused_by_embedding_worker`，標 `#[ignore]`。

## indexer race：`update_fields` NotFound

合計 4 次嘗試（1 次立即 + 3 次退避 200／500／1000 ms）。測試可把 backoff 設成 0 ms。

重試耗盡仍 NotFound：**仍提交 offset**。卡住 partition 等 indexer 沒有意義——indexer 可能永遠寫不進去（mapping 不符）。向量 metadata 可能已寫進 Postgres；漏掉的 overlay 靠 `--rebuild` 回填。這與 indexer 永久性 bulk 失敗同一慣例。

真失敗（Postgres／OpenSearch 連不上、非預期 `StorageError`）不 commit。SIGINT 不額外 commit。

## Bounded concurrency

`[embedding].concurrent_inferences` 當 tokio Semaphore；文字走 `embed_batch` + `[embedding].batch_size`。這兩個鍵沿用既有 `[embedding]`，不在 `EmbeddingWorkerSection` 重複。推論在 OpenSearch JVM（ml-commons），本行程不載模型。

## `osint-entities` mapping

完整定義在 `crates/embedding-worker/src/schema.rs`。`dynamic: strict`。`index.knn` 只能在建立 index 時開啟。

| 欄位 | 型別 | 用途 |
|---|---|---|
| `entity_id` | keyword | `_id` 的副本 |
| `entity_type` | keyword（lowercase） | 型別過濾 |
| `name` | keyword | 顯示 |
| `normalized_name` | keyword（lowercase） | 名稱過濾 |
| `description_vector_en` | knn_vector（384，hnsw／lucene／`cosinesimil`） | **V0.2 不寫入** |
| `description_vector_en_model_version` | keyword | 同上 |
| `description_vector_multi` | knn_vector（384，hnsw／lucene／`cosinesimil`） | e5 向量 |
| `description_vector_multi_model_version` | keyword | 寫入時的模型版本（內容雜湊） |

schema／k-NN 路徑的 e2e **沒有** `#[ignore]`——`opensearch-knn` 是官方映像內建。打 `_predict` 的路徑才 `#[ignore]`（要先跑 `scripts/opensearch-ml-setup.sh` 與 `opensearch-ml-setup-e5.sh`）。

## Rebuild（`--rebuild`）

CLAUDE.md §5：OpenSearch 是可重建的 projection。

```bash
make run-embedding-worker
make rebuild-embeddings
make rebuild-embeddings-drop
```

| 模式 | 行為 |
|---|---|
| `--rebuild` | 掃 `list_documents` 跳 `duplicate_of`、`list_entities` 跳 `merged_into`；force 重算後 overlay／index。**不刪任何 index** |
| `--rebuild --drop` | 先 `delete_index(entities_index)` 再 `ensure_index_with` 再 rebuild。**只刪 `osint-entities`** |

`--drop` 不能單獨使用。`--drop` 時若 `entities_index == documents_index` 必須拒絕，避免誤刪搜尋投影。

**永不** `delete_index("osint-documents")`。那個 index 是 indexer 的，本服務只疊加向量欄位；刪掉等於把搜尋投影整份清掉，而且本服務重建時不會把 title／body／entities 補回去。

## `/health` `/ready` `/metrics`

預設綁 `127.0.0.1:18087`（`[embedding_worker].bind`）。容器內覆寫成 `0.0.0.0:18087`。

`/ready` 檢查 **Postgres + OpenSearch**。不把 ml-commons 模型部署當獨立 ready check——模型沒部署會在第一次 `embed` 失敗，那是設定問題，不是「這個行程還沒準備好收流量」。

| metric | 意義 |
|---|---|
| `osint_queue_depth` | consumer lag |
| `osint_embedding_worker_document_applied_total` | Document overlay 成功 |
| `osint_embedding_worker_title_applied_total`／`body_applied_total` | 哪個欄位寫進去 |
| `osint_embedding_worker_entity_applied_total` | Entity 寫進 `osint-entities` |
| `osint_embedding_worker_skipped_cached_total` | `find_embedding` 命中 |
| `osint_embedding_worker_skipped_duplicate_total` | Document `duplicate_of` |
| `osint_embedding_worker_skipped_merged_total` | Entity `merged_into` |
| `osint_embedding_worker_skipped_no_description_total` | Entity 沒有 description |
| `osint_embedding_worker_document_missing_total`／`entity_missing_total` | payload 指向已刪列 |
| `osint_embedding_worker_update_fields_retry_total` | indexer lag 重試 |
| `osint_embedding_worker_update_fields_exhausted_total` | 重試耗盡仍 NotFound（仍 commit） |
| `osint_embedding_worker_conflict_total` | `put_embedding` UNIQUE |
| `osint_embedding_worker_redis_cache_hit_total` | Stage 5 Redis 暫存命中（沒打 ml-commons） |
| `osint_embedding_worker_errors_total` | 真正的失敗（不提交 offset） |

設定在 `[embedding_worker]`（`config/default.toml`）：

| 鍵 | 預設 | 說明 |
|---|---|---|
| `bind` | `127.0.0.1:18087` | health 埠 |
| `consumer_group` | `osint-embedding-worker` | Kafka group |
| `entities_index` | `osint-entities` | Entity 向量 index。改名等於換一個空投影，要跑 rebuild |
| `page_size` | 100 | rebuild 每頁筆數。硬上限，沒有「不限」 |

`batch_size`／`concurrent_inferences` 在 `[embedding]`，不是這裡。

## 連線與埠隔離

連 OpenSearch 必須 `verify_not_opencti_search` + `assert_opensearch_identity`。本機 9200 可能是 OpenCTI Elasticsearch；寫錯一個埠號會開始往別人的叢集寫資料，而且一路都不會報錯。

容器內不要設 `OSINT_STRICT_PORT_ISOLATION`、不要 `env_file: .env`。密鑰只走 SecretRef。

本行程 256m／0.25 CPU（compose.dev）；推論記憶體在 OpenSearch JVM（3g／1536m heap）。

## 已知限制

1. **Entity 不寫 `description_vector_en`**（見上；這是設計，不是還沒做）。
2. **不處理 `EventDescription`**（見上）。
3. **沒有 `embedding.completed` 生產者**（見上）。
4. **不訂 `embedding.requested`／`job.dispatched`**（見上）。
5. **`--rebuild` 不刪多餘文件**。Document overlay 不會清掉已不該存在的向量欄位；Entity 幽靈文件要靠 `--drop`。
6. **沒有 DLQ topic**。真正的失敗不提交 offset、靠 broker 重送；永久性錯誤（payload 缺 `document_id`）目前也走同一條。
7. **跨服務 backpressure 仍未接**。consumer lag 寫進 `osint_queue_depth`，本服務自己不降速。
8. **Document 同一語言空間只有一個向量**。title 與 body 互蓋，沒有分開存。
9. **Redis 快取是效能優化，不是正確性路徑。** 沒接上／過期／model 對不上都只是多打推論。key 不含模型名，讀取端必須核對 `EmbeddingVector.model`。

## 相關文件

- `docs/developer/embedding.md`：雙模型實測、語言路由、k-NN mapping
- `docs/developer/indexer.md`：`osint-documents` 四個向量欄位由本服務 overlay
- `docs/developer/events.md`：`entity.extracted` payload；`embedding.requested` 仍無生產者
- `docs/developer/storage-adapters.md`：`SearchStore::update_fields`／`vector_search`、`MockSearchStore`、`EmbeddingProvider`
- `docs/developer/entity-worker.md`：上游抽取
- `docs/developer/local-dev.md`：本機啟動與 e2e
