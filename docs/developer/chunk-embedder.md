# `osint-chunk-embedder`：段落向量服務（ADR-016 階段 5 M2）

```text
document.chunked ──▶ osint-chunk-embedder ──▶ 重讀 document_chunks（PostgreSQL）
                                          ├──▶ osint-inference（POST /v1/embeddings）
                                          └──▶ OpenSearch osint-chunks（_id = 段落 id）
```

段落由 chunk-worker 產生（見 `chunk-worker.md`），向量由 `services/inference`（`osint-inference`）計算，
這個服務只負責「讀段落 → 送推論 → 寫 index → 清掉過期向量」。推論服務的行為（逐筆推論、503 規則、
一致性門檻）見 `services/inference/README.md`。

**檢索 API 目前還沒有查 `osint-chunks`**——那是 M3 的事。現在寫進去的向量只有對帳（`GET /api/v1/ops/chunks`）會讀。

## 事件內容不可信，只取 `document_id`

payload 只用 `document_id`。段落文字、id、段數一律從 PostgreSQL 重讀：事件可能是舊的（之後又重切過），
也可能被竄改。`chunk_ids` 等欄位只給人看，不拿來決定寫什麼。

## 處理一份文件

| 情況 | 結果 | metric |
|---|---|---|
| 文件不在 PostgreSQL、`duplicate_of` 有值、或沒有段落 | 刪掉 index 裡這份文件的**所有**向量（`Cleared`） | `osint_chunk_embed_cleared_total` |
| 有段落 | 分批送推論 → `bulk_index` → 刪掉已不存在的段落 id | `osint_chunk_embed_documents_total`、`osint_chunk_embed_vectors_total`、`osint_chunk_embed_removed_total` |
| payload 缺 `document_id` | **錯誤**，寫 `failed_events` | `osint_chunk_embed_failed_total` |

步驟：
1. 讀文件與段落，再用 `ChunkStore::document_collection_ids` 查這份文件屬於哪些 Collection（依 id 升序，最多 1000 個）。
2. 段落每 `batch_size` 段一批，輸入是 `passage: ` + `chunker::embedding_text(header, text)`。
3. `bulk_index` 寫 `osint-chunks`，`_id` ＝ 段落 id，所以重送是覆寫不是新增。bulk 有任何一筆失敗就整份回錯誤。
4. 查出 index 裡這份文件的向量 id（最多 1000 筆），不在這次段落清單裡的刪掉。這處理「重切後段數變少」。

`batch_size` 只省 HTTP 來回。推論服務內部是一筆一筆跑的，因為動態 int8 量化下 padding 批次的向量不可重現
（見 `services/inference/README.md`「逐筆推論」與 ADR-016 的 2026-11-24 註記）。

## 推論失敗：暫時性退避、永久性記錄

| 推論回應 | 分類 | 處理 |
|---|---|---|
| 503、429、其他 5xx、連線失敗、逾時 | 暫時性 | 指數退避後重試**同一批**，`backoff_initial_ms`（1 s）起每次加倍、上限 `backoff_max_ms`（30 s）；有 `Retry-After` 時取兩者較大者。**不放棄** |
| 其他 4xx（例如超過 512 token 的 400）、回應格式不符、筆數或維度不對 | 永久性 | 回錯誤，寫 `failed_events` 後提交 offset |

每次重試 `osint_chunk_embed_retries_total` +1。推論服務停掉時，這個 metric 會一直往上爬，事件停在原地，不會遺失。

**退避中收到 SIGTERM／SIGINT**：這則**不提交** offset，行程結束；下次啟動 Redpanda 重送同一則。
寫入是覆寫，所以重送安全。

永久性錯誤與其他錯誤（PostgreSQL、OpenSearch bulk 失敗）都寫一列 `failed_events`（ADR-008），再照常提交，
一則毒訊息不會卡住 partition。

## 慢批暫停（讓出 CPU）

owner 裁決（ADR-016，2026-11-05）：同批工作每批延遲明顯變慢就暫停背景工作。實作：

- 量的是**每 token 耗時**（批次耗時 ÷ 這批段落 `token_count` 總和），取指數移動平均（α ＝ 0.1）當基準；前 5 個樣本只建基準不判定。
- 每 token 耗時 > 基準 × `slow_factor`（預設 3.0）→ 睡 `slow_pause_ms`（預設 2000 ms），`osint_chunk_embed_slow_pauses_total` +1。
- token 總數 < 64 的批次不判定、不進基準：HTTP 來回等固定成本占比太高。
- **慢批不併入基準**，否則持續變慢時基準跟著升，永遠不會觸發。
- `slow_factor` 必須 > 1（NaN 也擋），否則啟動失敗。
- 實作在 `src/slow.rs`（純邏輯，有單元測試）。

**為什麼不用每批耗時**（2026-11-24 回填實測推翻了原本的設計）：原本以「每批耗時」當基準。
語料多是短文件，一批常只有 1～2 段（約 17 ms），基準被拉到 18 ms；碰到一份 8 段長文
（123～246 ms）就超過 3 倍、觸發暫停。主機 load 0.5、完全沒有壓力，30 秒內卻誤暫停 13 次，
回填吞吐被壓到約每秒 15 段。批次耗時主要由 token 數決定，所以要以 token 正規化。

倍數 3.0 仍是暫定值（M2-4 回填時記錄是否觸發）。

## `osint-chunks` index

建立時的 settings：1 shard、0 replica、`refresh_interval: 1s`、`index.knn: true`
（k-NN 只能在建立 index 時打開）。

mapping 是 `dynamic: strict`，寫錯欄位名會被拒絕，不會悄悄多一個欄位：

| 欄位 | 型別 | 說明 |
|---|---|---|
| `chunk_id`、`document_id` | keyword | |
| `seq`、`strategy_version`、`token_count` | integer | |
| `text`、`header` | text（`standard`）＋ `.cjk` 子欄位（`cjk` analyzer） | 沿用 `osint-documents` 的 analyzer 慣例 |
| `text_s` | text（`cjk`） | 應用端轉的簡體副本（字級轉換，不做兩岸用詞轉換） |
| `embedding` | `knn_vector`，384 維，hnsw／lucene／cosinesimil，m 16、ef_construction 100 | 參數同 ADR-016 實驗 5-2e |
| `embedding_model`、`strategy`、`object_type`、`language`、`text_norm_version` | keyword | `embedding_model` 是推論服務回應裡的模型名稱 |
| `collection_ids` | keyword（陣列） | 給之後的段落檢索依 Collection 過濾（ADR-016 Q5：現在就放，之後才加要整個重建） |
| `published_at`、`collected_at`、`indexed_at` | date | 段落表沒有時間欄位，這裡冗餘一份文件的時間 |

文字欄位現在就放，是為了之後段落層級 BM25／混合檢索不必重建。

改 mapping（加欄位以外的任何修改）要 `--rebuild --drop`。換模型也要：向量空間不相通，不能新舊混用。

## `--rebuild`

```bash
make rebuild-chunk-vectors   # = cargo run -p chunk-embedder --bin osint-chunk-embedder -- --rebuild
# 先刪 index 再重建：cargo run -p chunk-embedder --bin osint-chunk-embedder -- --rebuild --drop
# 容器內：docker compose ... --profile app run --rm osint-chunk-embedder --rebuild
```

- 依 `document_chunks` 分頁掃有段落的文件，每頁 `rebuild_page_size` 份（預設 100），整份覆寫。
- 不消費也不發事件。
- 每頁之間、以及退避等待中檢查 SIGTERM／SIGINT，輸出會寫「被中斷」。
- 單份失敗只計數、記 error，不中斷整個 rebuild；最後只要有失敗就 exit 1。
- **`--drop` 單獨使用會被拒絕**（exit 2）：單獨刪掉 `osint-chunks` 會讓段落向量直接消失。

什麼時候要跑：
- 第一次部署，要替既有段落補向量（chunk-worker 的 `--rebuild` 不發事件，所以不會觸發這裡）。
- 段落重切（`make rebuild-chunks`）之後。
- 換模型、或 mapping 有破壞性變更 → `--rebuild --drop`。
- `GET /api/v1/ops/chunks` 穩定後 `extra_vectors > 0` → `--rebuild --drop`。

全量回填實測（2026-11-24，開發 DB）：6,098 份文件／16,833 段，`--rebuild --drop` 476 秒（約 35 段／秒），失敗 0；推論服務是瓶頸（CPU 中位數 395%／4 核上限，記憶體峰值 542 MiB），embedder 本身 CPU 中位數 2%、記憶體 < 8 MiB。

## 對帳：`GET /api/v1/ops/chunks`

core-api 的 Operations Center 路由（viewer 以上，唯讀、不寫稽核）。PostgreSQL 段落表對 `osint-chunks` 的 `_count`：

```json
{
  "index": "osint-chunks",
  "chunked_documents": 6098,
  "chunks": 16833,
  "max_token_count": 508,
  "vectors": 16833,
  "missing_vectors": 0,
  "extra_vectors": 0,
  "in_sync": true
}
```

（數字只是格式示意。）

- `missing_vectors` ＝ 段落數 − 向量數（下限 0）。回填中、或 embedder 落後時 > 0。
- `extra_vectors` ＝ 向量數 − 段落數（下限 0）。> 0 代表 index 有 PostgreSQL 已不存在的段落，要 `--rebuild --drop`。
- `in_sync` ＝ 兩者都是 0。
- 兩個計數**不是同一瞬間取的**，寫入進行中時差值會跳動；穩定後才有對帳意義。
- 沒接上 PostgreSQL 或 OpenSearch 時回 503，其他路由照常。

消費落後（lag）不在這裡：看 `GET /api/v1/ops/queues` 的 `chunk-embedder` 列（group `osint-chunk-embedder`，topic `document.chunked`）。

## 併發與資源

- 循序消費、一次一份文件，同 chunk-worker。要更高吞吐就加 partition 和實例，不對外部輸入無界 spawn（CLAUDE.md §6）。
- CPU 花在 `osint-inference`，這裡只做 HTTP＋JSON＋bulk。
- compose dev 限制是 `mem_limit: 128m`、`cpus: 0.50`（`docker/docker-compose.dev.yml`）。實際峰值還沒量（M2-4）。
- 推論服務自己有並發上限（`INFERENCE_MAX_PENDING`），滿了回 503，這裡退避。

## Health

`/health`（存活）、`/ready`、`/metrics`，埠 18092。

`/ready` 檢查三項：PostgreSQL、OpenSearch、推論服務的 `/ready`（2 秒逾時）。推論服務沒起來時 `/ready` 回 503，
但行程不會結束，事件也不會被丟掉。

metric：
- `osint_chunk_embed_documents_total`、`_vectors_total`、`_removed_total`、`_cleared_total`
- `osint_chunk_embed_retries_total`、`_slow_pauses_total`、`_failed_total`
- 共用的 `osint_queue_depth`（consumer lag，每 10 則或閒置逾時時量一次）、處理延遲、`failed_jobs`

## 設定（`[chunk_embedder]`）

環境變數覆寫：`OSINT__CHUNK_EMBEDDER__<鍵>`。

| 欄位 | 預設 | 說明 |
|---|---|---|
| `bind` | `127.0.0.1:18092` | health 埠；compose 覆寫成 `0.0.0.0:18092` |
| `consumer_group` | `osint-chunk-embedder` | |
| `inference_url` | `http://127.0.0.1:18091` | compose 覆寫成 `http://osint-inference:18091` |
| `inference_timeout_secs` | 60 | 單次推論請求逾時（逾時算暫時性錯誤） |
| `index` | `osint-chunks` | core-api 的 `/ops/chunks` 也讀這個值 |
| `batch_size` | 8 | 1..=32（推論服務單次上限 32），超出啟動失敗 |
| `backoff_initial_ms`／`backoff_max_ms` | 1000／30000 | |
| `slow_factor` | 3.0 | 必須 > 1 |
| `slow_pause_ms` | 2000 | |
| `rebuild_page_size` | 100 | |

## 容器

- Dockerfile target `osint-chunk-embedder`，image `osint-core/osint-chunk-embedder:0.1.0`，compose profile `app`，埠 18092。
- **刻意沒有 `depends_on: osint-inference`**：推論服務晚起或重啟時，這邊只是退避等待。
- 已加進 Makefile 的 `SERVICES`（`make image-scan` 的掃描清單；`make image-build` 走 compose `--profile app build`，本來就會建），以及 CI 的 image-build／SBOM／image-scan matrix。
- `osint-inference` 不在 Rust 的 matrix 裡，CI 另有 `inference` job：pip-audit、從 Dockerfile 的 `model` stage 取出模型檔、pytest、image build、trivy（CRITICAL 擋、HIGH 列出）、CycloneDX SBOM。

## 測試

```bash
make compose-up
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml --profile app up -d osint-inference
cargo test -p chunk-embedder
```

`tests/e2e.rs` 用一次性 DB（`TestDatabase`）、一次性 topic namespace（`TestTopics`）、一次性 index
（`osint-chunks-e2e-<uuid>`，Drop 時刪除，含 panic），推論打真的 `osint-inference`（`127.0.0.1:18091`，或 `INFERENCE_URL`）：

| 測試 | 驗什麼 |
|---|---|
| `embeds_chunks_shrinks_on_rechunk_and_clears_on_duplicate` | 寫入三段、欄位與 `collection_ids` 正確、collection filter 命中、簡體副本；重送冪等；重切成兩段時舊段被刪；文件標重複後向量清空；不存在的文件不報錯；缺 `document_id` 報錯 |
| `consumes_document_chunked_from_broker_and_rebuilds` | 從 broker 收 `document.chunked` 寫入；`rebuild`（page_size 2）補齊其餘文件；停止訊號先到時 `interrupted` |
| `unreachable_inference_is_retried_until_stopped` | 連不上推論服務時持續退避不放棄，停止訊號到了才回 |

單元測試：mapping 是 strict 且向量 384 維 cosinesimil；`--drop` 單獨使用被拒；`batch_size` 0／33、`slow_factor` 1.0／NaN 被拒；
慢批偵測（`slow.rs`）：同速率的長批不暫停（回填誤觸發的回歸）、每 token 耗時 4 倍時暫停且不抬高基準、極短批不判定、暖機期不判定。

## M2-4 實測（2026-11-24，開發 DB）

| 項目 | 結果 |
|---|---|
| 回填 | 6,098 份文件／16,833 段，476 秒（約 35 段／秒），失敗 0，`/ops/chunks` `in_sync: true` |
| 瓶頸 | osint-inference：CPU 中位數 395%（4 核上限），記憶體峰值 542 MiB（上限 1 GB） |
| embedder | CPU 中位數 2%、峰值 13%；容器記憶體 < 8 MiB |
| OpenSearch | 回填期間 CPU 中位數 8%、峰值 79%；容器記憶體 4.6 GiB（8 GB 上限內） |
| 主機 load1 | 中位數 4.4（24 核） |
| index 大小 | 209.5 MB（M2 計畫估 100 MB，實際約兩倍） |
| kNN 延遲（k＝200、`document_id` terms filter） | v0 90 題 p50 34 ms／p95 104 ms；OSINT 52 題 p50 30 ms／p95 93 ms |
| 查詢向量（推論服務） | p50 9 ms／p95 11 ms |
| 慢批暫停 | 修正後整輪 1 次（71 token 的小批，剛過 64 門檻，代價 2 秒） |

吞吐 35 段／秒低於 M2-1 在主機直接量的 59 段／秒。推測原因（**未拆開量**）：容器 cgroup 限制與 HTTP
開銷，以及回填時每段都帶 `passage:` 前綴與 header、token 數比 M2-1 的取樣多。

評估比較（只記錄、不是 gate）見 ADR-016 的 M2-4 註記與 `eval/README.md`。

## 還沒做

- 檢索 API 查 `osint-chunks`（M3）。
