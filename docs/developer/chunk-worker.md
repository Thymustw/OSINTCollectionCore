# `osint-chunk-worker`：段落切分服務（ADR-016 階段 5 M1-3）

```text
dedup.completed ──▶ osint-chunk-worker ──▶ document_chunks（PostgreSQL）
                                       └──▶ document.chunked
```

切段規則在 `crates/chunker`，見 `chunker.md`。這個服務負責讀文件、比對、寫庫、發事件。
段落表與 `ChunkStore` 的契約見 `storage-adapters.md`。

## 處理哪些文件

| 情況 | 結果 | metric |
|---|---|---|
| 事件 `is_duplicate=true` | 跳過 | `osint_chunk_skipped_duplicate_total` |
| 事件說不重複，但 DB `duplicate_of` 有值（事件較舊） | 跳過 | 同上 |
| 文件不在 PostgreSQL | 跳過、warn | `osint_chunk_document_missing_total` |
| payload 缺 `document_id` 或 `is_duplicate` | **錯誤**，寫 `failed_events` | `osint_chunk_failed_total` |
| 重切結果與庫內完全相同 | 不寫、不發事件 | `osint_chunk_unchanged_total` |
| 第一次切，或策略版本升了 | `replace_document_chunks` 後發 `document.chunked` | `osint_chunk_documents_total`、`osint_chunk_chunks_written_total` |

缺 `is_duplicate` 時報錯，不預設成 `false`。預設成 `false` 會讓所有重複文件都被切段，而且不會有任何跡象。

其他 metric：
- `osint_chunk_fallback_fixed_total`：`whole`／`record` 超過上限，在文件內部改走 300／100 打包。
- `osint_chunk_hard_cuts_total`：單句或單段超過上限，在 token 邊界硬切。
- `osint_chunk_truncated_total`：串接文字超過 `max_source_bytes` 被截斷。
- 共用的 `osint_queue_depth`（consumer lag）、處理延遲、`failed_jobs`。

## 冪等：比對而不是 claim

每次都重切，再和庫內段落比 id、偏移、文字、header、token 數、策略與版本，`created_at` 不比。
- 完全相同：不寫，也不發事件。重送的 `dedup.completed` 不會讓下游（M2 向量化）重做。
- 有任何不同：在同一交易裡刪舊寫新，再發事件。

不像 entity-worker 那樣寫 provenance claim，因為段落本身就是可整批重切的衍生資料（D5）。
「這份文件有沒有段落、是不是最新策略」已經寫在 `document_chunks` 上，再寫一列 claim 只是多一個會和它不一致的地方。

代價是每則事件都要重切一次再比對。成本見下方「開發 DB 實測」。

## `document.chunked` payload

partition key 是 `document_id`。

```json
{
  "document_id": "<uuid>",
  "chunk_count": 3,
  "chunk_ids": ["<uuid>", "..."],
  "strategy": "fixed",
  "strategy_version": 1,
  "max_token_count": 301,
  "truncated": false
}
```

段落有變更才發。沒有任何文字的文件切出 0 段：如果庫裡原本也沒有段落，算未變更，不發事件；
如果原本有段落（例如內容被改成空的），舊段落會被刪掉，並發一則 `chunk_count: 0`，讓下游知道要清掉舊向量。

## 併發與資源

- 循序消費，同 entity-worker。要更高吞吐就加 partition 和實例，不在這裡對外部輸入無界 spawn（CLAUDE.md §6）。
- tokenizer 是 CPU-bound，切段走 `spawn_blocking`，不卡 Tokio executor。
- tokenizer 約 17 MB，啟動時載入一次，跨執行緒共用。
- compose dev 限制是 `mem_limit: 384m`、`cpus: 0.50`。

## 失敗處理

處理失敗時寫一列 `failed_events`（ADR-008），然後照常提交 offset，一則毒訊息不會卡住 partition。
段落沒寫入；之後重送該事件或跑 `--rebuild` 會再試。

啟動檢查：tokenizer 檔不存在或 sha256 不符時，行程直接以 exit 1 結束，訊息會指出 `scripts/fetch-e5-tokenizer.sh`。
連 DB 之前就檢查，不會帶著錯的計數規則跑起來。

## `--rebuild`

```bash
make rebuild-chunks           # = cargo run -p chunk-worker --bin osint-chunk-worker -- --rebuild
# 容器內：docker compose ... run --rm osint-chunk-worker --rebuild
```

- 依 id 分頁掃全部非重複文件，每頁 `rebuild_page_size` 份（預設 200），沒變的不寫。
- **不發 `document.chunked`**：M2 的向量化自己從 PostgreSQL 重建，一次 rebuild 發上萬則事件只會把下游淹掉。
- 每頁之間檢查 SIGTERM／SIGINT，手上那頁跑完就停。輸出會寫「被中斷」，exit 0。
- 單份失敗只計數、記 error，不中斷整個 rebuild；最後只要有失敗就 exit 1。

什麼時候要跑：
- 升了任何策略版本（見 `chunker.md`「版本」）。
- 第一次部署，要補齊既有文件。
- 段落表被清掉。

## 設定（`[chunk_worker]`）

| 欄位 | 預設 | 說明 |
|---|---|---|
| `bind` | `127.0.0.1:18090` | health 埠；compose 覆寫成 `0.0.0.0:18090` |
| `consumer_group` | `osint-chunk-worker` | |
| `tokenizer_path` | `models/e5-small/tokenizer.json` | 容器內由 image 的 `OSINT__CHUNK_WORKER__TOKENIZER_PATH=/opt/e5/tokenizer.json` 覆寫 |
| `target_tokens`／`overlap_tokens` | 300／100 | D6 參考值 |
| `max_model_tokens` | 512 | e5 上限，含 `passage: ` 前綴與特殊 token |
| `max_header_tokens` | 64 | |
| `max_source_bytes` | 2097152 | 串接文字上限，超過截斷 |
| `rebuild_page_size` | 200 | |

**改任何切段參數都等於換策略**：要升 `chunker::Strategy::version`，再跑 `--rebuild`。
如果不升版本，新舊參數切出的段落會共用同一組 id 規則，混在一起，比對時又會被當成「有變更」整批改寫。

## 容器

Dockerfile 的 `e5-tokenizer` stage 在建置時用 curl 下載 tokenizer.json，再用 `sha256sum -c` 驗證，不符就 build 失敗。
`osint-chunk-worker` stage 把檔案放在 `/opt/e5/tokenizer.json`（root 擁有、0444），repo 不放這個檔。

## 開發 DB 實測（2026-11-18，容器版）

第一次啟動時，新的 consumer group 從頭重讀 `dedup.completed` 的全部歷史：

| 項目 | 數值 |
|---|---|
| 處理事件數 | 10,726 則，耗時 284 s，平均處理延遲 9.2 ms／則 |
| 寫入 | 6,098 份文件、16,833 段（等於開發 DB 全部非重複文件） |
| 文件已不在 PG（歷史事件） | 3,852 則，warn 後跳過 |
| 標記重複而跳過 | 776 則 |
| 失敗 | 0 |
| 改走 300／100 打包／硬切 | 1,387 份／12 次，與 `chunker` 離線語料驗收完全一致 |
| `token_count` | p50 274、p99 417、最大 508 |
| 記憶體 | 峰值 311 MiB，上限 384 MiB（81%）；restart 0、OOM 無 |
| CPU | 重讀期間約 11%（`docker stats` 單次取樣） |

接著用 SQL 驗收：
- 偏移：以 `substr(串接文字, start_char+1, len) = text` 逐段比對，16,833 段中 0 段不符。
- 覆蓋：有文字卻沒有段落的非重複文件 0 份。

容器內跑 `--rebuild`，結果是掃過 6,098 份、寫入 0、未變 6,098、失敗 0，耗時 51.8 s（約 118 份／秒，含逐份讀 DB 比對）。
這證實重切是冪等的。

記憶體峰值離上限只剩約 19%。如果之後的語料以長網頁為主（單份切出的段數更多），要再量一次峰值，必要時調高上限。

## 測試

```bash
scripts/fetch-e5-tokenizer.sh
cargo test -p chunk-worker
```

`tests/e2e.rs` 用一次性 DB（`TestDatabase`）與一次性 topic namespace（`TestTopics`），不碰開發 DB，也不和執行中的服務共用 topic：

| 測試 | 驗什麼 |
|---|---|
| `dedup_completed_to_document_chunked` | 事件進、段落寫入、`document.chunked` 出且 `chunk_ids` 與庫內一致；重送同一則回 `Unchanged`，3 秒內沒有第二則事件 |
| `duplicates_missing_and_bad_payloads_are_skipped` | 事件標重複、事件不重複但 DB 標重複、文件不存在、缺 `is_duplicate` |
| `rebuild_chunks_all_canonical_documents_and_is_idempotent` | page_size 2 掃 5 份正本加 1 份重複；第二次全部 `unchanged`；停止訊號先到時一份都不處理 |
