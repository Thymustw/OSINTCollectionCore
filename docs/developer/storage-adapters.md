# Storage Adapter Developer Guide

This live developer document is expected to evolve with implementation.

Primary specification: 內部架構文件 STORAGE_ARCHITECTURE.md（未隨原始碼公開）。

## Adapter Development Rule

A new adapter must:

1. identify the capability interface(s) it implements
2. implement health
3. define bounded connection/concurrency policy
4. map backend errors to stable storage errors
5. provide migrations/schema handling if applicable
6. pass capability conformance tests
7. expose metrics
8. document security and limitations
9. integrate with Operations Center where applicable

Do not add database-specific code to normal domain services.

## V0.1 crates（Phase 2 storage）

```text
crates/storage-core          capability traits、StorageError、health、conformance
crates/storage-postgres      CanonicalStore + RelationalStore（sqlx 0.9、rustls-ring）
crates/storage-sqlite        EmbeddedStore + RelationalStore（sqlx 0.9 sqlite）
crates/storage-opensearch    SearchStore + EmbeddingProvider（opensearch 2.4.0、rustls-tls；ml-commons `_predict`）
crates/storage-redis         KeyValueStore（redis =1.2.2；workspace rust-version 1.85）
crates/storage-s3            ObjectStore（object_store 0.14.1 aws + reqwest/rustls）
crates/storage-neo4j         GraphStore + ProjectionStore（neo4rs 0.8.0）
```

`GraphStore`／`EmbeddingProvider` trait 與記憶體 mock 在 `storage-core`
（V0.2 Phase 0g）。上層測試仍可注入
`storage_core::mock::{MockGraphStore, MockEmbeddingProvider, MockSearchStore}`；
真實圖投影走 `storage-neo4j`；真實 embedding 走
`storage_opensearch::MlCommonsEmbeddingProvider`。

Domain 只依賴 `storage-core`。具體 adapter 由 bootstrap／composition 注入。

## Capability 對應

| Trait | 實作 crate | 後端 |
|---|---|---|
| `CanonicalStore` | `storage-postgres` | PostgreSQL 17（Core 真實來源） |
| `EmbeddedStore` | `storage-sqlite` | SQLite（本機／App／投影，不是高併發 canonical） |
| `TransactionalStore` | postgres + sqlite | 跨表交易（V0.2 Phase 0e）。見下方「`TransactionalStore`」 |
| `RelationalStore` | postgres + sqlite | V0.1 18 張表的 CRUD；`put_*` = upsert；含 `source_network_rules`。V0.2 Phase 0c 另加 migration `0007` 的五張表；Phase 1e 再加 `0008`（`entities.merged_into`、`merge_history.merged_relationships`）；Phase 3 Step 1 再加 `0010`（`embeddings` metadata，`put_embedding` **不是** upsert）與 `0011`（`duplicate_groups.model`）。見 `schema-v0.2.md` |
| `SearchStore` | `storage-opensearch`；mock：`storage_core::mock::MockSearchStore` | OpenSearch 文件索引／查詢（`index`／`bulk_index`／`bulk_upsert_fields`／`query`／`search`／`delete`／`update_fields`／`vector_search`） |
| `ProjectionStore` | `storage-opensearch`、`storage-neo4j` | 投影進度／lag／重建狀態（V0.2 Phase 0f）。見下方「`ProjectionStore`」 |
| `GraphStore` | `storage-neo4j`；mock：`storage_core::mock::MockGraphStore` | 圖寫入／遍歷（V0.2 Phase 0g／Phase 2）。見下方「`GraphStore`」與「`storage-neo4j`」 |
| `EmbeddingProvider` | `storage-opensearch`（`MlCommonsEmbeddingProvider`）；mock：`storage_core::mock::MockEmbeddingProvider` | 文字→向量（V0.2 Phase 3）。見下方「`EmbeddingProvider`」 |
| `KeyValueStore` | `storage-redis` | get/set/set_ex/del/expire |
| `ObjectStore` | `storage-s3` | MinIO put/get/delete/exists |
| `HealthProvider` | 全部 | `health()` |
| `StorageAdapter` | 全部 | `CapabilityDescriptor` |

`insert_raw_evidence` 只插入。同一主鍵再寫必須回 `StorageError::Conflict`，不可變成 update。

### `SearchStore` 的兩條查詢路徑

| 方法 | 型別 | 用途 |
|---|---|---|
| `query()` | `SearchQuery`（原始 `query_string`） | **只給 conformance 與運維臨時查詢**。字串會被原樣交給後端查詢語言，使用者可以用 `欄位名:值`／`*`／`~` 存取任意欄位或做 wildcard DoS |
| `search()` | `StructuredSearch`（後端中立的語法樹 + 過濾 + 排序 + `search_after` + highlight） | **面向使用者的路徑**。任何使用者字串都只可能落在 `QueryExpr::Term`／`Phrase` 的值裡，不可能變成查詢語言的結構 |
| `update_fields()` | index + id + 部分欄位 JSON | OpenSearch `_update`，**不** `doc_as_upsert`。疊加欄位、不覆寫整個 `_source`。給 embedding-worker 事後補寫 `embedding_en` 等向量欄位用——用 `index()` 會把 title／body／entities 清空。文件不存在回 `NotFound`，不憑空建立一份殘缺文件 |
| `bulk_upsert_fields()` | `Vec<SearchDocument>` → `BulkIndexResult` | OpenSearch bulk `"update"` + `doc_as_upsert=true`。跟 `bulk_index` 一樣回逐筆失敗，但**只合併 `body` 裡列出的欄位**，其餘既有欄位維持原樣。給 indexer 用：它本來就要負責建立文件（所以允許 upsert），但不能把 embedding-worker 疊加的向量欄位整份取代掉。跟 `update_fields` 的差異是文件不存在時會建立，而不是回 `NotFound` |
| `vector_search()` | `VectorSearch`（欄位 + 向量 + k + filters） | k-NN 最近鄰。filter 放 knn 子句**內**（lucene engine 的 native filtered knn）；外層 bool post-filter 在 k 很小且最近鄰不符合條件時會回空。呼叫端自己選 `embedding_en`／`embedding_multi`，trait 不做語言判斷 |

`MockSearchStore`（V0.2 Phase 3 Step 3）給 embedding-worker 單元測試用：
`index()`／`bulk_index()` 整份覆寫 `_source`；`bulk_upsert_fields()` 已存在就合併
`body` 的鍵、不存在就整份寫入；`update_fields()` 合併欄位，缺 id 回 `NotFound`。
`set_update_not_found_remaining(n)` 即使文件已在 store 也先回 n 次 NotFound，
用來測 indexer lag 的重試。`vector_search` 是記憶體 brute-force cosine，
不是 HNSW。實作是 `Arc<Mutex<...>>`，可 `Clone` 後從 worker 取出同一份記憶體斷言。

`bulk_index` 與 `bulk_upsert_fields` 都回 `BulkIndexResult { indexed, errors, failures }`。
`failures` 逐筆帶 `id`／`status`／`reason`，`BulkFailure::is_retryable()` 區分暫時性
（429/502/503/504）與永久性（400）。**只回一個 `errors: 3` 沒辦法重試也沒辦法分類**，
呼叫端只能整批重送或整批放棄——那是資料靜默消失的入口。

`bulk_index` 整份取代 `_source`（OpenSearch `"index"` action）。`osint-documents` 從
V0.2 Phase 3 Step 2 起有兩個寫入者，indexer 改走 `bulk_upsert_fields`；新的 SearchStore
adapter 必須通過 `storage_core::conformance::assert_bulk_upsert_preserves_unknown_fields`。

新增 SearchStore adapter 必須通過 `storage_core::conformance::assert_structured_search`
（驗過濾真的過濾、`Not` 真的排除、hit 帶得回 `sort`、`search_after` 真的接續）。
呼叫端要先自己建好帶明確 mapping 的 index——建 index 的語法是後端專屬的，
放進 conformance 就等於把 OpenSearch 的 mapping DSL 塞進中立介面。

`source_network_rules`（migration `0002`）經 `put_network_rule`／`list_network_rules` 存取。Hard-deny 驗證在 `connector-sdk`，不在 SQL CHECK。

### Cursor 分頁的 list 方法

`RelationalStore` 上帶 cursor 的 list 方法（`osint-cli` 與 `GET /api/v1/jobs` 用）：

| 方法 | 排序 | 備註 |
|---|---|---|
| `list_sources(after, limit)` | `id DESC` | id 是 UUID v7 → 最新在前 |
| `list_connectors(after, limit)` | `id DESC` | **不等於時間序**，見下方警告 |
| `list_raw_evidence(after, limit)` | `id DESC` | UUID v7 |
| `list_raw_evidence_by_source(source_id, after, limit)` | `id DESC` | 先過濾 `source_id` |
| `list_documents(after, limit)` | `id DESC` | UUID v7 |
| `list_jobs(after, limit)` | `id DESC` | UUID v7 |
| `list_entities(after, limit)` | `id DESC` | **不等於時間序**，Entity 的 id 是 UUID v5 |
| `list_collections(after, limit)` | `id DESC` | UUID v7 |
| `list_events(after, limit)` | `id DESC` | UUID v7（V0.1 沒有寫入者） |
| `list_relationships(after, limit)` | `id DESC` | **不等於時間序**，id 是 UUID v5 |
| `list_jobs_by_status(status, after, limit)` | `id DESC` | 過濾在 SQL 裡做 |

Phase 6b（`GET /api/v1/*` 的過濾參數）新增的四個，語意與上表相同，
差別只在多一個**在 SQL 裡**求值的過濾條件：

| 方法 | 對應的 query | 過濾條件 |
|---|---|---|
| `list_connectors_by_enabled(enabled, after, limit)` | `?enabled=` | `enabled = $n`，`None` 代表不過濾 |
| `list_documents_filtered(object_type, include_duplicates, after, limit)` | `?object_type=`、`?include_duplicates=` | `object_type = $n`；`include_duplicates=false` 時加 `duplicate_of IS NULL` |
| `list_entities_by_type(entity_type, after, limit)` | `?entity_type=` | `entity_type = $n` |
| `list_relationships_by_type(relationship_type, after, limit)` | `?type=` | `relationship_type = $n` |

> ⚠️ **過濾一定要在 SQL 裡做，不可以取回一頁再 filter。** 一頁最多 100 筆，
> 「最近 100 筆剛好全是轉載」會讓 `GET /objects` 回一個空頁——
> 而「沒有文件」與「最近 100 筆都是重複」是完全不同的兩件事。

規則：

- `after` 是 **strictly less than**（`id < after`），所以「用上一頁最後一筆 id 當 cursor」不會重複。
- `limit` 由 adapter 夾在 `1..=100`。**傳更大的值不會報錯，只會靜默回 100 筆**；
  呼叫端要更多筆就得自己翻頁（`osint-cli` 的 `collect_paged!` 就是做這件事）。
- 排序鍵一律是 `id`，不是 `retrieved_at` / `observed_at`：排序鍵與 cursor 必須同一欄，
  否則同一時間戳的多筆在翻頁時會漏掉或重複。

> ⚠️ `list_connectors` 的 `id DESC` 不保證是時間序。
> `POST /api/v1/import` 建立的 connector 用 **UUID v5**（由 source + format 推導以保持冪等），
> 沒有時間戳。只有 source／raw evidence／document／job 的 id 是 UUID v7。

Collection 的三個關聯反查（`GET /api/v1/collections/{id}` 用，只夾 `limit`、不帶 cursor）：

| 方法 | 排序 | 來源表 |
|---|---|---|
| `list_collection_sources(collection_id, limit)` | `source_id ASC` | `collection_sources` |
| `list_collection_connectors(collection_id, limit)` | `connector_id ASC` | `collection_connectors` |
| `list_collection_objects(collection_id, limit)` | `object_id ASC` | `collection_objects` |

SPEC §7 的 Relations 在 Phase 6b 之前只寫得進去、讀不出來（只有 `link_*` 沒有
`list_*`），`GET /collections/{id}` 會永遠回空清單而且不會有任何錯誤。

非分頁的 `list_enabled_connectors()`（collector 排程用，只回 `enabled = true`）與
`list_provenance_by_raw_evidence(raw_evidence_id)`／`list_provenance_by_subject(subject_id)`
（依 `timestamp, id` 升序）維持一次取回全部。

### Entity／Relationship 的反查（Phase 4b）

只夾 `limit`（1..=100）、不帶 cursor，語意同 `list_duplicate_groups_by_canonical`：

| 方法 | 排序 | 用途 |
|---|---|---|
| `find_entity_by_normalized_name(entity_type, normalized_name)` | — | entity-worker 重用既有 Entity 的**唯一**依據 |
| `list_relationships_by_object(object_id, limit)` | `id DESC` | source 端**或** target 端命中都算 |
| `list_relationship_evidence(relationship_id, limit)` | `created_at, id` 升序 | SPEC §12「任何 relationship 必須能回查 evidence」的落地點 |
| `list_entity_extractions_by_object(object_id, limit)` | `id ASC` | 冪等檢查、`documents show` |
| `list_entity_extractions_by_entity(entity_id, limit)` | `id ASC` | `entities show` |

`find_entity_by_normalized_name` 是**完全相等**比對，不折疊大小寫——折疊放進 SQL 會讓查詢
走不到 `idx_entities_natural_key`，而且兩個 backend 的 collation 規則不同，
等於在 PG 與 SQLite 上有兩種語意。正規化是呼叫端的責任。

`list_relationships_by_object` 刻意合成一個方法而不是 `by_source` / `by_target`：
Acceptance E 要從 Entity 往回走（Entity 是 target），CLI 要從 Document 往下走（Document 是
source），拆兩個只會讓每個呼叫端各查一次再自己合併去重。

SQLite 的 `list_relationship_evidence` 依 `created_at` TEXT 排序。本 schema 寫入時一律是
RFC 3339 `Z`（見 `rfc3339()`），同一 UTC 位移下字典序等同時間序，與 PG 的 `TIMESTAMPTZ` 一致。

## Migrations

```text
migrations/postgres/   Core canonical（sqlx migrate；adapter 也可 `PostgresCanonicalStore::migrate`）
migrations/sqlite/     Embedded / App / projection（`SqliteEmbeddedStore::migrate`）
```

SQLite schema 語意對齊 PostgreSQL，但不共用同一份 SQL（無 JSONB / TIMESTAMPTZ）。見 `docs/developer/schema-v0.1.md`；V0.2 的 `0007`（Entity Resolution + `failed_events`）、`0008`（Entity Merge 欄位）與 `0009`（`entity_identifiers.normalized_value` 單欄索引）見 `docs/developer/schema-v0.2.md`。

## `RelationalStore` 的 V0.2 方法（Phase 0c）

`traits.rs` 裡以 `// ===== V0.2 =====` 分隔。表在 migration `0007`；`entities.merged_into` 與 `merge_history.merged_relationships` 在 `0008`；
`idx_entity_identifiers_normalized_value` 在 `0009`。
`crates/resolver`（Phase 1c）掃描式聚合會呼叫 `get_entity`、
`find_entity_by_normalized_name`、`list_entity_aliases_by_entity`、
`find_entity_aliases_by_text`、`list_relationships_by_object`、
`list_entity_identifiers_by_entity`、`find_entity_identifiers_by_normalized_value`、
`put_resolution_candidate`，以及 `EmbeddingProvider`／`GraphStore`。
`entity_identifiers` 的寫入衝突路徑仍由 entity-worker 呼叫
`find_entity_identifier_owner`／`put_resolution_candidate`。
`crates/merge` 會在交易裡呼叫 `list_*`／`put_*`／`delete_relationship`／
`put_merge_history`（見 `docs/developer/merge.md`）。
DLQ 重放仍沒有生產呼叫端。graph-worker（`osint-graph-worker`）是 `GraphStore` 的生產寫入端：訂 `relationship.changed`，只投影 Entity→Entity 的邊。

| 方法 | 排序／契約 |
|---|---|
| `put_entity_alias` / `get_entity_alias` | 依主鍵 upsert |
| `list_entity_aliases_by_entity(entity_id, limit)` | `id ASC`，`limit` 1..=100 |
| `find_entity_aliases_by_text(alias, limit)` | 精確比對 alias 文字，`id ASC`，`limit` 1..=100。resolver 反查「這個名字目前掛在哪些 Entity」 |
| `put_entity_identifier` / `get_entity_identifier` | 依主鍵 upsert；`(namespace, normalized_value)` UNIQUE |
| `list_entity_identifiers_by_entity(entity_id, limit)` | `id ASC`，`limit` 1..=100 |
| `find_entity_identifier_owner(namespace, normalized_value)` | 自然鍵反查既有 owner，最多一筆。entity-worker 寫入衝突時用它找既有 owner |
| `find_entity_identifiers_by_normalized_value(normalized_value, limit)` | **不限 namespace**，`id ASC`，`limit` 1..=100。`check_account_handle` 找「同一個 handle 出現在不同平台」 |
| `put_resolution_candidate` / `get_resolution_candidate` | 依主鍵 upsert；CHECK `a < b` + UNIQUE `(a, b, method)` |
| `list_resolution_candidates(status, after, limit)` | `id DESC`，cursor；`status` **在 SQL 裡**過濾，`None` = 不過濾 |
| `list_resolution_candidates_by_entity(entity_id, status, after, limit)` | `id DESC`，cursor；`entity_a_id` **或** `entity_b_id` 命中都算；`status` 在 SQL 裡過濾 |
| `put_merge_history` / `get_merge_history` | 依主鍵 upsert |
| `list_merge_history_by_entity(entity_id, limit)` | `id DESC`；`survivor_id` **或** `merged_id` 命中都算 |
| `put_failed_event(event) -> FailedEvent` | 自然鍵 `(topic, partition, offset)` upsert，**回傳實際存下來的那一列** |
| `get_failed_event` / `list_failed_events(after, limit)` | `id DESC`，cursor；**含已重放的列** |
| `mark_replayed(id, replayed_at)` | `replayed_at` 由呼叫端傳入，不由 adapter 取 `now()` |

三個「做錯了不會報錯」的地方，conformance 都有反向斷言：

- **`put_entity_identifier` 撞唯一鍵回 `Conflict` 是訊號，不是雜訊。**
  兩個 Entity 宣稱同一個識別碼時，寫入端應該去建 resolution candidate；
  吞掉的話識別碼會少記一筆而且毫無跡象。
- **`put_resolution_candidate` 的兩個約束錯誤型別不同**：CHECK（`a < b`）違反是
  `ConstraintViolation`，UNIQUE `(a, b, method)` 違反是 `Conflict`。
  呼叫端先用 `ResolutionCandidate::ordered_pair` 排序。
- **`put_failed_event` 回傳的 `id` 可能不是你傳進去那個。** 自然鍵是座標不是 `id`，
  衝突時 `attempt_count` 由資料庫 +1、`id`／`first_seen` 保留既有列。
  若只回 `()`，呼叫端會拿自己產生的 id 去查而永遠查不到——表面上看起來就只是
  「DLQ 裡沒有這筆」，正是 ADR-008 要避免的靜默損失。
  傳 `replayed_at: None` 會**清掉**先前的重放時間（重放後又失敗 = 還沒修好）。

`list_merge_history_by_entity` **照樣回傳已撤銷的 merge**（`undone_at` 非 NULL）。
它是歷史的一部分，過濾掉等於違反 SPEC_V0.2 Acceptance C；要不要顯示由呼叫端決定。

## Conformance

契約測試寫在 `storage-core::conformance`，由各 adapter 的 `tests/conformance.rs` 呼叫，打**真實本機 Docker**，不用 mock。

- 關聯式：插入 UUID v7 列，**不 TRUNCATE** 共用 Postgres。
- SQLite：`var/osint-conformance-<nanos>.sqlite`，測完刪檔。
- OpenSearch ProjectionStore：`assert_projection_store_contract`，狀態 index 用
  per-run 名稱 `osint-core-conformance-state-<uuid>`（**不可**用正式的
  `osint-projection-state`——這一支會 `reset_projection`，跑在正式那個上等於把真的
  indexer 進度清掉），測完整個刪除。
- OpenSearch：index 名 `osint-core-conformance-<uuid>`；`assert_opensearch_identity`（拒絕 Elasticsearch tagline `You Know, for Search`）是唯一的環境無關身分驗證。`verify_not_opencti_search`（函式名稱裡的 `opencti` 反映了原始撰寫時的本機衝突對象，函式名稱本身沒有改）只在本機 `.env` 設了 `OSINT_STRICT_PORT_ISOLATION=1` 時才額外擋埠 9200——這是本機專屬防線，不是通則，CI 等其他環境不會擋。ADR-005 的決策：該 port-isolation guard 原本被寫死為無條件規則，結果第一次 CI run 就失敗（CI 沒有那個本機衝突，`docker/docker-compose.yml` 正確地把 OpenSearch 綁在 9200，卻被 hard-coded 規則拒絕）；決策是把 hard rejection 改成由 `OSINT_STRICT_PORT_ISOLATION=1` opt-in，並改以 `assert_opensearch_identity` 驗實際遠端身分（環境無關），而非用 port 號推斷。
- S3：key prefix `conformance/<uuid>`；`verify_not_opencti_s3`（函式名稱同上，反映本機原始衝突對象）同樣只在本機開 `OSINT_STRICT_PORT_ISOLATION=1` 時才擋埠 9000。MinIO 沒有等同 OpenSearch 的身分驗證 API，所以這是目前唯一防線，僅在已知衝突的機器生效。可 `ensure_bucket`（`object_store` 本身沒有 CreateBucket，adapter 用同一套 rustls HTTP client 簽 SigV4 打 `PUT /{bucket}`），不清空既有物件。測試另外用 `delete_empty_bucket`（同樣是 SigV4 `DELETE /{bucket}`）清掉為驗證新建而建的空 bucket。
- Redis：key prefix `osint-core-conformance:`；TTL 用毫秒（PSETEX / PEXPIRE）。
- Neo4j GraphStore：`assert_graph_store_contract`，節點／邊用 per-run UUID，測完
  `delete_node`（`DETACH DELETE`）清掉。**不 DROP constraint**。
  `GRAPH_MAX_HOPS` 是 adapter 硬上限 10；`KNOWN_RELATIONSHIP_TYPES` 是建
  relationship uniqueness 的那 13 種。ProjectionStore 用 per-run 名稱
  `osint-conformance-graph-<uuid>`，測完 `reset_projection`（只刪
  `:ProjectionState` 那一點）。
  ⚠️ **例外：`wipe()` 那段斷言不是 per-run 隔離的。** `GraphStore::wipe`
  刪的是**整個資料庫**所有 `:Entity`（`MATCH (n:Entity) DETACH DELETE n`），
  跟上面「用 per-run UUID、只清自己建立的節點」的慣例不同——這是它本來的
  設計目的（給 `graph-worker --rebuild --drop` 用），沒辦法做成 per-run 隔離。
  **對本機共用 Neo4j 跑 `cargo test -p storage-neo4j` 會把
  `osint-graph-worker --rebuild` 投影出來的真實資料一起清空**（2026-09-13
  實測：1482 個 `:Entity` 節點變成 0）。不是資料遺失（PostgreSQL 才是
  canonical truth），但跑完如果需要圖投影，記得重新
  `cargo run -p graph-worker --bin osint-graph-worker -- --rebuild`。

跑測試前：

```bash
cp .env.example .env   # 若還沒有
# 本機常見情境：9200/9000 可能被其他本機服務占用。必須改成偏移埠：
# OPENSEARCH_URL=http://127.0.0.1:19200
# S3_ENDPOINT=http://127.0.0.1:19000
make compose-up
make migrate-postgres
cargo test --workspace --all-targets -- --nocapture
```

`config/default.toml` 仍寫 canonical 9200 / 9000。conformance 讀 `.env`；埠號閘門只在本機開了 `OSINT_STRICT_PORT_ISOLATION=1` 時才會拒絕連到 canonical 埠上的其他服務，其他環境（CI 等）不受影響。

## TLS / 依賴約束

- sqlx：`runtime-tokio` + `tls-rustls-ring`，不開 native-tls。
- OpenSearch client：`rustls-tls`。
- object_store：`default-features = false` + `aws`。`aws` 會開 `reqwest/rustls` 與 `aws-lc-rs`，不開 native-tls。crate 預設的 `fs` feature 關掉，因為我們只接 MinIO。
- redis：精確 `=1.2.2`。`1.3+` 的 rust-version 是 1.88，超過 workspace `1.85.0`。
- rdkafka：`cmake-build` + `libz`，不開 `ssl-vendored`（見 `docs/developer/events.md`）。

## `storage-postgres` 也實作 core-security 的兩個能力介面（Phase 6a）

`crates/storage-postgres/src/security.rs` 提供 `PostgresAuditLog`（實作
`core_security::AuditLog`）與 `PostgresApiTokenStore`（實作 `ApiTokenStore`），
表是 migration 0006 的 `audit_log` 與 `api_tokens`。

這與「capability interface → concrete adapter」是同一個形狀，只是能力介面定義在
`core-security` 而不是 `storage-core`。相依方向是 `storage-postgres → core-security`，
**`core-security` 不相依任何 storage crate**，這條不能破壞——它被 `connector-sdk`、
`collector`、`osint-cli` 等不碰資料庫的 crate 相依。完整取捨見該檔的模組註解與
`docs/developer/security.md`。

兩個 adapter 共用 `PostgresCanonicalStore` 的連線池（`PostgresAuditLog::new(&store)`），
不另開池——共用工作站上多一個池只是多一份連線配額（`CLAUDE.md` §7）。

## `RelationalStore` 的 list 方法（Phase 6a 補齊四個）

`list_collections`、`list_events`、`list_relationships`、`list_jobs_by_status`
的語意、排序與 cursor 契約見 `docs/developer/schema-v0.1.md`。
三件事對兩個 backend 都成立：

- cursor 是**嚴格小於** `after`，排序鍵與 cursor 必須是同一欄。
- `limit` 由 adapter 夾在 1..=100，傳 0 會被夾成 1（**不可**變成無界查詢）。
- 有過濾條件的（`list_jobs_by_status`）**過濾在 SQL 裡做**，不是取回來再 filter。

conformance（`storage-core::conformance::assert_relational_round_trip`）對這三點都有斷言，
新增 backend 時照樣會被驗到。

## `TransactionalStore`（V0.2 Phase 0e）

```rust
let tx = store.begin().await?;          // Box<dyn Transaction>
let db = tx.store();                    // &dyn RelationalStore，綁在這條交易上
db.put_entity(&survivor).await?;
db.put_merge_history(&history).await?;
tx.commit().await?;                     // 不 commit 就 drop → 回滾
```

實作與匯出型別：

```text
storage_core::{TransactionalStore, Transaction}   ← 介面
storage_postgres::PostgresTransaction             ← begin() 的具體型別
storage_sqlite::SqliteTransaction
```

PostgreSQL 與 SQLite 都實作。**存在的理由是 V0.2 Entity Merge**（SPEC §7）：
merge 橫跨 `entities`／`entity_aliases`／`entity_identifiers`／`relationships`／
`merge_history` 五張表，沒有交易就沒辦法保證 repoint 是原子的——
「合併到一半」不會有任何錯誤訊息提到它。

### 兩個介面設計取捨（都刻意）

| 決定 | 為什麼不選另一邊 |
|---|---|
| `begin()` 回 `Box<dyn Transaction>`，不是 `type Tx` | 關聯型別會讓 `TransactionalStore` 失去 object safety：服務只能寫 `Arc<dyn TransactionalStore<Tx = PostgresTransaction>>`，一寫就綁死後端 |
| `Transaction::store() -> &dyn RelationalStore`，不是 `Transaction: RelationalStore` | 繼承的話每個 adapter 要**再寫一份 80 個方法**的 SQL。交易 handle 內部持有同一個 store 型別（只把執行對象從連線池換成交易連線），交易內外用的是**同一份 SQL**，結構上不可能分岔 |

實作方式：adapter 內部有一個 `PgConn` / `SqliteConn` enum（`Pooled` 或 `Tx`），
`conn()` 決定這次查詢跑在哪條連線上。`RelationalStore` 的實作完全不知道自己在不在交易裡。

### 行為契約（`assert_transactional_contract` 會驗）

- `commit` 之後三張表的寫入都在
- `rollback` 之後全部不在
- **沒 commit 就 drop 也會回滾**（sqlx `Transaction::drop` 排一個 ROLLBACK）
- 交易內某一步失敗（例如撞 unique）→ 回 `Conflict`，回滾後**先前的寫入也不留**
- commit 之前，交易外讀不到交易內的寫入

### 已知限制

- **不支援巢狀交易**：在交易 handle 上再 `begin()` 回
  `StorageError::UnsupportedCapability { capability: "nested_transaction" }`。
  sqlx 能用 SAVEPOINT 疊，但「內層 rollback 之後外層還能不能繼續」的語意要呼叫端決定，
  讓它看起來能用而語意未定義比不支援危險。
- **SQLite 只有一個寫入者**：交易一旦寫入就握著整個 DB 的寫鎖，其他連線的寫入會等到
  `busy_timeout`（5 秒）後失敗。WAL 之下讀取不受影響。**交易要短——不要在交易裡做網路 I/O
  或呼叫模型。**
- PostgreSQL 在交易內發生錯誤後整個交易進入 aborted 狀態，只能 `rollback`；
  不要在收到錯誤後還想在同一條交易上補寫。
- 交易 handle 借用連線池的一條連線。同時開的交易數不能超過 `pool_max`，
  否則後續 `begin()` 會在 acquire 逾時。

## `ProjectionStore`（V0.2 Phase 0f）

投影的**進度與重建狀態**。介面在 `storage_core::traits`，
`storage-opensearch` 與 `storage-neo4j` 都實作。

```rust
store.checkpoint("osint-documents").await?          // Option<ProjectionCheckpoint>
store.save_checkpoint(&checkpoint).await?
store.projection_lag("osint-documents", now).await? // ProjectionLag
store.rebuild_status("osint-documents").await?      // 沒記錄 → RebuildStatus::idle
store.set_rebuild_status(&status).await?
store.reset_projection("osint-documents").await?    // 清 checkpoint + 重建狀態
```

`projection` 參數是**目標 index／graph 名**，不是後端名。同一個後端可以同時承載多個
投影，用後端名當鍵會讓它們互相覆寫。

型別：`ProjectionCheckpoint`、`ProjectionLag`、`RebuildStatus`、
`RebuildState`（`Idle` / `Running` / `Completed` / `Failed`，序列化成 snake_case
字串存進後端——**改掉那些字串等於讓既有的狀態列讀不回來**）。

### 語意（三條做錯了不會報錯的）

| 規則 | 做錯的後果 |
|---|---|
| 沒有 checkpoint 時 `lag_seconds` 是 `None`，**不是 `0`** | 0 會被讀成「沒落後」。一個從來沒跑過的投影在儀表板上變成健康的 |
| `rebuild_status` 查不到記錄回 `Idle`，**不是 `Err`** | 「還沒有人跑過重建」是正常狀態。變成錯誤會逼每個呼叫端去分辨「查不到」與「真的壞了」，而那兩者的錯誤型別一樣 |
| `last_source_at` 是**來源物件**的時間戳，不是投影寫入時間 | 寫入時間永遠是「剛剛」，lag 會永遠接近 0——一個看起來很健康但毫無資訊的數字 |

累加與「只前進」的合併邏輯在 `ProjectionCheckpoint::advance`，**不在 adapter 裡**：

- `objects_written` 累加；
- `last_source_at` **只前進不後退**。重建是 `id DESC`（最新在前）掃過來的，
  第二頁的來源時間戳比第一頁舊，直接覆寫會讓一次成功的 rebuild 結束後 checkpoint
  停在**最舊**那一頁，lag 看起來像落後好幾個月而投影其實是完整的；
- `last_object_id` 跟著 `last_source_at` 一起換，兩者必須是同一筆物件。

### 這個 trait 為什麼沒有 upsert / delete

`STORAGE_ARCHITECTURE.md` §7 的示意有 `upsert_projection` / `delete_projection`，
那一段明寫是 illustrative。實作**刻意不照抄**：寫入已經由各後端自己的能力介面涵蓋
（搜尋投影是 `SearchStore::index`／`bulk_index`／`delete`，圖投影會是 `GraphStore`）。
再疊一套中立的寫入介面等於兩條寫入路徑——bulk 的逐筆失敗（`BulkFailure`）、
mapping 衝突、nested 欄位在中立介面裡無處可放，只能退化成「成功／失敗」，
那正是資料靜默消失的入口；而且兩條路徑遲早分岔，分岔的那條最難被測到。

### 為什麼狀態放獨立 index（`osint-projection-state`）

OpenSearch 的狀態列存在**另一個** index，`_id` 就是投影名，不放進被投影的
`osint-documents`：

- `osint-indexer --rebuild --drop` 會**刪掉整個** `osint-documents`。狀態放裡面的話，
  「上次 rebuild 何時、寫了幾筆、失敗了嗎」會在重建開始的那一瞬間消失——
  而那正是重建當下最需要回報的東西（SPEC_V0.2 §27）。
- 反過來說，狀態因此**不會**隨投影一起消失，所以要清掉只能明確呼叫
  `reset_projection`。`--drop` 重建時 indexer 會在刪 index **之前**呼叫它
  （順序不能反：先寫 `Running` 再 reset，那個 `Running` 會被 reset 清掉，
  於是重建過程中 `rebuild_status` 是 `Idle`——看起來像沒人在重建）。

mapping 是 `dynamic: strict`（理由同 `osint-documents`）。checkpoint 與 rebuild 狀態
同在一列，所以寫入一律用 `_update` 的**部分更新**（`doc_as_upsert`）：整列覆寫的話
每次 flush 寫 checkpoint 都會把上次的重建紀錄清成 null。conformance 有一條專門驗
「兩者不會互相蓋掉」。

「有沒有 checkpoint」由 `checkpoint_updated_at` 有無值判斷，
「有沒有重建紀錄」由 `rebuild_state` 有無值判斷——兩者可以只存在一個。

### `refresh=true` 而不是 `wait_for`（實測）

兩者都保證「寫完就讀得到」，但 `wait_for` 是等到下一次排程 refresh，也就是等滿
`index.refresh_interval`（預設 1 秒）。2026-09-12 在本機 19200 實測同一個單 shard
index，5 次 `_update`：

| refresh | 5 次總耗時 |
|---|---|
| `wait_for` | 4985 ms（≈1 秒／次） |
| `true` | 44 ms（≈9 ms／次） |

rebuild 每頁寫兩次狀態，`wait_for` 等於每 100 份文件多花 2 秒純等待。
換成 `true` 之後 `storage-opensearch` 的 conformance 從 6.30 s 降到 0.69 s、
indexer 的兩支 rebuild e2e 從超過 60 s（各自）降到兩支合計 20.6 s。

> ⚠️ 同樣的推論**不適用於** `osint-documents`：那裡是高頻 bulk，每批強制 refresh
> 會把 segment 數量炸開。不要把這個選擇複製過去。

### 已知限制

- **累加沒有樂觀鎖。** `objects_written` 是「讀 → 加 → 寫」，沒有 `if_seq_no`。
  同一個投影跑**兩個以上** writer 時計數會少算。`last_source_at` 因為只前進，
  最壞情況是慢一批才更新。V0.2 的部署假設是一個投影一個 writer。
- **每次寫狀態都會先 `ensure_projection_state_index`**（一次 exists 檢查）。
  寫入頻率是每批一次，這個成本可接受；要省的話得在 adapter 裡快取「已確認存在」。
- 測試必須用 `with_projection_state_index()` 換成 per-run 名稱並在結尾刪除，
  否則會寫到正式的狀態 index（`CLAUDE.md` §15）。

### `GraphStore`（V0.2 Phase 0g）

PostgreSQL 仍是 relationship truth。這個 trait 是圖**投影**的寫入與查詢面
（SPEC_V0.2 §8／§9）。`POST /graph/rebuild` 走 `ProjectionStore`，不在這裡。

Neo4j adapter（`storage-neo4j`）同時實作 `GraphStore` + `ProjectionStore`。

| 方法 | 對應 Graph API | 備註 |
|---|---|---|
| `upsert_node`／`upsert_edge` | graph-worker 同步 | upsert |
| `delete_node` | 實體刪除／merge 後清理 | **連帶刪邊**。不連帶會留下幽靈邊，shortest path 會算錯而且不報錯 |
| `delete_edge` | 關係刪除 | 不存在也 `Ok` |
| `wipe` | `osint-graph-worker --rebuild --drop` | 刪掉所有 `:Entity` 節點與邊（`DETACH DELETE`），**不碰** `:ProjectionState`。空圖也 `Ok`。沒有這個方法的話 `--drop` 只能靠逐筆 `delete_node`，Entity 已從 Postgres 刪掉的幽靈節點清不掉 |
| `neighbors`／`relationships` | `GET .../neighbors`、`GET .../relationships` | 邊當**無向**；`entity_types` 過濾的是**結果**不是沿途（否則 1→org→domain 的兩跳會走不到 domain） |
| `shortest_path` | `GET /graph/path` | 找不到或節點不存在回 `Ok(None)`，不是 `Err` |
| `query` | `POST /graph/query` | 吃結構化 `GraphQuery`，**不吃** Cypher／Gremlin 字串 |

`GraphQuery` 由 `starts` + `GraphPattern` + `GraphTraversalOptions` 組成。
`starts` 不可為空——空起點等於掃完整張圖。`GraphPattern` 是
`Neighbors`／`Relationships`／`ShortestPath { to }`／`BoundedWalk { end }`，
對齊 SPEC §9 的讀 API，不是 Cypher 字串。`GraphTraversalOptions` 帶
`max_hops`（adapter 夾硬上限；mock 是 32）、關係／實體型別、信心門檻、
時間區間。時間過濾是邊的 `[first_seen, last_seen]` 與查詢區間**重疊**，
不是 `last_seen` 落在區間內。

沒有「原始查詢字串」的後門。運維臨時查詢走 Neo4j Browser。

### `storage-neo4j`（V0.2 Phase 2）

真實圖投影。連線由呼叫端注入（`Neo4jStore::connect(bolt_uri, username, password, pool_max)`），
**不**讀 env、不寫死 `bolt://127.0.0.1:7687`。驅動是 `neo4rs 0.8.0`（穩定版，不用 0.9 RC）。
`pool_max` 來自 `[storage.graph].pool_max`（預設 5）。

`osint-api` 的 graph-context resolution 走
`GraphContextResolver<PostgresCanonicalStore, Neo4jStore>`，**不**再把
`GraphStore` 塞進 `ResolverService`。`ResolverService` 只吃
`RelationalStore` + `EmbeddingProvider`；Neo4j 斷線只讓
`POST /entities/{id}/resolve/graph-context` 回 503。
單元測試仍可注入 `storage_core::mock::MockGraphStore`。

```rust
let store = Neo4jStore::connect(&uri, &user, &password, 5).await?;
store.health().await?;
store.upsert_node(&node).await?;
store.upsert_edge(&edge).await?;
```

啟動時 `connect` 會呼叫 `ensure_constraints`（也可單獨再跑；`IF NOT EXISTS` 冪等）：

- `:Entity` 上 `entity_id` unique
- `core_model::RelationshipType` 的 13 個變體各一條 relationship uniqueness
  （`relationship_id`）。Community 版的這個 constraint **必須綁特定關聯型別**，
  不能寫一條涵蓋全部型別的。執行期才出現的未知型別字串不建 constraint，
  靠 `MERGE (s)-[r:$($relType)]->(t)` 保證同一對同一型別只有一條。

#### Cypher schema

| 東西 | 決定 | 為什麼 |
|---|---|---|
| 節點 label | `:Entity` + 具體型別（`person` → `:Person`，`snake_to_pascal`） | 共用 constraint 掛 `:Entity`；型別 label 給運維在 Browser 裡看 |
| `entity_id` | hyphenated UUID 字串 | Neo4j 沒有 UUID 型別 |
| `attributes` | 整包 JSON 字串存 `attributes_json` | Neo4j 沒有原生 JSON；目前呼叫端多數是 `{}`，不拆 property |
| 時間 | RFC 3339 `Z` 字串（毫秒） | neo4rs 0.8 的 `From` 接 `DateTime<FixedOffset>` 不接 `Utc`；固定 `Z` 下字串比較等同時間序 |
| 邊方向 | 寫入 `source → target`；查詢 `-[r]-` 無向 | Neo4j 底層關聯有方向。讀取語意對齊 `MockGraphStore` |
| 關聯型別 | `associated_with` → `ASSOCIATED_WITH`（`to_uppercase`） | Cypher 型別慣例 |
| `max_hops` | 硬上限 **10**，超過回 `ConstraintViolation` | 無界 `[*]` 會掃完整張圖 |
| 關係型別過濾 | 變長路徑用 `[*1..N]` + `WHERE ALL(... type(r) IN $relTypes)` | Neo4j 5.26 的 `[:$any($list)*1..N]` 在變長時**不過濾**（單跳 `$any` 正常；`*1..1` 仍會掃到其他型別）。本機 Community 5.26.30 已實測 |

`delete_node` 用 `DETACH DELETE`。`delete_edge` 只靠 `relationship_id` property
定位（`MATCH ()-[r {relationship_id: $rid}]-() DELETE r`），不需要兩端 entity_id。

時間過濾是邊的 `[first_seen, last_seen]` 與查詢區間**重疊**：
`r.first_seen <= $to AND r.last_seen >= $from`。

#### 沒有 APOC 時怎麼設動態 label

`NEO4J_PLUGINS` 是空陣列，不能 `CALL apoc.create.addLabels`。
Neo4j 5.26 的 `$()` 動態語法（與關聯型別同一組擴充）在本機 Community 實測可用：

```cypher
MERGE (n:Entity {entity_id: $id})
SET n:$($label)
```

`REMOVE n:$($old)` 同樣可用，用來在 `entity_type` 變更時清掉舊的型別 label。
進 query 之前 label／關聯型別都經過 sanitize（ASCII 字母開頭、只含字母數字底線），
避免字串拼接造成 Cypher injection。未知的 `entity_type` 仍會寫入，不 panic。

#### `ProjectionStore` 為什麼存在 `:ProjectionState`

對齊 OpenSearch 把狀態放獨立 index 的理由：`--rebuild --drop` 會清空 `:Entity`。
狀態若掛在圖節點上，「上次 rebuild 何時、寫了幾筆」會在重建開始的瞬間消失。
`ProjectionStore` 的方法**只**操作 `:ProjectionState`，不碰 `:Entity`。
graph-worker 清空圖時必須排除這個 label（那是 worker 的責任）。

checkpoint 與 rebuild 狀態可以同在一個節點上，寫入用 `SET` 部分欄位，
不會整節點覆寫——conformance 有一條驗兩者不會互相蓋掉。

#### 已知限制

- unique constraint 只覆蓋 13 種既知 `RelationshipType`。未知型別靠 MERGE 冪等，
  **不**保證 `relationship_id` 全域唯一（兩條不同型別的邊可以碰巧同 id；
  呼叫端的 id 是 UUID v5／v7，實務上不會撞）。
- 寫入仍用 `$($relType)` 動態關聯型別（單跳 MERGE 已實測可用）。讀取不要用變長 `$any()`。
- `max_hops` 上限 10，比 mock 的 32 緊。Graph API 的預設是 1 跳。
- 累加沒有樂觀鎖（同 OpenSearch adapter）：一個投影一個 writer。
- 沒有 adapter metrics／連線 semaphore 接到 Resource Guard（與其他 adapter 一樣，列在「尚未做」）。

### `EmbeddingProvider`（V0.2 Phase 0g trait／Phase 3 生產 adapter）

不是資料庫 port。生產實作是 `storage_opensearch::MlCommonsEmbeddingProvider`，
包 OpenSearch ml-commons `_predict`。上層測試仍可注入 mock，或
`MockEmbeddingProvider::unsupported()` 測「還沒接語意」的路徑。

五條硬約束寫進 trait，不要留給呼叫端各自小心：

1. **per-language 路由**：`model_for(language)`／`embed` 的 `language`。
   英文 → MiniLM，中文／其他／`None` → e5。`None` **不是**英文——MiniLM
   中文檢索 top-1 2/5（`docs/developer/embedding.md` §5.2），默認英文會
   靜默得到無意義鄰居。語言偵測在 Core，不在 provider。
2. **維度可問**：`dimensions()`／`dimensions_for(language)` 是方法。
   兩個模型目前都 384，不要把 384 寫成常數。
3. **非對稱前綴**：`EmbeddingKind::{Query, Passage}`。實作依模型決定要不要
   加 `query: `／`passage: `。呼叫端不要自己拼（拼錯會靜默降低召回率）。
4. **回傳對齊 Embedding record**：`EmbeddingVector` 帶 `model`／
   `model_version`／`dimensions`／`content_hash`／`vector`。上游沒版本號時
   用 `model_content_hash_value` 頂替，不要填 ml-commons 的 `"1"`。
5. **向量空間不相通**：同一段文字、兩個模型，向量不可比。呼叫端用
   `model_for` 決定寫進哪個 k-NN 欄位／index。Document 分
   `embedding_en`／`embedding_multi`（Phase 3 Step 2）；Entity 寫進獨立
   `osint-entities` 的 `description_vector_multi`（Phase 3 Step 3，見
   `docs/developer/embedding-worker.md`）。

`content_hash` 的唯一定義是 `embedding_content_hash`：SHA-256 打在**原始
文字** UTF-8 上，不含前綴、不含模型名。re-generate 比的是內容有沒有變。

#### `MlCommonsEmbeddingProvider`（V0.2 Phase 3）

`connect(url)` 啟動時用內容雜湊查兩個模型的 `model_id`，兩個都必須是
`DEPLOYED`，否則回 `StorageError::Configuration` 並指出該跑哪支 setup
腳本。`model_id` **快取到重啟**：重新註冊後 id 會換，V0.2 接受「換過要
重啟服務」，每次 `embed` 重查太浪費。

`model_version` 填的是內容雜湊（MiniLM 上游 `config.json` 的 SHA-256、
e5 是**打包後 zip** 的 SHA-256），**不是** ml-commons 內部序號 `"1"`。
MiniLM 雜湊可由環境變數 `ML_MODEL_SHA256` 覆寫，與
`scripts/opensearch-ml-setup.sh` 同一顆變數。

`embed_batch` 真的走 `_predict` 的 `text_docs` 陣列。一批裡若同時有英文
與非英文，會拆成兩次呼叫（MiniLM／e5 各一批）再依原始順序組回——
不能假設呼叫端永遠傳同語言。兩次呼叫**串行**，避免兩個模型同時佔
JVM heap 觸發已知的 429 斷路器（`docs/developer/embedding.md` §4）。

錯誤分類：HTTP 429／`circuit_breaking_exception` → `Timeout`（暫時性，
可重試、把批次調小）；連不上／逾時 → `Unavailable`；回應缺
`inference_results` 或維度不符 → `CorruptionSuspected`。對應函式是
`classify_ml_http`，有單元測試，不靠撐爆 heap 來驗證。

conformance：`cargo test -p storage-opensearch --test embedding_conformance`。
**唯讀查詢＋推論**，不會 undeploy／delete 模型。

#### 已知限制

- 換模型（重新註冊）必須重啟吃新的 `model_id`。
- 尚未接到 Resource Guard 的 embedding 併發上限。
- 尚未把推論延遲／429 次數接到 Operations Center metrics。

## 尚未做（不要當成已完成）

- adapter metrics 接到各 adapter／Operations Center
- 連線 semaphore 尚未接到 Resource Guard
- SQLite 版的 `AuditLog` / `ApiTokenStore`（0006 只建了 schema，沒有 adapter）
