# Repository layout（V0.2 Phase 3：30 個 crate；resolver／merge 目前是函式庫；八個服務 binary 都在 compose 的 `app` profile）

Cargo workspace：`edition = "2024"`、`resolver = "3"`、`rust-version = "1.85.0"`；工具鏈 pin 在 `rust-toolchain.toml` 的 stable channel。

```text
Cargo.toml
rust-toolchain.toml
.cargo/config.toml
Makefile
.env.example
config/default.toml
crates/
  core-model/            V0.1 canonical domain types
  core-config/           TOML + 環境變數 + SecretRef
  core-observability/    JSON tracing、metrics registry、health/ready
  core-security/         JWT、API token、RBAC、AuditLog、SSRF IP 分類
  core-events/           Event envelope v1、Redpanda producer/consumer
  core-jobs/             Job 狀態機 + CanonicalStore CRUD + 派工
  core-api/              Axum skeleton（osint-api）＋ POST /api/v1/import 上傳入口
  storage-core/          capability traits、錯誤、health、conformance
  storage-postgres/      CanonicalStore + RelationalStore
  storage-sqlite/        EmbeddedStore + RelationalStore
  storage-opensearch/    SearchStore + EmbeddingProvider（ml-commons）
  storage-redis/         KeyValueStore
  storage-s3/            ObjectStore
  storage-neo4j/         GraphStore + ProjectionStore
  connector-sdk/         ConnectorTrait、SSRF Guard、NetworkRule、RawEvidence sink
  connector-rss/         RSS 2.0／Atom 1.0（feed-rs）
  connector-static-web/  Static Web（scraper）
  connector-rest-api/    REST JSON API
  import-format/         JSON／CSV 匯入的欄位對映與有界解析（core-api 與 normalizer 共用）
  collector/             排程服務（osint-collector）：cron、有界併發、raw.collected
  normalizer/            正規化服務（osint-normalizer）：raw.collected → Document
  deduplicator/          去重服務（osint-deduplicator）：object.normalized → DuplicateGroup（SPEC §15／§16）
  entity-worker/         抽取服務（osint-entity-worker）：dedup.completed → Entity/Relationship/Evidence（SPEC §17／§11／§12）
  resolver/              Entity resolution（SPEC_V0.2 §5／§6）：resolve_entity 聚合 normalized_name／alias／domain／semantic_similarity／account_handle；graph_context 走獨立 GraphContextResolver；無獨立 binary
  merge/                 Entity merge／undo（SPEC_V0.2 §7）：execute_merge／undo_merge；無獨立 binary
  indexer/               搜尋投影（osint-indexer）：entity.extracted → OpenSearch osint-documents（SPEC §18）＋搜尋語法解析
  graph-worker/          圖投影（osint-graph-worker）：relationship.changed → Neo4j（SPEC_V0.2 §8）；常駐模式也消費 job.dispatched 執行 graph_rebuild；只有 Entity→Entity 的邊才進圖
  embedding-worker/      向量投影（osint-embedding-worker）：entity.extracted → osint-documents overlay + osint-entities（SPEC_V0.2 §11–§14）；不訂 embedding.requested
  osint-cli/             本機唯讀查詢 CLI（osint-cli）：直連 DB/MinIO，不經 core-api
  acceptance/            跨服務驗收測試（SPEC §26 Acceptance F、failure/recovery）。無生產程式碼
docker/
  docker-compose.yml
  docker-compose.dev.yml
migrations/
  postgres/
  sqlite/
```

`osint-cli` 是**唯讀**的：它直接連 Core 的 PostgreSQL／MinIO，因此不受 API 的 RBAC 與 AuditLog
保護，所以刻意不提供任何寫入子命令——寫入一律走 `core-api`。用法見 `docs/user/cli.md`。

`indexer` 同時放**索引**與**查詢語法**，這是刻意的：index mapping（欄位型別、analyzer、
nested 結構）與查詢（要查哪些欄位、過濾掛在哪一欄、怎麼排序）是同一份契約的兩面。
分開放的話，改了 analyzer 卻沒改查詢欄位清單、或改了欄位名只改一邊——
**兩者都不會編譯失敗也不會執行失敗**，只會讓搜尋悄悄變得不準。
因此 `core-api` 與 `osint-cli` 都相依 `indexer`，只呼叫 `indexer::search::build()`，
沒有任何一方自己組查詢或寫死欄位字串。設計見 `docs/developer/indexer.md`。

Manual／JSON／CSV import 不是 connector，也沒有 crate：它們是 push 路徑，進入點是
`core-api` 的 `POST /api/v1/import`（見 `docs/developer/import-api.md`）。

`core-model` 除了 domain types，還放**跨服務必須一致**的兩份定義：
`content.rs`（SPEC §15 Stage 3 的 content hash）與 `url_norm.rs`（Stage 2 的 canonical URL）。
`url_norm` 原本在 `deduplicator` 底下，Phase 4b 搬過來讓 entity-worker 的 URL Entity
用同一套正規化規則——兩份實作分岔不會報錯，只會悄悄產生「看起來一樣但字串不同」的重複 Entity。
`deduplicator::url_norm` 仍以 re-export 保留原路徑。

尚未加入：resolver 的事件消費者（Phase 1h）、`email`／`external_id` 掃描方法（**刻意不做**，見 `docs/developer/resolver.md`）、自由文本 NER（V0.3，見 `entity-worker.md`）。`NetworkRule` 寫入 API 尚未接到 `core-api`（驗證函式在 `connector-sdk::validate_network_rule`）。`account_handle` 已實作。

API token **已經**持久化到 Postgres（Phase 6a 的 `PostgresApiTokenStore`，
migration 0006 建 `api_tokens` 表）。SQLite 版的 `ApiTokenStore` 與 `AuditLog`
仍未實作，雖然 `migrations/sqlite/0006` 已經把 schema 建好——
所以用 SQLite 當後端時 token 仍然只在記憶體裡。見 `docs/developer/security.md`
與 `docs/developer/schema-v0.1.md`。

識別碼使用 UUID v7。密鑰只存 `SecretRef`（`env:` / `file:` / `store:`），不寫明文。
