# 改一個功能要動哪些地方

這一頁用四個常見情境，列出改動要碰的檔案（依建議順序）、為什麼要碰、以及容易漏掉的地方，最後是送出前的驗證流程。

這些清單的用途是**檢查有沒有漏**，不是唯一的寫法。動手前先讀一個既有的同類實作（例如要加 API 端點，先讀一個相似的既有端點），照它的模式寫。

---

## 新增一個 API 端點

| 順序 | 檔案 | 做什麼、為什麼 |
|---|---|---|
| 1 | `crates/core-api/src/resources/<資源>.rs` | 寫處理函式（handler）。依資源分檔（`sources.rs`、`collections.rs`、`discovery.rs`……）；新資源就新開一個檔並加進 `resources/mod.rs` |
| 2 | `crates/core-api/src/resources/mod.rs` | 共用工具都在這裡：取得儲存介面（沒接上時回 503）、輸入驗證、`audit()` 稽核寫入、錯誤轉換。先看有沒有現成的，不要自己重寫 |
| 3 | `crates/core-api/src/routes.rs` | 把路由掛進**正確的權限分組**（見下表）。這是唯一的路由表 |
| 4 | 稽核 | 寫入類端點在成功與失敗時都呼叫 `resources::audit()`，傳入 `AuditEvent`，失敗時的 metadata 用 `rejected_metadata()` 組 |
| 5 | `crates/core-api/tests/` | 兩種測試：不需要資料庫的（認證、權限、輸入驗證）放 `*_api.rs`；需要真實 PostgreSQL 的（回應形狀、404、409）放 `*_e2e.rs` |
| 6 | `docs-site/docs/reference/api.md` | 端點表加一列，並更新頁尾的**路由數量統計** |
| 7 | `docs/developer/api-skeleton.md` | 開發者用的權限對照表加一列 |

`routes.rs` 裡的權限分組：

| 分組變數 | 需要的角色 | 放什麼 |
|---|---|---|
| `public` | 不需認證 | 只有 `/health`、`/ready`、`/metrics` |
| `read` | viewer 以上 | 唯讀的 `GET` |
| `protected` 上直接掛的 `POST` | viewer 以上 | 唯讀但用 `POST` 的查詢（搜尋、圖查詢，因為查詢條件是巢狀結構） |
| `write` | operator 以上 | 會改資料的 `POST`／`PATCH`／`DELETE` |
| `admin` | admin | 目前只有 API token 管理 |
| `import_routes`、`stix_import_routes` | operator 以上 | 檔案上傳。各自有較寬的請求大小上限，不放寬其他路由 |

!!! warning "容易漏的地方"
    - **掛錯分組**：把寫入端點掛進 `read`，viewer 就能改資料，而且不會有任何錯誤。
      寫一個「viewer 呼叫回 403」的測試來證明分組正確。
    - **稽核少傳 IP**：`audit()` 一定要傳 `ClientIp`。少了它稽核表的 IP 欄位會全是空值，同樣不會報錯。
    - **`PATCH` 要支援樂觀鎖**：既有的 `PATCH` 端點都要求 `If-Match` 標頭，回應帶 `ETag`。新增的 `PATCH` 照同樣模式。
    - **路由數量統計**：`api.md` 頁尾的總數要手動同步。

---

## 新增或修改資料表欄位

這是最容易漏東西的改動：一個欄位要穿過七、八個地方。

| 順序 | 檔案 | 做什麼、為什麼 |
|---|---|---|
| 1 | `migrations/postgres/NNNN_*.sql` | 新增一個 migration 檔，編號接續最大的那個。**不要改已經存在的 migration 檔**——已經套用過的資料庫不會重跑它 |
| 2 | `migrations/sqlite/NNNN_*.sql` | 同一個編號、同一個名稱，寫 SQLite 版。兩邊目前一一對應，保持下去 |
| 3 | `crates/core-model/src/<型別>.rs` | 領域型別加欄位 |
| 4 | `crates/storage-core/src/traits.rs` | 如果需要新的查詢或寫入方法，加在對應的 trait（通常是 `RelationalStore`） |
| 5 | `crates/storage-postgres/src/store.rs` 與 `mapping.rs` | 實作新方法、把資料列對映到型別。SQL 只能出現在這個 crate |
| 6 | `crates/storage-sqlite/src/store.rs` 與 `mapping.rs` | 同上，SQLite 版 |
| 7 | `crates/resolver/src/service.rs`、`semantic.rs`、`identifier_methods.rs` | 這三個檔的測試區各有一個 `MemoryStore`，手寫實作了整個 `RelationalStore`。trait 加方法時，三份都要補（不用到的回 unsupported 即可） |
| 8 | `crates/storage-core/src/conformance.rs` | 加一致性測試斷言；`storage-postgres/tests/conformance.rs` 與 `storage-sqlite/tests/conformance.rs` 會對兩個後端各跑一次，保證兩邊行為一樣 |
| 9 | 投影（如果這個欄位要能被搜尋或上圖） | `indexer`（`schema.rs`、`projection.rs`）或 `graph-worker`；改完後需要重建投影 |
| 10 | 文件 | 欄位對使用者可見時，更新 `docs-site/` 對應頁面與 `docs/developer/` 的 schema 說明 |

`crates/storage-core/src/mock.rs` 只有搜尋、圖、向量的 mock，沒有 `RelationalStore` 的 mock，所以第 7 步那三份手寫實作是目前唯一要同步的測試替身。

!!! danger "只加 SQL 檔、不重新編譯，容器啟動會失敗"
    `storage-postgres` 與 `storage-sqlite` 用 `sqlx::migrate!` 巨集在**編譯時**把 migration 檔內容嵌進執行檔。
    服務啟動時會用執行檔裡嵌的那一份去比對資料庫。

    所以：如果你用 `make migrate-postgres` 把新的 migration 套到資料庫，但容器裡跑的還是舊的 image，
    舊執行檔裡沒有這個版本，啟動時 sqlx 會回報 `migration NNNN was previously applied but is missing in the resolved migrations`。
    修法是重建 image（`make image-build` 或 `make compose-up-full`，後者會先 build）。

    本機用 `cargo` 跑時也一樣：**新增**一個 `.sql` 檔而沒有改任何 Rust 原始碼，`cargo build` 不會重新編譯這兩個 crate
    （sqlx 官方文件說明：編譯器預設只在原始碼改變時重跑巨集；本 repo 沒有用來監看 `migrations/` 目錄的 build script）。
    新增 migration 後，碰一下 `storage-postgres` 或 `storage-sqlite` 的原始碼，或跑 `cargo clean -p storage-postgres -p storage-sqlite`，確保重新嵌入。

!!! warning "容易漏的地方"
    - **SQLite 那一份**：只改 PostgreSQL 時，PostgreSQL 的測試全綠，SQLite 的一致性測試才會失敗。
    - **三份 `MemoryStore`**：trait 加方法後 `resolver` 編譯失敗，這個會被編譯器抓到；但如果你只跑某個服務的測試，可能要到全 workspace 編譯才發現。
    - **投影重建**：PostgreSQL 有了新欄位，OpenSearch／Neo4j 裡的舊資料沒有，要跑對應的 `--rebuild`。

---

## 新增一種連接器

| 順序 | 檔案 | 做什麼、為什麼 |
|---|---|---|
| 1 | 新 crate `crates/connector-<名稱>/` | 實作 `connector-sdk` 的 `ConnectorTrait`（`discover`、`collect`、`parse`、`create_raw_evidence`、`update_checkpoint`、`health`）。照 `connector-rss` 的結構寫，它最短 |
| 2 | 網路存取 | **一律透過 `connector-sdk` 的 `GuardedFetcher`** 發請求。它負責 SSRF 防護（預設拒絕私網與雲端 metadata 位址）、套用該來源的 `NetworkRule` 白名單、每網域限速。自己用 `reqwest` 直接連線會繞過這些 |
| 3 | 憑證 | 需要 API 金鑰時，只能從連接器的 `credential_reference`（`SecretRef`）解析，**不能**放進 `configuration` 欄位（那會被 API 原樣回傳）。參考 `connector-rest-api` |
| 4 | `Cargo.toml`（workspace 根目錄） | 把新 crate 加進 `members` |
| 5 | `crates/collector/src/registry.rs` | 在 `KnownConnectorKind` 加一個成員，在 `classify_connector_type` 加對應的 `connector_type` 字串 |
| 6 | `crates/collector/src/runner.rs` | 在依 `KnownConnectorKind` 分派的那個 `match` 加一個分支 |
| 7 | `crates/collector/Cargo.toml` | 加入新 crate 的相依 |
| 8 | `crates/normalizer/` | 如果收進來的內容要變成 Document，normalizer 要認得它。normalizer 依內容類型（`content.rs`）判斷怎麼解析；不認得的會被略過（不報錯） |
| 9 | `crates/connector-<名稱>/tests/e2e.rs` | 對一個本機假伺服器跑完整收集流程 |
| 10 | 文件 | `docs-site/docs/usage/` 與 `docs/developer/connector-sdk.md` |

!!! warning "容易漏的地方"
    - **匯入專用類型不能被排程**：`registry.rs` 的 `is_import_only_type` 列出 `manual_upload`、`json_import`、`csv_import`、`stix_import` 等沒有可抓網址的類型，collector 會直接略過。
      如果新的類型也是「只能被動匯入」，要加進這個清單，否則 collector 每輪都會把它當成未知類型處理。
    - **未知類型是靜默略過**：`connector_type` 拼錯不會報錯，collector 只會略過它。新增後實際建一個連接器確認真的有收到資料。
    - **normalizer 不認得 = 沒有 Document**：收集成功、原始證據也存了，但搜尋不到。這正是 REST API 連接器目前的狀況。
    - **測試建立的連接器要停用**：e2e 建立的連接器必須 `enabled=false` 或在測試結束時停用。
      留下啟用中的殘留，collector 一啟動就會每輪去收它。手動觸發收集的路徑不看 `enabled`，所以測試不需要把它打開。
      有原始證據的連接器刪不掉（外鍵沒有連帶刪除），停用即可。

---

## 新增一個事件消費者

| 順序 | 檔案 | 做什麼、為什麼 |
|---|---|---|
| 1 | 新 crate，含 `src/lib.rs`（邏輯）與 `src/main.rs`（消費迴圈） | 邏輯放 lib 才能單元測試；main 只負責連線、組裝與迴圈 |
| 2 | `crates/worker-skeleton/` | 用它提供 `/health`、`/ready`、`/metrics`。主要後端固定是 PostgreSQL，第二個後端（Neo4j、OpenSearch……）視需要，不需要時填 `NoSecondary` |
| 3 | `crates/core-config/src/lib.rs` 與 `config/default.toml` | 加一個設定區段，至少有 `bind`（health 埠）與 `consumer_group` |
| 4 | consumer group（消費者群組） | **新的消費者用自己獨立的 group 名稱**。Redpanda 在同一個 group 內把事件分給其中一個成員；和別的服務共用 group 等於兩個服務搶同一批事件，各自只看到一部分 |
| 5 | 訂閱 topic | 用 `core-events` 的 `EventConsumer::connect`，topic 名稱用 `EventTopic::...as_str()`，不要寫字串 |
| 6 | 冪等（重複處理不會產生重複結果） | 見下方說明 |
| 7 | 失敗處理 | 處理失敗時寫一筆 `FailedEvent`（`put_failed_event`），再提交進度。參考 `entity-worker/src/main.rs` 的寫法 |
| 8 | 優雅結束 | 在消費迴圈的 `tokio::select!` 裡監聽結束訊號 |
| 9 | `docker/Dockerfile`、`docker/docker-compose.yml`、`docker/docker-compose.dev.yml` | 加建置階段與服務定義（照 `osint-discovery-worker` 那一段抄） |
| 10 | `Makefile`、`.github/workflows/ci.yml` | 加 `run-*` 目標；CI 的容器建置、SBOM 與掃描矩陣各加一列 |
| 11 | `crates/core-api/src/main.rs` 的 `queue_bindings` | 如果要讓維運端點 `GET /api/v1/ops/queues` 看得到這個消費者的積壓，在這裡加一筆 |
| 12 | 文件 | `docs-site/docs/operations/services.md`（服務與埠號表）、`docs-site/docs/architecture/overview.md`、`data-flow.md` |

### 冪等怎麼做

同一則事件**一定會**被處理不只一次（服務重啟、提交進度前當掉、手動重送失敗事件）。既有服務用三種手法：

| 手法 | 做法 | 例子 |
|---|---|---|
| 確定性 ID | 衍生資料的 ID 用 UUID v5，由型別與內容推導。重跑產生同一個 ID，寫入變成更新而不是新增 | `entity-worker` 的 `entity_id`、`relationship_id` |
| 先查再做 | 處理前先查 PostgreSQL「這份是否已經處理過」，是就回 `AlreadyDone` | `entity-worker` 的 `extract_document` |
| 事件只當通知 | 不拿事件內容當資料，收到後一律從 PostgreSQL 重讀。亂序或重放的舊事件就不會蓋掉新狀態 | `graph-worker`、`embedding-worker` |

!!! warning "容易漏的地方"
    - **`job.dispatched` 是多個 worker 共用的 topic**：`graph-worker`、`stix-worker`、`discovery-worker` 都訂閱它，各自用不同的 group，收到後看 `job_type` 決定要不要處理。不是自己的就**安靜略過並提交**，不是錯誤。
    - **事件格式的 topic 已定義不代表有人發**：`core-events/src/topics.rs` 裡有不少 topic 只有定義、沒有生產者。訂閱前先 grep 確認有人真的會發。
    - **`/ops/queues` 不會自動出現新服務**：要手動加進 `queue_bindings`。

---

## 驗證流程

送出改動前依序跑：

| 順序 | 指令 | 檢查什麼 |
|---|---|---|
| 1 | `make disk` | `target/` 太大（超過約 10 GB）就先 `make clean`。這台機器與其他虛擬機共用磁碟 |
| 2 | `cargo fmt --all -- --check` | 格式 |
| 3 | `cargo clippy --workspace --all-targets -- -D warnings` | lint，任何警告都算失敗（CI 用同一條） |
| 4 | 停掉應用服務容器（見下方） | 避免容器搶走測試的事件 |
| 5 | `cargo test --workspace` | 全部測試。需要基礎設施容器（PostgreSQL、OpenSearch 等）在跑 |
| 6 | `make compose-up-full` | 測完把應用服務起回來 |
| 7 | `make docs-build` | 有改文件時：以嚴格模式建置文件網站，壞連結會讓它失敗 |

`make lint` 等於第 2、3 步合併。

### 跑測試前先停掉應用服務容器

`make compose-up-full` 起來的 10 個應用服務，和 e2e 測試**訂閱同一批 topic、用同一個 consumer group**。
容器開著時，它們會搶先消費測試發出的事件並處理掉，測試自己再處理時拿到「已處理過」，於是「預期新建」的斷言失敗。
這不是程式壞了，是測試環境被佔用。

只停應用服務，基礎設施照常：

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml --profile app stop \
  osint-api osint-collector osint-normalizer osint-deduplicator osint-entity-worker \
  osint-indexer osint-graph-worker osint-embedding-worker osint-stix-worker osint-discovery-worker

cargo test --workspace

make compose-up-full   # 測完再起回來
```

!!! warning "有一個已知會失敗的測試"
    `core-events` 的 Redpanda 往返測試在目前的本機環境一直失敗（訊息逾時），與容器開關無關，是既有的環境問題。
    所以目前**沒有一份「全綠」的基準**：看到這一個失敗是預期的，看到其他失敗才要追。

### 測試要清掉自己建立的東西

| 建立了什麼 | 要怎麼處理 |
|---|---|
| 連接器 | 建立時就設 `enabled=false`，或測試結束時停用 |
| OpenSearch 一次性 index、S3 物件、Redpanda topic | 測試結束時刪掉。留下來就是延遲發作的磁碟洩漏 |

更多本機開發環境的細節（埠號、容器內外設定差異、各 worker 怎麼啟動）在 repo 的 `docs/developer/local-dev.md`。
