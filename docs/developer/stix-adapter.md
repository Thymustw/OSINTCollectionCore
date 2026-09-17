# STIX Adapter（`osint-stix-worker`，V0.2 Phase 4，SPEC §18-19）

STIX 2.1 匯入／匯出管線由三個層次組成：

```text
crates/stix-adapter/    純函式庫：STIX 2.1 型別、bundle 結構驗證、雙向映射
crates/stix-worker/     常駐服務（osint-stix-worker）：執行 stix_import／stix_export
core-api                HTTP 入口：POST /api/v1/import/stix、POST /api/v1/export/stix、
                        GET /api/v1/jobs/{id}/result
```

手刻 STIX 型別、不依賴外部 crate。crates.io 上的 `stix2`／`rstix` 都是低採用度早期版本，不符合本專案 DevSecOps 相依審查門檻。範圍限定在 SPEC §19 七種映射需要的最小集合：Identity／Threat Actor／Malware／Vulnerability／Indicator／Relationship／SCO（domain-name／ipv4-addr／url／email-addr）。

## 型別與驗證（`crates/stix-adapter`）

### `StixId`

`{type_prefix}--{uuid}` 格式。`type_prefix` 必須符合 `[a-z][a-z0-9-]*`，UUID 部分經 `Uuid::parse_str` 驗證。

這層格式驗證是 STIX id 注入防護的第一道防線：SQL injection、路徑穿越、XSS 形狀的字串因為含大寫字母／分號／斜線／`<>` 而在這裡就被拒絕。見 `crates/core-api/tests/stix_api.rs::malicious_object_ids_are_rejected_as_400_not_500`。

### `StixObject`

手刻 tagged union（已知 11 種 + `Custom`（`x-` 開頭）+ `Unknown`（其餘））。已知型別都有 `extra: HashMap<String, Value>` 逃生艙——STIX 2.1 允許 custom properties，不能 `deny_unknown_fields`。

`StixObject::id()`：從已組出的物件直接拿 STIX id，不需要重新呼叫映射邏輯內部的 `compose_stix_id`（那個函式刻意保持私有）。

### `validate_bundle(raw, max_objects)`

只做結構驗證（根是 object、`type=="bundle"`、`id` 合法且前綴是 `bundle`、`objects` 是陣列且不超過 `max_objects`、每個物件有非空 `type` 與合法格式的 `id`），不做語意映射。

## 雙向映射（`crates/stix-adapter/src/mapping.rs`）

### `stix_object_to_entity`

- Identity 依 `identity_class` 分流：`individual` → Person、`organization` → Organization；`group`／`class`／`system`／`unknown`／其他一律回 `None`（這幾種身分類別在 SPEC §10 沒有對應的 EntityType）。
- ThreatActor／Malware／Vulnerability／Indicator 對應同名 EntityType。
- SCO（domain-name／ipv4-addr／url／email-addr）取 `value` 不是 `name`。
- 不認識的 type、Relationship、Custom、Unknown 一律回 `None`，不中止整批。

### `entity_to_stix_object`

10 種 EntityType 有原生對應（Person/Organization → Identity；ThreatActor/Malware/Vulnerability/Indicator/Domain/Ip/Url/Email → 對應 SDO/SCO）。

6 種沒有原生 STIX 對應的（Account／Hostname／Repository／Hash／Software／Location）產出 `CustomObject`（`type=x-osint-core-entity`），把 Core 型別與 id 塞進 `extra`（`x_osint_core_type`／`x_osint_core_id`），不丟 provenance。

**這是單向的**：Custom 的 STIX → Core 反向映射刻意不做（見測試 `unknown_and_relationship_and_custom_return_none`），避免只做一半。

### `normalize_name`

正規化規則全部抄既有慣例，不發明新規則：

| EntityType | 正規化規則 |
|---|---|
| Person／Organization／ThreatActor／Malware／Indicator／Account／Hostname／Software／Repository／Location | `entity-worker::normalize_person_or_org`（空白壓成單一半形空格＋`to_lowercase`） |
| Domain／Email | `to_ascii_lowercase` ＋ 去尾端 `.` |
| Ip | `IpAddr` parse 後 `Display`；解析失敗保留原文 |
| Url | `core_model::url_norm::canonicalize` |
| Vulnerability | `to_ascii_uppercase` |
| Hash | `to_ascii_lowercase` |

### `map_relationship_type`

9 種直接對應：`indicates`／`attributed-to`／`targets`／`mitigates`／`uses`／`located-at`／`derived-from`／`belongs-to`／`member-of`。其餘 fallback `RelationshipType::AssociatedWith`，原始字串保留在 `MappedRelationship::stix_relationship_type`（語意不丟）。

反向 `relationship_type_to_stix` 把 `AssociatedWith` 匯出成 STIX 慣用的 `related-to`。

### `StixExportFilter`／`StixTimeRange`

定義在 `crates/stix-adapter/src/export_filter.rs`，是 core-api 與 stix-worker **共用**的型別（避免兩邊各自 parse、行為分岔）。

`entity_types` 是 `Vec<core_model::EntityType>`，直接吃 EntityType 既有的 `#[serde(rename_all="snake_case")]`，不合法值得到清楚的 serde unknown-variant 錯誤。

`StixTimeRange::overlaps(first_seen, last_seen)` 語意：`first_seen <= to && last_seen >= from`，兩端缺省視為不限制，與 `storage_core::GraphTraversalOptions::time_range` 一致。

## `core_model::enums` 擴充

`EntityType` 新增 `ThreatActor`／`Malware`／`Indicator`（都納入 `ALL_ENTITY_TYPES`）；`RelationshipType` 新增 `Indicates`／`AttributedTo`／`Targets`／`Mitigates`；`SourceType` 新增 `StixImport`。三者欄位是 TEXT NOT NULL 無 CHECK constraint，加新 variant 不需要 migration。

## 決定性 id：與既有資料自然收斂

`Entity.id` 與 `Relationship.id` **直接重用** `entity_worker::entity_id(entity_type, normalized_name)`／`entity_worker::relationship_id(source, type, target)`（決定性 UUID v5）。這確保 STIX 匯入的 Entity 跟其他來源（例如 RSS 抽取）共用同一自然鍵時自然收斂成同一筆 canonical 記錄，不會產生重複。

STIX 匯入額外寫一筆 `EntityIdentifier`（`namespace="stix"`，`value`/`normalized_value` 是完整 STIX id 字串），用來反查「這個 STIX id 對到哪個 Entity」。

## HTTP 入口（`crates/core-api/src/resources/stix.rs`）

三個端點（operator 以上寫、viewer 以上讀），完整 curl 範例見 `docs/developer/api-skeleton.md` 的「STIX 2.1 匯入／匯出」一節：

### `POST /api/v1/import/stix`

驗證（`max_bundle_bytes`／`max_objects`／`validate_bundle`）都在 API 層先做完，通過才落地 RawEvidence + 建 `stix_import` Job。

**刻意不 publish `raw.collected`**：STIX bundle 已是結構化情報物件圖，不是待正規化的原始檔案。觸發 normalizer 會把整包 JSON 當一般文件抽取，語意完全不對。

### `POST /api/v1/export/stix`

只建立 `stix_export` Job（`parameters.filter` 存使用者傳入的 `StixExportFilter`）。沒有 stix-worker 在跑就會停在 `queued`。

### `GET /api/v1/jobs/{id}/result`

依 Job 完成後 `parameters.result_object_key` 去 ObjectStore 讀回匯出的 bundle JSON。

### ⚠️ 一個修過的真 bug（保留，當後人教訓）

`import_stix_inner` 存進物件儲存的 RawEvidence body 曾經是 `body.to_vec()`（HTTP 請求整個 envelope `{"source_id":..,"bundle":{..}}`），不是 `request.bundle` 本身。stix-worker 讀出來直接 parse 成 `StixBundle` 會因為缺頂層 `type` 欄位而失敗，import job 一律 Failed。

這個 bug 存在於 Step 2/3，兩邊的測試都繞過了真正的 HTTP handler（各自手寫 helper 直接把 bundle JSON 寫進物件儲存），直到 Acceptance G 用真正的 HTTP 全程跑一次才抓到。現在已修好：存進物件儲存的是 `serde_json::to_vec(&request.bundle)`。

**這是這個系統目前唯一測過「HTTP 匯入 → worker 消化」完整路徑的測試**（`crates/acceptance/tests/acceptance_g.rs`）。之後改動這段程式碼要留意這條路徑不能再繞過去測。

## stix-worker 執行邏輯（`crates/stix-worker`）

新建常駐 binary，消費 `job.dispatched`，只認 `stix_import`／`stix_export`（其餘含 `graph_rebuild` 一律 `Ignored` 並 commit）。

### Import（`src/service.rs`）

一個交易內兩趟寫入：

**第一趟**：逐一 `upsert_entity`（既有 attributes 先 extend，STIX 新值用 insert 覆寫同名 key——這跟 `entity-worker::upsert_entity` 的既有寫法完全一致，「不覆蓋別人資料」指的是不整份取代 attributes 物件）＋ 寫 STIX identifier ＋ Provenance（`action="stix_imported"`）。

**第二趟**：解析 Relationship 的 `source_ref`／`target_ref`（任一端不在本批映射表就跳過該筆，不中止整批）＋ 寫 Relationship ＋ Provenance。

超過 `[stix_worker].max_objects_per_tx`（去重後的 Entity 數）整批 Failed，不拆交易。

`tx.commit()` 後才 `publish_relationship_changes`（單則失敗只記 log，圖投影靠之後的變更事件或 `graph-worker --rebuild` 補回）。

Commit 後跑 `resolve_and_auto_approve`：對每個**新寫入**的 Entity 呼叫 `resolver.resolve_entity`，再撈這個 Entity**全部** Pending 候選（不只新寫入的那批——`resolve_entity` 自己五個掃描方法分數上限只到 0.55，唯一能到預設 `auto_confirm_score=0.95` 門檻的 `exact_identifier` 候選是 entity-worker 寫入衝突時在別處產生、預先寫進 DB 的，不在 `resolve_entity` 呼叫鏈上——只看新寫入的話，自動核准在預設門檻下永遠不會真的觸發高信心路徑），用 `group_candidates_for_auto_approval` 分組後逐組 `evaluate_pair`。

`[auto_approval].max_auto_merges_per_resolve` 是**整個 bundle 共用**一個計數器（不是每個 Entity 各自計數），避免有問題的大 bundle 觸發大量自動合併。完整自動核准機制見 `docs/developer/auto-approval.md`（ADR-012）。

### Export（`src/export.rs`）

依 `StixExportFilter` 決定要匯出哪些 Entity：

**有 `entity_ids`**：以這些 id 為種子；`depth` 有值就 BFS 逐層擴散。`time_range` 同時限制「能不能繼續往外走」跟「這條邊算不算數」——不然會沿著一條理應被時間窗排除的邊走到不該匯出的鄰居。`entity_types` 只在最後過濾「要匯出的節點」，不限制 BFS 能經過哪些型別的中繼節點（不然「以人為中心展開一層鄰居」若鄰居剛好是網域就走不出去）。

**沒有 `entity_ids`**：`depth` 若給了會被忽略（無種子可展開，只記 warn 不算錯誤）；依 `entity_types`（或全部）分頁掃整個資料表。

**已合併掉的 Entity（`merged_into.is_some()`）一律排除**：merge 執行時會把所有指向被合併 Entity 的 Relationship 都 repoint 到 survivor，被合併掉的那一列只是留給 undo 用的歷史紀錄，不是目前狀態的一部分。

超過 `[stix].max_objects` 整批 Failed，且在累加過程中**提早失敗**，不用等全部掃完才發現超標（`max_objects` 與 import 驗證 bundle 物件數共用同一個設定值——語意對稱：一邊擋「進來太多」，一邊擋「一次要撈出去太多」）。

Relationship 兩端都要在最終匯出的 Entity 集合裡，且通過 `time_range`，才會被寫進 bundle。

匯出端會為每個 Entity **重新組**一個 STIX id（`entity_to_stix_object` 內部用 `entity.id` 決定性算出），**不是**沿用 import 時的原始 STIX id——不同來源匯入的同一個 Entity，匯出時只會有一個 canonical 的 STIX id。

匯出完成後 `JobService::merge_parameters`（`crates/core-jobs/src/service.rs`）把 `result_object_key` 寫回 `Job.parameters`，**保留**原本的 `filter` 欄位（部分合併，不是整份覆寫）。

### `[stix]`／`[stix_worker]` 設定

```toml
[stix]
max_bundle_bytes = 52428800   # 50 MiB，import 上傳大小上限，超過 413
max_objects = 10000           # bundle 物件數上限（import）／匯出 Entity 數上限（export），超過 413/Failed

[stix_worker]
bind = "127.0.0.1:18088"      # 接續既有服務埠號序列 18080-18087
consumer_group = "osint-stix-worker"
max_objects_per_tx = 10000    # 單次 import 交易最多寫入幾個去重後的 Entity
```

## 本機啟動與驗證

```bash
make run-stix-worker      # 訂閱 job.dispatched，執行 stix_import／stix_export
cargo test -p stix-adapter
cargo test -p stix-worker --all-targets   # 含 tests/e2e.rs，需要本機 Postgres/MinIO
cargo test -p acceptance --test acceptance_g   # Import→Core→Export 全程 HTTP 驗收
```

`/health`／`/ready`／`/metrics` 綁 `127.0.0.1:18088`。`/ready` 檢查 Postgres + 物件儲存。

### Metrics（`osint_stix_worker_*`）

以下名稱已與 `crates/stix-worker/src/service.rs` 的字面字串對照確認：

| 名稱 | 意義 |
|---|---|
| `osint_stix_worker_job_completed_total` | 成功完成的 job 數（import 與 export 合計） |
| `osint_stix_worker_job_failed_total` | 失敗（轉 Failed 狀態）的 job 數 |
| `osint_stix_worker_job_ignored_total` | 不認識的 job_type，略過並 commit |
| `osint_stix_worker_entities_total` | import 成功後寫入的 Entity 數（累計） |
| `osint_stix_worker_relationships_total` | import 成功後寫入的 Relationship 數（累計） |
| `osint_stix_worker_skipped_total` | import 時跳過的 STIX 物件與 Relationship 數（累計） |
| `osint_stix_worker_auto_merges_total` | import 後自動核准合併的次數（累計） |
| `osint_stix_worker_export_entities_total` | export 成功後匯出的 Entity 數（累計） |
| `osint_stix_worker_export_relationships_total` | export 成功後匯出的 Relationship 數（累計） |

另有共用計數器 `osint_failed_jobs_total`（來自 `core-observability`，各服務通用），stix-worker 遇到 `Failed`／`TransitionFailed` 時同樣累加。

## Operations Center 可見性

STIX job（`stix_import`／`stix_export`）**沒有新增專屬的 ops endpoint**，全部透過既有的通用機制：

- `GET /api/v1/jobs?status=failed`（或其他狀態）：`job_type` 是字串欄位，`stix_import`／`stix_export` 自動出現，不需要任何程式碼改動。
- `POST /api/v1/jobs/{id}/retry`：對失敗的 `stix_import`／`stix_export` Job 一樣適用。
- `GET /api/v1/ops/dlq`：失敗 Job 清單同樣含 STIX job（V0.1 沒有 DLQ topic，這裡列的是 Job 狀態機的 `failed`）。
- `GET /api/v1/ops/queues`：`osint-stix-worker` 的 consumer group lag（`job.dispatched` topic）跟其他 worker 一樣走 `[broker]` 通用機制，不需要新增程式碼。
- stix-worker 自己的 `/health`／`/ready`／`/metrics`（`127.0.0.1:18088`）是行程層級的健康與資源可見性，跟其他服務一致。

這對齊 SPEC_V0.2 §26／§27：「STIX import/export jobs」的 Operations Center 可見性條目靠 Job 系統既有的通用 list／retry／DLQ 視圖滿足，不是要求一個 STIX 專屬的畫面。

## 已知限制

1. **Custom 物件的 STIX → Core 反向映射不做**（刻意的單向契約，見上）。
2. **`EXPORT_TRAVERSAL_LIMIT = 100`**：任何一個 Entity 若參與超過 100 條 Relationship，BFS 展開與最終匯出的 Relationship 都可能漏掉多出來的那些（`list_relationships_by_object` 本身的設計就是有界查詢）。
3. **沒有 `--rebuild`**：STIX 匯入不是可重建的投影（不像 graph-worker／embedding-worker 那樣可以從 Postgres 重新推導）。
4. **Document→Report、Collection→Grouping 映射不在範圍內**（超出 SPEC §19 的 Entity/Relationship 七種）。

## 相關文件

- `docs/user/stix.md`：使用者導向的 API 使用說明與 curl 範例
- `docs/developer/api-skeleton.md`：三個端點的完整 curl 範例與錯誤碼
- `docs/developer/auto-approval.md`：ADR-012 自動核准機制（stix-worker 匯入後會呼叫）
- `docs/developer/entity-worker.md`：`entity_id`／`relationship_id` 決定性 id 算法
- `docs/developer/jobs.md`：Job 狀態機、`job.dispatched` 消費者、`merge_parameters`
