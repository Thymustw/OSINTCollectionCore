# V0.2 schema notes

欄位來源：內部規格 V0.2 §3／§4／§5／§7（規格文件本身未隨原始碼公開，章節號留著方便查找對應決策）。ADR-008 的決策：V0.1 不實作 DLQ topic；`failed_events` 表是讓永久失敗事件可查詢的替代方案（完整 DLQ subsystem 包含保留策略、重放路徑與 RBAC，整套留到 V0.2 才做）。
V0.1 的 schema 慣例（PG vs SQLite 型別對照、cursor 分頁規則）見 `schema-v0.1.md`，這裡不重複。

## Migration 0007：Entity Resolution 四張表 + `failed_events`（Phase 0c）

`migrations/postgres/0007_v0_2_resolution_and_failed_events.sql` 與
`migrations/sqlite/0007_v0_2_resolution_and_failed_events.sql`。

| 表 | 規格 | 用途 |
|---|---|---|
| `entity_aliases` | §3 | 人看的名字。**允許多個 Entity 共用同一個 alias** |
| `entity_identifiers` | §4 | 命名空間內唯一的鍵。§6「exact identifier」的依據 |
| `resolution_candidates` | §5／§6 | 合併候選，一列 = 一個 (pair, method) |
| `merge_history` | §7 / Acceptance C | merge 稽核 + undo 依據 |
| `failed_events` | ADR-008 | 永久失敗事件的 canonical 紀錄（取代 DLQ topic） |

對應的 `core-model` 型別（Phase 0c 一起加，每個都有 serde round-trip 測試）：

| 型別 | 檔案 | 備註 |
|---|---|---|
| `EntityAlias` | `crates/core-model/src/entity_alias.rs` | `alias_type` 是自由字串（SPEC §3 沒列舉值）；`source_id` 可為 `None` |
| `EntityIdentifier` | `crates/core-model/src/entity_identifier.rs` | `namespace` 見下文與 T10 |
| `ResolutionCandidate` + `RESOLUTION_METHODS` | `crates/core-model/src/resolution.rs` | `RESOLUTION_METHODS` 是 §6 那十個名字的**參考清單，不是白名單**（§6 原文是「至少」），欄位仍是自由字串；`ordered_pair()` 負責排序 |
| `ResolutionStatus` | `crates/core-model/src/enums.rs` | V0.2 少數規格有列舉值的欄位，所以做成 enum。`confirmed` 與 `auto_confirmed` **刻意分開**——混成同一個值之後就再也分不出「哪些合併沒有人類看過」 |
| `MergeHistory` + `RepointedReference` + `MergedRelationship` + `AbsorberSnapshot` | `crates/core-model/src/merge.rs` | `RepointedReference` = `{table, row_id, column, previous_value}`；`MergedRelationship` 見 migration 0008 |
| `FailedEvent` | `crates/core-model/src/failed_event.rs` | `offset` 是 `i64` |

「是誰 undo 的」不放 `MergeHistory`，走 V0.1 既有的 `audit_log`（migration 0006）——
那張表本來就是記「誰對哪個資源做了什麼」的地方。

> ⚠️ **Phase 0 只建 schema；Phase 1c 起開始有寫入者。**
> `crates/resolver` 會寫 `resolution_candidates`（`normalized_name` 掃描，以及
> entity-worker 在 identifier 衝突時寫入的 `exact_identifier` 列）。
> `entity-worker` 會為 Domain／Ip／Url／Email／Vulnerability 以及 Account
> （per-platform `{platform}_handle`）寫 `entity_identifiers`
> （Hash／Person／Organization 刻意不寫，見 `docs/developer/entity-worker.md`）。
> `crates/merge` 會寫 `merge_history`（含 `repointed_references`／`merged_relationships`）
> 並改 `entities.merged_into`、repoint alias／identifier／extraction／relationship。
> `entity_aliases` 目前只有 merge 會改寫既有列，還沒有獨立的生產寫入者。
> `failed_events` 仍沒有生產寫入者。

### 規格沒寫、資料表必須補的欄位

- `entity_aliases.id` / `entity_identifiers.id` / `resolution_candidates` 以外全部的 `id`
  （SPEC 的欄位清單有列 `id`，但 `merge_history` 沒有——資料表需要主鍵）
- `merge_history.repointed_references`：undo 的**全部**依據，見下文
- `merge_history.undone_at`：不加它就只能「undo 時刪列」（歷史消失）或
  「留著不標記」（已撤銷的 merge 看起來仍生效），兩者都違反 Acceptance C
- `failed_events.id`：ADR-008 列的是 topic/partition/offset/… 沒有主鍵

### 與 SPEC §5 字面不同的一處：`method` 是單數

SPEC §5 的欄位寫 `methods`（複數）。實作落地成**單數** `method`，一對 Entity 被三種
方法命中就是三列，`(entity_a_id, entity_b_id, method)` 是 UNIQUE。

理由：`score` 與 `status` 都只有在「針對某一種方法」時才有明確意義。三種方法塞進
同一列之後，`score` 是誰的分數、審核者 reject 的是哪一條證據，都講不清楚。
SPEC 講的「這一對有哪些 methods」等於這張表上同一對的列集合。

**做 resolver 與 Console 的 Resolution Review 時要知道這個差異。**

### `entity_a_id < entity_b_id` 的 CHECK constraint

候選對是**無向**的：`(A,B)` 與 `(B,A)` 是同一件事。唯一索引分不出這兩者，
不強制順序的話同一對會存成兩列——而且**不會有任何錯誤**，只會讓 Review 畫面出現
重複項目，審核者可能確認一個、拒絕另一個。

所以兩份 migration 都加了 CHECK。呼叫端用
`core_model::ResolutionCandidate::ordered_pair(a, b)` 排好再寫。

SQLite 比的是 UUID 的**文字**形式，PG 比的是 16 個位元組。兩者順序相同：canonical
形式是固定長度小寫十六進位、連字號位置固定，ASCII 字典序等同位元組序。
⚠️ 前提是寫入端一律用小寫 canonical 形式（`Uuid::to_string` 就是）。

### `(namespace, normalized_value)` UNIQUE 的代價要知道

一個識別碼只屬於一個 Entity——這是「exact identifier 可以當合併依據」的前提。

代價是：**兩個 Entity 宣稱同一個識別碼時，第二次寫入會回 `StorageError::Conflict`。**
那不是錯誤處理的邊角，那正是 resolution 要偵測的訊號。寫入端收到 `Conflict`
應該去建一筆 `resolution_candidate`；**吞掉的話識別碼會少記一筆而且毫無跡象**。
組候選用 `resolver::resolution_candidate_from_identifier_conflict`（純函式，不寫 store）。
entity-worker 是第一個呼叫端：`upsert_entity` 收到 `Conflict` 後查 owner、組候選、
`put_resolution_candidate`。候選本身再撞 `Conflict`（同一對同一方法已存在）視為正常。
見 `docs/developer/entity-worker.md` 與 `docs/developer/resolver.md`。

### `namespace` 與 V0.1 報告 T10

T10 是已接受的限制：40 位 hex 分不出 SHA-1 與 git commit，entity-worker 只能標
`ambiguous_sha1`（confidence 0.7）。根源是值本身不帶型別資訊。

`namespace` 把型別搬到值外面：`("sha1", <40 hex>)` 與 `("git_commit", <40 hex>)`
是兩個不同的識別碼。entity-worker **這次明確跳過 Hash**：抽取端還分不出雜湊型別，
若用同一個 `namespace="hash"` 寫進去，exact_identifier 會把檔案雜湊與 commit id
誤判成同一個識別碼。T10 因此還沒消失。

### `merge_history.repointed_references`：undo 的全部依據

JSON 陣列，每個元素是 `{table, row_id, column, previous_value}`。

**為什麼不能事後掃描重建**：merge 完成後資料庫上已經沒有這個資訊了。掃描只看得到
「這些列現在指向 survivor」，分不出哪些是 merge 改過來的、哪些本來就指向 survivor。
把後者一起改回去會破壞無關的資料，而且不會報錯。

**為什麼存 `previous_value` 而不是假設等於 `merged_id`**：連續 merge 會讓同一列被
改寫兩次（A→B，之後 B→C）。undo 第二次時正確的還原值是 B；靠「還原成這次 merge 的
merged_id」去推，兩次 undo 順序一顛倒就會還原成錯的值。

adapter 讀到不是陣列或解不開的內容時回 `CorruptionSuspected`，**不是當成空陣列**——
空陣列的意思是「這次 merge 沒改過任何參照」，拿它代表「讀不懂」會讓 undo
靜默少還原一批參照。

`merged_id` **刻意不加外鍵**：merge 實作若選擇刪掉被併掉的那一列，外鍵會讓這筆歷史
寫不進去（或被一起刪掉），正好弄丟 Acceptance C 要保留的東西。要支援 undo 就不該刪
那一列，但那是 resolver 的決定，schema 這層不替它預設。

### `failed_events`：自然鍵是座標，不是 `id`

ADR-008 指定 V0.2 用 canonical 表而不是 Redpanda DLQ topic（topic 會過期，
過期就是靜默資料遺失）。

`(topic, partition, offset)` 是 broker 上一則訊息的完整座標，也是這張表的 UNIQUE。
同一則事件重試三次失敗是**一列 `attempt_count=3`**，不是三列——否則「有幾則事件壞掉」
與「壞掉的事件被試了幾次」會混在一起，DLQ 的筆數就沒有意義。

`"offset"` 在 PG 與 SQLite 都是保留字，DDL 與所有 SQL 都要加雙引號。
型別是 64 位（PG `BIGINT`）：長期執行的 topic 真的會超過 `i32`。

## Migration 0008：Entity Merge 欄位（Phase 1e）

`migrations/postgres/0008_v0_2_entity_merge_columns.sql` 與
`migrations/sqlite/0008_v0_2_entity_merge_columns.sql`。

兩個欄位都是為了讓 merge 可以 undo，而且不違反既有外鍵。
執行邏輯在 `crates/merge`（`MergeService::execute_merge`／`undo_merge`），
見 `docs/developer/merge.md`。

| 欄位 | 型別（PG / SQLite） | 用途 |
|---|---|---|
| `entities.merged_into` | `UUID NULL REFERENCES entities(id)` / `TEXT REFERENCES entities(id)` | 被併掉的 Entity **不刪列**，改指向 survivor |
| `merge_history.merged_relationships` | `JSONB NOT NULL DEFAULT '[]'` / `TEXT NOT NULL DEFAULT '[]'` | 因 relationship UNIQUE 撞號而被吸收或自迴圈刪除的列 |

### 為什麼不刪被併掉的 Entity

`resolution_candidates.entity_a_id`／`entity_b_id`、`entity_extractions.entity_id`
都有 FK 指向 `entities(id)`，且**沒有 `ON DELETE CASCADE`**（預設 `NO ACTION`）。
刪列會直接違反外鍵。undo 也需要這一列還在，才能把參照寫回去。

查詢一般 Entity 列表時應過濾 `merged_into IS NOT NULL`——那是 API 層的責任，
adapter 的 `list_entities` **不過濾**，與 `list_merge_history_by_entity` 回已撤銷
merge 的理由相同：過濾是呼叫端的決定，adapter 靜默丟掉會讓歷史查不到。

`entity-worker` 的 upsert 會**保留**既有的 `merged_into`。寫成 `None` 會把已併掉的
Entity 靜默復活。

0007 對 `merge_history.merged_id` 刻意不加外鍵，是因為當時還沒決定 merge 要不要
刪列。0008 決定了「不刪」，所以 `entities.merged_into` **有**自參照外鍵——指向的
survivor 必須真的存在。

### 為什麼 `RepointedReference` 不夠，要另開 `merged_relationships`

`RepointedReference` 只能表達「單欄位從 A 改成 B」。relationship 表有
`(source_object_id, relationship_type, target_object_id)` UNIQUE（migration 0005）。
merge 把指向 merged Entity 的端點改成 survivor 時，可能撞到 survivor 那邊已經
存在的同一條邊：

- **吸收合併**：撞號那條被刪，它的 evidence 搬到留下來那條；undo 需要 absorber
  合併前的 `evidence_count`／`confidence`／時間窗（`AbsorberSnapshot`），以及
  被刪那條的完整 `Relationship` 快照。
- **自迴圈刪除**：merged 與 survivor 之間原本就有直接關聯，repoint 後兩端變成
  同一個 Entity，語意無效，直接刪除、不吸收。`relationship_evidence` 會被
  `ON DELETE CASCADE` 一併刪掉，undo 只能重建 relationship 本身，evidence
  無法復原——這是已知限制，記在 `MergedRelationship` 的 doc comment。

空陣列代表「這次 merge 沒有任何 relationship 撞號」，**不是沒記錄**。adapter
讀到解不開的內容時回 `CorruptionSuspected`，與 `repointed_references` 同一套理由。

對應型別：`MergedRelationship`、`AbsorberSnapshot`（`crates/core-model/src/merge.rs`）。

## Migration 0009：`entity_identifiers.normalized_value` 單欄索引

`migrations/postgres/0009_v0_2_identifier_normalized_value_index.sql` 與
`migrations/sqlite/0009_v0_2_identifier_normalized_value_index.sql`。

`check_account_handle` 要找「這個 handle 不管掛在哪個 namespace 下」。
既有 `idx_entity_identifiers_natural (namespace, normalized_value)` 的前導欄
是 namespace，只查 `normalized_value` 走不到那條索引。

`(normalized_value, id)` 把排序欄也蓋進去，讓 `ORDER BY id ASC LIMIT n`
不必再回表排序。這不是 UNIQUE——同一個值出現在不同 namespace 是這個方法
要抓的訊號。

## Migration 0010：`embeddings`（Phase 3 Step 1）

`migrations/postgres/0010_v0_2_embeddings.sql` 與
`migrations/sqlite/0010_v0_2_embeddings.sql`。

PostgreSQL **只存 embedding metadata**，不含向量本體——向量只投影進
OpenSearch k-NN index（Step 2）。這張表回答「這個目標、這個模型、
這個內容雜湊算過了沒」，給 embedding-worker 判斷要不要重算。

| 欄位 | 型別（PG／SQLite） | 說明 |
|---|---|---|
| `id` | UUID／TEXT | 主鍵 |
| `target_id` | UUID／TEXT | 目標物件。**沒有 FK**：目標可以是 Document／Entity／Event |
| `target_type` | TEXT | `EmbeddingTarget`（`document_title`／`document_body`／`entity_description`／`event_description`） |
| `model` | TEXT | 例如 `huggingface/sentence-transformers/all-MiniLM-L6-v2` |
| `model_version` | TEXT | 模型內容 SHA-256，**不是** ml-commons 內部序號 |
| `dimensions` | INTEGER | 向量維度 |
| `content_hash` | TEXT | 原始文字（不含 query:/passage: 前綴）的 SHA-256 |
| `created_at` | TIMESTAMPTZ／TEXT | 寫入時間 |

唯一鍵 `idx_embeddings_target_model_hash (target_id, target_type, model, content_hash)`：
同一個 key 出現第二次代表呼叫端沒先 `find_embedding`。`put_embedding` **不是 upsert**，
撞號回 `StorageError::Conflict`。`list_embeddings_by_target` 依 `id` 升序，`limit` 夾在 1..=100。
對應 `RelationalStore` 三個方法都在 postgres／sqlite adapter 實作。
設定在 `config/default.toml` 的 `[embedding]`（`EmbeddingSection`）與
`[search_hybrid]`（`HybridSearchSection`）。


對應型別：`Embedding`（`crates/core-model/src/embedding.rs`）、
`EmbeddingTarget`（`crates/core-model/src/enums.rs`）、`EmbeddingId`。

> ⚠️ **Step 1 只建 schema 與 storage port；沒有 embedding-worker、沒有 API。**
> 表是空的是預期結果。

## Migration 0011：`duplicate_groups.model`（Phase 3 Step 1）

`migrations/postgres/0011_v0_2_duplicate_group_model.sql` 與
`migrations/sqlite/0011_v0_2_duplicate_group_model.sql`。

Semantic Dedup（SPEC §17「method/model」）的模型名稱寫在 `DuplicateGroup.model`。
Stage 1-4 的方法不靠模型，欄位是 `NULL`；Stage 5 才填。

## Migration 0012：`merge_history.auto_approval_audit`（ADR-012 Step 0）

`migrations/postgres/0012_v0_2_merge_history_auto_approval_audit.sql` 與
`migrations/sqlite/0012_v0_2_merge_history_auto_approval_audit.sql`。

| 欄位 | 型別（PG / SQLite） | 用途 |
|---|---|---|
| `merge_history.auto_approval_audit` | `JSONB NULL` / `TEXT NULL` | AI 輔助自動核准的完整稽核記錄。`NULL` = 人工 merge（向下相容） |

JSON schema 由 `resolver::auto_approval` 定義（`core-model` 不 import
那個 crate，避免循環相依，所以 `MergeHistory.auto_approval_audit` 的型別是
`Option<serde_json::Value>`）。完整 schema 見 `docs/developer/auto-approval.md`。

`NULL` = 人工 merge（向下相容）。自動核准 merge 的 `auto_approval_audit` 非 `NULL`，
包含觸發方法、分數、當時門檻快照，以及（若走 LLM 路徑）完整 prompt 與回應。

對應 `RelationalStore::update_resolution_candidate_status`：更新一筆
resolution candidate 的 `status`（`Pending` → `AutoConfirmed`）與 `reviewed_at`，
回 `true`／`false`（比照 `mark_replayed`，id 不存在不是 `NotFound`）。
生產呼叫端是 `resolver::auto_approval::AutoApprovalEvaluator::mark_candidates_auto_confirmed`，
在 merge 成功後批次更新這對 Entity 的所有 Pending 候選。

設定在 `config/default.toml` 的 `[auto_approval]`（`AutoApprovalSection`，
巢狀 `[auto_approval.llm]` 對應 `AutoApprovalLlmSection`），
**預設 `enabled = false`**。門檻是否自洽（`auto_confirm_score >= llm_review_score`）
由 `AutoApprovalSection::thresholds_are_sane()` 判斷；不自洽時 `core-api` 組裝端
記 `tracing::error!` 並強制停用，所有候選維持 Pending。
完整欄位說明與啟用方式見 `docs/developer/auto-approval.md`。

## 對照 V0.1 的 `/ops/dlq`

ADR-008 的 `GET /api/v1/ops/dlq` 目前仍回 `dlq_topic: null` + 失敗的 **Job** 清單。
0c 只建了 `failed_events` 的 schema 與 storage port，**沒有改 API**——
事件層級的失敗在 Phase 0c 之後仍然查不到，T14 尚未關閉。
