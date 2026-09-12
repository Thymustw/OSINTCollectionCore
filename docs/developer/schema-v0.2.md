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
| `MergeHistory` + `RepointedReference` | `crates/core-model/src/merge.rs` | `RepointedReference` = `{table, row_id, column, previous_value}` |
| `FailedEvent` | `crates/core-model/src/failed_event.rs` | `offset` 是 `i64` |

「是誰 undo 的」不放 `MergeHistory`，走 V0.1 既有的 `audit_log`（migration 0006）——
那張表本來就是記「誰對哪個資源做了什麼」的地方。

> ⚠️ **Phase 0 只建 schema；Phase 1c 起開始有寫入者。**
> `crates/resolver` 會寫 `resolution_candidates`（`normalized_name` 掃描，以及
> entity-worker 在 identifier 衝突時寫入的 `exact_identifier` 列）。
> `entity-worker` 會為 Domain／Ip／Url／Email／Vulnerability 寫
> `entity_identifiers`（Hash／Person／Organization 刻意不寫，見
> `docs/developer/entity-worker.md`）。
> `entity_aliases`／`merge_history`／`failed_events` 仍沒有生產寫入者。

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

## 對照 V0.1 的 `/ops/dlq`

ADR-008 的 `GET /api/v1/ops/dlq` 目前仍回 `dlq_topic: null` + 失敗的 **Job** 清單。
0c 只建了 `failed_events` 的 schema 與 storage port，**沒有改 API**——
事件層級的失敗在 Phase 0c 之後仍然查不到，T14 尚未關閉。
