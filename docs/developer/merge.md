# Entity Merge（`crates/merge`）

對應內部規格 V0.2 §7（Entity Merge）與 Acceptance C（可 undo、不遺失歷史 evidence）。

```text
crates/merge    函式庫；HTTP 入口在 osint-api
```

這個 crate **執行**一次已經決定的 merge，**不產生候選**——候選是 `crates/resolver` 的事。呼叫端把 survivor／merged 兩個 Entity id 丟進來，這裡在一筆交易裡改寫參照、處理 relationship UNIQUE 撞號、標記 `entities.merged_into`、寫 `MergeHistory`。

## `MergeService`

```text
MergeService<S: TransactionalStore>
  new(store, producer: Option<Arc<EventProducer>>)
  execute_merge(survivor_id, merged_id, reason, operator) -> Result<MergeHistory, MergeError>
  undo_merge(merge_history_id) -> Result<(), MergeError>
```

用具體型別參數，不包 `Arc<dyn TransactionalStore>`：`TransactionalStore::begin` 已經回 `Box<dyn Transaction>`，再包一層 dyn 沒有 object-safety 收益。

## HTTP API

`osint-api` 用同一份 `PostgresCanonicalStore` 組 `MergeService`，放進
`AppState.merge: Option<SharedMergeService>`（`Arc<MergeService<PostgresCanonicalStore>>`，
理由同 `SharedJobService`：泛型服務吃不了 `dyn RelationalStore`）。沒接 Postgres 時是
`None`，對應 handler 回 503。掛在：

| 方法 | 路徑 | 角色 | 成功碼 |
|---|---|---|---|
| POST | `/api/v1/entities/merge` | operator | 200（`MergeHistory`） |
| POST | `/api/v1/merge-history/{id}/undo` | operator | 204 |
| GET | `/api/v1/entities/{id}/merge-history` | viewer | 200 |

`reason` 空白回 400。型別不同回 400，已經被併掉／重複 undo 回 409。
成功與失敗都寫稽核（`entity.merge`／`merge.undo`）。
請求／回應形狀見 `docs/developer/api-skeleton.md`。

## 交易邊界

前置檢查（存在、不是自己、型別相同、兩端都還沒被併掉）在**交易外**做，少開一筆空交易。通過後：

```text
tx = store.begin()
db = tx.store()
…改寫…
tx.commit()
```

任何一步 `Err` 都讓 `Transaction` drop 回滾（sqlx 保證）。狀態只剩「全在」或「全不在」，不會留下 `merged_into` 已寫、參照還沒改的半成品。

`undo_merge` 同一套：檢查 `MergeHistory` 存在、尚未 undo、survivor 之後沒有再被併掉，然後開交易逆序還原。

`tx.commit()` **成功之後**才發 `relationship.changed`（payload 與 partition key
見 `docs/developer/events.md`）。交易內組好事件內容（含 absorber 合併後的
完整列、repoint 後的完整列），不在 commit 後再查一次。`producer` 為 `None`
時整段跳過。發送失敗只記 error，不讓 merge／undo 本身失敗——canonical store
已經改完，重跑 `execute_merge` 會被 `AlreadyMerged` 擋住。

## `execute_merge` 做什麼

1. 收集 merged Entity 的 relationship／alias／identifier／extraction，以及 survivor 的 relationship（用來判斷撞號）。
2. [`classify_relationships`](#碰撞分類) 把 merged 的 relationship 分成三桶。
3. **self_loop**：刪 relationship（evidence 被 `ON DELETE CASCADE` 一起刪）。
4. **collision**：把 absorbed 的 evidence 搬到 absorber，加總 `evidence_count`、取 `confidence` 較大者、擴時間窗，刪 absorbed。
5. **safe_repoint**：把等於 merged id 的那一端改成 survivor。
6. alias／identifier／extraction 的 `entity_id` 改成 survivor。
7. `merged.merged_into = Some(survivor_id)`（**不刪** merged 那一列）。
8. 寫 `MergeHistory`（含 `repointed_references` 與 `merged_relationships`）。

## 碰撞分類

`classify_relationships(merged_rels, survivor_rels, merged_id, survivor_id)` 是純函式。

對 merged 的每一條 relationship：

1. 把等於 `merged_id` 的端點換成 `survivor_id`（兩端都可能是）。
2. 新的兩端相等 → **self_loop**（merged 與 survivor 之間原本的直接邊，或兩端都是 merged）。
3. survivor 已經有同一組 `(relationship_type, new_source, new_target)` → **collision**。
4. 否則 → **safe_repoint**。

方向算在 UNIQUE 裡：`A --Owns--> B` 與 `B --Owns--> A` 不會互相吸收。三桶互斥，合起來就是 `merged_rels` 的全部。

## `undo_merge` 做什麼

`repointed_references` 與 `merged_relationships` 都**逆序**還原：連續 merge 可能改寫同一列兩次，後面的先還原才對（見 `RepointedReference` 的說明）。

- 依 `table`／`column` 把 `previous_value` 寫回去。用得到的 get 方法：`get_relationship`、`get_entity_alias`、`get_entity_identifier`、`get_entity_extraction`。找不到列回 `MergeError::ReferenceMissing`，不靜默跳過。
- 碰撞：`put_relationship(absorbed_snapshot)` 重建被刪的邊，evidence 搬回去，absorber 的聚合欄位還原成 `absorber_pre_merge`。
- 自迴圈：只重建 relationship 本身。
- `merged_into` 清回 `None`；`undone_at` 設成現在。歷史列**不刪**。

## 已知限制

### 自迴圈的 evidence 無法復原

merged 與 survivor 之間原本的直接關聯，repoint 後兩端會變成同一個 Entity，語意無效，直接刪除。`relationship_evidence` 有 `ON DELETE CASCADE`，undo 只能重建 relationship 列，evidence 回不來。這是 schema 限制，記在 `MergedRelationship` 的 doc comment；undo 時會 `warn` 一行。

### 收集上限是 100，不是 1000

`RelationalStore` 的 `list_*` 契約與兩個 adapter 的 `clamp_limit` 都把 `limit` 夾在 1..=100。傳 1000 會被靜默截成 100，呼叫端還以為收齊了。

`MERGE_REF_CAP` 因此對齊實際能拿到的上限（100）。回傳筆數達到上限就中止並回 `MergeError::CollectionTruncated`，同時 `warn`。剛好 100 筆與「還有更多」分不出來——對 merge 來說漏收會讓圖接錯且不報錯，所以寧可誤殺，不要半改。

這不是效能上限（resolver 的 50／100 那種「漏比只影響排名」）；這裡漏收直接影響資料正確性。真的需要超過 100 條邊的 Entity merge，要先改 storage 契約，不要在 merge crate 假裝 1000 有用。

### identifier 的 UNIQUE 不在 merge 路徑處理

`(namespace, normalized_value)` 全域唯一，兩個 Entity 不可能同時持有同一個識別碼——第二次寫入在 entity-worker 就會變成 resolution candidate。所以 merge 搬 identifier 時不該撞號。若真的撞了，`put_entity_identifier` 回 `StorageError::Conflict`，整筆交易回滾。

## 錯誤

| 變體 | 何時 |
|---|---|
| `EntityNotFound` | survivor／merged 任一不存在 |
| `SelfMerge` | 兩端同一個 id |
| `TypeMismatch` | `entity_type` 不同 |
| `AlreadyMerged` | 任一端 `merged_into` 已有值 |
| `HistoryNotFound` | undo 找不到那筆歷史 |
| `AlreadyUndone` | 重複 undo |
| `SurvivorLaterMerged` | survivor 之後又被併進別人，要先 undo 那一次 |
| `CollectionTruncated` | 某個 `list_*` 達到 100 筆 |
| `ReferenceMissing`／`UnknownReferenceTable`／`UnknownReferenceColumn` | undo 時歷史與資料庫對不上 |
| `MissingAbsorberSnapshot` | 碰撞紀錄缺少還原快照 |
| `Storage` | 底層 storage 錯誤（含 Conflict） |

## 測試

```bash
cargo test -p merge
```

`tests/sqlite.rs` 不接 Kafka（`producer = None`），只驗 merge／undo 的資料改寫。
`tests/events.rs` 對本機 Redpanda 真跑：safe_repoint 發 `"upserted"`、自迴圈
execute 發 `"deleted"`、undo 發 `"upserted"`。EventConsumer 不暴露 Kafka
message key，partition key 以 `correlation_id == relationship_id` 間接驗證。
