# Resolver（V0.2 Phase 1c）

對應內部規格 V0.2 §5（Resolution Candidate）、§6（Resolution Methods）。

```text
crates/resolver    函式庫；目前沒有獨立 binary／事件消費者
```

給 Entity 跑 resolution method，產出 `ResolutionCandidate` 寫進 canonical store。
**不做事件消費／發布**——`entity.resolution.requested`／`completed` 是之後 Phase 1h。

## 目前只實作 `normalized_name`

SPEC §6 列了十種方法（`core_model::RESOLUTION_METHODS` 是拼法來源，不是白名單）。
這個 crate 現在真正去查資料的只有「跨 `EntityType`、相同 `normalized_name`」。

| 方法 | 狀態 | 為什麼現在做／不做 |
|---|---|---|
| `normalized_name` | **已實作** | 資料在既有 `entities` 表，`find_entity_by_normalized_name` 就能查 |
| `exact_identifier` | 只有純函式 helper | 見下文；entity-worker 還沒寫 `entity_identifiers` |
| `alias`／`domain`／`url`／`account_handle`／`email`／`external_id` | 不做 | 依賴 identifier／alias 寫入者，空跑會讓「沒命中」與「沒資料」無法分辨 |
| `semantic_similarity`／`graph_context` | 不做 | 分別等 embedding 與 graph projection |

其餘方法是獨立交付項目，**不要**在這個 crate 裡順手加——骨架已經把聚合點留在
`ResolverService::resolve_entity`，每加一個方法就接進那條呼叫鏈。

## `ResolverService`

```text
ResolverService<S: RelationalStore>
  new(store)
  resolve_entity(entity_id) -> Result<Vec<ResolutionCandidate>, ResolverError>
  check_normalized_name(entity) -> Result<Vec<ResolutionCandidate>, ResolverError>
```

- `resolve_entity` 先 `get_entity`。找不到回 `ResolverError::EntityNotFound`，不 panic。
- 目前聚合呼叫只跑 `check_normalized_name`。
- 組好的 candidate 經 `put_resolution_candidate` 寫入。`StorageError::Conflict`
  （同一對同一方法已存在）視為已處理：記一行 log、continue，不讓整次 resolve 失敗。
- `check_normalized_name` **不寫 store**，只組候選，方便單測直接斷言內容。

### `normalized_name` 規則

對 `ALL_ENTITY_TYPES`（SPEC §10 的 13 個變體）逐一
`find_entity_by_normalized_name(type, entity.normalized_name)`。

- **跳過 entity 自己的 `entity_type`**。同 type 同名在 UUID v5 自然鍵下去重後
  已經是同一個 Entity；仍做防禦：查到 `id` 等於自己也跳過。
- 跨 type 命中：`score = 0.40`、`method = "normalized_name"`、
  `status = pending`、`id = Uuid::now_v7()`、`reviewed_at = None`。
- pair 用 `ResolutionCandidate::ordered_pair` 排序（migration 0007 CHECK
  `entity_a_id < entity_b_id`）。
- evidence：

```json
{
  "method": "normalized_name",
  "normalized_name": "...",
  "entity_a_type": "...",
  "entity_b_type": "...",
  "match_type": "cross_type_exact"
}
```

`entity_a_type`／`entity_b_type` 對齊排序後的 a／b，不是傳入參數的順序。

0.40 是弱訊號：Person「acme」與 Organization「acme」同名不代表同一實體，
只夠進 Review，遠不到 `auto_confirmed`。

## `exact_identifier` 衝突 helper

```text
resolution_candidate_from_identifier_conflict(
    existing_owner: &EntityIdentifier,
    conflicting_entity_id: EntityId,
) -> ResolutionCandidate
```

純函式，**不吃 store、不寫入**。設計給 identifier 寫入者在
`put_entity_identifier` 收到 `StorageError::Conflict` 時組候選
（那正是 schema 把 `(namespace, normalized_value)` 設 UNIQUE 的原因，
見 `docs/developer/schema-v0.2.md`）。

- `score = 0.95`、`method = "exact_identifier"`、`trigger = "write_conflict"`。
- 0.95 高到值得進 Review，但不到自動合併：SPEC 禁止只因同 username 就判定
  同一真實人物，namespace 衝突同樣可能是帳號共用或資料髒了。

**目前沒有任何呼叫端。** entity-worker 還沒寫識別碼。不要把這個 helper
的存在理解成「exact identifier 方法已經接上管線」。

## 錯誤

| 變體 | 何時 |
|---|---|
| `ResolverError::Storage` | 底層 `StorageError`（Conflict 在 persist 路徑已被吃掉） |
| `ResolverError::EntityNotFound` | `resolve_entity` 的 id 在 store 裡沒有對應列 |

## 測試策略

單元測試用 crate 內的記憶體 `RelationalStore` double（只實作
`get_entity`／`find_entity_by_normalized_name`／`put_resolution_candidate`），
不打真實 PostgreSQL／SQLite。理由：

- `normalized_name` 的比對、skip-self、Conflict 不中斷，都是這個 crate 的邏輯，
  不是 adapter 的 SQL。
- skip-self 必須能種「同 type 同名的第二個 Entity」來證明是程式跳過、
  不是碰巧查不到——真實 DB 的自然鍵 unique index 不允許這種列存在。
- adapter 對 `find_entity_by_normalized_name` 與 `put_resolution_candidate`
  的契約已由 `storage-core::conformance` 覆蓋。

未覆蓋：對真實 Docker Postgres 跑一次 end-to-end 的跨 type 同名。那要等
Phase 1h 接上事件之後，用 entity-worker 抽出的真實 Entity 再補。
