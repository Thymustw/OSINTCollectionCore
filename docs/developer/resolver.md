# Resolver（V0.2 Phase 1c）

對應內部規格 V0.2 §5（Resolution Candidate）、§6（Resolution Methods）。

```text
crates/resolver    函式庫；目前沒有獨立 binary／事件消費者
```

給 Entity 跑 resolution method，產出 `ResolutionCandidate` 寫進 canonical store。
**不做事件消費／發布**——`entity.resolution.requested`／`completed` 是之後 Phase 1h。

## 方法狀態

SPEC §6 列了十種方法（`core_model::RESOLUTION_METHODS` 是拼法來源，不是白名單）。

| 方法 | 狀態 | 為什麼現在做／不做 |
|---|---|---|
| `normalized_name` | **已實作，接進 `resolve_entity` 聚合** | 資料在既有 `entities` 表，`find_entity_by_normalized_name` 就能查 |
| `exact_identifier` | 純函式 helper + entity-worker 呼叫端 | 見下文；寫入衝突時組候選，不是掃描式方法 |
| `alias` | **已實作，接進 `resolve_entity` 聚合** | 讀 `entity_aliases`；V0.2 還沒有生產寫入者，真實表可能仍空 |
| `domain` | **已實作（走 Relationship，接進 `resolve_entity` 聚合）** | 見下文；舊版 identifier 反查是結構性死碼 |
| `url` | **已併入 `domain`，函式退場** | URL／Email 共用網域都由 entity-worker 寫成 Relationship |
| `account_handle`／`email`／`external_id` | 不做 | 同一個 UNIQUE 限制，需要各自的跨欄位設計，不是抄舊版 `check_domain` 的形狀 |
| `semantic_similarity` | **已實作，接進 `resolve_entity` 聚合** | 暴力 cosine，只適合驗證 plumbing；門檻 `SEMANTIC_SIMILARITY_THRESHOLD` |
| `graph_context` | **已實作，接進 `resolve_entity` 聚合** | 只依賴 `GraphStore`，不查 Entity 本體；門檻 `GRAPH_CONTEXT_THRESHOLD` |

骨架把聚合點留在 `ResolverService::resolve_entity`，每加一個方法就接進那條呼叫鏈。

## `ResolverService`

```text
ResolverService<S: RelationalStore, E: EmbeddingProvider, G: GraphStore>
  new(store, embedder, graph)
  resolve_entity(entity_id) -> Result<Vec<ResolutionCandidate>, ResolverError>
  check_normalized_name(entity) -> Result<Vec<ResolutionCandidate>, ResolverError>
```

- `resolve_entity` 先 `get_entity`。找不到回 `ResolverError::EntityNotFound`，不 panic。
- 聚合順序：`normalized_name` → `alias` → `domain` → `semantic_similarity` → `graph_context`。
- 組好的 candidate 經 `put_resolution_candidate` 寫入。`StorageError::Conflict`
  （同一對同一方法已存在）視為已處理：記一行 log、continue，不讓整次 resolve 失敗。
  其他 storage 錯誤（含 semantic／graph 路徑）用 `?` 往上傳播。
- `check_normalized_name`／`check_alias`／`check_domain`／`check_semantic_similarity`／
  `check_graph_context` **不寫 store**，只組候選；寫入由 `resolve_entity` 負責。
- `SEMANTIC_SIMILARITY_THRESHOLD = 0.85`、`GRAPH_CONTEXT_THRESHOLD = 0.5` 是合理預設，
  之後應該可設定，不是最終定案。

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

## `alias`／`domain`

```text
check_alias(store, entity)  -> Result<Vec<ResolutionCandidate>, ResolverError>
check_domain(store, entity) -> Result<Vec<ResolutionCandidate>, ResolverError>
```

兩個都是自由函式，**不寫 candidate 表**；`resolve_entity` 會呼叫它們再 persist。
pair 用 `ResolutionCandidate::ordered_pair` 排序，`status = pending`、
`id = Uuid::now_v7()`、`reviewed_at = None`。

### `alias` 規則

1. `list_entity_aliases_by_entity(entity.id, 100)` 取這個 Entity 的 alias。
2. 對每個 alias 文字 `find_entity_aliases_by_text`（精確比對，不做大小寫折疊）。
3. 過濾掉 `entity_id == entity.id`（不對自己產生候選）。
4. 同一對因多個共用 alias 命中多次時只留第一筆，`evidence` 不合併所有 alias。
5. 命中：`score = 0.55`、`method = "alias"`。

evidence：

```json
{
  "method": "alias",
  "shared_alias": "...",
  "entity_a_alias_confidence": 0.9,
  "entity_b_alias_confidence": 0.7
}
```

`entity_a_alias_confidence`／`entity_b_alias_confidence` 對齊排序後的 a／b，
取自兩邊各自 alias 列的 `confidence`。

0.55 是中等訊號：同一個顯示名稱出現在兩個 Entity 上夠進 Review，不到自動合併。
V0.2 還沒有 `entity_aliases` 的生產寫入者，真實表可能仍空——空 `Vec` 多半是「沒資料」，
不要讀成「確定沒有同一實體」。

### `domain` 規則（走 Relationship）

舊版對自己列上的 `namespace="domain"` identifier 做 `find_entity_identifier_owner`。
`(namespace, normalized_value)` UNIQUE 保證一個值只有一個 owner，而這個 owner
一定就是查詢者自己——**已用真實 PostgreSQL 驗證為結構性死碼**，不是測試 double
種不出資料。舊版 `check_url` 唯一會命中的路徑是正規化不一致的意外邊緣案例
（結尾斜線），不是設計要抓的訊號。

真正該比的訊號已經在 `relationships` 表：entity-worker 抽到 Email／URL 時會衍生
Domain Entity，並寫 `Email --AssociatedWith--> Domain` 或 `URL --BelongsTo--> Domain`
（`source` 是 Email／URL，`target` 是 Domain）。兩份文件抽到指向同一個網域的
Email／URL 會因 UUID v5 自然鍵收斂到同一個 Domain Entity，所以「共享網域」
這件事已經完整記錄。

新版步驟：

1. `list_relationships_by_object(entity.id, 100)`，留下 `AssociatedWith`／`BelongsTo`，
   另一端視為衍生 Domain Entity。
2. 對每個 Domain 再 `list_relationships_by_object(domain_id, 100)`，找其他一端。
3. 跳過自己；同一對因多個共用 Domain 命中多次時只留第一筆。
4. 命中：`score = 0.30`、`method = "domain"`。

evidence：

```json
{
  "method": "domain",
  "shared_domain_entity_id": "<domain entity uuid>",
  "relationship_types": ["associated_with", "belongs_to"]
}
```

`relationship_types` 是兩段邊各自的 type 字串：第一段是查詢 Entity 連到 Domain 的
那條，第二段是另一端連到同一個 Domain 的那條。方便 Review 判斷是 email 還是 url
牽的線；不合併所有可能路徑。

0.30 是弱訊號（共用網域基礎設施不代表同一實體）。

`AssociatedWith`／`BelongsTo` 目前**不是**專屬於 domain 衍生（`RelationshipType`
是共用列舉）。V0.2 只有 entity-worker 會寫這兩種 type；之後有其他寫入者要再檢視，
否則會把非 domain 的關聯誤當成共用網域。

`url` 方法已併入這條路徑，函式本體退場。SPEC §6 清單仍保留 `"url"` 這個名字，
只是 crate 不再實作獨立掃描。

## `semantic_similarity`

```text
check_semantic_similarity(store, embedder, entity, threshold)
  -> Result<Vec<ResolutionCandidate>, ResolverError>
```

同 type 的 Entity 用 embedding cosine 比對。**不寫 candidate 表**；
`resolve_entity` 用 `SEMANTIC_SIMILARITY_THRESHOLD`（0.85）呼叫。

### 跳過 identity-type

`Domain`／`Hostname`／`Ip`／`Url`／`Email`／`Hash` 的 name 本身就是識別碼，
「192.168.1.1」跟「192.168.1.2」語意相近不代表同一實體。這類直接回空 Vec，
**不呼叫** embedder、也不查 store。

### 怎麼比

1. 組合文字：優先 `entity.name`；`description` 是 `Some` 時用
   `"{name}: {description}"`。
2. `EmbeddingRequest { kind: Passage, language: None }`。語言偵測不在這個
   函式範圍（簡化，不是遺漏）；`None` 走多語模型。
3. `embedder.embed`。`StorageError::UnsupportedCapability` → 記一行 info、
   回空 Vec，**不是錯誤**（還沒接 ml-commons 時的預期路徑）。其他錯誤往上傳播。
4. `list_entities_by_type(Some(entity.entity_type), None, 100)` 取同 type
   其他 Entity。過濾掉自己，對每個候選同樣組文字、embed、算 cosine。
5. `cosine >= threshold` 才組候選：
   - `score = cosine * 0.8`
   - `method = "semantic_similarity"`
   - `status = pending`、`id = Uuid::now_v7()`、`reviewed_at = None`
   - pair 用 `ResolutionCandidate::ordered_pair` 排序
   - evidence：

```json
{
  "method": "semantic_similarity",
  "cosine_similarity": 1.0,
  "model": "intfloat/multilingual-e5-small-int8",
  "embedding_kind": "passage"
}
```

`model` 來自 `embedder.model_for(None)`（語言固定 `None` 時的那一個）。

### 效能限制

一次最多拉 100 筆同 type Entity **逐一 embed**。這是暴力比對，只適合驗證
邏輯正確，不是效能設計。真實規模需要 ANN／embedding index。超過 100 的列
這次看不到，靜默漏比——不要把空結果讀成「沒有相似實體」。

單元測試用 crate 內記憶體 `RelationalStore` double +
`storage_core::mock::MockEmbeddingProvider`，不打真實 DB 或真實 embedding
服務。mock 向量**不是**真語意：相同文字 cosine ≈ 1.0 可以斷言，不同文字
的 cosine 是偽隨機的，不要斷言「一定不命中」。真實語意品質要接上
ml-commons adapter 才能驗證。

## `graph_context`

```text
check_graph_context(graph, entity_id, threshold) -> Result<Vec<ResolutionCandidate>, ResolverError>
```

純圖結構比對，**不吃 `RelationalStore`、不寫 candidate 表、不查 Entity 本體**。
`resolve_entity` 用 `GRAPH_CONTEXT_THRESHOLD`（0.5）呼叫。

### 候選集合怎麼產生

1. `GraphTraversalOptions { max_hops: 1, 其餘 Default／None }` 取 A 的一跳鄰居 `N(A)`。
   空集合直接回空 Vec——沒鄰居就沒有 graph context 可比。
2. 對每個 B ∈ `N(A)` 再取 `N(B)`，把「不是 A、也不是 A 的鄰居」的節點收成候選 C
   （A 的鄰居的鄰居，2-hop）。
3. **候選數量上限 50。** 超過就截斷並 `warn`。這是**效能上限、不是精確窮舉**：
   hub 節點的 2-hop 會爆炸，漏比是刻意的，不要把「沒產出候選」讀成「圖上沒有相似節點」。

Mock 把邊當無向處理（與 `GraphStore` 契約一致），所以菱形 A—B、A—C、D—B、D—C
裡 A 與 D 互為 2-hop 候選。

### Jaccard 與 score

```text
jaccard = |N(A) ∩ N(C)| / |N(A) ∪ N(C)|
```

兩個鄰居集合都空時定義為 `0.0`（避免除以零）。`jaccard >= threshold` 才組候選：

- `score = jaccard * 0.7`（圖結構是弱訊號，遠不到自動合併）
- `method = "graph_context"`
- `status = pending`、`id = Uuid::now_v7()`、`reviewed_at = None`
- pair 用 `ResolutionCandidate::ordered_pair` 排序
- evidence：

```json
{
  "method": "graph_context",
  "jaccard_similarity": 1.0,
  "shared_neighbors": ["…", "…"],
  "entity_a_neighbor_count": 2,
  "entity_b_neighbor_count": 2,
  "shared_count": 2,
  "union_count": 2
}
```

`shared_neighbors` 是交集的 EntityId 字串列表。`entity_a_neighbor_count` 是查詢起點
A 的 `|N(A)|`，`entity_b_neighbor_count` 是候選 C 的 `|N(C)|`——**不**隨 ordered pair
對調。菱形圖（A 與 D 共享 {B,C}）Jaccard = 1.0，score 接近 0.7。

單元測試用 `storage_core::mock::MockGraphStore`，不打真實 Neo4j。真實遍歷效能
與 mock 行為是否一致，要等 Phase 2 的 `storage-neo4j` adapter。

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

**entity-worker 是第一個呼叫端。** `upsert_entity` 對 Domain／Ip／Url／Email／CVE
寫 `entity_identifiers`，撞到 `(namespace, normalized_value)` UNIQUE 時呼叫這個
helper 再 `put_resolution_candidate`。這**不是** `ResolverService::resolve_entity`
裡的掃描式方法——掃描式 `exact_identifier` 還沒做，不要把「寫入衝突會產候選」
理解成「resolve_entity 已經會跑 exact identifier」。

## 錯誤

| 變體 | 何時 |
|---|---|
| `ResolverError::Storage` | 底層 `StorageError`（Conflict 在 persist 路徑已被吃掉） |
| `ResolverError::EntityNotFound` | `resolve_entity` 的 id 在 store 裡沒有對應列 |

## 測試策略

單元測試用 crate 內的記憶體 `RelationalStore` double，不打真實 PostgreSQL／SQLite。
理由：

- `normalized_name` 的比對、skip-self、Conflict 不中斷，都是這個 crate 的邏輯，
  不是 adapter 的 SQL。
- skip-self 必須能種「同 type 同名的第二個 Entity」來證明是程式跳過、
  不是碰巧查不到——真實 DB 的自然鍵 unique index 不允許這種列存在。
- `check_domain` 用 Relationship 種子模擬「Email A → Domain X」「Email B → Domain X」，
  不需要 identifier UNIQUE 的假衝突。
- adapter 對 `find_entity_by_normalized_name`、`put_resolution_candidate`、
  `list_relationships_by_object` 的契約已由 `storage-core::conformance` 覆蓋。
- `resolve_entity` 聚合測試注入 `MockEmbeddingProvider::unsupported()` 與空的
  `MockGraphStore`，讓 semantic／graph 回空，不干擾 normalized_name／alias 斷言。

未覆蓋：對真實 Docker Postgres 跑一次 end-to-end 的 Relationship-based `check_domain`。
那要等 Phase 1h 接上事件之後，用 entity-worker 抽出的真實 Entity 再補。
這次 Relationship 設計沒有打過真實 Postgres，靠既有 conformance 保證
`list_relationships_by_object` 的契約。
