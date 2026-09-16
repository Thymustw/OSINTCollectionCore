# Resolver（V0.2 Phase 1c）

對應內部規格 V0.2 §5（Resolution Candidate）、§6（Resolution Methods）。

```text
crates/resolver    函式庫；HTTP 入口在 osint-api
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
| `account_handle` | **已實作，接進 `resolve_entity` 聚合** | 跨平台同 handle（namespace 字串不同，UNIQUE 擋不住）；分數 0.35 |
| `email`／`external_id` | **刻意不做** | 訊號已被 `exact_identifier` 寫入衝突機制蓋掉（entity-worker 寫 `email`／`cve` namespace 時，UNIQUE 撞號會直接觸發 `exact_identifier` 候選）。批次掃描版本會是跟舊版 `check_domain` 一樣的結構性死碼 |
| `semantic_similarity` | **已實作**；`resolve_entity` 仍聚合，另有獨立入口 `resolve_semantic_similarity` | 暴力 cosine，只適合驗證 plumbing；門檻 `SEMANTIC_SIMILARITY_THRESHOLD`。獨立入口預留給 Phase 3 接 ml-commons |
| `graph_context` | **已實作，獨立成 `GraphContextResolver`** | 只依賴 `GraphStore`，不查 Entity 本體；門檻 `GRAPH_CONTEXT_THRESHOLD`。不進 `resolve_entity`，避免 Neo4j 斷線拖累另外幾個方法 |

`resolve_entity` 只聚合五個只需要 Postgres 的方法。`graph_context` 走獨立的 `GraphContextResolver`／獨立 endpoint。`semantic_similarity` 雖然還在 `ResolverService` 上，但也有獨立呼叫 `resolve_semantic_similarity`；Phase 3 接 ml-commons 時若也需要解耦合，可以比照 `GraphContextResolver` 再抽一次。

## HTTP API

`osint-api` 把 `ResolverService` 放進
`AppState.resolver: Option<SharedResolverService>`
（`Arc<ResolverService<PostgresCanonicalStore, MockEmbeddingProvider>>`）。
組裝時注入的是 `MockEmbeddingProvider::unsupported()`：

- `check_semantic_similarity` 誠實回空（不是假造相似度）

`graph_context` **不**在這個 service 上。它走獨立欄位
`AppState.graph_resolver: Option<SharedGraphContextResolver>`
（`Arc<GraphContextResolver<PostgresCanonicalStore, Neo4jStore>>`）。
Neo4j 沒接上時這個欄位是 `None`，**只有**
`POST /api/v1/entities/{id}/resolve/graph-context` 回 503，
不影響 `POST /entities/{id}/resolve` 的另外幾個方法。這是刻意拆開的設計。

因此 `POST /api/v1/entities/{id}/resolve` **目前會真的產生候選的**是
`normalized_name`／`alias`／`domain`／`account_handle`。
`semantic_similarity` 在 API 組裝路徑上仍因 mock embedder 而誠實回空。
這不是 workaround，是已知限制——等 ml-commons adapter 接上才會補齊。

| 方法 | 路徑 | 角色 | 成功碼 |
|---|---|---|---|
| POST | `/api/v1/entities/{id}/resolve` | operator | 200（這次新寫入的候選；不含 graph_context） |
| POST | `/api/v1/entities/{id}/resolve/graph-context` | operator | 200（這次新寫入的 graph_context 候選；Neo4j 沒接上回 503） |
| GET | `/api/v1/entities/{id}/resolution-candidates` | viewer | 200（cursor 分頁，`?status=`） |

Entity 不存在回 404。成功與失敗都寫稽核
（`entity.resolve`／`entity.resolve_graph_context`）。
請求／回應形狀見 `docs/developer/api-skeleton.md`。

## `ResolverService`

```text
ResolverService<S: RelationalStore, E: EmbeddingProvider>
  new(store, embedder)
  resolve_entity(entity_id) -> Result<Vec<ResolutionCandidate>, ResolverError>
  resolve_semantic_similarity(entity_id) -> Result<Vec<ResolutionCandidate>, ResolverError>
  check_normalized_name(entity) -> Result<Vec<ResolutionCandidate>, ResolverError>
```

- `resolve_entity` 先 `get_entity`。找不到回 `ResolverError::EntityNotFound`，不 panic。
- 聚合順序：`normalized_name` → `alias` → `domain` → `semantic_similarity` → `account_handle`。
  **不含** `graph_context`（見下方 `GraphContextResolver`）。
- 組好的 candidate 經 `persist_candidate` 寫入。`StorageError::Conflict`
  （同一對同一方法已存在）視為已處理：記一行 log、continue，不讓整次 resolve 失敗。
  其他 storage 錯誤用 `?` 往上傳播。
- `check_normalized_name`／`check_alias`／`check_domain`／`check_semantic_similarity`／
  `check_graph_context`／`check_account_handle` **不寫 store**，只組候選；寫入由
  `resolve_entity`／`resolve_semantic_similarity`／`GraphContextResolver::resolve`
  經共用的 `persist_candidate` 負責。
- `resolve_semantic_similarity` 是獨立入口：先 `get_entity`（找不到同樣回
  `EntityNotFound`），再跑 `check_semantic_similarity`、persist。預留給 Phase 3。
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
check_alias(store, entity)           -> Result<Vec<ResolutionCandidate>, ResolverError>
check_domain(store, entity)          -> Result<Vec<ResolutionCandidate>, ResolverError>
check_account_handle(store, entity)  -> Result<Vec<ResolutionCandidate>, ResolverError>
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

### `account_handle` 規則

只對 `EntityType::Account` 有意義。非 Account 直接回空，**不查 store**。

1. `list_entity_identifiers_by_entity(entity.id, 100)`，只留 namespace 以 `_handle`
   結尾的（`github_handle`／`twitter_handle`／`telegram_handle`）。
2. 對每個 handle 的 `normalized_value` 呼叫
   `find_entity_identifiers_by_normalized_value`（**不限 namespace**）。
3. 丟掉自己、丟掉同 namespace（同平台不是這個方法要抓的訊號；生產路徑上
   UNIQUE 本來就擋得掉，記憶體 double 沒有 UNIQUE，所以顯式排除）、
   丟掉對方 namespace 不是 `_handle` 的列（`email` 碰巧同字串不算帳號）。
4. 同一對因多個平台命中多次時只留第一筆。
5. 命中：`score = 0.35`、`method = "account_handle"`。

0.35 放在 `domain`（0.30）之上、`alias`（0.55）之下：同一個 username 出現在
GitHub 與 Twitter 比「剛好共用一台主機」稍強一點，但仍遠不到能自動合併。
SPEC §6 明文禁止只因同 username 就判定同一真實人物。

evidence：

```json
{
  "method": "account_handle",
  "handle": "alice",
  "entity_a_namespace": "github_handle",
  "entity_b_namespace": "twitter_handle",
  "warning": "SPEC §6 禁止僅依 username 判定同人"
}
```

`entity_a_namespace`／`entity_b_namespace` 對齊排序後的 a／b。

這個方法能做，是因為跨平台 namespace 字串不同，UNIQUE 擋不住
`github_handle=alice` 與 `twitter_handle=alice`。同 namespace 的訊號
（兩個 Entity 都寫 `email=alice@x`）走 `exact_identifier` 寫入衝突，
不要再做一批掃描。

## `email`／`external_id`（刻意不做）

這兩個方法的「兩個 Entity 宣稱同一個值」訊號，早就被 `exact_identifier`
蓋掉：entity-worker 寫 `email`／`cve` namespace 時，撞到
`entity_identifiers` 的 `(namespace, normalized_value)` UNIQUE 就會觸發
衝突候選。做一個批次掃描版本會是第二個舊版 `check_domain` 式的死碼——
同一個 UNIQUE 保證「查自己 identifier 的目前 owner」永遠是自己。

SPEC §6 清單仍保留這兩個名字，crate 不再實作獨立掃描，也不要把「未做」
讀成待辦。

## `semantic_similarity`

```text
check_semantic_similarity(store, embedder, entity, threshold)
  -> Result<Vec<ResolutionCandidate>, ResolverError>
```

同 type 的 Entity 用 embedding cosine 比對。**不寫 candidate 表**；
`resolve_entity` 與獨立入口 `resolve_semantic_similarity` 都用
`SEMANTIC_SIMILARITY_THRESHOLD`（0.85）呼叫。

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
`GraphContextResolver::resolve` 用 `GRAPH_CONTEXT_THRESHOLD`（0.5）呼叫，
**不**由 `resolve_entity` 聚合。

```text
GraphContextResolver<S: RelationalStore, G: GraphStore>
  new(store, graph)
  resolve(entity_id) -> Result<Vec<ResolutionCandidate>, ResolverError>
```

- 先 `get_entity`。找不到回 `ResolverError::EntityNotFound`，與 `resolve_entity` 一致。
- 再 `check_graph_context`，命中的候選經 `persist_candidate` 寫入。
- HTTP 入口是 `POST /api/v1/entities/{id}/resolve/graph-context`。Neo4j 沒接上
  這條路由回 503，訊息會講明這不影響 `POST /entities/{id}/resolve`。

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
- 0.95 正好等於 ADR-012 的預設 `auto_confirm_score`。當 `auto_approval.enabled=true`
  時，`POST /api/v1/entities/{id}/resolve` 的 handler 會額外撈這個 Entity 全部
  Pending 候選——包含 entity-worker 之前寫入的 `exact_identifier` 候選——然後觸發
  高信心自動核准並 merge。功能預設關閉，啟用與操作方式見 `docs/developer/auto-approval.md`。

**entity-worker 是第一個呼叫端。** `upsert_entity` 對 Domain／Ip／Url／Email／CVE
以及 Account（per-platform `{platform}_handle`）寫 `entity_identifiers`，
撞到 `(namespace, normalized_value)` UNIQUE 時呼叫這個
helper 再 `put_resolution_candidate`。這**不是** `ResolverService::resolve_entity`
裡的掃描式方法——掃描式 `exact_identifier` 還沒做，不要把「寫入衝突會產候選」
理解成「resolve_entity 已經會跑 exact identifier」。

## 錯誤

| 變體 | 何時 |
|---|---|
| `ResolverError::Storage` | 底層 `StorageError`（Conflict 在 persist 路徑已被吃掉） |
| `ResolverError::EntityNotFound` | `resolve_entity`／`resolve_semantic_similarity`／`GraphContextResolver::resolve` 的 id 在 store 裡沒有對應列 |

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
- `resolve_entity` 聚合測試注入 `MockEmbeddingProvider::unsupported()`，
  讓 semantic 回空，不干擾 normalized_name／alias 斷言。`graph_context` 已不在
  這條呼叫鏈上。`GraphContextResolver` 與 `resolve_semantic_similarity` 各自有
  獨立測試（菱形圖命中、entity 不存在、空鄰居／unsupported embedder）。

未覆蓋：對真實 Docker Postgres 跑一次 end-to-end 的 Relationship-based `check_domain`。
那要等 Phase 1h 接上事件之後，用 entity-worker 抽出的真實 Entity 再補。
這次 Relationship 設計沒有打過真實 Postgres，靠既有 conformance 保證
`list_relationships_by_object` 的契約。
