# Discovery Worker（V0.3 Phase 3，SPEC §8）

```text
crates/discovery-worker    bin: osint-discovery-worker
```

目前生產路徑只有一種 Job：`discovery_run`。`run_graph_expansion` 會做兩件事，
共用同一個 `max_candidates_per_run` 計數器，整個呼叫只消耗 1 單位
`daily_request_budget`：

1. **Graph Expansion**（圖遍歷）
2. **Source Expansion**（關聯式查詢，Source 不在 Neo4j 圖裡）

## Graph Expansion

對來源 Entity 做 Neo4j 1-hop 鄰居查詢，把符合類型過濾的鄰居寫成
`Candidate`（`candidate_type: Entity`，`status: Pending`）＋
`CandidateEvidence`。

常數：

| 名稱 | 值 | 意義 |
|---|---|---|
| `DISCOVERY_METHOD_GRAPH_EXPANSION` | `"graph_expansion"` | 對齊 `core_model::DISCOVERY_METHODS` |
| `GRAPH_EXPANSION_SCORE` | `0.4` | 弱訊號：圖上有邊 ≠ 跟這次調查相關 |
| `GRAPH_EXPANSION_ENTITY_TYPES` | `vulnerability`／`repository`／`software`／`product` | 寫死在 worker 內部，**不**從 job parameters 讀、不對外開 API |

過濾在 storage 層做完（`GraphTraversalOptions.entity_types`）。字串必須是
serde snake_case（`"vulnerability"` 不是 `"Vulnerability"`）——Neo4j 端是
`IN $entityTypes` 精確比對，傳錯格式會靜默回空，不會報錯。

`software` 與 `product` 都保留，因為目前無法斷定使用者輸入的是哪種語意。
`Source` 不在這個清單裡：它根本不在 Neo4j 圖投影（每條 Cypher 寫死
`:Entity` label），走下面的關聯式路徑。

## Source Expansion

Source 不投影進 Neo4j。回答「這個 entity 關聯哪些情報來源」的唯一路徑是
三跳關聯式查詢：

```text
entity_extractions.entity_id
  → entity_extractions.object_id = provenance.subject_id
    AND provenance.action = 'derived_from'
  → provenance.raw_evidence_id = raw_evidence.id
  → raw_evidence.source_id
```

`RelationalStore::list_sources_by_entity` 在 SQL 層 `DISTINCT source_id`
（不在 Rust 層去重，否則 `limit` 會在去重前截斷）。`limit` 夾在 1..=100。
回傳 `Vec<SourceId>`，詳情再呼叫既有的 `get_source()`。

產出的 Candidate：

| 欄位 | 值 |
|---|---|
| `candidate_type` | `CandidateType::Source` |
| `discovery_method` | `DISCOVERY_METHOD_SOURCE_EXPANSION`（`"source_expansion"`，對齊 `DISCOVERY_METHODS` 既有參考值） |
| `value`／`normalized_value` | Source 的 `name`（`.trim().to_lowercase()`） |
| `confidence`／`score` | `SOURCE_EXPANSION_SCORE`（`0.4`） |
| `entity_id`（evidence） | 來源 Entity 的 id |
| `reason` | 自由文字說明三跳路徑 |

`SOURCE_EXPANSION_SCORE` 與 `GRAPH_EXPANSION_SCORE` 數值相同但**獨立宣告**：
關聯式查詢 vs 圖遍歷語意不同，方便未來各自調整。

這些 Candidate 與 Graph Expansion 產出的 Candidate **共用同一個 `written`
計數器**（對 `budget.max_candidates_per_run` 截斷），不另外開配額、也不
額外消耗 `daily_request_budget`。

## 已知限制

- **沒有查重**：同一個 Entity 對同一個 collection 重複跑會產生重複的
  Candidate 列。刻意先不做。
- 觸發點是 Entity（`POST /entities/{id}/discover` 或帶 `entity_id` 的
  `discovery_run` Job），不是 Seed。組織 seed 要先被解析成 Entity 才能走
  這條路。
- 類型過濾不對外開參數；要改目標類型只能改 worker 內部常數。
