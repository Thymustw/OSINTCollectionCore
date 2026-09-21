# AI 輔助自動核准（ADR-012）

完整設計決策與例外理由見 `docs/adr/ADR-012-ai-assisted-auto-approval.md`。
本文件是操作指南，著重在「如何設定、如何查證、如何恢復」，不重複 ADR 的完整論述。

```text
功能範圍：crates/resolver/src/auto_approval.rs（AutoApprovalEvaluator、AutoApprovalConfig、AutoApprovalOutcome）
          crates/ai-gateway/（LlmProvider trait、OpenAiCompatibleLlmProvider、MockLlmProvider、UnsupportedLlmProvider）
          crates/core-api/src/resources/merge.rs（整合進 POST /api/v1/entities/{id}/resolve）
```

## 這是什麼

`POST /api/v1/entities/{id}/resolve` 執行五個 Postgres-only 掃描方法後，多一步：
評估目標 Entity 全部 Pending 候選（含這次新寫入的、以及之前已存在的），
高信心的直接自動 merge，中間帶的交給本地 Qwen LLM 判斷。

**功能預設關閉**（`[auto_approval].enabled = false`）。關閉時行為與 ADR-012 之前完全一致——
所有候選維持 `Pending`，不做任何自動 merge。

這是對 `CLAUDE.md §5`「AI output cannot directly override policy/identity decisions」的
**刻意例外**——理由、範圍、安全網記錄在 ADR-012，不在這裡重複。

## 三段信心判斷

`AutoApprovalEvaluator::evaluate_pair` 的實際執行順序如下：

```
候選群組（同一對 Entity、可能多筆不同 method）
   ↓
取分數最高一筆為代表（best）
   ↓
┌─ best.score < llm_review_score（預設 0.70）
│     → Pending（不查 Entity、不打 LLM，最快回傳路徑）
│
├─ best.score 在 [llm_review_score, auto_confirm_score) 中間帶
│   ├─ 跨 entity_type → Pending（execute_merge 本身也會擋，這裡先防禦）
│   ├─ 讀取 Entity 失敗 → Pending
│   ├─ 呼叫 LLM（若 llm.enabled = false → provider 直接回 Unsupported → Pending）
│   │   ├─ LLM 失敗（Transient/Permanent/InvalidResponse/Unsupported）→ Pending
│   │   ├─ LLM 回應解析失敗（same_entity 欄位缺失或非 bool）→ Pending
│   │   ├─ LLM 判定 same_entity = false → Pending
│   │   └─ LLM 判定 same_entity = true → 走 commit_auto_merge
│
└─ best.score >= auto_confirm_score（預設 0.95）
    ├─ 跨 entity_type → Pending
    ├─ 讀取 Entity 失敗 → Pending
    └─ 不問 LLM → 直接走 commit_auto_merge
```

`commit_auto_merge` 成功後：
1. 呼叫 `execute_merge_with_audit`（寫 `merge_history`，含 `auto_approval_audit` JSON）
2. 把這對 Entity 的所有 Pending 候選標為 `AutoConfirmed`（`update_resolution_candidate_status`）
3. 回傳 `AutoApprovalOutcome::Merged { merge_history_id }`

任何步驟失敗（包含 merge 本身失敗）都收斂成 `AutoApprovalOutcome::Pending`，不往上拋錯誤。
這是刻意設計：`CLAUDE.md §5`「AI failure must not block base ingestion」。

### 候選分組邏輯

`maybe_evaluate_auto_approval` 呼叫 `group_candidates_for_auto_approval`：

- 把目標 Entity id 視為 survivor，另一端視為 merged。
- 依「另一端 Entity id」分組（同一對可能有多個 method 各一筆候選）。
- 去重（同 candidate id 只留一筆）。
- 依「另一端 id」升序排列，讓分組順序是決定性的
  （否則 `max_auto_merges_per_resolve` 上限砍到哪幾對會不確定）。

每組呼叫一次 `evaluate_pair`，直到 `auto_merged_pairs >= max_auto_merges_per_resolve` 才停。

### LLM 回應解析

LLM 預期回傳以下格式的 JSON：

```json
{"same_entity": true, "reasoning": "..."}
```

`parse_llm_verdict` 容錯處理：
- 去掉 ` ```json ... ``` ` markdown fence（大小寫都接受）
- 找第一個平衡的 `{...}` JSON object（允許前後有說明文字）
- 只讀 `same_entity` 欄位（布林值），忽略其他欄位

解析失敗（欄位缺失、型別不對、不是合法 JSON）→ `Pending`，不是錯誤。

**注意**：解析邏輯對真實 vLLM／llama.cpp 的回應格式**尚未用真實服務實測過**，
只用 wiremock 假 server 驗過（`openai.rs` 說明這一點）。

## Config 欄位說明

### `[auto_approval]`

| 欄位 | 預設值 | 意義 |
|---|---|---|
| `enabled` | `false` | 主開關。`false` 時此 section 其他所有欄位都不生效 |
| `auto_confirm_score` | `0.95` | ⚠️ 分數 ≥ 此值直接自動核准，不問 LLM。⚠️ 未驗證，見下方警告 |
| `llm_review_score` | `0.70` | ⚠️ 分數在 `[llm_review_score, auto_confirm_score)` 送 LLM。⚠️ 未驗證，見下方警告 |
| `max_auto_merges_per_resolve` | `3` | 單次 `resolve_entity` 呼叫最多觸發幾筆自動 merge，防止連環效應 |
| `survivor_strategy` | `"source"` | 自動核准後的 survivor 選擇策略。V0.2 只有 `"source"`（呼叫 `resolve_entity` 的目標 Entity 存活） |

### `[auto_approval.llm]`

| 欄位 | 預設值 | 意義 |
|---|---|---|
| `enabled` | `false` | LLM 中間帶審查開關。即使 `auto_approval.enabled = true`，也必須此處為 `true` 才打 LLM |
| `base_url` | `"http://ai-inference:8000/v1"` | OpenAI 相容 endpoint base URL（實際打 `{base_url}/chat/completions`，會去掉尾端 `/`） |
| `model` | `"qwen-primary"` | 對應 `docs/architecture/LOCAL_AI.md` 的 alias `qwen-primary`（Qwen3.8-27B UD-Q4_K_XL） |
| `timeout_secs` | `30` | 單次 HTTP 請求逾時（含連線 + 讀 body） |
| `max_retries` | `0` | 逾時後重試次數。預設 0：LLM 失敗直接退回 Pending，不重試（降級路徑本來就安全） |
| `max_concurrent` | `2` | 同時進行的 LLM 推論上限（bounded Semaphore，CLAUDE.md §6） |
| `max_tokens` | `512` | LLM 回應的 token 上限 |
| `temperature` | `0.0` | 0.0 = 確定性輸出，適合判斷任務 |
| `enable_reasoning` | `false` | 是否允許模型展開推理過程。`false` 時 `ai-gateway` 會在請求加 `chat_template_kwargs.enable_thinking=false`，關閉 chain-of-thought（更快、省 token）。`true` 時不加這個欄位，沿用 endpoint 預設。這是呼叫端明確宣告，不是依 `task_type` 自動推導——entity resolution 的二元判斷輸入已結構化，預設關閉 |

### 門檻自洽檢查

啟動時 `core-api` 呼叫 `AutoApprovalSection::thresholds_are_sane()`
（即 `auto_confirm_score >= llm_review_score`）：

- **不自洽**：記 `tracing::error!` 並強制停用自動核准（`effectively_enabled = false`），
  候選全部維持 Pending，不讓錯誤設定靜默生效。
  錯誤訊息原文：`auto_approval.enabled=true 但門檻不自洽（auto_confirm_score 應該 >= llm_review_score），已強制停用自動核准，所有候選維持 Pending`
- **自洽且啟用**：記 `tracing::warn!`，訊息帶 `auto_confirm_score`、`llm_review_score`、
  `llm_enabled` 欄位，提醒門檻未驗證。
  啟動警告原文：`AI 輔助自動核准已啟用（ADR-012）。門檻未經真實 OSINT 語料驗證，見 docs/adr/ADR-012-ai-assisted-auto-approval.md。建議上線初期定期抽查 merge_history WHERE auto_approval_audit IS NOT NULL`

## ⚠️ 門檻未經真實語料驗證

> **`auto_confirm_score = 0.95` 與 `llm_review_score = 0.70` 是根據現有分數分布的推論，
> 不是用真實 OSINT Entity pair 實測過的數字。**

這與 `docs/developer/deduplicator.md` 的 `similarity_threshold = 0.90` 是同一類問題，
但本功能的風險更高——誤判是破壞性的：**自動 merge 會立刻改寫資料**，
不像 deduplicator 的「標記重複」是可查看後再決定的標記。

在拿真實資料驗證 false positive rate 之前，**不建議在生產環境把 `enabled = true`**。

此外，不同 resolver 方法的分數尺度不可直接比較：`semantic_similarity` 的 0.70
跟 `alias` 的 0.70 可靠度不一定相同，全域門檻把兩者一視同仁。未來可能需要
per-method 門檻（ADR-012 Consequences 已記錄這個風險）。

## 啟用方式

最小設定（只開高信心路徑，不打 LLM）：

```toml
[auto_approval]
enabled = true
# auto_confirm_score = 0.95  # 預設值，目前只有 exact_identifier 能達到
```

完整設定（含 LLM 中間帶審查）：

```toml
[auto_approval]
enabled = true
auto_confirm_score = 0.95
llm_review_score = 0.70
max_auto_merges_per_resolve = 3

[auto_approval.llm]
enabled = true
base_url = "http://ai-inference:8000/v1"
model = "qwen-primary"
timeout_secs = 30
max_retries = 0
max_concurrent = 2
max_tokens = 512
temperature = 0.0
enable_reasoning = false
```

完全關閉（恢復 ADR-012 之前行為）：

```toml
[auto_approval]
enabled = false
```

## 監控與稽核

### 稽核日誌

每次 `POST /api/v1/entities/{id}/resolve` 成功時，稽核 `metadata` 含：

```json
{
  "candidate_count": 3,
  "auto_merged_pairs": 1,
  "auto_merged_history_ids": ["<uuid>"]
}
```

`auto_merged_pairs = 0` 代表這次沒有觸發自動核准（可能是功能關閉，
或候選分數低於 `llm_review_score`）。

### merge_history 查詢

查所有自動 merge：

```sql
SELECT id, survivor_id, merged_id, created_at, operator, auto_approval_audit
FROM merge_history
WHERE auto_approval_audit IS NOT NULL
ORDER BY created_at DESC;
```

`operator` 欄位的值：

| 值 | 來源 |
|---|---|
| JWT subject（例如 `"alice@example.com"`） | 人工 merge（`POST /api/v1/entities/merge`） |
| `"resolver:auto_confirm"` | `POST /api/v1/entities/{id}/resolve` 觸發的自動核准 |
| `"stix_import:auto_confirm"` | 未來 STIX 匯入路徑（V0.2 Phase 4 規劃中）觸發 |

⚠️ **自動核准的 `operator` 不是 JWT subject**，在稽核查詢中要特別注意：
「誰批准了這次 merge」的答案是「系統依設定的門檻自動決定」，不是某個登入使用者。

### `auto_approval_audit` JSON schema（version 1）

```json
{
  "version": 1,
  "source": "resolver",
  "decision_path": "high_confidence",
  "best_method": "exact_identifier",
  "best_score": 0.95,
  "all_candidate_scores": [
    {"method": "exact_identifier", "score": 0.95},
    {"method": "alias", "score": 0.55}
  ],
  "thresholds_used": {
    "auto_confirm_score": 0.95,
    "llm_review_score": 0.70
  },
  "llm": null,
  "decided_at": "2026-09-17T12:00:00Z"
}
```

`decision_path` 的可能值：`"high_confidence"`（分數路徑）、`"llm_review"`（LLM 判斷路徑）。

走 LLM 路徑時 `llm` 欄位為 object：

```json
"llm": {
  "model": "qwen-primary",
  "system_prompt_hash": "<sha256-hex>",
  "user_prompt": "Survivor:\n- name: ...\n...",
  "raw_response": "{\"same_entity\": true, \"reasoning\": \"...\"}",
  "parsed_same_entity": true,
  "latency_ms": 312
}
```

高信心路徑的 `llm` 是 `null`（未呼叫 LLM）。

### Metrics

ADR-012 記錄的 metric 名稱（目前為規格，實作在後續版本補上）：

- `auto_approval_merges_total{source, decision_path, method}`
- `auto_approval_llm_calls_total{outcome}`

自動核准合併筆數異常飆高時應觸發告警（ADR-012 建議：超過滾動平均 2 倍）。

## 出錯時的復原

自動 merge 完全沿用既有 `undo_merge`（`POST /api/v1/merge-history/{id}/undo`）：

1. 從稽核查詢找到問題的 `merge_history_id`
2. 呼叫 undo：

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/merge-history/<id>/undo \
  -H "Authorization: Bearer $TOKEN"
```

沒有另外為自動核准設計的復原機制。既有 `undo_merge` 的限制同樣適用（見 `docs/developer/merge.md`）：
自迴圈的 relationship evidence 無法復原；survivor 之後又被 merge 掉時要先 undo 那一次。

注意：**復原的前提是有人發現問題**。建議上線初期定期人工抽查
`merge_history WHERE auto_approval_audit IS NOT NULL`，核對判斷品質。

## 已知限制

1. **只有 `exact_identifier` 能觸發高信心路徑（預設門檻 0.95）。**
   `resolve_entity` 自己掃描的五個方法分數上限是 0.55（`alias`）。
   `exact_identifier` 不在 `resolve_entity` 的掃描鏈裡——它是寫入衝突副產品，
   由 entity-worker 在 `put_entity_identifier` 撞到 UNIQUE 時寫入。
   因此 `resolve_entity` 呼叫之後，handler 會額外撈這個 Entity 全部 Pending 候選
   （包含之前由 entity-worker 寫入的 `exact_identifier` 候選），才有機會觸發高信心路徑。
   這是刻意設計，測試 `resolve_auto_confirms_preseeded_exact_identifier` 驗證了這個行為。

2. **`graph_context` 候選不進自動核准。**
   `graph_context` 走獨立的 `GraphContextResolver`，不在 `resolve_entity` 的聚合鏈裡，
   其 score 範圍（Jaccard × 0.7，多落在 0.35 上下）也低於 `llm_review_score`，
   V0.2 現階段不納入。

3. **跨 `entity_type` 候選一律不評估。**
   `normalized_name` 的候選多為跨 type（score = 0.40），在 `evaluate_pair` 讀到
   Entity 後若 type 不同立刻回 Pending（`execute_merge` 本身也會以 `TypeMismatch` 擋住）。

4. **LLM 格式假設未對真實 vLLM／llama.cpp 實測。**
   請求組裝與回應解析依 OpenAI Chat Completions 公開格式；若真實 serving 路徑
   回的是多段 `content` 陣列或其他變體，會被當成 `AiGatewayError::InvalidResponse`
   並退回 Pending（不會出錯、但 LLM 永遠幫不上忙）。
   詳見 `crates/ai-gateway/src/openai.rs` 的模組說明。

5. **`max_auto_merges_per_resolve` 不是全域速率限制。**
   它限制的是單次 `POST /entities/{id}/resolve` 呼叫的自動 merge 數量上限，
   不是「系統每分鐘最多自動 merge 多少筆」。高頻率呼叫時仍可能產生大量自動 merge。

6. **門檻數字未驗證（見上方警告）。**
   `auto_confirm_score = 0.95` 與 `llm_review_score = 0.70` 均為推論值。

## 相關文件

- `docs/adr/ADR-012-ai-assisted-auto-approval.md`（設計決策、例外理由、完整風險評估）
- `docs/developer/merge.md`（`execute_merge`／`undo_merge` 的完整說明）
- `docs/developer/resolver.md`（五個掃描方法、`exact_identifier` 衝突 helper）
- `docs/developer/schema-v0.2.md`（migration 0012、`auto_approval_audit` 欄位）
- `docs/developer/storage-adapters.md`（`update_resolution_candidate_status`）
- `docs/architecture/LOCAL_AI.md`（Qwen3.8-27B UD-Q4_K_XL 執行環境）
