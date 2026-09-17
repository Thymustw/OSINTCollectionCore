//! 單一一對 Entity 的自動核准評估（ADR-012 Step 3），以及依另一端 id 分組候選。
//!
//! [`AutoApprovalEvaluator`] 只評估一對、必要時執行一次 merge；跨多對的計數上限
//! `max_auto_merges_per_resolve` 仍是呼叫端的狀態。分組規則抽成
//! [`group_candidates_for_auto_approval`]，讓 `core-api` 與 `stix-worker` 共用同一份，
//! 避免兩邊各寫一份之後行為分岔。
//!
//! 任何失敗（LLM、merge、讀不到 Entity）都收斂成 [`AutoApprovalOutcome::Pending`]，
//! 不往上拋錯誤：CLAUDE.md §5「AI failure must not block base ingestion」。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use ai_gateway::{
    AiGatewayError, ChatCompletionRequest, ChatCompletionResponse, ChatMessage, ChatRole,
    LlmProvider, Pricing, redact_credentials,
};
use chrono::Utc;
use core_events::EventProducer;
use core_model::{AiRun, Entity, EntityId, MergeHistoryId, ResolutionCandidate, ResolutionStatus};
use merge::MergeService;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use storage_core::TransactionalStore;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// `AutoApprovalEvaluator` 的門檻設定。呼叫端（Step 4 的 `core-api` 組裝）
/// 從 `core_config::AutoApprovalSection` 轉過來。
///
/// **不含 LLM 開關**——中間帶要不要真的打 LLM，完全由注入的 `L: LlmProvider`
/// 實例自己決定（`OpenAiCompatibleLlmProvider::new(cfg)` 在 `cfg.enabled == false`
/// 時已經會讓每次呼叫直接回 `AiGatewayError::Unsupported`）。評估器一律嘗試呼叫，
/// 讓 provider 自己短路，不在這裡重複一份「要不要打 LLM」的判斷——單一事實來源。
#[derive(Debug, Clone, PartialEq)]
pub struct AutoApprovalConfig {
    pub enabled: bool,
    pub auto_confirm_score: f64,
    pub llm_review_score: f64,
    /// LLM prompt 用的模型名稱／temperature／max_tokens——這些不是連線參數
    /// （那些在 `OpenAiCompatibleLlmProviderConfig`），是每次請求要填的欄位。
    pub llm_model: String,
    /// 寫進 `AiRun.model_version`。見 `AutoApprovalLlmSection::model_version`
    /// 的 doc comment——目前沒有真的在跑的推論服務，預設 `"unknown"`。
    pub llm_model_version: String,
    pub llm_temperature: f64,
    pub llm_max_tokens: u32,
}

/// 單一一對 Entity 的自動核准評估結果。**沒有 `Result` 包裝**——
/// 任何失敗模式（LLM 失敗、merge 失敗、讀不到 Entity）都收斂成 `Pending`，
/// 這是刻意的：CLAUDE.md §5「AI failure must not block base ingestion」，
/// 呼叫端（Step 4）不需要處理錯誤分支，一律拿到明確結果。
#[derive(Debug, Clone, PartialEq)]
pub enum AutoApprovalOutcome {
    /// 已自動核准並完成 merge。
    Merged { merge_history_id: MergeHistoryId },
    /// 維持 Pending，`reason` 給 tracing／稽核用，不是使用者文案。
    Pending { reason: String },
    /// `AutoApprovalConfig.enabled == false`，整個機制關閉。
    Disabled,
}

/// 評估一對 Entity 是否該自動核准並 merge。
pub struct AutoApprovalEvaluator<S, L>
where
    S: TransactionalStore + Clone,
    L: LlmProvider,
{
    store: S,
    merge: MergeService<S>,
    llm: L,
    config: AutoApprovalConfig,
}

const SYSTEM_PROMPT: &str = "You are an OSINT entity resolution expert. Determine whether the two \
entities below refer to the same real-world entity. Reply with JSON only: \
{\"same_entity\": true or false, \"reasoning\": \"...\"}";

const DESCRIPTION_CHAR_LIMIT: usize = 500;
const PROMPT_LIST_LIMIT: u32 = 10;

/// `SYSTEM_PROMPT` 的版本號。**改動 `SYSTEM_PROMPT` 的文字內容時要一起把這個
/// 值改掉**（例如 `"v2"`），`AiRun.prompt_version` 才能區分「這筆判斷用的是
/// 哪一版 prompt」——否則歷史紀錄裡舊版判斷跟新版判斷的 prompt_version 會混在
/// 一起看不出差異。
const PROMPT_VERSION: &str = "v1";
/// `core_model::AI_TASK_TYPES` 裡沒有專門的「entity resolution」task type，
/// `classification`（二元判斷 same_entity 是/否）語意最接近，不自己發明
/// 第九種——比照 `AI_TASK_TYPES` 檔頭「參考清單，不是白名單」的既有原則。
const AI_TASK_TYPE: &str = "classification";
/// 對應 `docs/architecture/LOCAL_AI.md` §15 的 `provider = local`——這裡呼叫的
/// 是自架的 OpenAI 相容 endpoint，不是雲端供應商。
const AI_PROVIDER: &str = "local";

impl<S, L> AutoApprovalEvaluator<S, L>
where
    S: TransactionalStore + Clone,
    L: LlmProvider,
{
    #[must_use]
    pub fn new(
        store: S,
        producer: Option<Arc<EventProducer>>,
        llm: L,
        config: AutoApprovalConfig,
    ) -> Self {
        let merge = MergeService::new(store.clone(), producer);
        Self {
            store,
            merge,
            llm,
            config,
        }
    }

    /// 評估一對 Entity 的所有候選（同一對、可能多種 method 各一筆），必要時
    /// 自動核准並執行 merge。
    ///
    /// - `survivor_id`／`merged_id`：呼叫端已經決定好哪個存活、哪個被併（Step 4
    ///   的 `survivor_strategy` 職責，這裡不重新判斷）。
    /// - `candidates`：這對 Entity 的所有 [`ResolutionCandidate`]（可能橫跨多個
    ///   method），只取分數最高的一筆當代表。
    /// - `source`：稽核用的來源標記，例如 `"resolver"`／`"stix_import"`，決定
    ///   `MergeHistory.operator` 寫成 `"{source}:auto_confirm"`。
    pub async fn evaluate_pair(
        &self,
        survivor_id: EntityId,
        merged_id: EntityId,
        candidates: &[ResolutionCandidate],
        source: &str,
    ) -> AutoApprovalOutcome {
        // 1. 機制關閉時立刻回，不做任何 I/O——預設關閉路徑必須是零成本的。
        if !self.config.enabled {
            return AutoApprovalOutcome::Disabled;
        }

        // 2. 空切片是防禦性檢查：理論上呼叫端不會傳空的，但不要 panic。
        if candidates.is_empty() {
            return AutoApprovalOutcome::Pending {
                reason: "沒有候選可評估".into(),
            };
        }

        // 3. 取分數最高的一筆當代表。用 `total_cmp` 而不是 `partial_cmp().unwrap()`：
        //    分數理論上不會是 NaN，但會 panic 的寫法是隱患。
        let best = candidates
            .iter()
            .max_by(|a, b| a.score.total_cmp(&b.score))
            .expect("candidates 非空，max_by 必有值");

        // 4. 低於中間帶門檻：絕大多數情況。不查 Entity、不打 LLM，盡快回 Pending。
        if best.score < self.config.llm_review_score {
            return AutoApprovalOutcome::Pending {
                reason: format!(
                    "分數 {} 低於 llm_review_score {}",
                    best.score, self.config.llm_review_score
                ),
            };
        }

        // 5. 中間帶／高信心才需要 Entity 內容。任一端讀不到 → Pending，不往上拋。
        let survivor = match self.load_entity(survivor_id).await {
            Some(entity) => entity,
            None => {
                return AutoApprovalOutcome::Pending {
                    reason: format!("找不到 survivor Entity `{survivor_id}`"),
                };
            }
        };
        let merged = match self.load_entity(merged_id).await {
            Some(entity) => entity,
            None => {
                return AutoApprovalOutcome::Pending {
                    reason: format!("找不到 merged Entity `{merged_id}`"),
                };
            }
        };

        // 6. 跨 type 不該進自動核准（`execute_merge` 也會擋），這裡先防禦。
        if survivor.entity_type != merged.entity_type {
            warn!(
                survivor_type = ?survivor.entity_type,
                merged_type = ?merged.entity_type,
                %survivor_id,
                %merged_id,
                "自動核准收到跨 type 候選，理論上呼叫端不該送來"
            );
            return AutoApprovalOutcome::Pending {
                reason: format!(
                    "entity_type 不同：survivor={:?} merged={:?}",
                    survivor.entity_type, merged.entity_type
                ),
            };
        }

        // 7. 高信心路徑：分數已過 auto_confirm，不再問 LLM。
        if best.score >= self.config.auto_confirm_score {
            let audit = self.build_audit(source, "high_confidence", best, candidates, None);
            return self
                .commit_auto_merge(survivor_id, merged_id, source, best, audit, candidates)
                .await;
        }

        // 8. 中間帶：llm_review_score <= score < auto_confirm_score，問 LLM。
        //    評估器一律嘗試呼叫，讓 provider 自己決定要不要真的打模型。
        let user_prompt = self.build_user_prompt(&survivor, &merged, best).await;
        let redacted_user_prompt = redact_credentials(&user_prompt);
        let request = ChatCompletionRequest {
            model: self.config.llm_model.clone(),
            messages: vec![
                ChatMessage {
                    role: ChatRole::System,
                    content: SYSTEM_PROMPT.to_string(),
                },
                ChatMessage {
                    role: ChatRole::User,
                    content: user_prompt.clone(),
                },
            ],
            temperature: self.config.llm_temperature,
            max_tokens: self.config.llm_max_tokens,
        };

        let started = Instant::now();
        let call_result = self.llm.chat_completion(&request).await;
        let latency_ms = started.elapsed().as_millis() as i64;

        // 9. 不管結果如何都留一筆 AiRun——這是這次改動要補的最大缺口：
        //    之前只有「LLM 判 true 且 merge 成功」這條路徑會留下任何持久化
        //    紀錄，其餘（判 false／解析失敗／呼叫失敗／未啟用）完全查不到
        //    AI 到底看過什麼、判斷結果是什麼。寫入失敗只記 error，不影響
        //    本次判斷（可觀測性本身的失敗不該擋住主流程，同 CLAUDE.md §5）。
        self.persist_ai_run(
            &request,
            &call_result,
            &redacted_user_prompt,
            best,
            latency_ms,
        )
        .await;

        match call_result {
            Ok(response) => match parse_llm_verdict(&response.content) {
                Some(true) => {
                    let redacted_response = redact_credentials(&response.content);
                    let llm = json!({
                        "model": response.model,
                        "system_prompt_hash": sha256_hex(SYSTEM_PROMPT),
                        "prompt_version": PROMPT_VERSION,
                        "user_prompt": redacted_user_prompt,
                        "raw_response": redacted_response,
                        "parsed_same_entity": true,
                        "latency_ms": latency_ms,
                    });
                    let audit = self.build_audit(source, "llm_review", best, candidates, Some(llm));
                    self.commit_auto_merge(survivor_id, merged_id, source, best, audit, candidates)
                        .await
                }
                Some(false) => {
                    // 正常判斷結果，不是錯誤——用 info 而不是 warn。
                    info!(
                        %survivor_id,
                        %merged_id,
                        method = %best.method,
                        score = best.score,
                        "LLM 判定不是同一實體，維持 Pending"
                    );
                    AutoApprovalOutcome::Pending {
                        reason: "LLM 判定不是同一實體".into(),
                    }
                }
                None => {
                    info!(
                        %survivor_id,
                        %merged_id,
                        method = %best.method,
                        raw_response = %redact_credentials(&response.content),
                        "LLM 回應無法解析 same_entity，維持 Pending"
                    );
                    AutoApprovalOutcome::Pending {
                        reason: "LLM 回應無法解析 same_entity".into(),
                    }
                }
            },
            Err(AiGatewayError::Unsupported) => {
                debug!(
                    %survivor_id,
                    %merged_id,
                    "LLM 未啟用，中間帶維持 Pending"
                );
                AutoApprovalOutcome::Pending {
                    reason: "LLM 未啟用".into(),
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %survivor_id,
                    %merged_id,
                    "LLM 呼叫失敗，維持 Pending"
                );
                AutoApprovalOutcome::Pending {
                    reason: format!("LLM 呼叫失敗：{err}"),
                }
            }
        }
    }

    /// 把這次 LLM 呼叫的完整脈絡寫成一筆 [`AiRun`]。見第 9 步的呼叫點註解。
    async fn persist_ai_run(
        &self,
        request: &ChatCompletionRequest,
        result: &Result<ChatCompletionResponse, AiGatewayError>,
        redacted_user_prompt: &str,
        best: &ResolutionCandidate,
        latency_ms: i64,
    ) {
        let run = build_ai_run(
            &self.config.llm_model_version,
            request,
            result,
            redacted_user_prompt,
            best,
            latency_ms,
        );
        if let Err(err) = self.store.put_ai_run(&run).await {
            error!(
                error = %err,
                ai_run_id = %run.id,
                "寫入 AiRun 失敗（不影響本次自動核准判斷，只損失這筆可觀測性紀錄）"
            );
        }
    }

    /// 讀 Entity；失敗或沒有這一列都記 warn 後回 `None`，讓呼叫端收斂成 Pending。
    async fn load_entity(&self, entity_id: EntityId) -> Option<Entity> {
        match self.store.get_entity(entity_id).await {
            Ok(Some(entity)) => Some(entity),
            Ok(None) => {
                warn!(%entity_id, "自動核准讀不到 Entity");
                None
            }
            Err(err) => {
                warn!(error = %err, %entity_id, "自動核准讀取 Entity 失敗");
                None
            }
        }
    }

    async fn commit_auto_merge(
        &self,
        survivor_id: EntityId,
        merged_id: EntityId,
        source: &str,
        best: &ResolutionCandidate,
        audit: Value,
        candidates: &[ResolutionCandidate],
    ) -> AutoApprovalOutcome {
        let reason = format!(
            "自動核准（{source}）：method={} score={}",
            best.method, best.score
        );
        let operator = format!("{source}:auto_confirm");
        match self
            .merge
            .execute_merge_with_audit(survivor_id, merged_id, reason, operator, Some(audit))
            .await
        {
            Ok(history) => {
                // 9. merge 已 commit。同一對可能有多個 method 各一筆，全部標 AutoConfirmed。
                //    這一步失敗只記 error（見 ADR-012：狀態沒同步是可事後修復的不一致），
                //    仍然回 Merged。
                self.mark_candidates_auto_confirmed(candidates).await;
                AutoApprovalOutcome::Merged {
                    merge_history_id: history.id,
                }
            }
            Err(err) => {
                warn!(
                    error = %err,
                    %survivor_id,
                    %merged_id,
                    "自動核准 merge 失敗，維持 Pending"
                );
                AutoApprovalOutcome::Pending {
                    reason: format!("merge 失敗：{err}"),
                }
            }
        }
    }

    async fn mark_candidates_auto_confirmed(&self, candidates: &[ResolutionCandidate]) {
        let reviewed_at = Utc::now();
        for candidate in candidates {
            match self
                .store
                .update_resolution_candidate_status(
                    candidate.id,
                    ResolutionStatus::AutoConfirmed,
                    reviewed_at,
                )
                .await
            {
                Ok(true) => {}
                Ok(false) => {
                    error!(
                        candidate_id = %candidate.id,
                        "自動核准後更新 candidate 狀態找不到該列（merge 已 commit，可事後修復）"
                    );
                }
                Err(err) => {
                    error!(
                        error = %err,
                        candidate_id = %candidate.id,
                        "自動核准後更新 candidate 狀態失敗（merge 已 commit，可事後修復）"
                    );
                }
            }
        }
    }

    fn build_audit(
        &self,
        source: &str,
        decision_path: &str,
        best: &ResolutionCandidate,
        candidates: &[ResolutionCandidate],
        llm: Option<Value>,
    ) -> Value {
        let all_candidate_scores: Vec<Value> = candidates
            .iter()
            .map(|c| {
                json!({
                    "method": c.method,
                    "score": c.score,
                })
            })
            .collect();
        json!({
            "version": 1,
            "source": source,
            "decision_path": decision_path,
            "best_method": best.method,
            "best_score": best.score,
            "all_candidate_scores": all_candidate_scores,
            "thresholds_used": {
                "auto_confirm_score": self.config.auto_confirm_score,
                "llm_review_score": self.config.llm_review_score,
            },
            "llm": llm,
            "decided_at": Utc::now().to_rfc3339(),
        })
    }

    async fn build_user_prompt(
        &self,
        survivor: &Entity,
        merged: &Entity,
        best: &ResolutionCandidate,
    ) -> String {
        let survivor_block = self.format_entity_block("Survivor", survivor).await;
        let merged_block = self.format_entity_block("Merged", merged).await;
        format!(
            "{survivor_block}\n\n{merged_block}\n\n\
Triggering candidate:\n\
- method: {}\n\
- score: {}\n\
- evidence: {}",
            best.method, best.score, best.evidence,
        )
    }

    async fn format_entity_block(&self, label: &str, entity: &Entity) -> String {
        let description = entity
            .description
            .as_deref()
            .map(|d| truncate_chars(d, DESCRIPTION_CHAR_LIMIT))
            .unwrap_or_else(|| "(none)".into());
        let aliases = self.load_alias_texts(entity.id).await;
        let identifiers = self.load_identifier_texts(entity.id).await;
        format!(
            "{label}:\n\
- name: {}\n\
- entity_type: {:?}\n\
- description: {description}\n\
- aliases: {aliases}\n\
- identifiers: {identifiers}",
            entity.name, entity.entity_type,
        )
    }

    /// 讀取失敗就跳過，不讓 alias 查詢拖垮整條中間帶路徑。
    async fn load_alias_texts(&self, entity_id: EntityId) -> String {
        match self
            .store
            .list_entity_aliases_by_entity(entity_id, PROMPT_LIST_LIMIT)
            .await
        {
            Ok(rows) if rows.is_empty() => "(none)".into(),
            Ok(rows) => rows
                .into_iter()
                .map(|row| row.alias)
                .collect::<Vec<_>>()
                .join(", "),
            Err(err) => {
                warn!(
                    error = %err,
                    %entity_id,
                    "組 LLM prompt 時讀 alias 失敗，略過此欄"
                );
                "(unavailable)".into()
            }
        }
    }

    /// 讀取失敗就跳過，理由同 [`Self::load_alias_texts`]。
    async fn load_identifier_texts(&self, entity_id: EntityId) -> String {
        match self
            .store
            .list_entity_identifiers_by_entity(entity_id, PROMPT_LIST_LIMIT)
            .await
        {
            Ok(rows) if rows.is_empty() => "(none)".into(),
            Ok(rows) => rows
                .into_iter()
                .map(|row| format!("{}={}", row.namespace, row.value))
                .collect::<Vec<_>>()
                .join(", "),
            Err(err) => {
                warn!(
                    error = %err,
                    %entity_id,
                    "組 LLM prompt 時讀 identifier 失敗，略過此欄"
                );
                "(unavailable)".into()
            }
        }
    }
}

/// 解析 LLM 回應，抓出 `same_entity` 布林值。
///
/// LLM 有時會在 JSON 前後加說明文字，所以不能直接 `serde_json::from_str`
/// 整段——先去掉 markdown code fence，再找第一個 `{` 到對應的 `}`
/// （字串內的括號不計），解析失敗回 `None`（呼叫端當成 `Pending`）。
fn parse_llm_verdict(content: &str) -> Option<bool> {
    let stripped = strip_markdown_fence(content);
    let json_str = extract_first_json_object(stripped)?;
    let value: Value = serde_json::from_str(json_str).ok()?;
    value.get("same_entity")?.as_bool()
}

/// 去掉常見的 ` ```json ... ``` ` 包裝。沒有 fence 就原樣 trim。
fn strip_markdown_fence(s: &str) -> &str {
    let trimmed = s.trim();
    let Some(rest) = trimmed.strip_prefix("```") else {
        return trimmed;
    };
    let rest = if let Some(after) = rest.strip_prefix("json") {
        after
    } else if let Some(after) = rest.strip_prefix("JSON") {
        after
    } else {
        rest
    };
    let rest = rest.strip_prefix('\r').unwrap_or(rest);
    let rest = rest.strip_prefix('\n').unwrap_or(rest);
    if let Some(end) = rest.rfind("```") {
        rest[..end].trim()
    } else {
        rest.trim()
    }
}

/// 從字串抓出第一個平衡的 JSON object。字串內的 `{`／`}` 不計深度。
fn extract_first_json_object(s: &str) -> Option<&str> {
    let start = s.find('{')?;
    let bytes = s.as_bytes();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for (i, &b) in bytes.iter().enumerate().skip(start) {
        if in_string {
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            continue;
        }
        match b {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[start..=i]);
                }
            }
            _ => {}
        }
    }
    None
}

fn sha256_hex(input: &str) -> String {
    hex::encode(Sha256::digest(input.as_bytes()))
}

/// 組一筆 [`AiRun`]，涵蓋 LLM 呼叫的成功／失敗全部結果。純函式（不碰
/// store），方便單元測試不需要真的接資料庫。
fn build_ai_run(
    model_version: &str,
    request: &ChatCompletionRequest,
    result: &Result<ChatCompletionResponse, AiGatewayError>,
    redacted_user_prompt: &str,
    best: &ResolutionCandidate,
    latency_ms: i64,
) -> AiRun {
    let input_reference = json!({
        "resolution_candidate_id": best.id,
        "method": best.method,
        "score": best.score,
        "system_prompt_hash": sha256_hex(SYSTEM_PROMPT),
        "prompt_version": PROMPT_VERSION,
        "user_prompt": redacted_user_prompt,
    });

    let (model, output, confidence, tokens, estimated_cost) = match result {
        Ok(response) => {
            let redacted_response = redact_credentials(&response.content);
            let parsed = parse_llm_verdict(&response.content);
            let output = json!({
                "raw_response": redacted_response,
                "parsed_same_entity": parsed,
            });
            // 沒有真的「模型自報信心值」——SYSTEM_PROMPT 只問 same_entity
            // 布林值。解析出乾淨的布林值代表拿到明確答案，用 1.0；解析失敗
            // 代表完全沒有可用資訊，用 0.0。這是「有沒有拿到可用結果」的
            // 代理值，不是模型算出來的機率。
            let confidence = if parsed.is_some() { 1.0 } else { 0.0 };
            let tokens = response
                .usage
                .map(|u| i64::from(u.prompt_tokens) + i64::from(u.completion_tokens))
                .unwrap_or(0);
            let estimated_cost = response
                .usage
                .map(|u| Pricing::free().estimate_cost(&u))
                .unwrap_or(0.0);
            (
                response.model.clone(),
                output,
                confidence,
                tokens,
                estimated_cost,
            )
        }
        Err(err) => {
            let output = json!({
                "error": redact_credentials(&err.to_string()),
            });
            (request.model.clone(), output, 0.0, 0, 0.0)
        }
    };

    AiRun {
        id: Uuid::now_v7(),
        task_type: AI_TASK_TYPE.to_string(),
        provider: AI_PROVIDER.to_string(),
        model,
        model_version: model_version.to_string(),
        prompt_version: PROMPT_VERSION.to_string(),
        input_reference,
        output,
        confidence,
        tokens,
        estimated_cost,
        duration_ms: latency_ms,
        created_at: Utc::now(),
    }
}

fn truncate_chars(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

/// 把候選依「另一端 Entity id」分組（`entity_id` 一律是 survivor，另一端是 merged），
/// 去重（同 id 的候選只留一筆），依「另一端 id」排序讓分組順序是決定性的
/// （否則 `max_auto_merges_per_resolve` 上限在同一批候選里砍到哪幾對會不確定）。
///
/// 兩端都不是目標 Entity 的候選會被跳過並記 warn，不讓整批失敗。
#[must_use]
pub fn group_candidates_for_auto_approval(
    entity_id: EntityId,
    candidates: &[ResolutionCandidate],
) -> Vec<(EntityId, Vec<ResolutionCandidate>)> {
    let mut groups: BTreeMap<EntityId, Vec<ResolutionCandidate>> = BTreeMap::new();
    let mut seen: HashSet<EntityId> = HashSet::new();
    for candidate in candidates {
        if !seen.insert(candidate.id) {
            continue;
        }
        let other = if candidate.entity_a_id == entity_id {
            candidate.entity_b_id
        } else if candidate.entity_b_id == entity_id {
            candidate.entity_a_id
        } else {
            warn!(
                candidate_id = %candidate.id,
                %entity_id,
                entity_a_id = %candidate.entity_a_id,
                entity_b_id = %candidate.entity_b_id,
                "自動核准分組遇到兩端都不是目標 Entity 的候選，已跳過"
            );
            continue;
        };
        groups.entry(other).or_default().push(candidate.clone());
    }
    groups.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    use ai_gateway::MockLlmProvider;
    use chrono::{TimeZone, Utc};
    use core_model::{Entity, EntityType, ResolutionCandidate};
    use serde_json::json;
    use storage_core::RelationalStore;
    use storage_core::conformance::find_workspace_root;
    use storage_sqlite::SqliteEmbeddedStore;
    use uuid::Uuid;

    fn ts() -> chrono::DateTime<chrono::Utc> {
        Utc.with_ymd_and_hms(2026, 9, 16, 12, 0, 0).unwrap()
    }

    fn entity(name: &str, entity_type: EntityType) -> Entity {
        Entity {
            id: Uuid::now_v7(),
            entity_type,
            name: name.into(),
            normalized_name: name.to_ascii_lowercase(),
            description: Some(format!("{name} description")),
            confidence: 0.9,
            first_seen: ts(),
            last_seen: ts(),
            merged_into: None,
            attributes: json!({}),
        }
    }

    fn candidate(a: EntityId, b: EntityId, method: &str, score: f64) -> ResolutionCandidate {
        let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(a, b);
        ResolutionCandidate {
            id: Uuid::now_v7(),
            entity_a_id,
            entity_b_id,
            score,
            method: method.into(),
            evidence: json!({"test": true}),
            status: ResolutionStatus::Pending,
            created_at: ts(),
            reviewed_at: None,
        }
    }

    fn enabled_config() -> AutoApprovalConfig {
        AutoApprovalConfig {
            enabled: true,
            auto_confirm_score: 0.95,
            llm_review_score: 0.70,
            llm_model: "qwen-primary".into(),
            llm_model_version: "test-fixture".into(),
            llm_temperature: 0.0,
            llm_max_tokens: 256,
        }
    }

    struct Harness {
        store: SqliteEmbeddedStore,
        path: PathBuf,
    }

    impl Drop for Harness {
        fn drop(&mut self) {
            cleanup(&self.path);
        }
    }

    fn cleanup(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    async fn open_harness() -> Harness {
        let root = find_workspace_root().expect("workspace root");
        let path: PathBuf = root.join(format!("var/osint-auto-approval-{}.sqlite", Uuid::now_v7()));
        let store = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 SQLite");
        store.migrate().await.expect("migrate");
        Harness { store, path }
    }

    fn evaluator(
        store: SqliteEmbeddedStore,
        llm: MockLlmProvider,
        config: AutoApprovalConfig,
    ) -> AutoApprovalEvaluator<SqliteEmbeddedStore, MockLlmProvider> {
        AutoApprovalEvaluator::new(store, None, llm, config)
    }

    #[test]
    fn parse_llm_verdict_pure_json_true() {
        assert_eq!(
            parse_llm_verdict(r#"{"same_entity": true, "reasoning": "match"}"#),
            Some(true)
        );
    }

    #[test]
    fn parse_llm_verdict_pure_json_false() {
        assert_eq!(
            parse_llm_verdict(r#"{"same_entity": false, "reasoning": "different"}"#),
            Some(false)
        );
    }

    #[test]
    fn parse_llm_verdict_leading_prose() {
        let content =
            "Sure, here is my answer:\n{\"same_entity\": true, \"reasoning\": \"aliases overlap\"}";
        assert_eq!(parse_llm_verdict(content), Some(true));
    }

    #[test]
    fn parse_llm_verdict_markdown_fence() {
        let content = "```json\n{\"same_entity\": false, \"reasoning\": \"different orgs\"}\n```";
        assert_eq!(parse_llm_verdict(content), Some(false));
    }

    #[test]
    fn parse_llm_verdict_missing_field() {
        assert_eq!(
            parse_llm_verdict(r#"{"reasoning": "forgot the flag"}"#),
            None
        );
    }

    #[test]
    fn parse_llm_verdict_invalid_json() {
        assert_eq!(parse_llm_verdict("this is not json at all"), None);
        assert_eq!(parse_llm_verdict("{not json"), None);
    }

    #[tokio::test]
    async fn disabled_returns_disabled_regardless_of_score() {
        let h = open_harness().await;
        let survivor = entity("acme", EntityType::Organization);
        let merged = entity("acme-inc", EntityType::Organization);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();

        let mut config = enabled_config();
        config.enabled = false;
        let llm = MockLlmProvider::always_same_entity(true);
        let eval = evaluator(h.store.clone(), llm.clone(), config);
        let cands = vec![candidate(survivor.id, merged.id, "exact_identifier", 0.99)];

        let outcome = eval
            .evaluate_pair(survivor.id, merged.id, &cands, "resolver")
            .await;
        assert_eq!(outcome, AutoApprovalOutcome::Disabled);
        assert_eq!(llm.call_count(), 0);
        let still = h.store.get_entity(merged.id).await.unwrap().unwrap();
        assert!(still.merged_into.is_none());
    }

    #[tokio::test]
    async fn below_llm_review_score_is_pending_without_reading_entities() {
        // 不 seed Entity：若這條路徑誤呼叫 get_entity，reason 會變成「找不到 Entity」。
        // 斷言 reason 帶分數與門檻，證明在讀 Entity 之前就回傳。
        let h = open_harness().await;
        let survivor_id = Uuid::now_v7();
        let merged_id = Uuid::now_v7();
        let llm = MockLlmProvider::always_same_entity(true);
        let eval = evaluator(h.store.clone(), llm.clone(), enabled_config());
        let cands = vec![candidate(survivor_id, merged_id, "normalized_name", 0.40)];

        let outcome = eval
            .evaluate_pair(survivor_id, merged_id, &cands, "resolver")
            .await;
        match outcome {
            AutoApprovalOutcome::Pending { reason } => {
                assert!(
                    reason.contains("0.4") && reason.contains("llm_review_score"),
                    "低分路徑的 reason 應帶分數與門檻，實際：{reason}"
                );
            }
            other => panic!("預期 Pending，得到 {other:?}"),
        }
        assert_eq!(llm.call_count(), 0);
    }

    #[tokio::test]
    async fn high_confidence_merges_with_high_confidence_audit() {
        let h = open_harness().await;
        let survivor = entity("acme-surv", EntityType::Organization);
        let merged = entity("acme-merged", EntityType::Organization);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();
        let cand = candidate(survivor.id, merged.id, "exact_identifier", 0.97);
        h.store.put_resolution_candidate(&cand).await.unwrap();

        let llm = MockLlmProvider::always_same_entity(true);
        let eval = evaluator(h.store.clone(), llm.clone(), enabled_config());
        let outcome = eval
            .evaluate_pair(survivor.id, merged.id, &[cand], "resolver")
            .await;

        let merge_history_id = match outcome {
            AutoApprovalOutcome::Merged { merge_history_id } => merge_history_id,
            other => panic!("預期 Merged，得到 {other:?}"),
        };
        assert_eq!(llm.call_count(), 0, "高信心路徑不該打 LLM");

        let history = h
            .store
            .get_merge_history(merge_history_id)
            .await
            .unwrap()
            .expect("merge_history 應存在");
        assert_eq!(history.operator, "resolver:auto_confirm");
        let audit = history
            .auto_approval_audit
            .expect("高信心 merge 必須帶稽核 JSON");
        assert_eq!(audit["decision_path"], "high_confidence");
        assert_eq!(audit["best_method"], "exact_identifier");
        assert_eq!(audit["source"], "resolver");
        assert_eq!(audit["llm"], Value::Null);
        assert_eq!(audit["version"], 1);

        let got_merged = h.store.get_entity(merged.id).await.unwrap().unwrap();
        assert_eq!(got_merged.merged_into, Some(survivor.id));
    }

    #[tokio::test]
    async fn type_mismatch_is_pending_and_does_not_merge() {
        let h = open_harness().await;
        let survivor = entity("acme-person", EntityType::Person);
        let merged = entity("acme-org", EntityType::Organization);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();

        let llm = MockLlmProvider::always_same_entity(true);
        let eval = evaluator(h.store.clone(), llm.clone(), enabled_config());
        let cands = vec![candidate(survivor.id, merged.id, "exact_identifier", 0.99)];
        let outcome = eval
            .evaluate_pair(survivor.id, merged.id, &cands, "resolver")
            .await;
        match outcome {
            AutoApprovalOutcome::Pending { reason } => {
                assert!(
                    reason.contains("entity_type"),
                    "跨 type 的 reason 應提到 entity_type，實際：{reason}"
                );
            }
            other => panic!("預期 Pending，得到 {other:?}"),
        }
        assert_eq!(llm.call_count(), 0);
        let still = h.store.get_entity(merged.id).await.unwrap().unwrap();
        assert!(still.merged_into.is_none());
    }

    #[tokio::test]
    async fn mid_band_llm_true_merges_with_llm_audit() {
        let h = open_harness().await;
        let survivor = entity("alice-surv", EntityType::Person);
        let merged = entity("alice-merged", EntityType::Person);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();
        let cand = candidate(survivor.id, merged.id, "semantic_similarity", 0.82);
        h.store.put_resolution_candidate(&cand).await.unwrap();

        let llm = MockLlmProvider::always_same_entity(true);
        let eval = evaluator(h.store.clone(), llm.clone(), enabled_config());
        let outcome = eval
            .evaluate_pair(survivor.id, merged.id, &[cand], "stix_import")
            .await;

        let merge_history_id = match outcome {
            AutoApprovalOutcome::Merged { merge_history_id } => merge_history_id,
            other => panic!("預期 Merged，得到 {other:?}"),
        };
        assert_eq!(llm.call_count(), 1);

        let history = h
            .store
            .get_merge_history(merge_history_id)
            .await
            .unwrap()
            .expect("merge_history 應存在");
        assert_eq!(history.operator, "stix_import:auto_confirm");
        let audit = history
            .auto_approval_audit
            .expect("中間帶 merge 必須帶稽核");
        assert_eq!(audit["decision_path"], "llm_review");
        assert!(audit["llm"].is_object(), "audit.llm 不該是 null");
        assert_eq!(audit["llm"]["parsed_same_entity"], true);
        assert_eq!(audit["llm"]["model"], "qwen-primary");
        assert!(
            audit["llm"]["user_prompt"]
                .as_str()
                .unwrap()
                .contains("alice")
        );
        assert!(audit["llm"]["system_prompt_hash"].as_str().unwrap().len() == 64);
    }

    #[tokio::test]
    async fn mid_band_llm_false_is_pending() {
        let h = open_harness().await;
        let survivor = entity("bob-surv", EntityType::Person);
        let merged = entity("bob-merged", EntityType::Person);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();

        let llm = MockLlmProvider::always_same_entity(false);
        let eval = evaluator(h.store.clone(), llm.clone(), enabled_config());
        let cands = vec![candidate(
            survivor.id,
            merged.id,
            "semantic_similarity",
            0.80,
        )];
        let outcome = eval
            .evaluate_pair(survivor.id, merged.id, &cands, "resolver")
            .await;
        match outcome {
            AutoApprovalOutcome::Pending { reason } => {
                assert!(reason.contains("不是同一實體"), "實際 reason：{reason}");
            }
            other => panic!("預期 Pending，得到 {other:?}"),
        }
        assert_eq!(llm.call_count(), 1);
        let still = h.store.get_entity(merged.id).await.unwrap().unwrap();
        assert!(still.merged_into.is_none());
    }

    #[tokio::test]
    async fn mid_band_llm_transient_error_is_pending() {
        let h = open_harness().await;
        let survivor = entity("carol-surv", EntityType::Person);
        let merged = entity("carol-merged", EntityType::Person);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();

        let llm = MockLlmProvider::always_error(AiGatewayError::Transient {
            message: "逾時".into(),
        });
        let eval = evaluator(h.store.clone(), llm.clone(), enabled_config());
        let cands = vec![candidate(
            survivor.id,
            merged.id,
            "semantic_similarity",
            0.80,
        )];
        let outcome = eval
            .evaluate_pair(survivor.id, merged.id, &cands, "resolver")
            .await;
        match outcome {
            AutoApprovalOutcome::Pending { reason } => {
                assert!(reason.contains("LLM 呼叫失敗"), "實際 reason：{reason}");
            }
            other => panic!("預期 Pending，得到 {other:?}"),
        }
        assert_eq!(llm.call_count(), 1);
        let still = h.store.get_entity(merged.id).await.unwrap().unwrap();
        assert!(still.merged_into.is_none());
    }

    #[tokio::test]
    async fn ai_run_persisted_when_llm_confirms_same_entity() {
        let h = open_harness().await;
        let survivor = entity("eve-surv", EntityType::Person);
        let merged = entity("eve-merged", EntityType::Person);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();
        let cand = candidate(survivor.id, merged.id, "semantic_similarity", 0.82);
        h.store.put_resolution_candidate(&cand).await.unwrap();

        let eval = evaluator(
            h.store.clone(),
            MockLlmProvider::always_same_entity(true),
            enabled_config(),
        );
        let _ = eval
            .evaluate_pair(survivor.id, merged.id, &[cand], "resolver")
            .await;

        let runs = h.store.list_ai_runs(None, None, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "本次 LLM 呼叫必須留一筆 AiRun");
        let run = &runs[0];
        assert_eq!(run.task_type, AI_TASK_TYPE);
        assert_eq!(run.confidence, 1.0);
        assert_eq!(run.prompt_version, PROMPT_VERSION);
        assert_eq!(run.model_version, "test-fixture");
        assert_eq!(run.output["parsed_same_entity"], true);
    }

    #[tokio::test]
    async fn ai_run_persisted_when_llm_rejects_same_entity() {
        let h = open_harness().await;
        let survivor = entity("frank-surv", EntityType::Person);
        let merged = entity("frank-merged", EntityType::Person);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();
        let cand = candidate(survivor.id, merged.id, "semantic_similarity", 0.80);
        h.store.put_resolution_candidate(&cand).await.unwrap();

        let eval = evaluator(
            h.store.clone(),
            MockLlmProvider::always_same_entity(false),
            enabled_config(),
        );
        let _ = eval
            .evaluate_pair(survivor.id, merged.id, &[cand], "resolver")
            .await;

        let runs = h.store.list_ai_runs(None, None, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "判 false 也必須留一筆 AiRun");
        let run = &runs[0];
        // 拿到明確的 false 判斷，仍然代表模型給了可用答案 → confidence 1.0。
        assert_eq!(run.confidence, 1.0);
        assert_eq!(run.output["parsed_same_entity"], false);
    }

    #[tokio::test]
    async fn ai_run_persisted_when_llm_call_fails() {
        let h = open_harness().await;
        let survivor = entity("grace-surv", EntityType::Person);
        let merged = entity("grace-merged", EntityType::Person);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();
        let cand = candidate(survivor.id, merged.id, "semantic_similarity", 0.80);
        h.store.put_resolution_candidate(&cand).await.unwrap();

        let eval = evaluator(
            h.store.clone(),
            MockLlmProvider::always_error(AiGatewayError::Transient {
                message: "逾時".into(),
            }),
            enabled_config(),
        );
        let _ = eval
            .evaluate_pair(survivor.id, merged.id, &[cand], "resolver")
            .await;

        let runs = h.store.list_ai_runs(None, None, 10).await.unwrap();
        assert_eq!(runs.len(), 1, "呼叫失敗也必須留一筆 AiRun");
        let run = &runs[0];
        assert_eq!(run.confidence, 0.0);
        assert_eq!(run.tokens, 0);
    }

    #[tokio::test]
    async fn ai_run_uses_redacted_user_prompt_when_provided() {
        // `MockLlmProvider` 不能自訂回應 text（Step A 範圍，不動它），所以這裡
        // 直接測 `build_ai_run` 自由函式：呼它時傳的 `redacted_user_prompt`
        // 必須原樣進 `AiRun.input_reference.user_prompt`——這在驗證「呼叫端真的
        // 有先呼叫 `redact_credentials` 才傳進來」這條契約的輸出端。
        let request = ChatCompletionRequest {
            model: "qwen-primary".into(),
            messages: vec![],
            temperature: 0.0,
            max_tokens: 16,
        };
        let cand = candidate(Uuid::now_v7(), Uuid::now_v7(), "semantic_similarity", 0.80);
        let run = build_ai_run(
            "test-fixture",
            &request,
            &Ok(ChatCompletionResponse {
                content: r#"{"same_entity": true, "reasoning": "ok"}"#.to_string(),
                model: "qwen-primary".into(),
                usage: None,
            }),
            "Bearer [已省略] xyz",
            &cand,
            12,
        );
        assert_eq!(
            run.input_reference["user_prompt"].as_str().unwrap(),
            "Bearer [已省略] xyz",
        );
        // 未遮罩的謊言版 `Bearer secret-token-value` 不應原樣存在。
        assert!(
            !run.input_reference["user_prompt"]
                .as_str()
                .unwrap()
                .contains("secret-token-value")
        );
    }

    #[tokio::test]
    async fn successful_merge_marks_all_pair_candidates_auto_confirmed() {
        let h = open_harness().await;
        let survivor = entity("duo-surv", EntityType::Organization);
        let merged = entity("duo-merged", EntityType::Organization);
        h.store.put_entity(&survivor).await.unwrap();
        h.store.put_entity(&merged).await.unwrap();

        let cand_id = candidate(survivor.id, merged.id, "exact_identifier", 0.97);
        let cand_alias = candidate(survivor.id, merged.id, "alias", 0.88);
        h.store.put_resolution_candidate(&cand_id).await.unwrap();
        h.store.put_resolution_candidate(&cand_alias).await.unwrap();

        let llm = MockLlmProvider::always_same_entity(false);
        let eval = evaluator(h.store.clone(), llm, enabled_config());
        let outcome = eval
            .evaluate_pair(
                survivor.id,
                merged.id,
                &[cand_id.clone(), cand_alias.clone()],
                "resolver",
            )
            .await;
        assert!(
            matches!(outcome, AutoApprovalOutcome::Merged { .. }),
            "高信心應 merge，得到 {outcome:?}"
        );

        let got_id = h
            .store
            .get_resolution_candidate(cand_id.id)
            .await
            .unwrap()
            .unwrap();
        let got_alias = h
            .store
            .get_resolution_candidate(cand_alias.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got_id.status, ResolutionStatus::AutoConfirmed);
        assert_eq!(got_alias.status, ResolutionStatus::AutoConfirmed);
        assert!(got_id.reviewed_at.is_some());
        assert!(got_alias.reviewed_at.is_some());
    }

    fn grouping_candidate(id: Uuid, a: Uuid, b: Uuid, method: &str) -> ResolutionCandidate {
        let (entity_a_id, entity_b_id) = ResolutionCandidate::ordered_pair(a, b);
        ResolutionCandidate {
            id,
            entity_a_id,
            entity_b_id,
            score: 0.5,
            method: method.into(),
            evidence: json!({}),
            status: ResolutionStatus::Pending,
            created_at: ts(),
            reviewed_at: None,
        }
    }

    #[test]
    fn groups_by_other_end_regardless_of_a_or_b_direction() {
        let entity = Uuid::from_u128(10);
        // 比 entity 小：entity 會落在 b；比 entity 大：entity 會落在 a。
        let smaller = Uuid::from_u128(1);
        let larger = Uuid::from_u128(20);
        let c_small = grouping_candidate(Uuid::from_u128(100), entity, smaller, "alias");
        let c_large = grouping_candidate(Uuid::from_u128(101), entity, larger, "domain");

        let groups =
            group_candidates_for_auto_approval(entity, &[c_small.clone(), c_large.clone()]);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, smaller);
        assert_eq!(groups[0].1.len(), 1);
        assert_eq!(groups[0].1[0].id, c_small.id);
        assert_eq!(groups[1].0, larger);
        assert_eq!(groups[1].1.len(), 1);
        assert_eq!(groups[1].1[0].id, c_large.id);
    }

    #[test]
    fn same_pair_multiple_methods_share_one_group() {
        let entity = Uuid::from_u128(10);
        let other = Uuid::from_u128(20);
        let a = grouping_candidate(Uuid::from_u128(1), entity, other, "exact_identifier");
        let b = grouping_candidate(Uuid::from_u128(2), entity, other, "alias");
        let groups = group_candidates_for_auto_approval(entity, &[a, b]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, other);
        assert_eq!(groups[0].1.len(), 2);
    }

    #[test]
    fn skips_candidates_that_do_not_involve_entity() {
        let entity = Uuid::from_u128(10);
        let unrelated_a = Uuid::from_u128(1);
        let unrelated_b = Uuid::from_u128(2);
        let stray = grouping_candidate(Uuid::from_u128(9), unrelated_a, unrelated_b, "alias");
        let groups = group_candidates_for_auto_approval(entity, &[stray]);
        assert!(groups.is_empty());
    }

    #[test]
    fn grouping_order_is_deterministic() {
        let entity = Uuid::from_u128(50);
        let others = [
            Uuid::from_u128(3),
            Uuid::from_u128(1),
            Uuid::from_u128(9),
            Uuid::from_u128(2),
        ];
        let input: Vec<_> = others
            .iter()
            .enumerate()
            .map(|(i, other)| {
                grouping_candidate(
                    Uuid::from_u128(100 + i as u128),
                    entity,
                    *other,
                    "normalized_name",
                )
            })
            .collect();
        let first = group_candidates_for_auto_approval(entity, &input);
        let second = group_candidates_for_auto_approval(entity, &input);
        let keys: Vec<_> = first.iter().map(|(k, _)| *k).collect();
        assert_eq!(keys, vec![others[1], others[3], others[0], others[2]]);
        assert_eq!(first, second);
    }

    #[test]
    fn duplicate_candidate_ids_are_kept_once() {
        let entity = Uuid::from_u128(10);
        let other = Uuid::from_u128(20);
        let c = grouping_candidate(Uuid::from_u128(1), entity, other, "alias");
        let groups = group_candidates_for_auto_approval(entity, &[c.clone(), c]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.len(), 1);
    }
}
