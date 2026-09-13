//! OpenSearch ml-commons [`EmbeddingProvider`] 生產實作。
//!
//! 語言路由與非對稱前綴完全比照 [`storage_core::mock::MockEmbeddingProvider`]：
//! 英文走 MiniLM（不加前綴），其他語言／未知走 e5-small（`query:`／`passage:`）。
//! 差別只在向量是打 `_predict` 拿到的真值，不是確定性假向量。

use async_trait::async_trait;
use opensearch::OpenSearch;
use opensearch::http::Method;
use opensearch::http::headers::HeaderMap;
use opensearch::http::request::JsonBody;
use serde_json::{Value, json};
use storage_core::{
    EmbeddingKind, EmbeddingModelRef, EmbeddingProvider, EmbeddingRequest, EmbeddingVector,
    HealthProvider, StorageError, StorageHealth, embedding_content_hash,
};

use crate::{build_opensearch_client, map_os};

/// MiniLM 在 ml-commons 裡的名稱。與 `scripts/opensearch-ml-setup.sh` 的
/// `ML_MODEL_NAME` 預設值相同。
pub const MINILM_MODEL_NAME: &str = "huggingface/sentence-transformers/all-MiniLM-L6-v2";

/// MiniLM 上游 `config.json` 宣告的 `model_content_hash_value`。
///
/// 這是查 `model_id` 的鍵，也是寫進 [`EmbeddingVector::model_version`] 的值。
/// ml-commons 文件裡的 `model_version` 是它自己的遞增序號 `"1"`，換上游版本
/// 不會變，拿去判斷「這筆 embedding 要不要重算」會永遠判斷錯。
///
/// 可由環境變數 `ML_MODEL_SHA256` 覆寫（與 setup 腳本同一顆變數），
/// 方便換版本時不必改程式。預設值對齊 `opensearch-ml-setup.sh`。
pub const MINILM_CONTENT_HASH: &str =
    "89b6737ca1745a89eafdf37cc7de2a3aae05aca9aac32ac50a3349add67206bb";

/// e5-small int8 在 ml-commons 裡的名稱。
pub const E5_MODEL_NAME: &str = "intfloat/multilingual-e5-small-int8";

/// e5-small **打包後 zip** 的 SHA-256（不是 ONNX 檔本身）。
///
/// `opensearch-ml-setup-e5.sh` 冪等檢查用的是 `ZIP_SHA256`，因為
/// `model_content_hash_value` 記的是註冊進去的那份 zip。ONNX 檔的 hash
/// （`dd476dd0…`）跟這顆不一樣，拿去查會穩定 0 筆。
pub const E5_CONTENT_HASH: &str =
    "e8f6bd1be427a518c2160f1742fd3b70a1a1e0c01b4a93edf98671c8128c9ff6";

/// 兩個模型目前都是 384。寫在這裡當「找不到 `embedding_dimension` 時」
/// 的後備，不是編譯期契約——真正的維度以連線時從模型文件讀到的為準。
const FALLBACK_DIMENSIONS: usize = 384;

/// 後端名稱。錯誤分類與 health 都用這個字，不要跟 SearchStore 的
/// `"opensearch"` 混在一起——embedding 掛了不該讓搜尋看起來也掛。
const BACKEND: &str = "opensearch-ml";

/// OpenSearch ml-commons 的生產 [`EmbeddingProvider`]。
///
/// # `model_id` 快取策略（V0.2）
///
/// `connect` 時查一次兩個模型的 `model_id` 並存起來，之後 `embed` 不再重查。
/// 模型被重新註冊後 `model_id` 會換——V0.2 接受「換過要重啟服務」。
/// 每次 `embed` 都打 `_search` 會把推論延遲加上一次查詢，而且
/// `model_id` 在正常運作下不會自己變；變了代表有人重跑 setup 腳本，
/// 那本來就該重啟吃新設定。
pub struct MlCommonsEmbeddingProvider {
    client: OpenSearch,
    minilm: DeployedModel,
    e5: DeployedModel,
}

struct DeployedModel {
    /// ml-commons 內部 id（每次重新註冊都會變）。
    model_id: String,
    /// 給呼叫端看的穩定名稱。
    name: &'static str,
    /// 內容雜湊，當 `model_version` 用。
    content_hash: String,
    dimensions: usize,
}

impl MlCommonsEmbeddingProvider {
    /// 連上 OpenSearch，查兩個模型都必須是 `DEPLOYED`。
    ///
    /// 任一個找不到或狀態不是 `DEPLOYED` 就回 [`StorageError::Configuration`]，
    /// 訊息會指出該跑哪支 setup 腳本。這是啟動期錯誤，不是暫時性失敗。
    pub async fn connect(url: &str) -> Result<Self, StorageError> {
        let client = build_opensearch_client(url)?;
        let minilm_hash = std::env::var("ML_MODEL_SHA256")
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| MINILM_CONTENT_HASH.to_string());
        let minilm = lookup_deployed(
            &client,
            MINILM_MODEL_NAME,
            &minilm_hash,
            "scripts/opensearch-ml-setup.sh",
        )
        .await?;
        let e5 = lookup_deployed(
            &client,
            E5_MODEL_NAME,
            E5_CONTENT_HASH,
            "scripts/opensearch-ml-setup-e5.sh",
        )
        .await?;
        Ok(Self { client, minilm, e5 })
    }

    fn route(&self, language: Option<&str>) -> &DeployedModel {
        if is_english(language) {
            &self.minilm
        } else {
            &self.e5
        }
    }

    fn prefixed_text(english: bool, kind: EmbeddingKind, text: &str) -> String {
        if english {
            text.to_string()
        } else {
            match kind {
                EmbeddingKind::Query => format!("query: {text}"),
                EmbeddingKind::Passage => format!("passage: {text}"),
            }
        }
    }

    async fn predict(
        &self,
        model: &DeployedModel,
        texts: &[String],
    ) -> Result<Vec<Vec<f32>>, StorageError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let path = format!("/_plugins/_ml/_predict/text_embedding/{}", model.model_id);
        let body = json!({
            "text_docs": texts,
            "return_number": true,
            "target_response": ["sentence_embedding"],
        });
        let response = self
            .client
            .send::<JsonBody<Value>, ()>(
                Method::Post,
                &path,
                HeaderMap::new(),
                None,
                Some(JsonBody::new(body)),
                None,
            )
            .await
            .map_err(map_os)?;
        let status = response.status_code();
        let payload: Value = response.json().await.map_err(map_os)?;
        if !status.is_success() {
            return Err(classify_ml_http(status.as_u16(), &payload, model.name));
        }
        parse_inference_results(&payload, texts.len(), model.dimensions, model.name)
    }
}

/// 語言路由：BCP 47 主語言 `en`（大小寫不敏感，`en-US`／`en_GB` 都算）才走 MiniLM。
/// `None` 與其他語言走 e5。與 mock 的 `is_english` 完全相同。
fn is_english(language: Option<&str>) -> bool {
    language.is_some_and(|lang| {
        lang.split(['-', '_'])
            .next()
            .is_some_and(|primary| primary.eq_ignore_ascii_case("en"))
    })
}

async fn lookup_deployed(
    client: &OpenSearch,
    name: &'static str,
    content_hash: &str,
    setup_script: &str,
) -> Result<DeployedModel, StorageError> {
    // 端點用 plugin API `/_plugins/_ml/models/_search`，與 setup 腳本一致，
    // 不要打內部 index `/.plugins-ml-model`——那是實作細節，plugin 路徑才是契約。
    //
    // 查詢鍵只有 `model_content_hash_value`。名稱是展示用，hash 才是身分；
    // 加 `name.keyword` 在改名時會讓「模型明明在」查成 0 筆。
    // `must_not chunk_number` 必要：模型本體以分塊文件存在同一個 index。
    let body = json!({
        "size": 5,
        "_source": ["model_state", "model_content_hash_value", "model_config.embedding_dimension"],
        "query": {
            "bool": {
                "must": [
                    {"term": {"model_content_hash_value": content_hash}}
                ],
                "must_not": [
                    {"exists": {"field": "chunk_number"}}
                ]
            }
        }
    });
    let response = client
        .send::<JsonBody<Value>, ()>(
            Method::Post,
            "/_plugins/_ml/models/_search",
            HeaderMap::new(),
            None,
            Some(JsonBody::new(body)),
            None,
        )
        .await
        .map_err(map_os)?;
    let status = response.status_code();
    let payload: Value = response.json().await.map_err(map_os)?;
    if !status.is_success() {
        return Err(StorageError::Unavailable {
            backend: BACKEND,
            message: format!(
                "查詢模型 `{name}`（hash {content_hash}）失敗：HTTP {status}。\
                 請確認 OpenSearch ml-commons 已啟用，並跑過 `{setup_script}`"
            ),
        });
    }

    let hit =
        payload
            .pointer("/hits/hits/0")
            .cloned()
            .ok_or_else(|| StorageError::Configuration {
                message: format!(
                    "OpenSearch 裡找不到內容雜湊 `{content_hash}` 的模型 `{name}` \
                 （或只找到分塊文件）。請跑 `{setup_script}` 註冊並部署；\
                 部署完成後 `model_state` 必須是 DEPLOYED"
                ),
            })?;

    let model_id = hit
        .get("_id")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| StorageError::CorruptionSuspected {
            message: format!(
                "模型 `{name}` 的搜尋命中沒有 `_id`。請確認 OpenSearch ml-commons \
                 版本與 docs/developer/embedding.md 記錄的一致"
            ),
        })?
        .to_string();

    let state = hit
        .pointer("/_source/model_state")
        .and_then(Value::as_str)
        .unwrap_or("");
    if state != "DEPLOYED" {
        return Err(StorageError::Configuration {
            message: format!(
                "模型 `{name}`（id `{model_id}`）目前狀態是 `{state}`，不是 DEPLOYED，\
                 無法推論。請跑 `{setup_script}` 完成部署；不要在本機共用叢集上手動 undeploy"
            ),
        });
    }

    let dimensions = hit
        .pointer("/_source/model_config/embedding_dimension")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .filter(|&n| n > 0)
        .unwrap_or(FALLBACK_DIMENSIONS);

    Ok(DeployedModel {
        model_id,
        name,
        content_hash: content_hash.to_string(),
        dimensions,
    })
}

fn parse_inference_results(
    payload: &Value,
    expected_count: usize,
    expected_dim: usize,
    model_name: &str,
) -> Result<Vec<Vec<f32>>, StorageError> {
    let results = payload
        .get("inference_results")
        .and_then(Value::as_array)
        .ok_or_else(|| StorageError::CorruptionSuspected {
            message: format!(
                "模型 `{model_name}` 的 `_predict` 回應缺少 `inference_results`。\
                 若 `return_number` 沒帶，ml-commons 會回 base64 而不是數字陣列。\
                 請確認 OpenSearch ml-commons 版本與 docs/developer/embedding.md 記錄的一致。\
                 回應摘要：{}",
                truncate_json(payload)
            ),
        })?;

    if results.len() != expected_count {
        return Err(StorageError::CorruptionSuspected {
            message: format!(
                "模型 `{model_name}` 回了 {} 筆向量，送出的是 {expected_count} 筆。\
                 批次對應會錯位。請確認 ml-commons 版本與文件記錄的一致",
                results.len()
            ),
        });
    }

    let mut out = Vec::with_capacity(results.len());
    for (i, result) in results.iter().enumerate() {
        let data = result
            .pointer("/output/0/data")
            .and_then(Value::as_array)
            .ok_or_else(|| StorageError::CorruptionSuspected {
                message: format!(
                    "模型 `{model_name}` 第 {i} 筆缺少 `output[0].data`。\
                     請確認 OpenSearch ml-commons 版本與 docs/developer/embedding.md 記錄的一致。\
                     回應摘要：{}",
                    truncate_json(payload)
                ),
            })?;
        if data.len() != expected_dim {
            return Err(StorageError::CorruptionSuspected {
                message: format!(
                    "模型 `{model_name}` 第 {i} 筆是 {} 維，預期 {expected_dim} 維。\
                     index mapping 的 dimension 會跟著錯。請確認部署的是文件記錄的那份模型",
                    data.len()
                ),
            });
        }
        let mut vec = Vec::with_capacity(data.len());
        for (j, v) in data.iter().enumerate() {
            let n = v.as_f64().ok_or_else(|| StorageError::CorruptionSuspected {
                message: format!(
                    "模型 `{model_name}` 第 {i} 筆第 {j} 維不是數字（可能是漏帶 `return_number` \
                     而回了 base64）。請確認呼叫格式與 docs/developer/embedding.md 一致"
                ),
            })?;
            vec.push(n as f32);
        }
        out.push(vec);
    }
    Ok(out)
}

/// 把 ml-commons HTTP 錯誤對到穩定的 [`StorageError`] 變體。
///
/// - 429／`circuit_breaking_exception` → [`StorageError::Timeout`]（暫時性，可重試；
///   heap 斷路器是已知會發生的情況，見 `docs/developer/embedding.md` §4）
/// - 503／502 → [`StorageError::Unavailable`]
/// - 模型未部署相關 → [`StorageError::Configuration`]
/// - 其他 → [`StorageError::Unknown`]
///
/// 公開給單元測試直接打，不必撐爆 JVM heap 才能驗證分類。
#[must_use]
pub fn classify_ml_http(status: u16, body: &Value, model_name: &str) -> StorageError {
    let dumped = body.to_string();
    let sanitized = StorageError::sanitize(&dumped);
    let is_circuit = status == 429
        || dumped.contains("circuit_breaking_exception")
        || dumped.contains("CircuitBreakingException");
    if is_circuit {
        return StorageError::Timeout {
            backend: BACKEND,
            message: format!(
                "模型 `{model_name}` 推論被 JVM heap 斷路器擋下（HTTP {status}）。\
                 這通常不是容器記憶體不足，而是 heap 超過 jvm_heap_memory_threshold(85)。\
                 請稍後再試、把批次調小，或確認 OPENSEARCH_JAVA_OPTS 是 -Xmx1536m。\
                 詳見 docs/developer/embedding.md §4。回應：{}",
                truncate_str(&sanitized)
            ),
        };
    }
    if status == 503 || status == 502 {
        return StorageError::Unavailable {
            backend: BACKEND,
            message: format!(
                "模型 `{model_name}` 推論時 OpenSearch 回 HTTP {status}。請稍後再試。\
                 回應：{}",
                truncate_str(&sanitized)
            ),
        };
    }

    let undeployed = dumped.contains("UNDEPLOYED")
        || dumped.contains("DEPLOY_FAILED")
        || dumped.contains("undeploy")
        || dumped.contains("not deployed")
        || dumped.contains("Model is not deployed");
    if undeployed {
        return StorageError::Configuration {
            message: format!(
                "模型 `{model_name}` 目前不是 DEPLOYED，無法推論。\
                 英文模型請跑 `scripts/opensearch-ml-setup.sh`，\
                 多語模型請跑 `scripts/opensearch-ml-setup-e5.sh`。\
                 不要在本機共用叢集上手動 undeploy。回應：{}",
                truncate_str(&sanitized)
            ),
        };
    }

    StorageError::Unknown {
        backend: BACKEND,
        message: format!(
            "模型 `{model_name}` 推論失敗：HTTP {status}。回應：{}",
            truncate_str(&sanitized)
        ),
    }
}

fn truncate_json(value: &Value) -> String {
    truncate_str(&value.to_string())
}

fn truncate_str(s: &str) -> String {
    const LIMIT: usize = 500;
    if s.len() <= LIMIT {
        s.to_string()
    } else {
        let end = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= LIMIT)
            .last()
            .unwrap_or(0);
        format!("{}…", &s[..end])
    }
}

#[async_trait]
impl HealthProvider for MlCommonsEmbeddingProvider {
    async fn health(&self) -> Result<StorageHealth, StorageError> {
        let response = self
            .client
            .send::<(), ()>(Method::Get, "/", HeaderMap::new(), None, None, None)
            .await
            .map_err(map_os)?;
        if !response.status_code().is_success() {
            return Ok(StorageHealth::down(
                BACKEND,
                format!("GET / 回 {}", response.status_code()),
            ));
        }
        Ok(StorageHealth::ok(
            BACKEND,
            format!(
                "MiniLM id={}（{} 維）、e5 id={}（{} 維）已快取；\
                 模型重新註冊後必須重啟本服務才會拿到新 id",
                self.minilm.model_id, self.minilm.dimensions, self.e5.model_id, self.e5.dimensions
            ),
        )
        .with_details(json!({
            "minilm_model_id": self.minilm.model_id,
            "e5_model_id": self.e5.model_id,
            "minilm_dimensions": self.minilm.dimensions,
            "e5_dimensions": self.e5.dimensions,
        })))
    }
}

#[async_trait]
impl EmbeddingProvider for MlCommonsEmbeddingProvider {
    fn dimensions(&self) -> usize {
        // 語言未知走 e5，與 `model_for(None)` 一致。
        self.e5.dimensions
    }

    fn dimensions_for(&self, language: Option<&str>) -> usize {
        self.route(language).dimensions
    }

    fn model_for(&self, language: Option<&str>) -> EmbeddingModelRef {
        let model = self.route(language);
        EmbeddingModelRef {
            model: model.name.to_string(),
            model_version: model.content_hash.clone(),
            dimensions: model.dimensions,
        }
    }

    async fn embed(&self, request: &EmbeddingRequest) -> Result<EmbeddingVector, StorageError> {
        let out = self.embed_batch(std::slice::from_ref(request)).await?;
        out.into_iter().next().ok_or_else(|| StorageError::Unknown {
            backend: BACKEND,
            message: "embed_batch 對單筆請求回了空陣列".into(),
        })
    }

    async fn embed_batch(
        &self,
        requests: &[EmbeddingRequest],
    ) -> Result<Vec<EmbeddingVector>, StorageError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }

        // 一批可能同時有英文與非英文。混著打同一個 `_predict` 會讓整批
        // 被同一個模型處理——那是靜默錯誤（維度看起來對、鄰居毫無意義）。
        // 拆成兩次呼叫，各自走批次 API，再依原始索引組回。
        let mut english_idx = Vec::new();
        let mut english_texts = Vec::new();
        let mut other_idx = Vec::new();
        let mut other_texts = Vec::new();

        for (i, request) in requests.iter().enumerate() {
            let english = is_english(request.language.as_deref());
            let prefixed = Self::prefixed_text(english, request.kind, &request.text);
            if english {
                english_idx.push(i);
                english_texts.push(prefixed);
            } else {
                other_idx.push(i);
                other_texts.push(prefixed);
            }
        }

        // 兩次 `_predict` 串行。並行會讓兩個模型同時佔 JVM heap，
        // 本機共用叢集在 heap 85% 門檻附近已知會 429（embedding.md §4）。
        let english_vecs = self.predict(&self.minilm, &english_texts).await?;
        let other_vecs = self.predict(&self.e5, &other_texts).await?;

        let mut slots: Vec<Option<EmbeddingVector>> = (0..requests.len()).map(|_| None).collect();
        fill_slots(
            &mut slots,
            &english_idx,
            english_vecs,
            requests,
            &self.minilm,
        )?;
        fill_slots(&mut slots, &other_idx, other_vecs, requests, &self.e5)?;

        slots
            .into_iter()
            .enumerate()
            .map(|(i, v)| {
                v.ok_or_else(|| StorageError::Unknown {
                    backend: BACKEND,
                    message: format!("embed_batch 第 {i} 筆沒組回向量"),
                })
            })
            .collect()
    }
}

fn fill_slots(
    slots: &mut [Option<EmbeddingVector>],
    indices: &[usize],
    vectors: Vec<Vec<f32>>,
    requests: &[EmbeddingRequest],
    model: &DeployedModel,
) -> Result<(), StorageError> {
    if indices.len() != vectors.len() {
        return Err(StorageError::CorruptionSuspected {
            message: format!(
                "模型 `{}` 回了 {} 筆，送出的是 {} 筆，無法組回原始順序",
                model.name,
                vectors.len(),
                indices.len()
            ),
        });
    }
    for (slot, vector) in indices.iter().copied().zip(vectors) {
        let request = &requests[slot];
        slots[slot] = Some(EmbeddingVector {
            model: model.name.to_string(),
            model_version: model.content_hash.clone(),
            dimensions: model.dimensions,
            content_hash: embedding_content_hash(&request.text),
            vector,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_message(err: &StorageError) -> String {
        err.to_string()
    }

    #[test]
    fn english_routing_matches_mock() {
        assert!(is_english(Some("en")));
        assert!(is_english(Some("en-US")));
        assert!(is_english(Some("EN_GB")));
        assert!(!is_english(Some("zh")));
        assert!(!is_english(Some("zh-Hant")));
        assert!(!is_english(None));
        assert!(!is_english(Some("")));
        assert!(!is_english(Some("fr")));
    }

    #[test]
    fn prefix_only_applies_to_e5() {
        let text = "Microsoft 是一家科技公司";
        assert_eq!(
            MlCommonsEmbeddingProvider::prefixed_text(true, EmbeddingKind::Query, text),
            text
        );
        assert_eq!(
            MlCommonsEmbeddingProvider::prefixed_text(true, EmbeddingKind::Passage, text),
            text
        );
        assert_eq!(
            MlCommonsEmbeddingProvider::prefixed_text(false, EmbeddingKind::Query, text),
            format!("query: {text}")
        );
        assert_eq!(
            MlCommonsEmbeddingProvider::prefixed_text(false, EmbeddingKind::Passage, text),
            format!("passage: {text}")
        );
    }

    #[test]
    fn http_429_is_retryable_timeout() {
        let body = json!({
            "error": {
                "type": "circuit_breaking_exception",
                "reason": "Memory Circuit Breaker is open, data too large"
            },
            "status": 429
        });
        let err = classify_ml_http(429, &body, MINILM_MODEL_NAME);
        match err {
            StorageError::Timeout { backend, message } => {
                assert_eq!(backend, BACKEND);
                assert!(message.contains("斷路器"), "{message}");
                assert!(
                    message.contains("稍後再試") || message.contains("批次調小"),
                    "{message}"
                );
                assert!(
                    message.contains("opensearch-ml-setup") || message.contains("embedding.md"),
                    "{message}"
                );
            }
            other => panic!("預期 Timeout，得到 {other}"),
        }
    }

    #[test]
    fn circuit_breaker_in_body_even_without_429() {
        // 有時狀態碼不是 429，但 body 仍帶 circuit_breaking_exception。
        let body = json!({"error": {"type": "circuit_breaking_exception"}});
        let err = classify_ml_http(500, &body, E5_MODEL_NAME);
        assert!(
            matches!(err, StorageError::Timeout { .. }),
            "got {}",
            err_message(&err)
        );
    }

    #[test]
    fn http_503_is_unavailable() {
        let err = classify_ml_http(503, &json!({"error": "unavailable"}), E5_MODEL_NAME);
        match err {
            StorageError::Unavailable { backend, message } => {
                assert_eq!(backend, BACKEND);
                assert!(message.contains("503"), "{message}");
            }
            other => panic!("預期 Unavailable，得到 {other}"),
        }
    }

    #[test]
    fn undeployed_model_is_configuration() {
        let body = json!({"error": {"reason": "Model is not deployed"}});
        let err = classify_ml_http(400, &body, MINILM_MODEL_NAME);
        match err {
            StorageError::Configuration { message } => {
                assert!(message.contains("opensearch-ml-setup.sh"), "{message}");
                assert!(message.contains("opensearch-ml-setup-e5.sh"), "{message}");
            }
            other => panic!("預期 Configuration，得到 {other}"),
        }
    }

    #[test]
    fn other_http_error_is_unknown() {
        let err = classify_ml_http(400, &json!({"error": "bad request"}), MINILM_MODEL_NAME);
        assert!(
            matches!(
                err,
                StorageError::Unknown {
                    backend: BACKEND,
                    ..
                }
            ),
            "got {}",
            err_message(&err)
        );
    }

    #[test]
    fn parse_rejects_missing_inference_results() {
        let err = parse_inference_results(&json!({"status": "ok"}), 1, 384, "m").unwrap_err();
        assert!(
            matches!(err, StorageError::CorruptionSuspected { .. }),
            "got {}",
            err_message(&err)
        );
        assert!(err_message(&err).contains("return_number"));
    }

    #[test]
    fn parse_rejects_wrong_dimension() {
        let payload = json!({
            "inference_results": [
                {"output": [{"data": [0.1, 0.2]}]}
            ]
        });
        let err = parse_inference_results(&payload, 1, 384, "m").unwrap_err();
        match err {
            StorageError::CorruptionSuspected { message } => {
                assert!(message.contains("2 維"), "{message}");
                assert!(message.contains("384"), "{message}");
            }
            other => panic!("預期 CorruptionSuspected，得到 {other}"),
        }
    }

    #[test]
    fn parse_accepts_matching_vectors() {
        let payload = json!({
            "inference_results": [
                {"output": [{"data": [0.1, 0.2, 0.3]}]},
                {"output": [{"data": [0.4, 0.5, 0.6]}]}
            ]
        });
        let vecs = parse_inference_results(&payload, 2, 3, "m").unwrap();
        assert_eq!(vecs.len(), 2);
        assert_eq!(vecs[0], vec![0.1, 0.2, 0.3]);
        assert_eq!(vecs[1], vec![0.4, 0.5, 0.6]);
    }

    #[test]
    fn content_hash_constants_match_documented_values() {
        assert_eq!(MINILM_CONTENT_HASH.len(), 64);
        assert_eq!(E5_CONTENT_HASH.len(), 64);
        assert_ne!(MINILM_CONTENT_HASH, E5_CONTENT_HASH);
    }
}
