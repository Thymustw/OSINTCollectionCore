//! SPEC §15 五階段去重 + §16 duplicate group。
//!
//! # 硬性規則（SPEC §16）
//!
//! **不得刪掉 duplicate evidence。** 這個服務不呼叫任何 `delete_*`：判定為重複時
//! 只寫一列 `DuplicateGroup` 並在 Document 上標 `duplicate_of`，
//! RawEvidence 與 Document 本體原封不動。Acceptance B／C 會實際數筆數驗證這件事。
//!
//! # 冪等
//!
//! 兩道互相獨立的保險：
//! 1. `DuplicateGroup.id` 是 **UUID v5**（namespace + member document id），不是 v7。
//!    同一份 Document 重複處理多少次，算出來都是同一個 id，`put_duplicate_group`
//!    是依主鍵 upsert，所以只會有一列。
//! 2. `provenance` 的部分 unique index `idx_provenance_dedup_subject`：
//!    同一個 `subject_id` 只能有一列 `action='deduplicated'`。並發兩個 consumer
//!    同時處理同一份 Document 時，輸家會拿到 `Conflict` 並回報 `AlreadyDone`。
//!
//! 落地順序沿用 normalizer 的 **write-then-claim**（先寫 group／Document，最後才 claim）。
//! 理由同 `docs/developer/collector-normalizer.md`：claim-first 的 crash window 會造成
//! 「宣稱處理過但其實沒寫」的靜默失效，而 write-then-claim 的 crash window 只會造成
//! 重跑——而且因為第 1 點是 v5 id，重跑連重複列都不會產生。

use std::sync::Arc;

use chrono::{DateTime, Utc};
use core_events::{EventProducer, EventTopic};
use core_model::{Document, Provenance};
use core_observability::MetricsRegistry;
use serde_json::{Value, json};
use storage_core::RelationalStore;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

use crate::error::DeduplicatorError;
use crate::semantic::{SemanticDuplicateDetector, SemanticOutcome, UnsupportedSemanticDetector};
use crate::{simhash, url_norm};

pub const PROCESSOR: &str = "deduplicator";
/// provenance 的冪等 claim 動作名。改這個字串等於讓所有既有 claim 失效，會重跑全部 Document。
pub const ACTION_DEDUPLICATED: &str = "deduplicated";

/// `DuplicateGroup.id` 的 UUID v5 namespace。
///
/// 固定常數，**不可更動**：改了之後同一份 Document 會算出不同的 group id，
/// 舊列不會被 upsert 覆蓋，而是多出一列——冪等保證直接失效（且不會報錯）。
const DUPLICATE_GROUP_NAMESPACE: Uuid = Uuid::from_u128(0x0199_4b2a_6d10_7c44_9f3b_1a2c_5d6e_7f80);

/// `duplicate_of` 鏈最多往上追幾層。正常情況永遠是 1 層（duplicate 直接指向 canonical），
/// 設 8 是為了在資料被外力改壞時**有界失敗**，而不是無限迴圈。
const MAX_CHAIN_DEPTH: u32 = 8;

/// 命中的階段（SPEC §15）。字串值會寫進 `DuplicateGroup.method`、provenance 與事件裡，
/// 改字串會讓既有資料無法對照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupStage {
    /// Stage 1：platform + external_id。
    PlatformExternalId,
    /// Stage 2：canonical URL。
    CanonicalUrl,
    /// Stage 3：SHA256(normalized content)。
    ContentHash,
    /// Stage 4：SimHash 近似重複。
    Simhash,
    /// Stage 5：語意重複（V0.1 不實作，見 `semantic.rs`）。
    Semantic,
}

impl DedupStage {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PlatformExternalId => "platform_external_id",
            Self::CanonicalUrl => "canonical_url",
            Self::ContentHash => "content_sha256",
            Self::Simhash => "simhash",
            Self::Semantic => "semantic",
        }
    }

    /// 階段序號（1..=5），給 log 與事件用。
    #[must_use]
    pub fn number(self) -> u8 {
        match self {
            Self::PlatformExternalId => 1,
            Self::CanonicalUrl => 2,
            Self::ContentHash => 3,
            Self::Simhash => 4,
            Self::Semantic => 5,
        }
    }
}

impl std::fmt::Display for DedupStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 一份 Document 的去重結果。
#[derive(Debug, Clone, PartialEq)]
pub enum DedupOutcome {
    /// 沒有命中任何階段：這份就是 canonical。
    Canonical { document_id: Uuid },
    /// 命中：已建立／更新 duplicate group。
    Duplicate {
        document_id: Uuid,
        canonical_object_id: Uuid,
        stage: DedupStage,
        similarity: f64,
        group_id: Uuid,
    },
    /// 已經處理過（provenance claim 存在）。重複消費同一個事件會走這條。
    AlreadyDone {
        document_id: Uuid,
        canonical_object_id: Option<Uuid>,
    },
    /// 事件提到的 Document 不在 DB。記 log 後跳過，不讓 consumer 掛掉。
    DocumentMissing { document_id: Uuid },
}

impl DedupOutcome {
    #[must_use]
    pub fn document_id(&self) -> Uuid {
        match self {
            Self::Canonical { document_id }
            | Self::Duplicate { document_id, .. }
            | Self::AlreadyDone { document_id, .. }
            | Self::DocumentMissing { document_id } => *document_id,
        }
    }
}

/// 去重的可調上限。全部都有硬性預設值，沒有「不限」這個選項（CLAUDE.md §5）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DedupBounds {
    /// Stage 1～3 每次最多取回幾筆候選。取第一筆（最舊）當 canonical，
    /// 其餘只用來判斷「是不是有一堆同鍵的資料」並記 log。
    pub candidate_limit: u32,
    /// Stage 4 的掃描上限。見 `find_simhash_candidates` 的契約說明。
    pub simhash_scan_limit: u32,
    /// Stage 4 的 Hamming 距離門檻。
    pub simhash_max_distance: u32,
}

impl Default for DedupBounds {
    fn default() -> Self {
        Self {
            candidate_limit: 20,
            simhash_scan_limit: 500,
            simhash_max_distance: simhash::DEFAULT_MAX_DISTANCE,
        }
    }
}

/// 生產用 deduplicator。
#[derive(Clone)]
pub struct Deduplicator {
    store: PostgresCanonicalStore,
    producer: Option<Arc<EventProducer>>,
    metrics: MetricsRegistry,
    bounds: DedupBounds,
    semantic: Arc<dyn SemanticDuplicateDetector>,
}

impl Deduplicator {
    #[must_use]
    pub fn new(
        store: PostgresCanonicalStore,
        producer: Option<Arc<EventProducer>>,
        metrics: MetricsRegistry,
        bounds: DedupBounds,
    ) -> Self {
        Self {
            store,
            producer,
            metrics,
            bounds,
            semantic: Arc::new(UnsupportedSemanticDetector),
        }
    }

    /// 換掉 Stage 5 的實作。V0.2 接上語意判斷時用。
    #[must_use]
    pub fn with_semantic_detector(mut self, detector: Arc<dyn SemanticDuplicateDetector>) -> Self {
        self.semantic = detector;
        self
    }

    #[must_use]
    pub fn store(&self) -> &PostgresCanonicalStore {
        &self.store
    }

    #[must_use]
    pub fn bounds(&self) -> DedupBounds {
        self.bounds
    }

    /// 處理一則 `object.normalized`：payload 的 `document_ids` 逐一去重。
    ///
    /// 單一 Document 失敗不會中止整批——一則事件可能帶十幾份 Document，
    /// 讓其中一份的錯誤吃掉其他份的處理機會沒有意義。失敗的那份會記 error log
    /// 並留在結果外，重新消費該事件時會再試一次（claim 還沒寫，不會被當成已處理）。
    pub async fn handle_payload(
        &self,
        payload: &Value,
    ) -> Result<Vec<DedupOutcome>, DeduplicatorError> {
        let ids = payload
            .get("document_ids")
            .and_then(Value::as_array)
            .ok_or_else(|| DeduplicatorError::MissingField {
                field: "document_ids".into(),
            })?;

        let mut outcomes = Vec::with_capacity(ids.len());
        for raw in ids {
            let Some(id) = raw.as_str().and_then(|s| Uuid::parse_str(s).ok()) else {
                tracing::warn!(value = %raw, "document_ids 含非 UUID 元素，跳過這一筆");
                continue;
            };
            match self.dedup_document(id).await {
                Ok(outcome) => outcomes.push(outcome),
                Err(err) => {
                    tracing::error!(
                        document_id = %id,
                        error = %err,
                        "這份 Document 去重失敗，繼續處理同一事件的其他 Document"
                    );
                }
            }
        }
        Ok(outcomes)
    }

    /// 對單一 Document 跑完五個階段。
    pub async fn dedup_document(
        &self,
        document_id: Uuid,
    ) -> Result<DedupOutcome, DeduplicatorError> {
        if let Some(done) = self.existing_claim(document_id).await? {
            return Ok(done);
        }

        let Some(document) = self.store.get_document(document_id).await? else {
            tracing::warn!(
                %document_id,
                "object.normalized 提到的 Document 不在 Postgres。\
                 事件可能早於資料被清掉，或 normalizer 寫入失敗；跳過不中斷消費"
            );
            return Ok(DedupOutcome::DocumentMissing { document_id });
        };

        let now = Utc::now();
        let keys = self.derive_keys(&document).await?;
        let hit = self.find_duplicate(&document, &keys).await?;

        // 不管有沒有命中，derive 出來的三個鍵都要寫回去：下一份 Document 要靠它們比對。
        // 只在命中時才寫的話，canonical 那份永遠沒有 external_key／simhash，
        // 後面九份就全都比不中——這正是「不報錯但靜默失效」的典型形狀。
        let mut updated = document.clone();
        updated.external_key = keys.external_key.clone();
        if let Some(url) = &keys.canonical_url {
            updated.canonical_url = Some(url.clone());
        }
        updated.simhash = keys.simhash;
        updated.duplicate_of = hit.as_ref().map(|h| h.canonical_object_id);

        let group_id = match &hit {
            Some(hit) => Some(self.write_group(&updated, hit, now).await?),
            None => None,
        };
        self.store.put_document(&updated).await?;

        match self.claim(&updated, hit.as_ref(), &keys, now).await {
            Ok(()) => {}
            Err(DeduplicatorError::Storage(storage_core::StorageError::Conflict { .. })) => {
                // 另一個 consumer 搶先 claim 了。回報它的結果，不要覆寫。
                if let Some(done) = self.existing_claim(document_id).await? {
                    return Ok(done);
                }
                return Err(DeduplicatorError::Storage(
                    storage_core::StorageError::Conflict {
                        message: format!(
                            "deduplicated provenance 衝突，但讀不到既有列。\
                             請查 `SELECT * FROM provenance WHERE subject_id = '{document_id}' AND action = '{ACTION_DEDUPLICATED}'`"
                        ),
                    },
                ));
            }
            Err(err) => return Err(err),
        }

        let outcome = match (hit, group_id) {
            (Some(hit), Some(group_id)) => {
                self.metrics.inc_duplicate(1);
                tracing::info!(
                    %document_id,
                    canonical_object_id = %hit.canonical_object_id,
                    stage = hit.stage.as_str(),
                    stage_number = hit.stage.number(),
                    similarity = hit.similarity,
                    "判定為重複，已建立 duplicate group（未刪除任何證據）"
                );
                DedupOutcome::Duplicate {
                    document_id,
                    canonical_object_id: hit.canonical_object_id,
                    stage: hit.stage,
                    similarity: hit.similarity,
                    group_id,
                }
            }
            _ => {
                tracing::info!(%document_id, "五個階段都沒有命中，這份是 canonical");
                DedupOutcome::Canonical { document_id }
            }
        };
        self.publish(&outcome).await?;
        Ok(outcome)
    }

    /// 讀既有的 `deduplicated` claim。有就代表這份處理過了。
    async fn existing_claim(
        &self,
        document_id: Uuid,
    ) -> Result<Option<DedupOutcome>, DeduplicatorError> {
        let rows = self.store.list_provenance_by_subject(document_id).await?;
        let Some(claim) = rows.iter().find(|p| p.action == ACTION_DEDUPLICATED) else {
            return Ok(None);
        };
        let canonical_object_id = claim
            .metadata
            .get("canonical_object_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok());
        Ok(Some(DedupOutcome::AlreadyDone {
            document_id,
            canonical_object_id,
        }))
    }

    /// 算出五階段需要的鍵。CPU-bound 的 SimHash 丟到 `spawn_blocking`。
    async fn derive_keys(&self, document: &Document) -> Result<DedupKeys, DeduplicatorError> {
        let platform = self.platform_of(document).await?;
        let external_id = document
            .attributes
            .get("external_id")
            .and_then(Value::as_str)
            .map(str::to_string);
        // 兩者都有值才構成 Stage 1 的鍵。只有其中一個時不要拿空字串湊——
        // 那會讓所有「沒有 platform 的來源」互相撞在同一個鍵上。
        let external_key = match (platform.as_deref(), external_id.as_deref()) {
            (Some(platform), Some(external_id))
                if !platform.is_empty() && !external_id.is_empty() =>
            {
                Some(format!("{platform}|{external_id}"))
            }
            _ => None,
        };

        let canonical_url = document
            .source_url
            .as_deref()
            .and_then(url_norm::canonicalize);
        if canonical_url.is_none() {
            if let Some(raw) = document.source_url.as_deref() {
                tracing::debug!(
                    document_id = %document.id,
                    source_url = %url_norm::short(raw),
                    "source_url 不是合法絕對 URL，Stage 2 對這份不適用"
                );
            }
        }

        // Stage 3 的 hash 由 normalizer 寫；這裡只在它缺席時補算（例如舊資料），
        // 用的是 core-model 裡的同一份定義，不會分岔。
        let content_hash = document.normalized_content_hash.clone().or_else(|| {
            Some(core_model::content_hash(
                document.title.as_deref(),
                document.summary.as_deref(),
                document.body.as_deref(),
            ))
        });

        // SimHash 要掃過整篇正文並做數百次 SHA256，是 CPU-bound 工作。
        // CLAUDE.md §6：CPU-heavy 不可 block Tokio executor thread。
        let text = simhash_input(document);
        let simhash = tokio::task::spawn_blocking(move || simhash::fingerprint(&text))
            .await
            .map_err(|err| DeduplicatorError::HashTaskFailed {
                message: err.to_string(),
            })?;

        Ok(DedupKeys {
            platform,
            external_id,
            external_key,
            canonical_url,
            content_hash,
            simhash,
        })
    }

    /// 從 Document → RawEvidence → Source 取 `platform`。
    ///
    /// `platform` 存在 Source 上而不是 Document 上，所以 Stage 1 必須繞這一圈。
    /// 任一環節缺席就回 `None`（Stage 1 對這份不適用），不是錯誤。
    async fn platform_of(&self, document: &Document) -> Result<Option<String>, DeduplicatorError> {
        let Some(raw_id) = document
            .attributes
            .get("raw_evidence_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
        else {
            return Ok(None);
        };
        let Some(evidence) = self.store.get_raw_evidence(raw_id).await? else {
            return Ok(None);
        };
        let Some(source) = self.store.get_source(evidence.source_id).await? else {
            return Ok(None);
        };
        Ok(source.platform)
    }

    /// 依序跑 Stage 1～5，第一個命中就回傳。
    async fn find_duplicate(
        &self,
        document: &Document,
        keys: &DedupKeys,
    ) -> Result<Option<DuplicateHit>, DeduplicatorError> {
        if let Some(key) = &keys.external_key {
            let candidates = self
                .store
                .find_document_ids_by_external_key(key, document.id, self.bounds.candidate_limit)
                .await?;
            if let Some(canonical) = self.first_canonical(&candidates).await? {
                return Ok(Some(DuplicateHit {
                    canonical_object_id: canonical,
                    stage: DedupStage::PlatformExternalId,
                    // 同一個平台的同一個 external_id 就是同一筆，沒有程度問題。
                    similarity: 1.0,
                }));
            }
        }

        if let Some(url) = &keys.canonical_url {
            let candidates = self
                .store
                .find_document_ids_by_canonical_url(url, document.id, self.bounds.candidate_limit)
                .await?;
            if let Some(canonical) = self.first_canonical(&candidates).await? {
                return Ok(Some(DuplicateHit {
                    canonical_object_id: canonical,
                    stage: DedupStage::CanonicalUrl,
                    similarity: 1.0,
                }));
            }
        }

        if let Some(hash) = &keys.content_hash {
            let candidates = self
                .store
                .find_document_ids_by_content_hash(hash, document.id, self.bounds.candidate_limit)
                .await?;
            if let Some(canonical) = self.first_canonical(&candidates).await? {
                return Ok(Some(DuplicateHit {
                    canonical_object_id: canonical,
                    stage: DedupStage::ContentHash,
                    similarity: 1.0,
                }));
            }
        }

        if let Some(fingerprint) = keys.simhash {
            let candidates = self
                .store
                .find_simhash_candidates(
                    fingerprint,
                    self.bounds.simhash_max_distance,
                    document.id,
                    self.bounds.simhash_scan_limit,
                )
                .await?;
            // 最近的（距離最小的）優先；同距離時取較舊的那筆。
            let best = candidates.iter().min_by_key(|c| {
                (
                    simhash::hamming_distance(c.simhash, fingerprint),
                    c.id.as_u128(),
                )
            });
            if let Some(best) = best {
                let distance = simhash::hamming_distance(best.simhash, fingerprint);
                if let Some(canonical) = self.resolve_canonical(best.id).await? {
                    return Ok(Some(DuplicateHit {
                        canonical_object_id: canonical,
                        stage: DedupStage::Simhash,
                        similarity: simhash::similarity(distance),
                    }));
                }
            }
        }

        match self.semantic.detect(document).await? {
            SemanticOutcome::Hit {
                canonical_object_id,
                similarity,
            } => Ok(Some(DuplicateHit {
                canonical_object_id,
                stage: DedupStage::Semantic,
                similarity,
            })),
            // V0.1 的實作永遠走這裡。Unsupported 與 NoMatch 對流程的效果相同，
            // 差別只在寫進 provenance 的字串——那是「這份資料是在有沒有 Stage 5 的年代處理的」
            // 唯一的判斷依據。
            SemanticOutcome::Unsupported | SemanticOutcome::NoMatch => Ok(None),
        }
    }

    /// 取候選清單中第一筆（最舊）並解析出它的 canonical。
    async fn first_canonical(
        &self,
        candidates: &[Uuid],
    ) -> Result<Option<Uuid>, DeduplicatorError> {
        let Some(first) = candidates.first().copied() else {
            return Ok(None);
        };
        if candidates.len() >= self.bounds.candidate_limit as usize {
            tracing::warn!(
                candidate_limit = self.bounds.candidate_limit,
                "同一個 dedup 鍵的候選數已達上限，可能還有更多同鍵資料。\
                 canonical 仍取最舊的那筆，結果正確，但若這個訊息頻繁出現代表上游在重複灌資料"
            );
        }
        self.resolve_canonical(first).await
    }

    /// 沿著 `duplicate_of` 往上追到 canonical。
    ///
    /// 候選一定比自己舊（見 `find_document_ids_by_*` 的 `before` 契約），
    /// 所以鏈一定是遞減的、不可能成環；`MAX_CHAIN_DEPTH` 是資料被外力改壞時的保險，
    /// 觸發時**回報錯誤而不是靜默取最後一個**——靜默的話會建出指向錯誤 canonical 的 group。
    async fn resolve_canonical(&self, start: Uuid) -> Result<Option<Uuid>, DeduplicatorError> {
        let mut current = start;
        for _ in 0..MAX_CHAIN_DEPTH {
            let Some(doc) = self.store.get_document(current).await? else {
                // 鏈斷了（指向的 Document 被刪）。回報目前這一段的起點，
                // 總比讓整份 Document 處理失敗好。
                tracing::warn!(
                    document_id = %current,
                    "duplicate_of 指向不存在的 Document，鏈在這裡斷掉"
                );
                return Ok(None);
            };
            match doc.duplicate_of {
                Some(next) if next != current => current = next,
                _ => return Ok(Some(current)),
            }
        }
        Err(DeduplicatorError::DuplicateChainTooDeep {
            id: start.to_string(),
            max: MAX_CHAIN_DEPTH,
        })
    }

    /// 寫（或 upsert）duplicate group。回傳 group id。
    async fn write_group(
        &self,
        document: &Document,
        hit: &DuplicateHit,
        now: DateTime<Utc>,
    ) -> Result<Uuid, DeduplicatorError> {
        let id = duplicate_group_id(document.id);
        // first_seen 語意是「這組重複關係第一次被看到的時間」。已存在就沿用舊值，
        // 不要每次重跑都往後推——那會讓「第一次發現」這個欄位變成「最後一次處理」。
        let first_seen = match self.store.get_duplicate_group(id).await? {
            Some(existing) => existing.first_seen,
            None => now,
        };
        let group = core_model::DuplicateGroup {
            id,
            canonical_object_id: hit.canonical_object_id,
            member_object_id: Some(document.id),
            // member 的 RawEvidence 一併記下來：SPEC §16 的 member 可以是 object 或
            // raw evidence，兩個都填才能從 group 直接回查到「這份重複的原始證據還在」。
            member_raw_evidence_id: document
                .attributes
                .get("raw_evidence_id")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok()),
            method: hit.stage.as_str().into(),
            similarity: hit.similarity,
            first_seen,
        };
        self.store.put_duplicate_group(&group).await?;
        Ok(id)
    }

    /// 佔下 `deduplicated` claim（unique index 保證只有一列）。
    async fn claim(
        &self,
        document: &Document,
        hit: Option<&DuplicateHit>,
        keys: &DedupKeys,
        now: DateTime<Utc>,
    ) -> Result<(), DeduplicatorError> {
        let claim = Provenance {
            id: Uuid::now_v7(),
            subject_id: document.id,
            action: ACTION_DEDUPLICATED.into(),
            parent_id: hit.map(|h| h.canonical_object_id),
            raw_evidence_id: document
                .attributes
                .get("raw_evidence_id")
                .and_then(Value::as_str)
                .and_then(|s| Uuid::parse_str(s).ok()),
            processor: PROCESSOR.into(),
            processor_version: env!("CARGO_PKG_VERSION").into(),
            timestamp: now,
            metadata: json!({
                "is_duplicate": hit.is_some(),
                "stage": hit.map(|h| h.stage.as_str()),
                "stage_number": hit.map(|h| h.stage.number()),
                "canonical_object_id": hit.map(|h| h.canonical_object_id),
                "similarity": hit.map(|h| h.similarity),
                "external_key": keys.external_key,
                "canonical_url": keys.canonical_url,
                "content_sha256": keys.content_hash,
                "simhash": keys.simhash,
                "semantic_detector": self.semantic.detector_id(),
                "simhash_max_distance": self.bounds.simhash_max_distance,
            }),
        };
        self.store.put_provenance(&claim).await?;
        Ok(())
    }

    async fn publish(&self, outcome: &DedupOutcome) -> Result<(), DeduplicatorError> {
        let Some(producer) = &self.producer else {
            return Ok(());
        };
        let document_id = outcome.document_id();
        let payload = match outcome {
            DedupOutcome::Duplicate {
                canonical_object_id,
                stage,
                similarity,
                group_id,
                ..
            } => json!({
                "document_id": document_id,
                "is_duplicate": true,
                "stage": stage.as_str(),
                "stage_number": stage.number(),
                "canonical_object_id": canonical_object_id,
                "similarity": similarity,
                "duplicate_group_id": group_id,
            }),
            DedupOutcome::Canonical { .. } => json!({
                "document_id": document_id,
                "is_duplicate": false,
                "stage": Value::Null,
                "canonical_object_id": document_id,
            }),
            // 已處理／找不到都不發事件：下游的 entity extraction 對同一份 Document
            // 收到兩次 dedup.completed 沒有意義，而且會讓「事件數 == 處理數」失效。
            DedupOutcome::AlreadyDone { .. } | DedupOutcome::DocumentMissing { .. } => {
                return Ok(());
            }
        };
        producer
            .publish(
                EventTopic::DedupCompleted,
                Some(&document_id.to_string()),
                Some(document_id),
                payload,
            )
            .await?;
        Ok(())
    }
}

/// 五階段要用到的判斷依據。
#[derive(Debug, Clone, PartialEq)]
struct DedupKeys {
    platform: Option<String>,
    external_id: Option<String>,
    external_key: Option<String>,
    canonical_url: Option<String>,
    content_hash: Option<String>,
    simhash: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
struct DuplicateHit {
    canonical_object_id: Uuid,
    stage: DedupStage,
    similarity: f64,
}

/// `DuplicateGroup.id` = UUID v5(namespace, member document id)。
///
/// 決定性的 id 就是冪等的來源：重跑算出同一個 id → upsert 同一列 → 不會有第二個 group。
#[must_use]
pub fn duplicate_group_id(member_object_id: Uuid) -> Uuid {
    Uuid::new_v5(&DUPLICATE_GROUP_NAMESPACE, member_object_id.as_bytes())
}

/// SimHash 的輸入：標題 + 摘要 + 正文。
///
/// 標題也納入是刻意的：轉載通常連標題一起搬，標題是強訊號。
/// 但標題只佔全文很小比例，權重不會壓過正文。
fn simhash_input(document: &Document) -> String {
    let mut text = String::new();
    for part in [
        document.title.as_deref(),
        document.summary.as_deref(),
        document.body.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        text.push_str(part);
        text.push(' ');
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::DocumentType;

    fn document(title: &str, body: &str) -> Document {
        Document {
            id: Uuid::now_v7(),
            object_type: DocumentType::Article,
            schema_version: "1".into(),
            title: Some(title.into()),
            body: Some(body.into()),
            summary: None,
            language: None,
            author: None,
            published_at: None,
            modified_at: None,
            observed_at: Utc::now(),
            collected_at: Utc::now(),
            source_url: None,
            canonical_url: None,
            normalized_content_hash: None,
            confidence: 0.8,
            labels: Vec::new(),
            attributes: json!({}),
            external_key: None,
            simhash: None,
            duplicate_of: None,
        }
    }

    #[test]
    fn stage_names_are_stable() {
        // 這些字串會寫進 DuplicateGroup.method 與 dedup.completed。
        // 改動等於讓既有資料無法對照，所以釘住。
        assert_eq!(
            DedupStage::PlatformExternalId.as_str(),
            "platform_external_id"
        );
        assert_eq!(DedupStage::CanonicalUrl.as_str(), "canonical_url");
        assert_eq!(DedupStage::ContentHash.as_str(), "content_sha256");
        assert_eq!(DedupStage::Simhash.as_str(), "simhash");
        assert_eq!(DedupStage::Semantic.as_str(), "semantic");
        assert_eq!(DedupStage::PlatformExternalId.number(), 1);
        assert_eq!(DedupStage::Semantic.number(), 5);
    }

    #[test]
    fn duplicate_group_id_is_deterministic_per_member() {
        let member = Uuid::now_v7();
        assert_eq!(duplicate_group_id(member), duplicate_group_id(member));
        assert_ne!(
            duplicate_group_id(member),
            duplicate_group_id(Uuid::now_v7())
        );
        assert_eq!(
            duplicate_group_id(member).get_version(),
            Some(uuid::Version::Sha1),
            "必須是 v5；換成 v7 就失去冪等性"
        );
    }

    #[test]
    fn simhash_input_joins_title_summary_body() {
        let mut doc = document("標題", "正文");
        doc.summary = Some("摘要".into());
        assert_eq!(simhash_input(&doc), "標題 摘要 正文 ");
        doc.summary = None;
        doc.body = None;
        assert_eq!(simhash_input(&doc), "標題 ");
    }

    #[test]
    fn outcome_exposes_document_id_for_every_variant() {
        let id = Uuid::now_v7();
        assert_eq!(
            DedupOutcome::Canonical { document_id: id }.document_id(),
            id
        );
        assert_eq!(
            DedupOutcome::DocumentMissing { document_id: id }.document_id(),
            id
        );
        assert_eq!(
            DedupOutcome::AlreadyDone {
                document_id: id,
                canonical_object_id: None
            }
            .document_id(),
            id
        );
        assert_eq!(
            DedupOutcome::Duplicate {
                document_id: id,
                canonical_object_id: Uuid::now_v7(),
                stage: DedupStage::Simhash,
                similarity: 0.98,
                group_id: duplicate_group_id(id),
            }
            .document_id(),
            id
        );
    }

    #[test]
    fn default_bounds_are_finite() {
        let bounds = DedupBounds::default();
        assert_eq!(bounds.candidate_limit, 20);
        assert_eq!(bounds.simhash_scan_limit, 500);
        assert_eq!(bounds.simhash_max_distance, simhash::DEFAULT_MAX_DISTANCE);
    }
}
