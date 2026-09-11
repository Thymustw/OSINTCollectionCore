//! SPEC §17 entity extraction + §11／§12 Relationship 與 RelationshipEvidence + §14 Provenance。
//!
//! # 只處理 canonical Document
//!
//! `dedup.completed` 帶 `is_duplicate`。`true` 的直接跳過：那份 Document 已經指向
//! canonical，對它再抽一次只會產生**指向重複文件的**第二組 extraction 與 relationship。
//! Entity 本身不會重複（自然鍵擋住），但 `entity_extractions` 會多出一批
//! `object_id` 指向重複文件的列，讓「這個 CVE 出現在幾篇文章」全部多算。
//!
//! # 冪等（三道互相獨立的保險）
//!
//! 1. **provenance claim**：`idx_provenance_entity_subject` 保證同一個 `subject_id`
//!    只能有一列 `action='entity_extracted'`。重複消費同一事件會在入口就被擋下。
//! 2. **決定性 id**：Entity／Relationship／RelationshipEvidence／EntityExtraction 的 id
//!    全部是 **UUID v5**，由各自的自然鍵推導。所有 `put_*` 都是依主鍵 upsert，
//!    所以重跑寫的是同一列，不會長出第二列。
//! 3. **資料庫唯一鍵**：`idx_entities_natural_key` 與 `idx_relationships_natural_key`
//!    擋掉任何繞過第 2 點的寫入路徑。
//!
//! 第 2 點是主力，第 1 點省掉重複工，第 3 點是防止「有人用別的方式塞資料」。
//! 只靠 claim 是不夠的——claim 寫入前 crash 就會重跑，那時要靠 v5 id 才不會產生重複。
//!
//! # 落地順序：write-then-claim
//!
//! 沿用 normalizer／deduplicator 的順序（先寫 Entity/Relationship/Extraction，最後才
//! claim）。理由見 `docs/developer/collector-normalizer.md`：claim-first 的 crash window
//! 會造成「宣稱處理過但其實沒寫」的靜默失效，而 write-then-claim 的 crash window
//! 只會造成重跑——而且因為第 2 點是 v5 id，重跑連重複列都不會產生。

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use core_events::{EventProducer, EventTopic};
use core_model::{
    Document, Entity, EntityExtraction, EntityType, Provenance, Relationship, RelationshipEvidence,
    RelationshipType,
};
use core_observability::MetricsRegistry;
use serde_json::{Value, json};
use storage_core::RelationalStore;
use storage_postgres::PostgresCanonicalStore;
use uuid::Uuid;

use crate::error::EntityWorkerError;
use crate::extract::{
    self, DerivedLink, EXTRACTOR_VERSION, Extracted, ExtractionBounds, ExtractionInput,
    truncate_on_char_boundary,
};

pub const PROCESSOR: &str = "entity-worker";

/// provenance 的冪等 claim 動作名。
///
/// **改這個字串等於讓所有既有 claim 失效**，會重跑全部 Document。
/// 而且 `migrations/*/0005_entity_natural_key.sql` 的部分索引把它寫死在 `WHERE` 子句裡，
/// 只改這裡不改 migration 的話唯一性保證會**靜默消失**（不報錯，只是不再擋重複）。
pub const ACTION_ENTITY_EXTRACTED: &str = "entity_extracted";

/// 各種衍生 id 的 UUID v5 namespace。
///
/// 固定常數，**不可更動**：改了之後同一個輸入會算出不同 id，舊列不會被 upsert 覆蓋，
/// 而是多出一列——冪等保證直接失效（且不會報錯）。
/// 四個 namespace 彼此不同，避免「Entity 的自然鍵」與「Relationship 的自然鍵」
/// 剛好算出同一個 UUID。
const ENTITY_NAMESPACE: Uuid = Uuid::from_u128(0x0199_5c31_7e20_7a55_8b4c_2d3e_6f70_8091);
const RELATIONSHIP_NAMESPACE: Uuid = Uuid::from_u128(0x0199_5c31_7e20_7a55_8b4c_2d3e_6f70_8092);
const REL_EVIDENCE_NAMESPACE: Uuid = Uuid::from_u128(0x0199_5c31_7e20_7a55_8b4c_2d3e_6f70_8093);
const EXTRACTION_NAMESPACE: Uuid = Uuid::from_u128(0x0199_5c31_7e20_7a55_8b4c_2d3e_6f70_8094);

/// 從 `Document.attributes` 取 Organization 的欄位名（依序找，全部都取）。
///
/// 只從**結構化欄位**取是 V0.1 的刻意範圍限制（見 `extract.rs` 模組說明）。
const ORGANIZATION_FIELDS: &[&str] = &[
    "publisher",
    "organization",
    "organisation",
    "vendor",
    "feed_title",
    "site_name",
];

/// 一份 Document 的處理結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtractOutcome {
    /// 抽取完成。
    Extracted {
        document_id: Uuid,
        /// 建立或重用的 Entity 數（去重後）。
        entity_count: usize,
        /// 寫入的 `entity_extractions` 筆數（同一 Entity 在文中出現多次會有多筆）。
        extraction_count: usize,
        /// 建立或更新的 Relationship 數。
        relationship_count: usize,
        /// 命中數超過上限而被截斷。
        truncated: bool,
    },
    /// `is_duplicate = true`，跳過不抽取。
    SkippedDuplicate { document_id: Uuid },
    /// 已經處理過（provenance claim 存在）。重複消費同一個事件會走這條。
    AlreadyDone { document_id: Uuid },
    /// 事件提到的 Document 不在 DB。記 log 後跳過，不讓 consumer 掛掉。
    DocumentMissing { document_id: Uuid },
}

impl ExtractOutcome {
    #[must_use]
    pub fn document_id(&self) -> Uuid {
        match self {
            Self::Extracted { document_id, .. }
            | Self::SkippedDuplicate { document_id }
            | Self::AlreadyDone { document_id }
            | Self::DocumentMissing { document_id } => *document_id,
        }
    }
}

/// 生產用 entity-worker。
#[derive(Clone)]
pub struct EntityWorker {
    store: PostgresCanonicalStore,
    producer: Option<Arc<EventProducer>>,
    metrics: MetricsRegistry,
    bounds: ExtractionBounds,
}

impl EntityWorker {
    #[must_use]
    pub fn new(
        store: PostgresCanonicalStore,
        producer: Option<Arc<EventProducer>>,
        metrics: MetricsRegistry,
        bounds: ExtractionBounds,
    ) -> Self {
        Self {
            store,
            producer,
            metrics,
            bounds,
        }
    }

    #[must_use]
    pub fn store(&self) -> &PostgresCanonicalStore {
        &self.store
    }

    #[must_use]
    pub fn bounds(&self) -> ExtractionBounds {
        self.bounds
    }

    /// 處理一則 `dedup.completed`。
    ///
    /// payload 形狀由 `deduplicator::Deduplicator::publish` 決定：
    /// `{ document_id, is_duplicate, stage, canonical_object_id, ... }`。
    pub async fn handle_payload(
        &self,
        payload: &Value,
    ) -> Result<ExtractOutcome, EntityWorkerError> {
        let document_id = payload
            .get("document_id")
            .and_then(Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            .ok_or_else(|| EntityWorkerError::MissingField {
                field: "document_id".into(),
            })?;
        // 缺 `is_duplicate` 要**報錯**而不是預設成 false。預設成 false 會讓所有重複文件
        // 都被抽取，那正是這個服務最該避免的事——而且不會有任何錯誤跡象。
        let is_duplicate = payload
            .get("is_duplicate")
            .and_then(Value::as_bool)
            .ok_or_else(|| EntityWorkerError::MissingField {
                field: "is_duplicate".into(),
            })?;

        if is_duplicate {
            tracing::debug!(
                %document_id,
                "dedup.completed 標記為重複，跳過抽取（duplicate 已指向 canonical）"
            );
            self.metrics.inc("osint_entity_skipped_duplicate_total", 1);
            return Ok(ExtractOutcome::SkippedDuplicate { document_id });
        }
        self.extract_document(document_id).await
    }

    /// 對單一 Document 做抽取。
    pub async fn extract_document(
        &self,
        document_id: Uuid,
    ) -> Result<ExtractOutcome, EntityWorkerError> {
        if self.already_claimed(document_id).await? {
            return Ok(ExtractOutcome::AlreadyDone { document_id });
        }

        let Some(document) = self.store.get_document(document_id).await? else {
            tracing::warn!(
                %document_id,
                "dedup.completed 提到的 Document 不在 Postgres。\
                 事件可能早於資料被清掉，或 deduplicator 與本服務看的不是同一個資料庫；\
                 跳過不中斷消費"
            );
            return Ok(ExtractOutcome::DocumentMissing { document_id });
        };

        // 第二道防線：即使事件說 is_duplicate=false，DB 上的 duplicate_of 才是事實。
        // 事件可能是舊的（在該 Document 被判成重複之前發出）。
        if let Some(canonical) = document.duplicate_of {
            tracing::info!(
                %document_id,
                canonical_object_id = %canonical,
                "Document 在 DB 上已標記為重複（事件內容較舊），跳過抽取"
            );
            self.metrics.inc("osint_entity_skipped_duplicate_total", 1);
            return Ok(ExtractOutcome::SkippedDuplicate { document_id });
        }

        let now = Utc::now();
        let raw_evidence_id = raw_evidence_id_of(&document);

        // ---- CPU-bound 的 regex 掃描 ----
        // CLAUDE.md §6：CPU-heavy work must not block Tokio executor threads。
        // 一份 256 KiB 的文章要跑八組 regex，在 executor thread 上做會卡住
        // 同一個 runtime 上的所有 I/O（含 health endpoint 與 broker 心跳）。
        let input = extraction_input(&document, self.bounds);
        let bounds = self.bounds;
        let result = tokio::task::spawn_blocking(move || extract::extract_all(&input, bounds))
            .await
            .map_err(|err| EntityWorkerError::ExtractionTaskFailed {
                message: err.to_string(),
            })?;

        if result.truncated {
            tracing::warn!(
                %document_id,
                total_candidates = result.total_candidates,
                max_extractions = self.bounds.max_extractions,
                "抽取數超過上限，已截斷。這份 Document 的 entity 不完整——\
                 若頻繁出現，代表上游在灌 IOC 傾印檔，請調 [entity_worker].max_extractions \
                 或在 connector 端過濾"
            );
            self.metrics.inc("osint_entity_truncated_total", 1);
        }

        // ---- 落地 ----
        // 自然鍵 → EntityId。同一個 CVE 在文中出現三次，Entity 只寫一次，extraction 寫三次。
        //
        // 用 `BTreeMap` 而不是 `HashMap`：`entity.extracted` 事件會帶 `entity_ids` 陣列，
        // HashMap 的迭代順序每次執行都不同，同一份 Document 重跑會發出**內容相同但順序不同**
        // 的事件——下游若拿 payload 做比對就會誤判成有變動。
        //
        // key 是 `(entity_type 的 serde 字串, normalized_name)`。用字串是因為 `EntityType`
        // 只 derive 了 `Hash`/`Eq` 沒有 `Ord`；serde 字串與寫進資料庫的、以及 v5 id 的
        // 雜湊輸入是同一套，不會分岔。
        let mut entity_ids: BTreeMap<(String, String), Uuid> = BTreeMap::new();
        let mut extraction_count = 0_usize;
        let mut relationship_ids: BTreeSet<Uuid> = BTreeSet::new();

        for item in &result.items {
            let key = natural_key(item.entity_type, &item.normalized_name);
            let entity_id = match entity_ids.get(&key) {
                Some(id) => *id,
                None => {
                    let id = self.upsert_entity(item, now).await?;
                    entity_ids.insert(key.clone(), id);
                    id
                }
            };

            self.write_extraction(&document, entity_id, item).await?;
            extraction_count += 1;

            // Document → Entity 的 relationship（SPEC §11）+ evidence（SPEC §12）。
            let rel_type = mention_relationship_type(item);
            let rel_id = self
                .upsert_relationship(document.id, rel_type, entity_id, item.confidence, now)
                .await?;
            self.write_relationship_evidence(rel_id, &document, raw_evidence_id, item, now)
                .await?;
            relationship_ids.insert(rel_id);
        }

        // Domain ↔ URL / Domain ↔ Email 的 relationship。
        // 必須在上面那個迴圈跑完之後：來源那一端（URL／Email）的 Entity 要先存在。
        for item in &result.items {
            let Some(link) = &item.derived_from else {
                continue;
            };
            let target_key = natural_key(item.entity_type, &item.normalized_name);
            let source_key = natural_key(link.entity_type, &link.normalized_name);
            let (Some(target_id), Some(source_id)) =
                (entity_ids.get(&target_key), entity_ids.get(&source_key))
            else {
                // 來源端被截斷截掉了。跳過這條邊而不是報錯——少一條關聯好過整份失敗。
                tracing::debug!(
                    %document_id,
                    "衍生關聯的來源端不在本次抽取結果中（多半是被上限截斷），跳過這條邊"
                );
                continue;
            };
            let rel_type = derived_relationship_type(link);
            let rel_id = self
                .upsert_relationship(*source_id, rel_type, *target_id, item.confidence, now)
                .await?;
            self.write_relationship_evidence(rel_id, &document, raw_evidence_id, item, now)
                .await?;
            relationship_ids.insert(rel_id);
        }

        // ---- claim（write-then-claim 的最後一步）----
        let outcome = ExtractOutcome::Extracted {
            document_id,
            entity_count: entity_ids.len(),
            extraction_count,
            relationship_count: relationship_ids.len(),
            truncated: result.truncated,
        };
        match self
            .claim(&document, raw_evidence_id, &outcome, &result, now)
            .await
        {
            Ok(()) => {}
            Err(EntityWorkerError::Storage(storage_core::StorageError::Conflict { .. })) => {
                // 另一個 consumer 搶先 claim 了。它寫的內容與我們算出來的相同
                // （抽取是確定性的、id 是 v5），所以直接回報「已處理」，不要覆寫。
                tracing::info!(
                    %document_id,
                    "另一個 consumer 已經 claim 這份 Document，本次視為已處理"
                );
                return Ok(ExtractOutcome::AlreadyDone { document_id });
            }
            Err(err) => return Err(err),
        }

        self.metrics
            .inc("osint_entity_extracted_total", extraction_count as u64);
        self.metrics
            .inc("osint_entities_total", entity_ids.len() as u64);
        self.metrics
            .inc("osint_relationships_total", relationship_ids.len() as u64);
        tracing::info!(
            %document_id,
            entity_count = entity_ids.len(),
            extraction_count,
            relationship_count = relationship_ids.len(),
            truncated = result.truncated,
            "entity 抽取完成"
        );

        self.publish(&outcome, &entity_ids).await?;
        Ok(outcome)
    }

    /// 這份 Document 是否已有 `entity_extracted` claim。
    async fn already_claimed(&self, document_id: Uuid) -> Result<bool, EntityWorkerError> {
        let rows = self.store.list_provenance_by_subject(document_id).await?;
        Ok(rows.iter().any(|p| p.action == ACTION_ENTITY_EXTRACTED))
    }

    /// 建立或重用 Entity。
    ///
    /// 先用自然鍵查既有列：查到就只更新 `last_seen`（與可能變動的 attributes），
    /// **保留原本的 `first_seen`**——那個欄位的語意是「第一次看到這個實體」，
    /// 每次重跑都往後推的話它會變成「最後一次處理時間」，失去全部意義。
    async fn upsert_entity(
        &self,
        item: &Extracted,
        now: DateTime<Utc>,
    ) -> Result<Uuid, EntityWorkerError> {
        let existing = self
            .store
            .find_entity_by_normalized_name(item.entity_type, &item.normalized_name)
            .await?;

        let mut attributes = serde_json::Map::new();
        if let Some(existing) = &existing {
            // 既有 attributes 先保留：別的抽取器（或 V0.3 的 AI）可能寫過東西進去，
            // 直接覆蓋等於默默刪掉別人的資料。
            if let Some(map) = existing.attributes.as_object() {
                attributes.extend(map.clone());
            }
        }
        for (key, value) in &item.attributes {
            attributes.insert(key.clone(), value.clone());
        }

        let entity = Entity {
            id: existing.as_ref().map_or_else(
                || entity_id(item.entity_type, &item.normalized_name),
                |e| e.id,
            ),
            entity_type: item.entity_type,
            // `name` 保留**第一次**看到的原文寫法。後面看到不同大小寫時不覆蓋——
            // 沒有理由認為後來那份比較正確，而且會讓這一欄每處理一份文件就跳動一次。
            name: existing
                .as_ref()
                .map_or_else(|| item.name.clone(), |e| e.name.clone()),
            normalized_name: item.normalized_name.clone(),
            description: existing.as_ref().and_then(|e| e.description.clone()),
            // 信心取兩者較高者：同一個實體被高信心的抽取器命中過就不該被降回去。
            confidence: existing
                .as_ref()
                .map_or(item.confidence, |e| e.confidence.max(item.confidence)),
            first_seen: existing.as_ref().map_or(now, |e| e.first_seen),
            last_seen: now,
            attributes: Value::Object(attributes),
        };

        match self.store.put_entity(&entity).await {
            Ok(()) => Ok(entity.id),
            Err(storage_core::StorageError::Conflict { .. }) => {
                // 並發：另一個 worker 在我們查完之後、寫入之前先建了同一個 Entity，
                // 而且它用的 id 與我們算的不同（例如資料是舊版程式寫的）。
                // 重查一次拿它的 id，不要讓整份 Document 失敗。
                let found = self
                    .store
                    .find_entity_by_normalized_name(item.entity_type, &item.normalized_name)
                    .await?
                    .ok_or_else(|| EntityWorkerError::EntityConflict {
                        entity_type: format!("{:?}", item.entity_type).to_lowercase(),
                        normalized_name: item.normalized_name.clone(),
                    })?;
                Ok(found.id)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// 寫一列 `entity_extractions`（SPEC §17）。
    async fn write_extraction(
        &self,
        document: &Document,
        entity_id: Uuid,
        item: &Extracted,
    ) -> Result<(), EntityWorkerError> {
        let extraction = EntityExtraction {
            id: extraction_id(document.id, entity_id, item.extractor, item.text_offset),
            object_id: document.id,
            entity_id,
            extractor: item.extractor.into(),
            extractor_version: EXTRACTOR_VERSION.into(),
            confidence: item.confidence,
            text_offset: item.text_offset,
            excerpt: item.excerpt.clone(),
        };
        self.store.put_entity_extraction(&extraction).await?;
        Ok(())
    }

    /// 建立或更新 Relationship（SPEC §11）。
    ///
    /// `evidence_count` 累加、`last_seen` 更新、`first_seen` 保留。
    async fn upsert_relationship(
        &self,
        source_object_id: Uuid,
        relationship_type: RelationshipType,
        target_object_id: Uuid,
        confidence: f64,
        now: DateTime<Utc>,
    ) -> Result<Uuid, EntityWorkerError> {
        let id = relationship_id(source_object_id, relationship_type, target_object_id);
        let existing = self.store.get_relationship(id).await?;
        let relationship = Relationship {
            id,
            source_object_id,
            relationship_type,
            target_object_id,
            confidence: existing
                .as_ref()
                .map_or(confidence, |r| r.confidence.max(confidence)),
            first_seen: existing.as_ref().map_or(now, |r| r.first_seen),
            last_seen: now,
            // ⚠️ 這裡**不能**用 `existing.evidence_count + 1`：同一份 Document 重跑時
            // evidence 是 upsert（v5 id）不會變多，但這個計數會每跑一次加一，
            // 於是「邊數」與「證據數」永遠對不上。改成重跑後重新數一次。
            evidence_count: 0,
            created_at: existing.as_ref().map_or(now, |r| r.created_at),
            updated_at: now,
        };
        self.store.put_relationship(&relationship).await?;
        Ok(id)
    }

    /// 寫一列 `relationship_evidence`（SPEC §12）並把 `evidence_count` 更新成實際筆數。
    ///
    /// **SPEC §12：任何 relationship 必須能回查 evidence。** 這個方法是那條要求的落地點：
    /// 每建一條邊就必定寫一筆 evidence，而且 evidence 帶 `object_id`（哪份 Document）
    /// 與 `raw_evidence_id`（哪筆原始證據），讓 Acceptance E 的鏈能一路走回 Source／Connector。
    async fn write_relationship_evidence(
        &self,
        relationship_id: Uuid,
        document: &Document,
        raw_evidence_id: Option<Uuid>,
        item: &Extracted,
        now: DateTime<Utc>,
    ) -> Result<(), EntityWorkerError> {
        let evidence = RelationshipEvidence {
            id: rel_evidence_id(relationship_id, document.id, item.text_offset),
            relationship_id,
            object_id: document.id,
            raw_evidence_id,
            excerpt: item.excerpt.clone(),
            confidence: item.confidence,
            created_at: now,
        };
        self.store.put_relationship_evidence(&evidence).await?;

        // 數一次實際筆數再寫回去。見 upsert_relationship 對 evidence_count 的說明。
        let count = self
            .store
            .list_relationship_evidence(relationship_id, EVIDENCE_COUNT_LIMIT)
            .await?
            .len();
        if let Some(mut relationship) = self.store.get_relationship(relationship_id).await? {
            let counted = i32::try_from(count).unwrap_or(i32::MAX);
            if relationship.evidence_count != counted {
                relationship.evidence_count = counted;
                relationship.updated_at = now;
                self.store.put_relationship(&relationship).await?;
            }
        }
        Ok(())
    }

    /// 佔下 `entity_extracted` claim（unique index 保證只有一列）。
    async fn claim(
        &self,
        document: &Document,
        raw_evidence_id: Option<Uuid>,
        outcome: &ExtractOutcome,
        result: &extract::ExtractionResult,
        now: DateTime<Utc>,
    ) -> Result<(), EntityWorkerError> {
        let ExtractOutcome::Extracted {
            entity_count,
            extraction_count,
            relationship_count,
            truncated,
            ..
        } = outcome
        else {
            return Ok(());
        };
        let claim = Provenance {
            id: Uuid::now_v7(),
            subject_id: document.id,
            action: ACTION_ENTITY_EXTRACTED.into(),
            // parent 是這份 Document 自己的來源；entity 是從 Document 衍生的，
            // 鏈的形狀是 RawEvidence → Document → Entity。
            parent_id: raw_evidence_id,
            raw_evidence_id,
            processor: PROCESSOR.into(),
            processor_version: env!("CARGO_PKG_VERSION").into(),
            timestamp: now,
            metadata: json!({
                "entity_count": entity_count,
                "extraction_count": extraction_count,
                "relationship_count": relationship_count,
                "truncated": truncated,
                "total_candidates": result.total_candidates,
                // 抽取器版本寫進 claim：之後要判斷「哪些 Document 是舊規則抽的、
                // 需要重跑」時，這是唯一的依據。
                "extractor_version": EXTRACTOR_VERSION,
                "max_extractions": self.bounds.max_extractions,
                "max_scan_bytes": self.bounds.max_scan_bytes,
            }),
        };
        self.store.put_provenance(&claim).await?;
        Ok(())
    }

    async fn publish(
        &self,
        outcome: &ExtractOutcome,
        entity_ids: &BTreeMap<(String, String), Uuid>,
    ) -> Result<(), EntityWorkerError> {
        let Some(producer) = &self.producer else {
            return Ok(());
        };
        let ExtractOutcome::Extracted {
            document_id,
            entity_count,
            extraction_count,
            relationship_count,
            truncated,
        } = outcome
        else {
            // 跳過／已處理／找不到都不發事件：下游對同一份 Document 收到兩次
            // entity.extracted 沒有意義，而且會讓「事件數 == 處理數」失效。
            return Ok(());
        };
        let payload = json!({
            "document_id": document_id,
            "entity_ids": entity_ids.values().collect::<Vec<_>>(),
            "entity_count": entity_count,
            "extraction_count": extraction_count,
            "relationship_count": relationship_count,
            "truncated": truncated,
            "extractor_version": EXTRACTOR_VERSION,
        });
        producer
            .publish(
                EventTopic::EntityExtracted,
                Some(&document_id.to_string()),
                Some(*document_id),
                payload,
            )
            .await?;
        Ok(())
    }
}

/// 數 evidence 時的上限。與 `RelationalStore` 的 1..=100 夾制一致；
/// 超過 100 筆證據的邊，`evidence_count` 會停在 100（已知限制，寫在開發文件）。
const EVIDENCE_COUNT_LIMIT: u32 = 100;

/// Document → Entity 的關聯種類（SPEC §11 的 13 種裡選）。
///
/// 對應表（規格只列了可用的 type，沒有規定哪種 entity 配哪種 type，這是本專案的決定）：
///
/// | Entity | relationship | 為什麼 |
/// |---|---|---|
/// | Person（來自 `author`） | `authored_by` | 語意精確：這個人**寫了**這份文件 |
/// | Organization（來自 `attributes`） | `published_by` | 發布者不等於作者 |
/// | URL | `references` | 文件**引用**了這個連結，比 `mentions` 精確 |
/// | 其餘（CVE／IP／Domain／Email／Hash） | `mentions` | 只知道「提到了」，不宜過度解讀 |
///
/// 刻意**不用** `affects`（Document affects CVE 語意不通）與 `links_to`
/// （那是 URL→URL 的關係，不是 Document→URL）。
fn mention_relationship_type(item: &Extracted) -> RelationshipType {
    match item.entity_type {
        EntityType::Person => RelationshipType::AuthoredBy,
        EntityType::Organization => RelationshipType::PublishedBy,
        EntityType::Url => RelationshipType::References,
        _ => RelationshipType::Mentions,
    }
}

/// 衍生 Entity 之間的關聯種類。方向一律是「來源 → Domain」。
///
/// * URL → Domain：`belongs_to`。這個 URL **屬於**那個網域，是結構上的從屬關係。
/// * Email → Domain：`associated_with`。信箱與網域有關聯，但說「屬於」過強——
///   `soc@example.com` 不代表這個信箱由 example.com 擁有（可能只是轉發位址）。
fn derived_relationship_type(link: &DerivedLink) -> RelationshipType {
    match link.entity_type {
        EntityType::Url => RelationshipType::BelongsTo,
        _ => RelationshipType::AssociatedWith,
    }
}

/// `Entity.id` = UUID v5(namespace, `entity_type|normalized_name`)。
///
/// 決定性的 id 就是冪等的來源：重跑算出同一個 id → upsert 同一列 → 不會有第二個 Entity。
/// `entity_type` 一定要進雜湊輸入——只用名字的話，同名的 Domain 與 Hostname 會撞成一個。
#[must_use]
pub fn entity_id(entity_type: EntityType, normalized_name: &str) -> Uuid {
    let key = format!("{}|{normalized_name}", entity_type_key(entity_type));
    Uuid::new_v5(&ENTITY_NAMESPACE, key.as_bytes())
}

/// `Relationship.id` = UUID v5(namespace, `source|type|target`)，即它的自然鍵。
#[must_use]
pub fn relationship_id(
    source_object_id: Uuid,
    relationship_type: RelationshipType,
    target_object_id: Uuid,
) -> Uuid {
    let key = format!(
        "{source_object_id}|{}|{target_object_id}",
        relationship_type_key(relationship_type)
    );
    Uuid::new_v5(&RELATIONSHIP_NAMESPACE, key.as_bytes())
}

/// `RelationshipEvidence.id` = UUID v5(namespace, `relationship|object|offset`)。
///
/// 加進 `text_offset` 是刻意的：同一份 Document 在**不同位置**提到同一個 Entity
/// 是兩筆獨立的證據，應該各留一列。只用 (relationship, object) 的話，
/// 十次提及會塌成一列，`evidence_count` 就永遠是 1。
#[must_use]
pub fn rel_evidence_id(relationship_id: Uuid, object_id: Uuid, text_offset: Option<i32>) -> Uuid {
    let key = format!(
        "{relationship_id}|{object_id}|{}",
        text_offset.map_or_else(|| "none".to_string(), |o| o.to_string())
    );
    Uuid::new_v5(&REL_EVIDENCE_NAMESPACE, key.as_bytes())
}

/// `EntityExtraction.id` = UUID v5(namespace, `object|entity|extractor|offset`)。
///
/// `extractor` 也進雜湊：同一個 Domain 可能同時被 `regex-domain` 與 `derived-url-host`
/// 命中，那是兩筆來自不同規則的抽取紀錄，不該互相覆蓋。
#[must_use]
pub fn extraction_id(
    object_id: Uuid,
    entity_id: Uuid,
    extractor: &str,
    text_offset: Option<i32>,
) -> Uuid {
    let key = format!(
        "{object_id}|{entity_id}|{extractor}|{}",
        text_offset.map_or_else(|| "none".to_string(), |o| o.to_string())
    );
    Uuid::new_v5(&EXTRACTION_NAMESPACE, key.as_bytes())
}

/// Entity 的自然鍵，用來在單次處理內去重。與 [`entity_id`] 的雜湊輸入同一套字串。
fn natural_key(entity_type: EntityType, normalized_name: &str) -> (String, String) {
    (entity_type_key(entity_type), normalized_name.to_string())
}

/// enum → 穩定字串。
///
/// **不要**改用 `format!("{:?}")`：Debug 的輸出不是穩定契約，改個 variant 名稱就會讓
/// 所有既有 Entity 的 v5 id 算出不同結果，冪等靜默失效。這裡走 serde 的
/// `rename_all = "snake_case"`，與寫進資料庫的字串是同一套。
fn entity_type_key(entity_type: EntityType) -> String {
    serde_json::to_value(entity_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        // serde 對 unit variant 一定序列化成字串，走不到這裡；
        // 真的走到就用 Debug，寧可 id 不同也不要 panic。
        .unwrap_or_else(|| format!("{entity_type:?}"))
}

fn relationship_type_key(relationship_type: RelationshipType) -> String {
    serde_json::to_value(relationship_type)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| format!("{relationship_type:?}"))
}

/// Document 的 `attributes.raw_evidence_id`（normalizer 寫入）。
///
/// Acceptance E 的鏈靠它從 Entity 一路走回 RawEvidence → Source → Connector。
fn raw_evidence_id_of(document: &Document) -> Option<Uuid> {
    document
        .attributes
        .get("raw_evidence_id")
        .and_then(Value::as_str)
        .and_then(|s| Uuid::parse_str(s).ok())
}

/// 組出抽取器的輸入：`title` + `summary` + `body` + 結構化欄位。
fn extraction_input(document: &Document, bounds: ExtractionBounds) -> ExtractionInput {
    // 上限在**串接時**就套用，不是串完再截。`documents.body` 沒有長度限制，
    // 先把一份 50 MB 的正文複製進記憶體再丟掉 99% 是白花的配置——而且那是外部輸入
    // 控制的大小（CLAUDE.md §5：external content is untrusted）。
    let mut text = String::new();
    for part in [
        document.title.as_deref(),
        document.summary.as_deref(),
        document.body.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let remaining = bounds.max_scan_bytes.saturating_sub(text.len());
        if remaining == 0 {
            break;
        }
        text.push_str(truncate_on_char_boundary(part, remaining));
        // 用換行而不是空白分隔：`標題` 與 `body 第一個字` 之間若只有空白，
        // 兩個相鄰的 token 有機會被 regex 當成一個（例如標題結尾是網域、正文開頭是 TLD）。
        text.push('\n');
    }

    let organizations = ORGANIZATION_FIELDS
        .iter()
        .filter_map(|field| document.attributes.get(*field))
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();

    ExtractionInput {
        text,
        author: document.author.clone(),
        organizations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_model::DocumentType;

    fn document() -> Document {
        Document {
            id: Uuid::now_v7(),
            object_type: DocumentType::Article,
            schema_version: "1".into(),
            title: Some("標題".into()),
            body: Some("正文".into()),
            summary: Some("摘要".into()),
            language: None,
            author: Some("Bob".into()),
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
    fn action_name_is_stable() {
        // 這個字串同時寫在 migrations/*/0005 的部分索引 WHERE 子句裡。
        // 只改其中一邊，唯一性保證會靜默消失。
        assert_eq!(ACTION_ENTITY_EXTRACTED, "entity_extracted");
    }

    #[test]
    fn entity_id_is_deterministic_and_type_sensitive() {
        let a = entity_id(EntityType::Domain, "example.com");
        assert_eq!(a, entity_id(EntityType::Domain, "example.com"));
        assert_ne!(
            a,
            entity_id(EntityType::Hostname, "example.com"),
            "同名不同型別必須是不同 Entity，否則 Domain 與 Hostname 會撞在一起"
        );
        assert_ne!(a, entity_id(EntityType::Domain, "example.org"));
        assert_eq!(
            a.get_version(),
            Some(uuid::Version::Sha1),
            "必須是 v5；換成 v7 就失去冪等性"
        );
    }

    #[test]
    fn entity_type_key_uses_serde_names_not_debug() {
        // Debug 是 `Vulnerability`，serde 是 `vulnerability`。用 Debug 的話
        // 改個 variant 名稱就會讓所有既有 Entity 的 id 變掉。
        assert_eq!(entity_type_key(EntityType::Vulnerability), "vulnerability");
        assert_eq!(entity_type_key(EntityType::Ip), "ip");
        assert_eq!(
            relationship_type_key(RelationshipType::AuthoredBy),
            "authored_by"
        );
    }

    #[test]
    fn relationship_id_is_deterministic_and_directional() {
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        assert_eq!(
            relationship_id(a, RelationshipType::Mentions, b),
            relationship_id(a, RelationshipType::Mentions, b)
        );
        assert_ne!(
            relationship_id(a, RelationshipType::Mentions, b),
            relationship_id(b, RelationshipType::Mentions, a),
            "方向不同就是不同的邊"
        );
        assert_ne!(
            relationship_id(a, RelationshipType::Mentions, b),
            relationship_id(a, RelationshipType::References, b)
        );
    }

    #[test]
    fn evidence_id_distinguishes_offsets() {
        let rel = Uuid::now_v7();
        let obj = Uuid::now_v7();
        assert_ne!(
            rel_evidence_id(rel, obj, Some(10)),
            rel_evidence_id(rel, obj, Some(99)),
            "同一份文件在不同位置提及是兩筆獨立證據"
        );
        assert_eq!(
            rel_evidence_id(rel, obj, None),
            rel_evidence_id(rel, obj, None)
        );
    }

    #[test]
    fn extraction_id_distinguishes_extractors() {
        let obj = Uuid::now_v7();
        let ent = Uuid::now_v7();
        assert_ne!(
            extraction_id(obj, ent, "regex-domain", Some(1)),
            extraction_id(obj, ent, "derived-url-host", Some(1)),
            "同一個 Domain 被兩種規則命中是兩筆抽取紀錄"
        );
    }

    #[test]
    fn relationship_types_follow_the_documented_mapping() {
        let make = |kind: EntityType| Extracted {
            entity_type: kind,
            name: "x".into(),
            normalized_name: "x".into(),
            extractor: "t",
            confidence: 1.0,
            text_offset: None,
            excerpt: None,
            attributes: BTreeMap::new(),
            derived_from: None,
        };
        assert_eq!(
            mention_relationship_type(&make(EntityType::Person)),
            RelationshipType::AuthoredBy
        );
        assert_eq!(
            mention_relationship_type(&make(EntityType::Organization)),
            RelationshipType::PublishedBy
        );
        assert_eq!(
            mention_relationship_type(&make(EntityType::Url)),
            RelationshipType::References
        );
        for kind in [
            EntityType::Vulnerability,
            EntityType::Ip,
            EntityType::Domain,
            EntityType::Email,
            EntityType::Hash,
        ] {
            assert_eq!(
                mention_relationship_type(&make(kind)),
                RelationshipType::Mentions,
                "{kind:?} 應該用 mentions"
            );
        }
    }

    #[test]
    fn derived_relationship_types() {
        assert_eq!(
            derived_relationship_type(&DerivedLink {
                entity_type: EntityType::Url,
                normalized_name: "x".into()
            }),
            RelationshipType::BelongsTo
        );
        assert_eq!(
            derived_relationship_type(&DerivedLink {
                entity_type: EntityType::Email,
                normalized_name: "x".into()
            }),
            RelationshipType::AssociatedWith
        );
    }

    #[test]
    fn extraction_input_joins_title_summary_body() {
        let doc = document();
        let input = extraction_input(&doc, ExtractionBounds::default());
        assert_eq!(input.text, "標題\n摘要\n正文\n");
        assert_eq!(input.author.as_deref(), Some("Bob"));
    }

    #[test]
    fn extraction_input_reads_organizations_from_attributes() {
        let mut doc = document();
        doc.attributes = json!({
            "publisher": "Example Press",
            "vendor": "Example Vendor",
            "unrelated": "ignored",
        });
        let input = extraction_input(&doc, ExtractionBounds::default());
        assert!(input.organizations.contains(&"Example Press".to_string()));
        assert!(input.organizations.contains(&"Example Vendor".to_string()));
        assert!(!input.organizations.contains(&"ignored".to_string()));
    }

    #[test]
    fn extraction_input_stops_at_max_scan_bytes() {
        let mut doc = document();
        doc.body = Some("x".repeat(10_000));
        let input = extraction_input(
            &doc,
            ExtractionBounds {
                max_extractions: 500,
                max_scan_bytes: 64,
            },
        );
        // 上限在串接時就套用，所以正文不會被整份複製進來。
        // 唯一的超額是每段之後補的 `\n`（最多 3 個 byte）。
        assert!(
            input.text.len() <= 64 + 3,
            "實際長度 {}：上限必須在串接時生效，不是串完再截——\
             否則一份 50 MB 的 body 會先被完整複製一次",
            input.text.len()
        );
        assert!(input.text.contains("標題"), "截斷不該把前面的欄位也吃掉");
    }

    #[test]
    fn raw_evidence_id_is_read_from_attributes() {
        let mut doc = document();
        let raw = Uuid::now_v7();
        doc.attributes = json!({ "raw_evidence_id": raw });
        assert_eq!(raw_evidence_id_of(&doc), Some(raw));
        doc.attributes = json!({});
        assert_eq!(raw_evidence_id_of(&doc), None);
    }

    #[test]
    fn outcome_exposes_document_id_for_every_variant() {
        let id = Uuid::now_v7();
        assert_eq!(
            ExtractOutcome::SkippedDuplicate { document_id: id }.document_id(),
            id
        );
        assert_eq!(
            ExtractOutcome::AlreadyDone { document_id: id }.document_id(),
            id
        );
        assert_eq!(
            ExtractOutcome::DocumentMissing { document_id: id }.document_id(),
            id
        );
        assert_eq!(
            ExtractOutcome::Extracted {
                document_id: id,
                entity_count: 1,
                extraction_count: 2,
                relationship_count: 1,
                truncated: false,
            }
            .document_id(),
            id
        );
    }
}
