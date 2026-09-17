//! `stix_export` 的執行邏輯：查 Entity／Relationship、組 STIX bundle、寫進物件儲存。
//!
//! 與 `service.rs` 的 `run_import` 分開，是因為那個檔案已經不小（~900 行），
//! export 是獨立的一條路徑，拆檔讓兩邊各自好讀。
//!
//! # 已知限制（刻意不假裝完整）
//!
//! `EXPORT_TRAVERSAL_LIMIT = 100` 代表任何一個 Entity 若參與超過 100 條
//! Relationship，多出來的鄰居／關係會被漏掉（`list_relationships_by_object`
//! 本身文件就說這個方法的 `limit` 是設計上就要夾在 1..=100 的有界查詢，
//! 不是這裡新增的限制）。同一個 Entity 過大的扇出（supernode）不會被完整匯出。
//!
//! # time_range 只作用在 Relationship，不作用在 Entity
//!
//! Entity 沒有一個能代表「觀測窗」的單一區間（`first_seen`／`last_seen` 橫跨
//! 整個生命週期，跟 Relationship 的「這段關係在這段時間內被觀測到」語意不同）。
//! `time_range` 一律只篩 Relationship（情境 A 的 BFS 邊過濾、以及
//! [`select_export_relationships`] 的最終過濾）。之後有人想加 Entity 層級的
//! 時間過濾不會誤以為漏掉了——是這裡刻意不做的決定。

use std::collections::{HashMap, HashSet};

use core_jobs::JobService;
use core_model::{Entity, Relationship};
use serde_json::json;
use stix_adapter::{
    StixBundle, StixExportFilter, StixId, StixObject, entity_to_stix_object,
    relationship_to_stix_object,
};
use storage_core::{EmbeddingProvider, ObjectStore, StorageError, TransactionalStore};
use uuid::Uuid;

use crate::error::ImportError;
use ai_gateway::LlmProvider;

/// BFS 一層一層展開時，每個節點最多抓幾條關係。夾在 `list_relationships_by_object`
/// 的 1..=100 內。
const EXPORT_TRAVERSAL_LIMIT: u32 = 100;
/// entity_types 分頁掃描每次抓幾筆。頁面回傳長度 `< 這值` 代表掃完。
const EXPORT_PAGE_SIZE: u32 = 100;

/// 一次 `stix_export` 的結果摘要。
pub(super) struct ExportReport {
    pub(super) entity_count: usize,
    pub(super) relationship_count: usize,
    pub(super) result_object_key: String,
}

impl<S, E, L, O> crate::service::StixWorker<S, E, L, O>
where
    S: TransactionalStore + Clone,
    E: EmbeddingProvider,
    L: LlmProvider,
    O: ObjectStore + Clone,
{
    /// `stix_export` 執行本體。讀 Job 的 `parameters.filter`、查 Entity／
    /// Relationship、組 bundle、寫進物件儲存、回傳結果摘要。
    pub(super) async fn run_export(
        &self,
        jobs: &JobService<S>,
        job_id: Uuid,
    ) -> Result<ExportReport, ImportError> {
        let job = jobs.get(job_id).await.map_err(|err| {
            ImportError::Storage(StorageError::Unknown {
                backend: "jobs",
                message: format!("讀取 stix_export Job `{job_id}` 失敗：{err}"),
            })
        })?;
        let params = job
            .parameters
            .as_ref()
            .ok_or_else(|| ImportError::MissingParameter {
                field: "parameters".into(),
            })?;

        // 沒有 `filter` 這個 key 就當成空物件。`export_stix` handler 一定塞
        // `{"filter": ...}`，這裡只是防禦手動建的 Job。
        let empty_filter = json!({});
        let filter_value = params.get("filter").unwrap_or(&empty_filter);
        let filter: StixExportFilter =
            serde_json::from_value(filter_value.clone()).map_err(|err| {
                ImportError::InvalidFilter {
                    message: err.to_string(),
                }
            })?;

        let selected = self.select_export_entities(&filter).await?;
        let relationships = self.select_export_relationships(&selected, &filter).await?;

        // 對每個選中的 Entity 組 STIX 物件，同時把 `(entity.id, stix_id)`
        // 存起來，等等建 Relationship 的 source_ref／target_ref 用。
        // `.id()` 對 `entity_to_stix_object` 回傳的已知型別一定不為 None——
        // 用 expect，不要吞掉這個不該發生的狀況。
        let mut stix_ids: HashMap<Uuid, StixId> = HashMap::with_capacity(selected.len());
        let mut objects: Vec<StixObject> = Vec::with_capacity(selected.len());
        for entity in &selected {
            let stix = entity_to_stix_object(entity);
            let id = stix
                .id()
                .expect("entity_to_stix_object 一律回帶 id")
                .clone();
            stix_ids.insert(entity.id, id);
            objects.push(stix);
        }

        let mut relationship_count = 0usize;
        for rel in &relationships {
            // 理論上兩端一定查得到（select_export_relationships 已篩過兩端都在
            // selected 裡）。查不到是內部邏輯錯誤——記一行 log 跳過這條，不 panic
            // 也不讓整個匯出失敗，這只是保險線。
            let (Some(source), Some(target)) = (
                stix_ids.get(&rel.source_object_id),
                stix_ids.get(&rel.target_object_id),
            ) else {
                tracing::error!(
                    relationship_id = %rel.id,
                    source_object_id = %rel.source_object_id,
                    target_object_id = %rel.target_object_id,
                    "stix_export 遇到兩端不在匯出集合的 Relationship，內部邏輯錯誤，跳過這條"
                );
                continue;
            };
            let stix_rel = relationship_to_stix_object(rel, source.clone(), target.clone());
            objects.push(StixObject::Relationship(stix_rel));
            relationship_count += 1;
        }

        let bundle = StixBundle {
            type_: "bundle".into(),
            id: StixId::parse(format!("bundle--{}", Uuid::now_v7()))
                .expect("組出來的 bundle id 一定合法"),
            objects,
        };
        let bytes = serde_json::to_vec(&bundle).map_err(|err| {
            ImportError::Storage(StorageError::Unknown {
                backend: "stix_export_serialize",
                message: err.to_string(),
            })
        })?;

        let key = stix_adapter::export_result_object_key(job_id);
        self.objects
            .put(&key, &bytes, Some("application/stix+json"))
            .await?;

        Ok(ExportReport {
            entity_count: selected.len(),
            relationship_count,
            result_object_key: key,
        })
    }

    /// 依 filter 決定要匯出哪些 Entity。已合併掉的 Entity（`merged_into`）一律排除。
    /// 情境 A（`entity_ids`）與情境 B（分頁掃描）走不同路徑，最後統一套
    /// `entity_types` 最終過濾。
    async fn select_export_entities(
        &self,
        filter: &StixExportFilter,
    ) -> Result<Vec<Entity>, ImportError> {
        let mut all = match &filter.entity_ids {
            Some(ids) => self.select_export_entities_by_ids(filter, ids).await?,
            None => self.select_export_entities_by_scan(filter).await?,
        };
        if let Some(types) = &filter.entity_types {
            let before = all.len();
            all.retain(|e| types.contains(&e.entity_type));
            if all.len() != before {
                tracing::debug!(
                    before,
                    after = all.len(),
                    "entity_types 最終過濾掉了物件（使用者給的 entity_ids 與 \
                     entity_types 互相矛盾是正常被濾掉，不是系統的錯）"
                );
            }
        }
        Ok(all)
    }

    /// 情境 A：以 `entity_ids` 為種子，可選 BFS 逐層擴散。
    async fn select_export_entities_by_ids(
        &self,
        filter: &StixExportFilter,
        ids: &[Uuid],
    ) -> Result<Vec<Entity>, ImportError> {
        let mut seeds: HashMap<Uuid, Entity> = HashMap::new();
        let mut missing = 0usize;
        for &id in ids {
            // 使用者傳重複 id 時 HashMap 天然去重，只留一筆。
            if seeds.contains_key(&id) {
                continue;
            }
            let Some(entity) = self.store.get_entity(id).await? else {
                tracing::warn!(%id, "stix_export 找不到 entity_id `{id}`，跳過這筆，不中斷整批");
                missing += 1;
                continue;
            };
            if let Some(survivor) = entity.merged_into {
                tracing::warn!(
                    %id,
                    %survivor,
                    "stix_export 略過已合併掉的 Entity `{id}`（merged_into=`{survivor}`），\
                     要查就查 survivor"
                );
                continue;
            }
            seeds.insert(id, entity);
        }
        if missing > 0 {
            tracing::warn!(
                missing,
                "stix_export 有 entity_ids 找不到對應 Entity，已跳過"
            );
        }

        if let Some(depth) = filter.depth {
            if depth > 0 {
                self.bfs_expand(filter, depth, &mut seeds).await?;
            }
        }

        Ok(seeds.into_values().collect())
    }

    /// BFS：以種子集合的 id 為 frontier，逐層向外走 `depth` 輪，沿途把合條件的
    /// 鄰居加進種子集合。每加一筆就檢查是否超過 `max_export_objects`，提早失敗省 I/O。
    ///
    /// `entity_types` 不限制中繼節點——它只篩「最終要匯出的節點」。不然像
    /// 「以人為中心，展開一層鄰居」這種需求，若鄰居剛好是網域就會走不出去。
    async fn bfs_expand(
        &self,
        filter: &StixExportFilter,
        depth: u32,
        seeds: &mut HashMap<Uuid, Entity>,
    ) -> Result<(), ImportError> {
        let mut visited: HashSet<Uuid> = seeds.keys().copied().collect();
        let mut frontier: HashSet<Uuid> = seeds.keys().copied().collect();

        for _ in 0..depth {
            let mut next_frontier: HashSet<Uuid> = HashSet::new();
            for &id in &frontier {
                let rels = self
                    .store
                    .list_relationships_by_object(id, EXPORT_TRAVERSAL_LIMIT)
                    .await?;
                for rel in rels {
                    // time_range 同時限制「能不能繼續往外走」與「這條邊最終算不算數」：
                    // 不然會沿著一條理應被時間窗排除的邊走到不該匯出的鄰居。
                    if let Some(range) = &filter.time_range {
                        if !range.overlaps(rel.first_seen, rel.last_seen) {
                            continue;
                        }
                    }
                    let other = if rel.source_object_id == id {
                        rel.target_object_id
                    } else {
                        rel.source_object_id
                    };
                    if !visited.insert(other) {
                        continue;
                    }
                    let Some(neighbor) = self.store.get_entity(other).await? else {
                        tracing::warn!(
                            %other,
                            "stix_export BFS 走到查不到的 Entity，跳過這筆，不往下擴散"
                        );
                        continue;
                    };
                    if let Some(survivor) = neighbor.merged_into {
                        tracing::warn!(
                            %other,
                            %survivor,
                            "stix_export BFS 走到已合併掉的 Entity `{other}`（merged_into=`{survivor}`），\
                             跳過且不繼續往外擴散"
                        );
                        continue;
                    }
                    seeds.insert(other, neighbor);
                    next_frontier.insert(other);
                    if seeds.len() > self.max_export_objects {
                        return Err(ImportError::TooManyExportObjects {
                            matched: seeds.len(),
                            max: self.max_export_objects,
                        });
                    }
                }
            }
            // 下一層沒有新節點可擴散，提早結束，不用把 depth 輪跑完。
            if next_frontier.is_empty() {
                break;
            }
            frontier = next_frontier;
        }
        Ok(())
    }

    /// 情境 B：沒有 `entity_ids`，依 `entity_types`（或全部）分頁掃描整個資料表。
    ///
    /// 若 `depth` 有值會 `tracing::warn!`「depth 只有在提供 entity_ids 時才有意義」，
    /// 但不當作錯誤——使用者可能是複製一份 filter JSON 忘記拿掉不相關欄位。
    async fn select_export_entities_by_scan(
        &self,
        filter: &StixExportFilter,
    ) -> Result<Vec<Entity>, ImportError> {
        if filter.depth.is_some() {
            tracing::warn!("stix_export filter 沒有 entity_ids，depth 沒有意義，被忽略");
        }

        let mut collected: HashMap<Uuid, Entity> = HashMap::new();
        match &filter.entity_types {
            Some(types) => {
                let mut seen = HashSet::new();
                for t in types {
                    if !seen.insert(t) {
                        continue;
                    }
                    self.scan_entities_page(Some(t), None, &mut collected)
                        .await?;
                }
            }
            None => {
                self.scan_entities_page(None, None, &mut collected).await?;
            }
        }
        Ok(collected.into_values().collect())
    }

    /// 依 `entity_type`（`None` 不過濾）游標分頁掃描 Entity，加入 `collected`。
    /// `list_entities`／`list_entities_by_type` 依 id **遞減**、`id < after`——
    /// 所以一頁裡最小的 id 就是下一頁該從哪繼續的游標。
    async fn scan_entities_page(
        &self,
        entity_type: Option<&core_model::EntityType>,
        after: Option<Uuid>,
        collected: &mut HashMap<Uuid, Entity>,
    ) -> Result<(), ImportError> {
        let mut after = after;
        loop {
            let page = self
                .store
                .list_entities_by_type(entity_type.copied(), after, EXPORT_PAGE_SIZE)
                .await?;
            let page_len = page.len() as u32;
            if page_len == 0 {
                break;
            }
            // 依 id 遞減排序，最後一筆（min）就是下一頁游標。
            after = page.last().map(|e| e.id);
            for entity in page {
                if entity.merged_into.is_some() {
                    continue;
                }
                if collected.insert(entity.id, entity).is_some() {
                    // 跨型別不會重複（型別不同 id 不同不會衝突），這裡純保險。
                    continue;
                }
                if collected.len() > self.max_export_objects {
                    return Err(ImportError::TooManyExportObjects {
                        matched: collected.len(),
                        max: self.max_export_objects,
                    });
                }
            }
            // 頁面回傳長度 < EXPORT_PAGE_SIZE 就代表這個型別掃完了。
            if page_len < EXPORT_PAGE_SIZE {
                break;
            }
        }
        Ok(())
    }

    /// 收集兩端都在 `selected` 裡、且通過 `time_range` 的 Relationship。
    ///
    /// 同一條關係會從它的兩端各被 `list_relationships_by_object` 掃到一次，
    /// 用 `relationship.id` 當 key 去重。
    async fn select_export_relationships(
        &self,
        selected: &[Entity],
        filter: &StixExportFilter,
    ) -> Result<Vec<Relationship>, ImportError> {
        let ids: HashSet<Uuid> = selected.iter().map(|e| e.id).collect();
        let mut collected: HashMap<Uuid, Relationship> = HashMap::new();
        for &id in &ids {
            let rels = self
                .store
                .list_relationships_by_object(id, EXPORT_TRAVERSAL_LIMIT)
                .await?;
            for rel in rels {
                if !(ids.contains(&rel.source_object_id) && ids.contains(&rel.target_object_id)) {
                    continue;
                }
                if let Some(range) = &filter.time_range {
                    if !range.overlaps(rel.first_seen, rel.last_seen) {
                        continue;
                    }
                }
                collected.insert(rel.id, rel);
            }
        }
        Ok(collected.into_values().collect())
    }
}
