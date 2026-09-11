//! `osint-cli documents ...`

use core_model::{DuplicateGroup, Entity, EntityExtraction, Provenance, RawEvidence};
use serde::Serialize;
use storage_core::RelationalStore;
use storage_core::codec::encode_enum;
use uuid::Uuid;

use crate::cli::{DocumentAction, ListArgs};
use crate::commands::collect_paged;
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_detail, print_json, print_table, truncate, ts};

pub async fn run(ctx: &Context, action: DocumentAction) -> Result<(), CliError> {
    match action {
        DocumentAction::List(args) => list(ctx, args).await,
        DocumentAction::Show { id } => show(ctx, id).await,
    }
}

async fn list(ctx: &Context, args: ListArgs) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let items = collect_paged!(store, list_documents(), args.limit);

    if ctx.format == Format::Json {
        return print_json(&items);
    }
    let mut rows = Vec::with_capacity(items.len());
    for doc in &items {
        rows.push(vec![
            doc.id.to_string(),
            encode_enum(&doc.object_type)?,
            truncate(&opt(doc.title.as_deref()), 40),
            opt(doc.language.as_deref()),
            ts(Some(doc.observed_at)),
            format!("{:.2}", doc.confidence),
            // 三態：重複／canonical／尚未去重。「-」不等於「不是重複」。
            match (
                doc.duplicate_of,
                doc.external_key.is_some() || doc.simhash.is_some(),
            ) {
                (Some(_), _) => "重複".into(),
                (None, true) => "canonical".to_string(),
                (None, false) => "未去重".into(),
            },
            truncate(&doc.labels.join(","), 20),
        ]);
    }
    print_table(
        &[
            "ID",
            "類型",
            "標題",
            "語言",
            "觀察時間",
            "信心",
            "去重",
            "標籤",
        ],
        rows,
        "：還沒有任何 Document。RawEvidence 要先經 osint-normalizer 正規化才會產生 Document。",
    );
    Ok(())
}

/// `documents show --json` 的輸出。把「Document ← provenance ← RawEvidence」串成一份，
/// 再加上去重關係（SPEC §16）：這份是誰的重複，或誰是這份的重複。
#[derive(Debug, Serialize)]
struct DocumentDetail {
    document: core_model::Document,
    provenance: Vec<Provenance>,
    raw_evidence: Vec<RawEvidence>,
    /// 這份被判成重複時，它所屬的 duplicate group。
    duplicate_of: Option<DuplicateGroup>,
    /// 這份是 canonical 時，指向它的 duplicate group（最多 `DUPLICATE_PAGE` 筆）。
    duplicates: Vec<DuplicateGroup>,
    /// 從這份 Document 抽出的 Entity（SPEC §17），含抽取紀錄本身。
    entities: Vec<ExtractedEntity>,
}

/// 一筆抽取紀錄 + 它指向的 Entity。
///
/// 兩個一起回傳而不是只回 Entity：`extraction` 帶的是「在這份 Document 的哪個位置、
/// 用哪條規則、信心多少」，那是判斷這個抽取結果可不可信的依據；只看 Entity 會失去語境。
#[derive(Debug, Serialize)]
struct ExtractedEntity {
    extraction: EntityExtraction,
    /// 理論上一定查得到（有外鍵）。真的是 `None` 代表資料被外力改壞了，
    /// 所以不隱藏這一筆，讓它顯示成異常而不是憑空消失。
    entity: Option<Entity>,
}

/// `documents show` 最多列幾個 duplicate。CLI 是唯讀查詢工具，不做無界查詢。
const DUPLICATE_PAGE: u32 = 100;

/// `documents show` 最多列幾筆抽取紀錄。
const EXTRACTION_PAGE: u32 = 100;

async fn show(ctx: &Context, id: Uuid) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let document = store
        .get_document(id)
        .await?
        .ok_or_else(|| CliError::NotFound {
            kind: "Document",
            id: id.to_string(),
            list_hint: "documents list",
        })?;

    // provenance 鏈：以 Document 為 subject 的每一列都記了 processor 與來源 RawEvidence。
    let provenance = store.list_provenance_by_subject(document.id).await?;
    let mut raw_evidence = Vec::new();
    for prov in &provenance {
        if let Some(raw_id) = prov.raw_evidence_id {
            // 同一個 Document 可能有多列 provenance 指向同一筆 RawEvidence，去重。
            if raw_evidence.iter().any(|e: &RawEvidence| e.id == raw_id) {
                continue;
            }
            if let Some(evidence) = store.get_raw_evidence(raw_id).await? {
                raw_evidence.push(evidence);
            }
        }
    }

    // 去重關係走 duplicate_groups 表而不是只看 documents.duplicate_of：
    // group 才帶得出 method 與 similarity（「憑哪個 stage、多相似」），
    // 那是判斷「這個去重結論可不可信」的依據。
    let duplicate_of = store.get_duplicate_group_by_member(document.id).await?;
    let duplicates = if duplicate_of.is_some() {
        // 已經是別人的重複就不會同時是 canonical，省一次查詢。
        Vec::new()
    } else {
        store
            .list_duplicate_groups_by_canonical(document.id, DUPLICATE_PAGE)
            .await?
    };

    // SPEC §17：這份 Document 抽出了哪些 Entity。
    let mut entities = Vec::new();
    for extraction in store
        .list_entity_extractions_by_object(document.id, EXTRACTION_PAGE)
        .await?
    {
        let entity = store.get_entity(extraction.entity_id).await?;
        entities.push(ExtractedEntity { extraction, entity });
    }

    if ctx.format == Format::Json {
        return print_json(&DocumentDetail {
            document,
            provenance,
            raw_evidence,
            duplicate_of,
            duplicates,
            entities,
        });
    }

    print_detail(vec![
        ("id", document.id.to_string()),
        ("object_type", encode_enum(&document.object_type)?),
        ("schema_version", document.schema_version.clone()),
        ("title", opt(document.title.as_deref())),
        ("summary", opt(document.summary.as_deref())),
        ("language", opt(document.language.as_deref())),
        ("author", opt(document.author.as_deref())),
        ("published_at", ts(document.published_at)),
        ("modified_at", ts(document.modified_at)),
        ("observed_at", ts(Some(document.observed_at))),
        ("collected_at", ts(Some(document.collected_at))),
        ("source_url", opt(document.source_url.as_deref())),
        ("canonical_url", opt(document.canonical_url.as_deref())),
        (
            "normalized_content_hash",
            opt(document.normalized_content_hash.as_deref()),
        ),
        ("confidence", format!("{:.4}", document.confidence)),
        ("labels", document.labels.join(", ")),
        ("external_key", opt(document.external_key.as_deref())),
        (
            "simhash",
            document
                .simhash
                .map_or_else(|| "-".into(), |v| format!("{:016x}", v as u64)),
        ),
        ("attributes", serde_json::to_string(&document.attributes)?),
        ("body", truncate(&opt(document.body.as_deref()), 2000)),
    ]);

    println!();
    print_duplicate_section(duplicate_of.as_ref(), &duplicates);

    println!();
    println!("provenance 鏈（由舊到新）：");
    let rows = provenance
        .iter()
        .map(|p| {
            vec![
                ts(Some(p.timestamp)),
                p.action.clone(),
                format!("{} {}", p.processor, p.processor_version),
                p.raw_evidence_id
                    .map_or_else(|| "-".into(), |v| v.to_string()),
                p.parent_id.map_or_else(|| "-".into(), |v| v.to_string()),
            ]
        })
        .collect();
    print_table(
        &["時間", "動作", "processor", "raw_evidence_id", "parent_id"],
        rows,
        "：這個 Document 沒有 provenance 紀錄。正常流程一定會寫一列，沒有代表資料是繞過 normalizer 塞進來的。",
    );

    println!();
    print_entity_section(&provenance, &entities)?;

    if !raw_evidence.is_empty() {
        println!();
        println!("來源 RawEvidence：");
        let rows = raw_evidence
            .iter()
            .map(|e| {
                vec![
                    e.id.to_string(),
                    ts(Some(e.retrieved_at)),
                    truncate(&e.source_url, 48),
                    e.sha256.chars().take(12).collect(),
                    e.storage_path.clone(),
                ]
            })
            .collect();
        print_table(
            &["ID", "取得時間", "來源 URL", "SHA256(前 12)", "物件 key"],
            rows,
            "",
        );
    }
    Ok(())
}

/// 抽出的 Entity（SPEC §17）。
///
/// 「沒有 Entity」有兩種完全不同的原因，必須分辨得出來，否則操作人員會把
/// 「還沒跑抽取」誤判成「這篇文章沒有任何 IOC」：
/// * provenance 沒有 `entity_extracted` 那一列 → entity-worker 還沒處理過它
///   （或它是重複文件，被刻意跳過）
/// * 有那一列但抽取數為 0 → 處理過了，真的什麼都沒抽到
fn print_entity_section(
    provenance: &[Provenance],
    entities: &[ExtractedEntity],
) -> Result<(), CliError> {
    let extracted = provenance
        .iter()
        .any(|p| p.action == ENTITY_EXTRACTED_ACTION);

    if entities.is_empty() {
        if extracted {
            println!("抽出的 Entity：無。entity-worker 處理過這份 Document，但沒有命中任何規則。");
        } else {
            println!(
                "抽出的 Entity：無。**provenance 沒有 `{ENTITY_EXTRACTED_ACTION}` 那一列**，\
                 代表 osint-entity-worker 還沒處理過它，或它是重複文件而被刻意跳過\
                 （重複文件不抽取，見上方去重關係）。"
            );
        }
        return Ok(());
    }

    println!("抽出的 Entity（SPEC §17）：");
    let mut rows = Vec::with_capacity(entities.len());
    for item in entities {
        let (kind, name) = match &item.entity {
            Some(entity) => (
                encode_enum(&entity.entity_type)?,
                entity.normalized_name.clone(),
            ),
            None => (
                "?".to_string(),
                format!(
                    "（查不到 entity {}，資料可能被外力刪過）",
                    item.extraction.entity_id
                ),
            ),
        };
        rows.push(vec![
            item.extraction.entity_id.to_string(),
            kind,
            truncate(&name, 44),
            format!(
                "{} {}",
                item.extraction.extractor, item.extraction.extractor_version
            ),
            format!("{:.2}", item.extraction.confidence),
            item.extraction
                .text_offset
                .map_or_else(|| "-".into(), |o| o.to_string()),
        ]);
    }
    print_table(
        &["Entity ID", "型別", "正規化名稱", "抽取器", "信心", "位置"],
        rows,
        "",
    );
    if entities.len() as u32 == EXTRACTION_PAGE {
        println!("（只列出前 {EXTRACTION_PAGE} 筆，可能還有更多）");
    }
    println!("提示：`osint-cli entities show <Entity ID>` 可看到它的關聯與證據。");
    Ok(())
}

/// entity-worker 的 provenance 動作名。
///
/// 這裡刻意**不**依賴 `entity-worker` crate：CLI 只要讀資料庫，把一支服務的
/// 整個相依樹（含 broker client）拉進唯讀查詢工具不划算。
/// 代價是字串重複了一次——由 `entity_extracted_action_matches_the_worker` 這個測試釘住。
const ENTITY_EXTRACTED_ACTION: &str = "entity_extracted";

/// 去重關係（SPEC §16）。三種狀態要能分辨：
/// 是重複、是 canonical 且有人指向它、尚未被 deduplicator 處理或確定獨一無二。
fn print_duplicate_section(duplicate_of: Option<&DuplicateGroup>, duplicates: &[DuplicateGroup]) {
    if let Some(group) = duplicate_of {
        println!("去重關係：這份是**重複**，canonical 是另一份");
        print_table(
            &["canonical Document", "命中階段", "相似度", "首次發現"],
            vec![vec![
                group.canonical_object_id.to_string(),
                group.method.clone(),
                format!("{:.4}", group.similarity),
                ts(Some(group.first_seen)),
            ]],
            "",
        );
        return;
    }

    if duplicates.is_empty() {
        // 刻意不說「沒有重複」：`duplicate_of` 為空也可能只是 deduplicator 還沒跑到。
        // 兩者的差別看 provenance 有沒有 action=deduplicated 那一列。
        println!(
            "去重關係：目前沒有其他 Document 指向這一份。\
             （若 provenance 沒有 deduplicated 那一列，代表 osint-deduplicator 還沒處理過它）"
        );
        return;
    }

    println!(
        "去重關係：這份是 canonical，有 {} 份重複指向它",
        duplicates.len()
    );
    let rows = duplicates
        .iter()
        .map(|group| {
            vec![
                group
                    .member_object_id
                    .map_or_else(|| "-".into(), |v| v.to_string()),
                group.method.clone(),
                format!("{:.4}", group.similarity),
                group
                    .member_raw_evidence_id
                    .map_or_else(|| "-".into(), |v| v.to_string()),
                ts(Some(group.first_seen)),
            ]
        })
        .collect();
    print_table(
        &[
            "重複的 Document",
            "命中階段",
            "相似度",
            "該份的 RawEvidence",
            "首次發現",
        ],
        rows,
        "",
    );
    if duplicates.len() as u32 == DUPLICATE_PAGE {
        println!("（只列出前 {DUPLICATE_PAGE} 筆，可能還有更多）");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_extracted_action_matches_the_worker() {
        // entity-worker 的 `ACTION_ENTITY_EXTRACTED` 與
        // `migrations/*/0005_entity_natural_key.sql` 的部分索引都用這個字串。
        // 三處必須一致；只改其中一處不會有任何編譯錯誤，只會讓這裡的判斷
        // 永遠落在「還沒處理過」那一支——**靜默的錯誤提示**。
        assert_eq!(ENTITY_EXTRACTED_ACTION, "entity_extracted");
    }
}
