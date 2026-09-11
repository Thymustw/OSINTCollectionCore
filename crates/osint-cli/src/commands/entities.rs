//! `osint-cli entities ...`
//!
//! `show` 刻意把 SPEC §26 Acceptance E 的那條鏈整條印出來：
//! Entity → Relationship → RelationshipEvidence → RawEvidence → Source／Connector。
//! 操作人員要能在不寫 SQL 的情況下回答「這個 IOC 是從哪裡來的」。

use core_model::{Entity, EntityExtraction, Relationship, RelationshipEvidence};
use serde::Serialize;
use storage_core::RelationalStore;
use storage_core::codec::{decode_enum, encode_enum};
use uuid::Uuid;

use crate::cli::{EntityAction, ListArgs};
use crate::commands::collect_paged;
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_detail, print_json, print_table, truncate, ts};

/// `show` 各段落的查詢上限。CLI 是唯讀查詢工具，不做無界查詢。
const PAGE: u32 = 100;

pub async fn run(ctx: &Context, action: EntityAction) -> Result<(), CliError> {
    match action {
        EntityAction::List { list, entity_type } => self::list(ctx, list, entity_type).await,
        EntityAction::Show { id } => show(ctx, id).await,
    }
}

async fn list(ctx: &Context, args: ListArgs, entity_type: Option<String>) -> Result<(), CliError> {
    // `--entity-type` 先驗證再查。打錯字時直接回「沒有資料」是最糟的回應——
    // 使用者會以為資料庫是空的，而不是自己少打一個字母。
    let wanted = match entity_type.as_deref() {
        None => None,
        Some(raw) => Some(
            decode_enum::<core_model::EntityType>(raw, "entity_type").map_err(|_| {
                CliError::InvalidArgument {
                    message: format!(
                        "`{raw}` 不是有效的 entity type。可用值：person, organization, account, \
                     domain, hostname, ip, url, email, vulnerability, software, repository, \
                     hash, location"
                    ),
                }
            })?,
        ),
    };

    let store = ctx.store().await?;
    let items = collect_paged!(store, list_entities(), args.limit);
    let items: Vec<Entity> = match wanted {
        // 過濾在程式端做（storage 沒有依型別分頁的方法）。因此 `--limit` 的語意是
        // 「掃描這麼多筆」而不是「回這麼多筆」，要講清楚，見下面的提示。
        Some(kind) => items
            .into_iter()
            .filter(|e| e.entity_type == kind)
            .collect(),
        None => items,
    };

    if ctx.format == Format::Json {
        return print_json(&items);
    }
    let mut rows = Vec::with_capacity(items.len());
    for entity in &items {
        rows.push(vec![
            entity.id.to_string(),
            encode_enum(&entity.entity_type)?,
            truncate(&entity.normalized_name, 52),
            format!("{:.2}", entity.confidence),
            ts(Some(entity.first_seen)),
            ts(Some(entity.last_seen)),
        ]);
    }
    print_table(
        &["ID", "型別", "正規化名稱", "信心", "首次出現", "最後出現"],
        rows,
        "：還沒有任何 Entity。Document 要先經 osint-entity-worker 抽取才會產生 Entity；\
         而 entity-worker 只處理 canonical（非重複）的 Document。",
    );
    if wanted.is_some() {
        println!(
            "（`--entity-type` 是在取回的前 {} 筆之內過濾，不是資料庫層的篩選；\
             要看更多請調高 `--limit`）",
            args.limit
        );
    }
    Ok(())
}

/// `entities show --json` 的輸出：Acceptance E 的整條鏈。
#[derive(Debug, Serialize)]
struct EntityDetail {
    entity: Entity,
    /// 這個 Entity 被哪些 object 抽出過。
    extractions: Vec<EntityExtraction>,
    /// 這個 Entity 參與的關聯，以及每一條的證據。
    relationships: Vec<RelationshipDetail>,
}

#[derive(Debug, Serialize)]
struct RelationshipDetail {
    relationship: Relationship,
    evidence: Vec<RelationshipEvidence>,
}

async fn show(ctx: &Context, id: Uuid) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let entity = store
        .get_entity(id)
        .await?
        .ok_or_else(|| CliError::NotFound {
            kind: "Entity",
            id: id.to_string(),
            list_hint: "entities list",
        })?;

    let extractions = store
        .list_entity_extractions_by_entity(entity.id, PAGE)
        .await?;

    let mut relationships = Vec::new();
    for relationship in store.list_relationships_by_object(entity.id, PAGE).await? {
        // SPEC §12：任何 relationship 都要能回查 evidence。這裡就是那條要求的使用點。
        let evidence = store
            .list_relationship_evidence(relationship.id, PAGE)
            .await?;
        relationships.push(RelationshipDetail {
            relationship,
            evidence,
        });
    }

    if ctx.format == Format::Json {
        return print_json(&EntityDetail {
            entity,
            extractions,
            relationships,
        });
    }

    print_detail(vec![
        ("id", entity.id.to_string()),
        ("entity_type", encode_enum(&entity.entity_type)?),
        ("name", entity.name.clone()),
        ("normalized_name", entity.normalized_name.clone()),
        ("description", opt(entity.description.as_deref())),
        ("confidence", format!("{:.4}", entity.confidence)),
        ("first_seen", ts(Some(entity.first_seen))),
        ("last_seen", ts(Some(entity.last_seen))),
        ("attributes", serde_json::to_string(&entity.attributes)?),
    ]);

    println!();
    println!("抽取紀錄（這個 Entity 出現在哪些 Document）：");
    let rows = extractions
        .iter()
        .map(|e| {
            vec![
                e.object_id.to_string(),
                format!("{} {}", e.extractor, e.extractor_version),
                format!("{:.2}", e.confidence),
                e.text_offset.map_or_else(|| "-".into(), |o| o.to_string()),
                truncate(&opt(e.excerpt.as_deref()), 60),
            ]
        })
        .collect();
    print_table(
        &["Document", "抽取器", "信心", "位置", "上下文"],
        rows,
        "：這個 Entity 沒有抽取紀錄。正常流程一定會寫至少一列。",
    );

    println!();
    println!("關聯與證據（SPEC §11／§12）：");
    let mut rows = Vec::new();
    for detail in &relationships {
        let rel = &detail.relationship;
        let direction = if rel.target_object_id == entity.id {
            format!(
                "{} ──{}──> 本實體",
                rel.source_object_id,
                encode_enum(&rel.relationship_type)?
            )
        } else {
            format!(
                "本實體 ──{}──> {}",
                encode_enum(&rel.relationship_type)?,
                rel.target_object_id
            )
        };
        rows.push(vec![
            direction,
            format!("{:.2}", rel.confidence),
            detail.evidence.len().to_string(),
            detail
                .evidence
                .first()
                .and_then(|e| e.raw_evidence_id)
                .map_or_else(|| "-".into(), |v| v.to_string()),
        ]);
    }
    print_table(
        &["關聯", "信心", "證據數", "RawEvidence（第一筆）"],
        rows,
        "：這個 Entity 沒有任何關聯。",
    );
    println!(
        "提示：拿上表的 RawEvidence id 執行 `osint-cli raw show <id>`，\
         即可看到它的 Source 與 Connector（SPEC §26 Acceptance E 的可追溯鏈）。"
    );
    Ok(())
}
