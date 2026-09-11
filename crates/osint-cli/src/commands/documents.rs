//! `osint-cli documents ...`

use core_model::{Provenance, RawEvidence};
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
            truncate(&doc.labels.join(","), 20),
        ]);
    }
    print_table(
        &["ID", "類型", "標題", "語言", "觀察時間", "信心", "標籤"],
        rows,
        "：還沒有任何 Document。RawEvidence 要先經 osint-normalizer 正規化才會產生 Document。",
    );
    Ok(())
}

/// `documents show --json` 的輸出。把「Document ← provenance ← RawEvidence」串成一份。
#[derive(Debug, Serialize)]
struct DocumentDetail {
    document: core_model::Document,
    provenance: Vec<Provenance>,
    raw_evidence: Vec<RawEvidence>,
}

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

    if ctx.format == Format::Json {
        return print_json(&DocumentDetail {
            document,
            provenance,
            raw_evidence,
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
        ("attributes", serde_json::to_string(&document.attributes)?),
        ("body", truncate(&opt(document.body.as_deref()), 2000)),
    ]);

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
