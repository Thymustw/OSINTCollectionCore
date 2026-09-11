//! `osint-cli sources ...`

use storage_core::RelationalStore;
use storage_core::codec::encode_enum;
use uuid::Uuid;

use crate::cli::{ListArgs, SourceAction};
use crate::commands::collect_paged;
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_detail, print_json, print_table, truncate, ts};

pub async fn run(ctx: &Context, action: SourceAction) -> Result<(), CliError> {
    match action {
        SourceAction::List(args) => list(ctx, args).await,
        SourceAction::Show { id } => show(ctx, id).await,
    }
}

async fn list(ctx: &Context, args: ListArgs) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let items = collect_paged!(store, list_sources(), args.limit);

    if ctx.format == Format::Json {
        return print_json(&items);
    }
    let mut rows = Vec::with_capacity(items.len());
    for source in &items {
        rows.push(vec![
            source.id.to_string(),
            truncate(&source.name, 28),
            encode_enum(&source.source_type)?,
            if source.enabled { "是" } else { "否" }.to_string(),
            truncate(&opt(source.base_url.as_deref()), 46),
            ts(source.last_seen),
        ]);
    }
    print_table(
        &["ID", "名稱", "類型", "啟用", "Base URL", "最後出現"],
        rows,
        "：還沒有任何 Source。可用 `POST /api/v1/import` 匯入，或由 connector 建立。",
    );
    Ok(())
}

async fn show(ctx: &Context, id: Uuid) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let source = store
        .get_source(id)
        .await?
        .ok_or_else(|| CliError::NotFound {
            kind: "Source",
            id: id.to_string(),
            list_hint: "sources list",
        })?;

    if ctx.format == Format::Json {
        return print_json(&source);
    }
    print_detail(vec![
        ("id", source.id.to_string()),
        ("name", source.name.clone()),
        ("source_type", encode_enum(&source.source_type)?),
        ("platform", opt(source.platform.as_deref())),
        ("base_url", opt(source.base_url.as_deref())),
        ("description", opt(source.description.as_deref())),
        ("language", opt(source.language.as_deref())),
        ("country", opt(source.country.as_deref())),
        ("enabled", source.enabled.to_string()),
        (
            "collection_policy",
            serde_json::to_string(&source.collection_policy)?,
        ),
        ("created_at", ts(Some(source.created_at))),
        ("updated_at", ts(Some(source.updated_at))),
        ("last_seen", ts(source.last_seen)),
    ]);
    Ok(())
}
