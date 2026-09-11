//! `osint-cli connectors ...`

use storage_core::RelationalStore;
use uuid::Uuid;

use crate::cli::{ConnectorAction, ListArgs, PAGE_SIZE};
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_detail, print_json, print_table, truncate, ts};

pub async fn run(ctx: &Context, action: ConnectorAction) -> Result<(), CliError> {
    match action {
        ConnectorAction::List { list, all } => list_connectors(ctx, list, all).await,
        ConnectorAction::Show { id } => show(ctx, id).await,
    }
}

async fn list_connectors(ctx: &Context, args: ListArgs, all: bool) -> Result<(), CliError> {
    let store = ctx.store().await?;
    // `--all` 才看得到停用的 connector。預設過濾掉，是因為日常想知道的是
    // 「現在會跑的有哪些」；停用的留在清單裡反而容易誤判排程狀況。
    //
    // 過濾在 CLI 這一層做而不是查 list_enabled_connectors：後者沒有分頁，
    // 兩條路徑的 --limit 語意會不一致。
    //
    // 注意這裡**不能**用 collect_paged! 再 retain：那樣 `--limit 20` 會先取 20 筆
    // 再濾掉停用的，使用者只看到 3 筆卻以為系統裡只有 3 個啟用的 connector。
    // 要一直翻頁到湊滿 limit 筆「符合條件」的為止。
    //
    // 另外：connector 的排序只是 id 遞減，不等於時間序。import 建立的 connector
    // 用 UUID v5（由 source + format 推導以保持冪等），沒有時間戳。

    let mut items = Vec::new();
    let mut cursor: Option<Uuid> = None;
    while (items.len() as u32) < args.limit {
        let page = store.list_connectors(cursor, PAGE_SIZE).await?;
        let got = page.len() as u32;
        cursor = page.last().map(|c| c.id);
        items.extend(page.into_iter().filter(|c| all || c.enabled));
        if got < PAGE_SIZE {
            break;
        }
    }
    items.truncate(args.limit as usize);

    if ctx.format == Format::Json {
        return print_json(&items);
    }
    let rows = items
        .iter()
        .map(|c| {
            vec![
                c.id.to_string(),
                truncate(&c.name, 24),
                c.connector_type.clone(),
                if c.enabled { "是" } else { "否" }.to_string(),
                c.status.clone(),
                c.error_count.to_string(),
                ts(c.last_success),
            ]
        })
        .collect();
    let hint = if all {
        "：還沒有任何 Connector。"
    } else {
        "：沒有啟用中的 Connector。加 `--all` 看含停用的。"
    };
    print_table(
        &["ID", "名稱", "類型", "啟用", "狀態", "錯誤數", "最後成功"],
        rows,
        hint,
    );
    Ok(())
}

async fn show(ctx: &Context, id: Uuid) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let connector = store
        .get_connector(id)
        .await?
        .ok_or_else(|| CliError::NotFound {
            kind: "Connector",
            id: id.to_string(),
            list_hint: "connectors list --all",
        })?;

    if ctx.format == Format::Json {
        return print_json(&connector);
    }
    print_detail(vec![
        ("id", connector.id.to_string()),
        ("source_id", connector.source_id.to_string()),
        ("name", connector.name.clone()),
        ("type", connector.connector_type.clone()),
        ("version", connector.version.clone()),
        ("enabled", connector.enabled.to_string()),
        (
            "configuration",
            serde_json::to_string(&connector.configuration)?,
        ),
        // credential_reference / proxy_reference 存的是 SecretRef 字串（例如
        // `env:NVD_TOKEN`），不是明文密鑰，可以直接顯示。
        (
            "credential_reference",
            opt(connector.credential_reference.as_deref()),
        ),
        ("schedule", opt(connector.schedule.as_deref())),
        ("rate_limit", serde_json::to_string(&connector.rate_limit)?),
        ("timeout", serde_json::to_string(&connector.timeout)?),
        ("proxy_reference", opt(connector.proxy_reference.as_deref())),
        ("checkpoint", serde_json::to_string(&connector.checkpoint)?),
        ("last_run", ts(connector.last_run)),
        ("last_success", ts(connector.last_success)),
        ("status", connector.status.clone()),
        ("error_count", connector.error_count.to_string()),
    ]);
    Ok(())
}
