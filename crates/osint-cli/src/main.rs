//! `osint-cli`：本機唯讀查詢工具。
//!
//! 定位：在 console（Phase 6）出現之前，讓人能從終端機看到「系統裡到底有什麼資料」。
//! 它直接連 Core 的 PostgreSQL／MinIO，**不經過 core-api**，因此也不受 API 的
//! RBAC 與 AuditLog 保護——只在自己已經能存取資料庫的機器上使用。
//!
//! 唯讀是刻意的：任何寫入都必須走 core-api，那條路徑才留得下稽核紀錄。

mod cli;
mod commands;
mod context;
mod error;
mod output;

use clap::Parser;
use storage_core::conformance::load_workspace_dotenv;

use crate::cli::{Cli, Command};
use crate::context::Context;
use crate::error::CliError;
use crate::output::Format;

fn main() {
    // 與其他 binary 一致：先載入 workspace 根目錄的 `.env`，已設定的環境變數不覆蓋。
    load_workspace_dotenv();
    let cli = Cli::parse();

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(err) => {
            eprintln!("無法建立 Tokio runtime：{err}");
            std::process::exit(1);
        }
    };

    match runtime.block_on(run(cli)) {
        Ok(true) => {}
        // health 有服務不健康時以 exit code 2 結束，方便腳本判斷。
        // 不用 1 是為了和「指令本身失敗」區分開來。
        Ok(false) => std::process::exit(2),
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

/// 回傳值代表「結果是否全部正常」。目前只有 `health` 會回 `false`。
async fn run(cli: Cli) -> Result<bool, CliError> {
    let ctx = Context::load(Format::from_flag(cli.json))?;
    match cli.command {
        Command::Sources { action } => commands::sources::run(&ctx, action).await?,
        Command::Connectors { action } => commands::connectors::run(&ctx, action).await?,
        Command::Raw { action } => commands::raw::run(&ctx, action).await?,
        Command::Documents { action } => commands::documents::run(&ctx, action).await?,
        Command::Entities { action } => commands::entities::run(&ctx, action).await?,
        Command::Jobs { action } => commands::jobs::run(&ctx, action).await?,
        Command::Health => return commands::health::run(&ctx).await,
    }
    Ok(true)
}
