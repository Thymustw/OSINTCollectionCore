//! `osint-cli jobs ...`

use storage_core::RelationalStore;
use storage_core::codec::encode_enum;

use crate::cli::JobAction;
use crate::commands::collect_paged;
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_json, print_table, truncate, ts};

pub async fn run(ctx: &Context, action: JobAction) -> Result<(), CliError> {
    let JobAction::List(args) = action;
    let store = ctx.store().await?;
    let items = collect_paged!(store, list_jobs(), args.limit);

    if ctx.format == Format::Json {
        return print_json(&items);
    }
    let mut rows = Vec::with_capacity(items.len());
    for job in &items {
        rows.push(vec![
            job.id.to_string(),
            job.job_type.clone(),
            encode_enum(&job.status)?,
            ts(Some(job.created_at)),
            ts(job.completed_at),
            job.retry_count.to_string(),
            truncate(&opt(job.error.as_deref()), 40),
        ]);
    }
    print_table(
        &["ID", "類型", "狀態", "建立時間", "完成時間", "重試", "錯誤"],
        rows,
        "：還沒有任何 Job。collector 排到工作時才會建立。",
    );
    Ok(())
}
