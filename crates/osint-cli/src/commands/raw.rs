//! `osint-cli raw ...`

use sha2::{Digest, Sha256};
use storage_core::{ObjectStore, RelationalStore};
use uuid::Uuid;

use crate::cli::{ListArgs, RawAction};
use crate::commands::collect_paged;
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_detail, print_json, print_table, truncate, ts};

pub async fn run(ctx: &Context, action: RawAction) -> Result<(), CliError> {
    match action {
        RawAction::List { list, source } => list_raw(ctx, list, source).await,
        RawAction::Show {
            id,
            body,
            max_body_bytes,
        } => show(ctx, id, body, max_body_bytes).await,
    }
}

async fn list_raw(ctx: &Context, args: ListArgs, source: Option<Uuid>) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let items = match source {
        Some(source_id) => {
            collect_paged!(store, list_raw_evidence_by_source(source_id), args.limit)
        }
        None => collect_paged!(store, list_raw_evidence(), args.limit),
    };

    if ctx.format == Format::Json {
        return print_json(&items);
    }
    let rows = items
        .iter()
        .map(|e| {
            vec![
                e.id.to_string(),
                ts(Some(e.retrieved_at)),
                truncate(&e.source_url, 48),
                opt(e.content_type.as_deref()),
                e.content_length
                    .map_or_else(|| "-".into(), |n| n.to_string()),
                e.http_status.map_or_else(|| "-".into(), |n| n.to_string()),
                e.sha256.chars().take(12).collect(),
            ]
        })
        .collect();
    let hint = if source.is_some() {
        "：這個 Source 底下還沒有 RawEvidence。先確認 id 正確（`osint-cli sources list`）。"
    } else {
        "：還沒有任何 RawEvidence。先跑 collector 或 `POST /api/v1/import`。"
    };
    print_table(
        &[
            "ID",
            "取得時間",
            "來源 URL",
            "Content-Type",
            "位元組",
            "HTTP",
            "SHA256(前 12)",
        ],
        rows,
        hint,
    );
    Ok(())
}

async fn show(ctx: &Context, id: Uuid, want_body: bool, max_bytes: usize) -> Result<(), CliError> {
    let store = ctx.store().await?;
    let evidence = store
        .get_raw_evidence(id)
        .await?
        .ok_or_else(|| CliError::NotFound {
            kind: "RawEvidence",
            id: id.to_string(),
            list_hint: "raw list",
        })?;

    let body = if want_body {
        Some(fetch_body(ctx, &evidence.storage_path).await?)
    } else {
        None
    };

    if ctx.format == Format::Json {
        let mut value = serde_json::to_value(&evidence)?;
        if let Some(bytes) = &body {
            let rendered = render_body(bytes, max_bytes);
            value["_body"] = serde_json::json!({
                "bytes": bytes.len(),
                "sha256": sha256_hex(bytes),
                "utf8": rendered.is_text,
                "truncated": rendered.truncated,
                "text": rendered.text,
            });
        }
        return print_json(&value);
    }

    print_detail(vec![
        ("id", evidence.id.to_string()),
        ("source_id", evidence.source_id.to_string()),
        ("connector_id", evidence.connector_id.to_string()),
        (
            "collection_id",
            evidence
                .collection_id
                .map_or_else(|| "-".into(), |v| v.to_string()),
        ),
        ("external_id", opt(evidence.external_id.as_deref())),
        ("source_url", evidence.source_url.clone()),
        ("retrieved_at", ts(Some(evidence.retrieved_at))),
        ("content_type", opt(evidence.content_type.as_deref())),
        ("mime_type", opt(evidence.mime_type.as_deref())),
        (
            "content_length",
            evidence
                .content_length
                .map_or_else(|| "-".into(), |v| v.to_string()),
        ),
        ("sha256", evidence.sha256.clone()),
        ("storage_path", evidence.storage_path.clone()),
        (
            "http_status",
            evidence
                .http_status
                .map_or_else(|| "-".into(), |v| v.to_string()),
        ),
        (
            "http_headers",
            serde_json::to_string(&evidence.http_headers)?,
        ),
        ("metadata", serde_json::to_string(&evidence.metadata)?),
        ("collector_version", evidence.collector_version.clone()),
    ]);

    if let Some(bytes) = body {
        let rendered = render_body(&bytes, max_bytes);
        println!();
        println!(
            "--- body（{} bytes，SHA256 {}）---",
            bytes.len(),
            sha256_hex(&bytes)
        );
        match rendered.text {
            Some(text) => {
                println!("{text}");
                if rendered.truncated {
                    println!(
                        "--- 已截斷：只顯示前 {max_bytes} bytes，完整內容共 {} bytes。用 --max-body-bytes 調整。---",
                        bytes.len()
                    );
                }
            }
            None => {
                println!(
                    "內容不是合法 UTF-8（可能是 PDF／圖片等二進位），不印出來以免弄亂終端機。"
                );
                println!(
                    "要取原始內容，請直接從物件儲存讀 key `{}`（bucket `{}`）。",
                    evidence.storage_path, ctx.cfg.storage.object.bucket
                );
            }
        }
    }
    Ok(())
}

async fn fetch_body(ctx: &Context, storage_path: &str) -> Result<Vec<u8>, CliError> {
    let objects = ctx.objects()?;
    let key = object_key(storage_path, &ctx.cfg.storage.object.bucket);
    objects
        .get(&key)
        .await
        .map_err(|err| CliError::ObjectStoreUnavailable {
            message: err.to_string(),
        })?
        .ok_or(CliError::NotFound {
            kind: "RawEvidence body（物件儲存裡的內容）",
            id: key,
            list_hint: "raw show <id>",
        })
}

/// `raw_evidence.storage_path` 平常存的就是物件 key（`raw/{source}/{y}/{m}/{d}/{id}`）。
/// 舊資料或測試 fixture 可能寫成 `s3://{bucket}/{key}`，這裡一併處理，
/// 否則會拿整串去查而得到「找不到」——那個錯誤完全不指向真因。
fn object_key(storage_path: &str, bucket: &str) -> String {
    let trimmed = storage_path
        .strip_prefix("s3://")
        .unwrap_or(storage_path)
        .to_string();
    trimmed
        .strip_prefix(&format!("{bucket}/"))
        .unwrap_or(&trimmed)
        .to_string()
}

struct RenderedBody {
    is_text: bool,
    truncated: bool,
    text: Option<String>,
}

/// 決定 body 怎麼呈現。
///
/// 先截斷再驗 UTF-8 會把多位元組字元切一半而誤判成二進位，所以**先驗整份**，
/// 確認是文字後才依字元邊界截斷。
fn render_body(bytes: &[u8], max_bytes: usize) -> RenderedBody {
    match std::str::from_utf8(bytes) {
        Err(_) => RenderedBody {
            is_text: false,
            truncated: false,
            text: None,
        },
        Ok(text) => {
            if text.len() <= max_bytes {
                RenderedBody {
                    is_text: true,
                    truncated: false,
                    text: Some(text.to_string()),
                }
            } else {
                let mut end = max_bytes;
                while end > 0 && !text.is_char_boundary(end) {
                    end -= 1;
                }
                RenderedBody {
                    is_text: true,
                    truncated: true,
                    text: Some(text[..end].to_string()),
                }
            }
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_key_is_unchanged() {
        assert_eq!(
            object_key("raw/abc/2026/09/12/def", "raw-evidence"),
            "raw/abc/2026/09/12/def"
        );
    }

    #[test]
    fn s3_url_is_stripped() {
        assert_eq!(
            object_key("s3://raw-evidence/raw/a/b", "raw-evidence"),
            "raw/a/b"
        );
    }

    #[test]
    fn binary_body_is_not_rendered() {
        // PDF 開頭 + 一個非法 UTF-8 位元組。
        let bytes = b"%PDF-1.7\n\xff\xfe binary";
        let rendered = render_body(bytes, 1024);
        assert!(!rendered.is_text);
        assert!(rendered.text.is_none());
    }

    #[test]
    fn text_body_is_rendered_whole() {
        let rendered = render_body(b"hello", 1024);
        assert!(rendered.is_text);
        assert!(!rendered.truncated);
        assert_eq!(rendered.text.as_deref(), Some("hello"));
    }

    #[test]
    fn truncation_respects_char_boundary() {
        // 「漏」是 3 bytes。max=4 時只能留 3 bytes，不能切出半個字元。
        let rendered = render_body("漏洞".as_bytes(), 4);
        assert!(rendered.is_text);
        assert!(rendered.truncated);
        assert_eq!(rendered.text.as_deref(), Some("漏"));
    }

    #[test]
    fn sha256_matches_known_value() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
