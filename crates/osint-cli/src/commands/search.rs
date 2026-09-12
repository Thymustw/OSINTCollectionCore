//! `osint-cli search`：直接查 OpenSearch 投影（SPEC §18）。
//!
//! 查詢的組法與 `POST /api/v1/search` **完全相同**——兩邊都呼叫
//! `indexer::search::build`。分成兩份實作會出現「CLI 查得到、API 查不到」，
//! 而且不會有任何錯誤訊息。

use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use indexer::search::{EntityFilter, SearchRequest};
use indexer::{DateField, schema};
use serde::Serialize;
use serde_json::Value;
use storage_core::{SearchHit, SearchStore};

use crate::cli::SearchArgs;
use crate::context::Context;
use crate::error::CliError;
use crate::output::{Format, opt, print_json, print_table, truncate};

/// JSON 輸出的一筆結果。
#[derive(Debug, Serialize)]
pub struct Hit {
    pub document_id: String,
    pub score: Option<f64>,
    pub title: Option<String>,
    pub snippet: Option<String>,
    pub object_type: Option<String>,
    pub language: Option<String>,
    pub source_id: Option<String>,
    pub connector_id: Option<String>,
    pub raw_evidence_id: Option<String>,
    pub canonical_url: Option<String>,
    pub published_at: Option<String>,
    pub observed_at: Option<String>,
    pub entities: Vec<Value>,
}

#[derive(Debug, Serialize)]
pub struct Output {
    pub total: u64,
    pub index: String,
    pub hits: Vec<Hit>,
}

pub async fn run(ctx: &Context, args: SearchArgs) -> Result<(), CliError> {
    let request = to_request(&args)?;
    let index = ctx.cfg.indexer.index.clone();
    // build 會在打到 OpenSearch **之前**擋下語法錯與不合理的區間，
    // 那時使用者看到的是「怎麼改」而不是「沒有結果」。
    let query =
        indexer::search::build(&index, &request).map_err(|err| CliError::InvalidArgument {
            message: err.to_string(),
        })?;

    let store = ctx.search().await?;
    let hits = store.search(query).await?;

    let output = Output {
        total: hits.total,
        index: index.clone(),
        hits: hits.hits.iter().map(to_hit).collect(),
    };

    if ctx.format == Format::Json {
        print_json(&output)?;
        return Ok(());
    }

    let rows = output
        .hits
        .iter()
        .map(|hit| {
            vec![
                hit.document_id.clone(),
                hit.score.map_or_else(|| "-".into(), |s| format!("{s:.2}")),
                truncate(hit.title.as_deref().unwrap_or("-"), 40),
                truncate(&strip_highlight(hit.snippet.as_deref().unwrap_or("-")), 60),
                opt(hit.published_at.as_deref()),
            ]
        })
        .collect();
    print_table(
        &["document_id", "score", "title", "snippet", "published_at"],
        rows,
        &format!(
            "index `{index}` 沒有符合的文件。\n\
             若確定資料已經進 PostgreSQL，代表投影還沒建立——請跑 `make run-indexer`（常駐）\
             或 `make rebuild-index`（一次性補齊）。\n\
             要看完整欄位（含 raw_evidence_id）請加 --json。"
        ),
    );
    if !output.hits.is_empty() {
        println!(
            "符合條件共 {} 筆（本頁顯示 {} 筆）。加 --json 可取得 raw_evidence_id 與 entities。",
            output.total,
            output.hits.len()
        );
    }
    Ok(())
}

fn to_request(args: &SearchArgs) -> Result<SearchRequest, CliError> {
    let entity = match args
        .entity
        .as_deref()
        .map(str::trim)
        .filter(|e| !e.is_empty())
    {
        Some(raw) => Some(parse_entity(raw)?),
        None => None,
    };
    Ok(SearchRequest {
        query: args.query.clone(),
        source_id: args.source,
        connector_id: args.connector,
        entity,
        date_from: args
            .from
            .as_deref()
            .map(|raw| parse_date(raw, false))
            .transpose()?,
        date_to: args
            .to
            .as_deref()
            .map(|raw| parse_date(raw, true))
            .transpose()?,
        date_field: parse_date_field(&args.date_field)?,
        language: args.lang.clone(),
        object_type: args.object_type.clone(),
        include_duplicates: args.include_duplicates,
        limit: Some(args.limit),
        // CLI 一次只取一頁。要翻頁請用 API——終端機互動下「下一頁游標」沒有使用情境，
        // 而把 cursor 貼來貼去比直接調大 -n 更麻煩。
        cursor: None,
    })
}

/// `型別:名稱` 或只給名稱。
///
/// 只在**第一個** `:` 切：entity 名稱本身可能含冒號（IPv6、URL）。
fn parse_entity(raw: &str) -> Result<EntityFilter, CliError> {
    let (entity_type, name) = match raw.split_once(':') {
        Some((kind, rest)) if is_entity_type(kind) => (Some(kind.to_string()), rest.trim()),
        // `example.com`、`2001:db8::1`、`https://x/y`——沒有已知型別前綴時整串當名稱。
        _ => (None, raw),
    };
    if name.is_empty() {
        return Err(CliError::InvalidArgument {
            message: format!(
                "--entity `{raw}` 沒有名稱。請用 `型別:名稱`（例如 \
                 `vulnerability:CVE-2026-0001`）或只給名稱（例如 `example.com`）"
            ),
        });
    }
    Ok(EntityFilter {
        entity_type,
        name: name.to_string(),
    })
}

/// SPEC §10 的 entity 型別。
///
/// 白名單而不是「有冒號就當型別」：`2001:db8::1` 的 `2001` 會被當成型別，
/// 於是查詢變成「型別 2001、名稱 db8::1」——永遠查不到，而且不會報錯。
const ENTITY_TYPES: &[&str] = &[
    "person",
    "organization",
    "account",
    "domain",
    "hostname",
    "ip",
    "url",
    "email",
    "file",
    "hash",
    "vulnerability",
    "malware",
    "campaign",
    "location",
    "event",
];

fn is_entity_type(value: &str) -> bool {
    ENTITY_TYPES.contains(&value.to_ascii_lowercase().as_str())
}

fn parse_date_field(raw: &str) -> Result<DateField, CliError> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "effective" | "" => Ok(DateField::Effective),
        "published" => Ok(DateField::Published),
        "observed" => Ok(DateField::Observed),
        other => Err(CliError::InvalidArgument {
            message: format!(
                "--date-field `{other}` 不認得。可用值：effective（預設）／published／observed"
            ),
        }),
    }
}

/// `YYYY-MM-DD` 或完整 RFC3339。
///
/// `end = true` 時把純日期補成當天 23:59:59——否則 `--to 2026-09-10` 會變成
/// 當天 00:00:00，**把整個 9/10 排除在外**，而使用者的意思幾乎一定是包含當天。
fn parse_date(raw: &str, end: bool) -> Result<DateTime<Utc>, CliError> {
    let trimmed = raw.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(trimmed) {
        return Ok(dt.with_timezone(&Utc));
    }
    if let Ok(date) = NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
        let time = if end {
            date.and_hms_opt(23, 59, 59)
        } else {
            date.and_hms_opt(0, 0, 0)
        };
        if let Some(naive) = time {
            if let Some(dt) = Utc.from_local_datetime(&naive).single() {
                return Ok(dt);
            }
        }
    }
    Err(CliError::InvalidArgument {
        message: format!(
            "時間 `{raw}` 格式不對。請用 `YYYY-MM-DD`（例如 2026-09-10）\
             或完整 RFC3339（例如 2026-09-10T12:00:00Z）"
        ),
    })
}

fn to_hit(hit: &SearchHit) -> Hit {
    let source = &hit.source;
    Hit {
        document_id: hit.id.clone(),
        score: hit.score,
        title: text(source, schema::F_TITLE),
        snippet: snippet(hit),
        object_type: text(source, schema::F_OBJECT_TYPE),
        language: text(source, schema::F_LANGUAGE),
        source_id: text(source, schema::F_SOURCE_ID),
        connector_id: text(source, schema::F_CONNECTOR_ID),
        raw_evidence_id: text(source, schema::F_RAW_EVIDENCE_ID),
        canonical_url: text(source, "canonical_url"),
        published_at: text(source, schema::F_PUBLISHED_AT),
        observed_at: text(source, schema::F_OBSERVED_AT),
        entities: source
            .get(schema::F_ENTITIES)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    }
}

fn text(source: &Value, field: &str) -> Option<String> {
    source
        .get(field)
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn snippet(hit: &SearchHit) -> Option<String> {
    for field in [schema::F_TITLE, schema::F_SUMMARY, schema::F_BODY] {
        if let Some(fragment) = hit.highlights.get(field).and_then(|frags| frags.first()) {
            return Some(fragment.clone());
        }
    }
    text(&hit.source, schema::F_SUMMARY).or_else(|| text(&hit.source, schema::F_BODY))
}

/// 表格輸出時拿掉 highlight 的 `<em>` 標記——終端機不會渲染它們，只會變成雜訊。
/// `--json` 保留原樣，讓呼叫端自己決定怎麼呈現。
fn strip_highlight(text: &str) -> String {
    text.replace("<em>", "").replace("</em>", "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entity_with_known_type_prefix_is_split() {
        let filter = parse_entity("vulnerability:CVE-2026-0001").unwrap();
        assert_eq!(filter.entity_type.as_deref(), Some("vulnerability"));
        assert_eq!(filter.name, "CVE-2026-0001");
    }

    #[test]
    fn ipv6_is_not_mistaken_for_a_type_prefix() {
        // `2001` 不是已知型別，整串都是名稱。
        let filter = parse_entity("2001:db8::1").unwrap();
        assert_eq!(filter.entity_type, None);
        assert_eq!(filter.name, "2001:db8::1");
    }

    #[test]
    fn url_is_not_mistaken_for_a_type_prefix() {
        let filter = parse_entity("https://example.com/a").unwrap();
        assert_eq!(filter.entity_type, None);
        assert_eq!(filter.name, "https://example.com/a");
    }

    #[test]
    fn bare_name_has_no_type() {
        let filter = parse_entity("example.com").unwrap();
        assert_eq!(filter.entity_type, None);
        assert_eq!(filter.name, "example.com");
    }

    #[test]
    fn entity_type_without_name_is_rejected() {
        let err = parse_entity("ip:").unwrap_err();
        assert!(err.to_string().contains("沒有名稱"), "{err}");
    }

    #[test]
    fn plain_date_to_covers_the_whole_day() {
        // `--to 2026-09-10` 若變成當天 00:00:00，整個 9/10 都會被排除。
        let to = parse_date("2026-09-10", true).unwrap();
        assert_eq!(to.to_rfc3339(), "2026-09-10T23:59:59+00:00");
        let from = parse_date("2026-09-10", false).unwrap();
        assert_eq!(from.to_rfc3339(), "2026-09-10T00:00:00+00:00");
    }

    #[test]
    fn rfc3339_is_accepted_as_is() {
        let dt = parse_date("2026-09-10T12:00:00Z", true).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-09-10T12:00:00+00:00");
    }

    #[test]
    fn bad_date_says_which_formats_work() {
        let err = parse_date("2026/09/10", false).unwrap_err();
        assert!(err.to_string().contains("YYYY-MM-DD"), "{err}");
        assert!(err.to_string().contains("RFC3339"), "{err}");
    }

    #[test]
    fn date_field_values() {
        assert_eq!(parse_date_field("effective").unwrap(), DateField::Effective);
        assert_eq!(parse_date_field("PUBLISHED").unwrap(), DateField::Published);
        assert_eq!(parse_date_field("observed").unwrap(), DateField::Observed);
        let err = parse_date_field("created").unwrap_err();
        assert!(err.to_string().contains("effective"), "{err}");
    }

    #[test]
    fn highlight_markup_is_stripped_for_the_table() {
        assert_eq!(strip_highlight("a <em>b</em> c"), "a b c");
    }
}
