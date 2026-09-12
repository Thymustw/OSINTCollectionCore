//! clap 子命令定義。
//!
//! 只有查詢子命令。這裡刻意沒有 create/update/delete：寫入必須走 core-api，
//! 那條路徑才有 RBAC 與 AuditLog（見 `docs/user/cli.md`）。

use clap::{Args, Parser, Subcommand};
use uuid::Uuid;

/// 每次向 storage 要一頁的筆數上限。`RelationalStore` 的 list 方法把 limit 夾在 1..=100，
/// 所以要拿更多筆就得用 cursor continue 翻頁，不能直接傳一個大 limit（會被靜默夾掉）。
pub const PAGE_SIZE: u32 = 100;

#[derive(Debug, Parser)]
#[command(
    name = "osint-cli",
    version,
    about = "OSINT Intelligence Core 本機唯讀查詢工具",
    long_about = "OSINT Intelligence Core 本機唯讀查詢工具。

這是「本機管理工具」，不是給遠端使用者的介面：
  * 它直接連 Core 的 PostgreSQL／MinIO，不經過 core-api。
  * 因此它不受 API 的 RBAC 與 AuditLog 保護——只在你自己能存取資料庫的機器上用。
  * 它只做查詢。任何寫入／刪除都要走 POST/PATCH/DELETE /api/v1/...，
    那條路徑才有授權與稽核紀錄。

設定來源與其他 binary 相同：config/default.toml → OSINT_CONFIG_FILE → OSINT__* 環境變數，
密鑰走 SecretRef（例如 DATABASE_URL）。執行前請先 `make compose-up`。"
)]
pub struct Cli {
    /// 改輸出成 JSON（方便接 jq／腳本）。預設是人類可讀表格。
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// 採集來源（Source）
    Sources {
        #[command(subcommand)]
        action: SourceAction,
    },
    /// 連接器（Connector）
    Connectors {
        #[command(subcommand)]
        action: ConnectorAction,
    },
    /// 原始證據（RawEvidence）
    Raw {
        #[command(subcommand)]
        action: RawAction,
    },
    /// 正規化後的文件（Document）
    Documents {
        #[command(subcommand)]
        action: DocumentAction,
    },
    /// 抽取出的實體（Entity）
    Entities {
        #[command(subcommand)]
        action: EntityAction,
    },
    /// 工作（Job）
    Jobs {
        #[command(subcommand)]
        action: JobAction,
    },
    /// 搜尋已索引的 Document（SPEC §18）
    ///
    /// 直接查 OpenSearch，不經 core-api——與其他子命令一致。
    /// 查詢語法（片語、AND/OR/NOT、括號）見 `docs/user/search.md`。
    Search(SearchArgs),
    /// 對 PostgreSQL／MinIO／Redis／OpenSearch／Redpanda 各做一次 health check
    Health,
}

/// 所有 list 子命令共用的分頁參數。
#[derive(Debug, Args)]
pub struct ListArgs {
    /// 最多列出幾筆（依 id 遞減；id 是 UUID v7 時等同最新的在前）。超過 100 會自動翻頁取得。
    #[arg(long, short = 'n', default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=10_000))]
    pub limit: u32,
}

#[derive(Debug, Subcommand)]
pub enum SourceAction {
    /// 列出 Source
    List(ListArgs),
    /// 顯示單一 Source 的完整欄位
    Show {
        /// Source 的 UUID
        id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
pub enum ConnectorAction {
    /// 列出 Connector（預設只顯示 enabled）
    List {
        #[command(flatten)]
        list: ListArgs,
        /// 連停用（enabled = false）的 connector 也一起列出
        #[arg(long)]
        all: bool,
    },
    /// 顯示單一 Connector 的完整欄位
    Show {
        /// Connector 的 UUID
        id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
pub enum RawAction {
    /// 列出 RawEvidence
    List {
        #[command(flatten)]
        list: ListArgs,
        /// 只列出這個 Source 底下的 RawEvidence
        #[arg(long, value_name = "SOURCE_ID")]
        source: Option<Uuid>,
    },
    /// 顯示單一 RawEvidence 的 metadata，可選擇一併印出 MinIO 內容
    Show {
        /// RawEvidence 的 UUID
        id: Uuid,
        /// 從物件儲存取回內容並印出。非 UTF-8（PDF 等）只會印大小與 SHA256，
        /// 不會把二進位往終端機噴。
        #[arg(long)]
        body: bool,
        /// `--body` 印出的位元組上限，超過就截斷並提示。
        #[arg(long, default_value_t = 65_536, value_name = "BYTES")]
        max_body_bytes: usize,
    },
}

#[derive(Debug, Subcommand)]
pub enum DocumentAction {
    /// 列出 Document
    List(ListArgs),
    /// 顯示單一 Document，含 provenance 鏈（哪個 processor 從哪筆 RawEvidence 產生）
    Show {
        /// Document 的 UUID
        id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
pub enum EntityAction {
    /// 列出 Entity
    ///
    /// ⚠️ 排序依 `id` 遞減，而 Entity 的 id 是 UUID v5（由 entity_type + normalized_name
    /// 推導），**沒有時間序**。要看最新的請改用 `--json` 後自己依 `last_seen` 排。
    List {
        #[command(flatten)]
        list: ListArgs,
        /// 只列出這個型別（例如 `vulnerability`／`ip`／`domain`／`email`／`hash`）
        #[arg(long, value_name = "TYPE")]
        entity_type: Option<String>,
    },
    /// 顯示單一 Entity，含它出現在哪些 Document 與相關的 Relationship
    Show {
        /// Entity 的 UUID
        id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
pub enum JobAction {
    /// 列出 Job
    List(ListArgs),
}

/// `osint-cli search` 的參數。八種搜尋（SPEC §18）各對應一個。
#[derive(Debug, Args)]
pub struct SearchArgs {
    /// 查詢字串。支援 `"片語"`、`AND`／`OR`／`NOT`（必須全大寫）與括號。
    ///
    /// 留空時只用下面的過濾條件（例如只給 --source 就是列出該來源的文件）。
    #[arg(default_value = "")]
    pub query: String,

    /// 只找這個 Source 底下的文件
    #[arg(long, value_name = "SOURCE_ID")]
    pub source: Option<Uuid>,

    /// 只找這個 Connector 產生的文件
    #[arg(long, value_name = "CONNECTOR_ID")]
    pub connector: Option<Uuid>,

    /// 只找提到這個實體的文件。格式 `型別:名稱`（例如 `vulnerability:CVE-2026-0001`）
    /// 或只給名稱（例如 `example.com`）。名稱大小寫不敏感。
    #[arg(long, value_name = "TYPE:NAME")]
    pub entity: Option<String>,

    /// 起始時間。`YYYY-MM-DD` 或完整 RFC3339（`2026-09-10T12:00:00Z`）
    #[arg(long, value_name = "DATE")]
    pub from: Option<String>,

    /// 結束時間。格式同 --from
    #[arg(long, value_name = "DATE")]
    pub to: Option<String>,

    /// --from/--to 比對哪個時間欄位。
    /// `effective`（預設，published 有值就用它、否則用 observed）／`published`／`observed`
    #[arg(long, value_name = "FIELD", default_value = "effective")]
    pub date_field: String,

    /// 只找這個語言（例如 `en`／`zh`）
    #[arg(long, value_name = "LANG")]
    pub lang: Option<String>,

    /// 只找這個物件型別（`article`／`web_page`／`post`／`report`／`advisory`…）
    #[arg(long = "type", value_name = "OBJECT_TYPE")]
    pub object_type: Option<String>,

    /// 連重複文件也列出來。預設不列——偵錯用：正常情況下 index 裡沒有 duplicate，
    /// 真的查到東西代表有 duplicate 漏進索引。
    #[arg(long)]
    pub include_duplicates: bool,

    /// 最多列出幾筆（上限 100）
    #[arg(long, short = 'n', default_value_t = 20, value_parser = clap::value_parser!(u32).range(1..=100))]
    pub limit: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn cli_definition_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn json_flag_is_global() {
        let cli = Cli::try_parse_from(["osint-cli", "sources", "list", "--json"]).unwrap();
        assert!(cli.json);
    }

    #[test]
    fn limit_defaults_to_20() {
        let cli = Cli::try_parse_from(["osint-cli", "documents", "list"]).unwrap();
        match cli.command {
            Command::Documents {
                action: DocumentAction::List(args),
            } => assert_eq!(args.limit, 20),
            other => panic!("預期 documents list，得到 {other:?}"),
        }
    }

    #[test]
    fn limit_zero_is_rejected() {
        // limit 0 沒有意義，要在 parse 階段就擋下來，而不是送到 SQL 再被夾成 1。
        assert!(Cli::try_parse_from(["osint-cli", "jobs", "list", "--limit", "0"]).is_err());
    }

    #[test]
    fn entities_list_accepts_a_type_filter() {
        let cli =
            Cli::try_parse_from(["osint-cli", "entities", "list", "--entity-type", "ip"]).unwrap();
        match cli.command {
            Command::Entities {
                action: EntityAction::List { entity_type, .. },
            } => assert_eq!(entity_type.as_deref(), Some("ip")),
            other => panic!("預期 entities list，得到 {other:?}"),
        }
    }

    #[test]
    fn entities_show_requires_a_uuid() {
        assert!(Cli::try_parse_from(["osint-cli", "entities", "show", "CVE-2026-0001"]).is_err());
        assert!(
            Cli::try_parse_from([
                "osint-cli",
                "entities",
                "show",
                "0199f3aa-1b2c-7d3e-8f40-a1b2c3d4e5f6"
            ])
            .is_ok()
        );
    }

    #[test]
    fn bad_uuid_is_rejected() {
        assert!(Cli::try_parse_from(["osint-cli", "sources", "show", "not-a-uuid"]).is_err());
    }

    #[test]
    fn there_is_no_write_subcommand() {
        // 唯讀是這支工具的安全前提。有人之後加了寫入子命令，這個測試要先擋下來。
        for verb in ["create", "delete", "update", "put", "import"] {
            assert!(
                Cli::try_parse_from(["osint-cli", "sources", verb]).is_err(),
                "osint-cli 不該有寫入子命令 `{verb}`；寫入必須走 core-api 才有 RBAC/audit"
            );
        }
    }
}
