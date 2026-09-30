# 命令列工具 osint-cli

`osint-cli` 讓你從終端機直接查看系統裡的資料：有哪些資料來源、最近收了什麼原始證據、產生了哪些文件、某份文件的來源鏈是什麼、五個後端服務有沒有在跑。

---

## 前提：這是本機開發工具

`osint-cli` **直接連資料庫，不經過 API**。因此：

- **需要** Rust 工具鏈（在本機 `cargo build`）
- **需要** repo 根目錄的 `.env`（含 `DATABASE_URL` 等密鑰，可從 `.env.example` 複製）
- **沒有** RBAC 權限控管
- **沒有** 稽核紀錄

!!! warning "只在你自己已經能存取 Core 資料庫的機器上使用"
    CLI 能讀到什麼，取決於 `.env` 裡的 `DATABASE_URL` 指到哪裡。它不會另外做一次權限檢查。正式環境的資料操作請走 API。

所有子指令都是**唯讀**的，刻意沒有實作任何寫入或刪除功能。

---

## 安裝與執行

```bash
cp .env.example .env   # 已有 .env 就跳過
make compose-up        # 確認 PostgreSQL / SeaweedFS / Redis / OpenSearch / Redpanda 都在跑
```

執行方式：

```bash
make run-cli ARGS="documents list"
```

等同直接跑 cargo：

```bash
cargo run -p osint-cli --bin osint-cli -- documents list
```

以下範例都用 `make run-cli ARGS="..."` 格式。

---

## 共通選項

| 選項 | 說明 |
|---|---|
| `--json` | 改輸出 JSON，可直接接 `jq` |
| `--limit N` / `-n N` | list 類最多列出幾筆，預設 20，範圍 1–10,000 |
| `--help` | 每一層子指令都有 |

**排序**：一律依 `id` 由大到小。`Source`、`RawEvidence`、`Document`、`Job` 的 id 是 UUID v7（前 48 bit 是毫秒時間戳），所以等同「最新的在前」。Connector 例外——它的 id 是 UUID v5，沒有時間序。

**Exit code**：

| 值 | 意思 |
|---|---|
| 0 | 正常 |
| 1 | 指令失敗（連不上、找不到、設定錯） |
| 2 | 只有 `health` 會用：指令本身跑成功，但有服務不健康 |

---

## 子指令

### `health`

對五個後端各做一次真實的健康檢查，平行執行，單項逾時 10 秒：

```bash
make run-cli ARGS="health"
```

```text
┌──────────────┬──────┬────────────────────────┬──────────────────────────┐
│ 服務         ┆ 狀態 ┆ 位址                   ┆ 訊息                     │
╞══════════════╪══════╪════════════════════════╪══════════════════════════╡
│ PostgreSQL   ┆ OK   ┆ env:DATABASE_URL       ┆ SELECT 1 成功            │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ SeaweedFS/S3 ┆ OK   ┆ http://127.0.0.1:8333  ┆ bucket 存在              │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Redis        ┆ OK   ┆ env:REDIS_URL          ┆ PING PONG                │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ OpenSearch   ┆ OK   ┆ http://127.0.0.1:19200 ┆ cluster status=green     │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ Redpanda     ┆ OK   ┆ 127.0.0.1:9092         ┆ metadata OK，1 個 broker │
└──────────────┴──────┴────────────────────────┴──────────────────────────┘
共 5 筆
```

PostgreSQL 與 Redis 的「位址」欄顯示 SecretRef 名稱而不是連線字串，避免把密碼印到終端機。

一項失敗不會中斷其他項。有任一項 DOWN 時 exit code 是 2。

---

### `sources list`

```bash
make run-cli ARGS="sources list --limit 5"
```

```text
┌──────────────────────────────────────┬─────────────────┬──────┬──────┬─────────────────────────────┬──────────────────────┐
│ ID                                   ┆ 名稱            ┆ 類型 ┆ 啟用 ┆ Base URL                    ┆ 最後出現             │
╞══════════════════════════════════════╪═════════════════╪══════╪══════╪═════════════════════════════╪══════════════════════╡
│ 01a091e3-c2ce-7613-bb68-7fb9d4d07657 ┆ conformance-rss ┆ rss  ┆ 是   ┆ https://example.invalid/rss ┆ 2026-09-10T12:00:00Z │
└──────────────────────────────────────┴─────────────────┴──────┴──────┴─────────────────────────────┴──────────────────────┘
共 1 筆
```

### `sources show <id>`

縱向列出全部欄位（含 `collection_policy` 的 JSON）：

```bash
make run-cli ARGS="sources show 01a091e3-c2ce-7613-bb68-7fb9d4d07657"
```

---

### `connectors list`

預設只顯示 `enabled = true` 的連接器。要看含停用的加 `--all`：

```bash
make run-cli ARGS="connectors list"          # 只有啟用中的
make run-cli ARGS="connectors list --all -n 10"  # 含停用的
```

`--limit` 算的是**過濾後**的筆數，不是資料庫翻出的前 N 筆。

### `connectors show <id>`

`credential_reference` / `proxy_reference` 顯示的是 SecretRef 字串（例如 `env:NVD_TOKEN`），不是明文密鑰。

---

### `raw list`

```bash
make run-cli ARGS="raw list --limit 2"
```

```text
┌──────────────────────────────────────┬──────────────────────┬──────────────────────────────────────────────────┬──────────────────┬────────┬──────┬───────────────┐
│ ID                                   ┆ 取得時間             ┆ 來源 URL                                         ┆ Content-Type     ┆ 位元組 ┆ HTTP ┆ SHA256(前 12) │
╞══════════════════════════════════════╪══════════════════════╪══════════════════════════════════════════════════╪══════════════════╪════════╪══════╪═══════════════╡
│ 01a0f107-3605-71e3-b50c-b10e7ea7a68e ┆ 2026-09-30T06:36:16Z ┆ stix://import/01a0f107-35c4-7445-ac48-e7cb911e2… ┆ application/json ┆ 197    ┆ -    ┆ ed70ff81d0ae  │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 01a0f0fd-ee54-728a-88ab-dfe02db79d5d ┆ 2026-09-30T06:26:08Z ┆ import://json/demo-advisory.json                 ┆ application/json ┆ 1041   ┆ -    ┆ 5bbd3ce1af19  │
└──────────────────────────────────────┴──────────────────────┴──────────────────────────────────────────────────┴──────────────────┴────────┴──────┴───────────────┘
共 2 筆
```

篩選特定來源：

```bash
make run-cli ARGS="raw list --source <SOURCE_ID>"
```

### `raw show <id>`

只看 metadata：

```bash
make run-cli ARGS="raw show <ID>"
```

加 `--body` 從物件儲存取回實際內容（預設最多 65,536 bytes，可用 `--max-body-bytes` 調整）：

```bash
make run-cli ARGS="raw show <ID> --body --max-body-bytes 300"
```

二進位內容不會噴到終端機——如果內容不是合法 UTF-8，只印大小與 SHA256。

---

### `documents list`

```bash
make run-cli ARGS="documents list --limit 3"
```

```text
┌──────────────────────────────────────┬──────────┬─────────────────────────────────────────┬──────┬──────────────────────┬──────┬──────┬──────┐
│ ID                                   ┆ 類型     ┆ 標題                                    ┆ 語言 ┆ 觀察時間             ┆ 信心 ┆ 去重 ┆ 標籤 │
╞══════════════════════════════════════╪══════════╪═════════════════════════════════════════╪══════╪══════════════════════╪══════╪══════╪══════╡
│ 01a0f100-fd58-74d0-bbf5-cdf9ad4bc61e ┆ advisory ┆ 威脅情報：針對 Acme Router 的攻擊活動… ┆ -    ┆ 2026-09-30T06:29:29Z ┆ 0.80 ┆ 重複 ┆      │
└──────────────────────────────────────┴──────────┴─────────────────────────────────────────┴──────┴──────────────────────┴──────┴──────┴──────┘
共 1 筆
```

「去重」欄的三種值：

| 值 | 意思 |
|---|---|
| `canonical` | 處理過，判定這份不是任何人的重複 |
| `重複` | 處理過，判定為重複；`documents show` 會告訴你 canonical 是哪一份 |
| `未去重` | **還沒被處理過**，不代表它不是重複 |

### `documents show <id>`

這是 provenance（來源鏈）的入口，除了文件本身，還會印出 provenance 鏈與來源 RawEvidence：

```bash
make run-cli ARGS="documents show <ID>"
```

從 provenance 鏈的 `raw_evidence_id` 可以往回追：

```bash
make run-cli ARGS="raw show <RAW_EVIDENCE_ID> --body"
```

整條鏈是：**Document → Provenance → RawEvidence → 原始位元組**。

加 `--json` 後可以接 `jq`：

```bash
make run-cli ARGS="documents show <ID> --json" | jq '.provenance[].processor'
make run-cli ARGS="documents show <ID> --json" | jq '.entities[].entity.normalized_name'
```

---

### `entities list`

```bash
make run-cli ARGS="entities list --limit 3"
make run-cli ARGS="entities list --entity-type vulnerability"
```

```text
┌──────────────────────────────────────┬──────────────┬──────────────────────────┬──────┬──────────────────────┬──────────────────────┐
│ ID                                   ┆ 型別         ┆ 正規化名稱               ┆ 信心 ┆ 首次出現             ┆ 最後出現             │
╞══════════════════════════════════════╪══════════════╪══════════════════════════╪══════╪══════════════════════╪══════════════════════╡
│ ffe0bc3f-b930-5cf9-9fbe-ff214ba76aaf ┆ domain       ┆ t9428370.example.com     ┆ 0.95 ┆ 2026-09-11T22:21:06Z ┆ 2026-09-11T22:21:06Z │
└──────────────────────────────────────┴──────────────┴──────────────────────────┴──────┴──────────────────────┴──────────────────────┘
共 1 筆
```

!!! note "`--entity-type` 在取回的前 N 筆裡過濾"
    這不是資料庫層篩選。要看更多請調高 `--limit`。

!!! note "Entity 的排序沒有時間序"
    Entity 的 id 是 UUID v5（由 `entity_type` + `normalized_name` 推導），沒有時間戳，所以排序不代表建立時間先後。要看最新的請用 `--json` 後自己依 `last_seen` 排。

### `entities show <id>`

印出 Entity 本身、它出現在哪些 Document、它參與的關聯與每條關聯的證據數：

```bash
make run-cli ARGS="entities show a673ae4e-29e8-5da8-b737-6d66195f2ea8"
```

---

### `search "<查詢>"`

在搜尋索引（OpenSearch）裡找文件：

```bash
make run-cli ARGS="search 'CVE-2026-31337'"
```

```text
┌──────────────────────────────────────┬───────┬─────────────────────────────────────────────────┬──────────────────────────────────────────────────┬──────────────────────┐
│ document_id                          ┆ score ┆ title                                           ┆ snippet                                          ┆ published_at         │
╞══════════════════════════════════════╪═══════╪═════════════════════════════════════════════════╪══════════════════════════════════════════════════╪══════════════════════╡
│ 01a0f0d3-405a-703d-ba36-25f2f6959184 ┆ 7.56  ┆ 威脅情報：針對 Acme Router 的攻擊活動持續擴大 ┆ 研究團隊觀察到攻擊者持續利用 CVE-2026-31337，… ┆ 2026-09-29T00:00:00Z │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 01a0f0d3-405a-703d-ba36-25f1ba42bd02 ┆ 6.47  ┆ Acme Router 韌體多個遠端程式碼執行漏洞        ┆ Acme 公司發布安全公告，修補 Acme Router 韌體…  ┆ 2026-09-28T00:00:00Z │
└──────────────────────────────────────┴───────┴─────────────────────────────────────────────────┴──────────────────────────────────────────────────┴──────────────────────┘
共 2 筆
符合條件共 2 筆（本頁顯示 2 筆）。加 --json 可取得 raw_evidence_id 與 entities。
```

常用選項：

| 選項 | 說明 |
|---|---|
| `--source` / `--connector` | 限定來源／連接器（UUID） |
| `--entity TYPE:NAME` | 找提到該實體的文件，型別可省略 |
| `--from` / `--to` | `YYYY-MM-DD` 或 RFC3339 |
| `--lang` / `--type` | 語言、物件型別 |
| `-n` | 筆數，上限 100 |
| `--include-duplicates` | 偵錯用，正常情況不需要 |

表格輸出只有 document_id、分數、標題、片段、發布時間；要 `raw_evidence_id` 與 entities 請加 `--json`。

!!! warning "`search` 查的是 OpenSearch 投影，不是 PostgreSQL"
    `osint-indexer` 沒跑過的話投影是空的，會看到「沒有符合的文件」但資料其實在資料庫裡。用 `make rebuild-index` 補齊索引。

---

### `jobs list`

```bash
make run-cli ARGS="jobs list --limit 3"
```

```text
┌──────────────────────────────────────┬─────────────┬───────────┬──────────────────────┬──────────────────────┬──────┬──────┐
│ ID                                   ┆ 類型        ┆ 狀態      ┆ 建立時間             ┆ 完成時間             ┆ 重試 ┆ 錯誤 │
╞══════════════════════════════════════╪═════════════╪═══════════╪══════════════════════╪══════════════════════╪══════╪══════╡
│ 01a0f107-3611-76ff-a351-569a93966c04 ┆ stix_import ┆ completed ┆ 2026-09-30T06:36:16Z ┆ 2026-09-30T06:36:16Z ┆ 0    ┆ -    │
└──────────────────────────────────────┴─────────────┴───────────┴──────────────────────┴──────────────────────┴──────┴──────┘
共 1 筆
```

---

## 用 `--json` 接腳本

```bash
# 最近 5 筆 Document 的標題
make run-cli ARGS="documents list -n 5 --json" | jq -r '.[].title'

# 某個 source 底下所有 RawEvidence 的總位元組數
make run-cli ARGS="raw list --source <source_id> -n 1000 --json" \
  | jq '[.[].content_length] | add'

# CI 裡等後端服務起來
until make run-cli ARGS="health --json" 2>/dev/null | jq -e 'all(.healthy)' >/dev/null; do
  sleep 2
done
```

---

## 排錯

| 症狀 | 原因與處理 |
|---|---|
| `連不上 PostgreSQL` | `.env` 的 `DATABASE_URL` 不對，或沒跑 `make compose-up` |
| `relation "documents" does not exist` | Migration 沒跑。CLI 會附上 `make migrate-postgres` 的提示，手動執行即可 |
| `讀取設定失敗` | 不在 repo 根目錄執行，或 `OSINT_CONFIG_FILE` 指到不存在的檔 |
| `找不到 Document ...` | id 打錯或被截斷；訊息會告訴你用哪個 list 子指令查 |
| `search` 查不到東西，但 `documents list` 有資料 | 搜尋查的是 OpenSearch 投影，跑 `make rebuild-index` 補齊 |
| `health` 顯示 OpenSearch 連到奇怪的東西 | 本機若同時跑 OpenCTI，9200 是它的 Elasticsearch；確認 `.env` 的 `OPENSEARCH_URL` 用的是 19200 |
