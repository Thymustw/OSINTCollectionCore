# osint-cli — 本機查詢工具

`osint-cli` 讓你從終端機看到「系統裡到底有什麼資料」：有哪些 Source 與 Connector、
最近收了什麼 RawEvidence、產生了哪些 Document、某筆 Document 的 provenance 鏈是什麼、
五個後端服務活著沒有。

在 Operations Console（Phase 6）出現之前，這是最低成本的可視化手段。

---

## 先讀這段：它是什麼、不是什麼

| | |
|---|---|
| **是** | 本機管理工具。直接連 Core 的 PostgreSQL 與 MinIO。 |
| **不是** | 給遠端使用者的介面。它**不經過 core-api**，因此**沒有 RBAC、沒有 AuditLog**。 |
| **能做** | 查詢。列出、顯示單筆、看 provenance、health check。 |
| **不能做** | 任何寫入或刪除。刻意沒有實作。 |

**為什麼唯讀**：寫入操作必須走 `core-api`（`POST/PATCH/DELETE /api/v1/...`），
那條路徑才有授權檢查與稽核紀錄。如果 CLI 也能寫，就等於開了一條沒有稽核的後門。

**只在你自己已經能存取 Core 資料庫的機器上使用它。** 它能讀到什麼，
完全取決於 `DATABASE_URL` 指到哪裡——它不會、也無法再做一次權限檢查。

---

## 準備

```bash
make compose-up          # 啟動 PostgreSQL / MinIO / Redis / OpenSearch / Redpanda
make migrate-postgres    # 第一次執行，或 schema 有更新時
cp .env.example .env     # 已經有 .env 就跳過
```

設定來源與 `osint-api`／`osint-collector`／`osint-normalizer` **完全相同**：

```text
config/default.toml → OSINT_CONFIG_FILE 指的檔案 → OSINT__* 環境變數
```

密鑰走 SecretRef（`DATABASE_URL`、`MINIO_ROOT_USER` 等寫在 `.env`）。
CLI 不會自己另外讀環境變數，所以它看到的設定跟服務看到的一致。

執行：

```bash
make run-cli ARGS="documents list"
# 或直接跑編好的 binary
cargo run -p osint-cli --bin osint-cli -- documents list
./target/debug/osint-cli documents list
```

以下範例都用 `osint-cli` 代表這個 binary。

---

## 共通選項

| 選項 | 說明 |
|---|---|
| `--json` | 改輸出 JSON（合法 JSON，可直接接 `jq`）。預設是表格。 |
| `--limit N` / `-n N` | list 類最多列出幾筆，預設 20，範圍 1..=10000。 |
| `--help` | 每一層子命令都有。 |

`--limit` 超過 100 時 CLI 會自動用 cursor 連續翻頁取足。

**排序**：一律依 `id` 由大到小。`Source`／`RawEvidence`／`Document`／`Job` 的 id 是
UUID v7（前 48 bit 是毫秒時間戳），所以等同「最新的在前」。
**`Connector` 例外**：`POST /api/v1/import` 建立的 connector 用 UUID v5
（由 source + format 推導以保持冪等），沒有時間戳，因此 `connectors list`
的順序只是 id 順序，不是建立時間順序。

**Exit code**：

| 值 | 意思 |
|---|---|
| 0 | 正常 |
| 1 | 指令失敗（連不上、找不到、設定錯） |
| 2 | 只有 `health` 會用：指令本身跑成功，但有服務不健康 |

---

## 子命令

### `sources list`

```bash
osint-cli sources list --limit 5
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

```bash
osint-cli sources show 01a091e3-c2ce-7613-bb68-7fb9d4d07657
```

縱向列出全部欄位（含 `collection_policy` 的 JSON）。

### `connectors list`

**預設只顯示 `enabled = true` 的**。要看含停用的加 `--all`：

```bash
osint-cli connectors list              # 只有啟用中的
osint-cli connectors list --all -n 10  # 含停用的
```

```text
┌──────────────────────────────────────┬────────────────┬──────┬──────┬──────┬────────┬──────────┐
│ ID                                   ┆ 名稱           ┆ 類型 ┆ 啟用 ┆ 狀態 ┆ 錯誤數 ┆ 最後成功 │
╞══════════════════════════════════════╪════════════════╪══════╪══════╪══════╪════════╪══════════╡
│ 01a091e3-b905-71c1-81fe-1fa9d1b84d17 ┆ e2e-rss-...    ┆ rss  ┆ 是   ┆ idle ┆ 0      ┆ -        │
└──────────────────────────────────────┴────────────────┴──────┴──────┴──────┴────────┴──────────┘
```

`--limit` 算的是**過濾後**的筆數：`connectors list -n 20` 會一直翻頁直到湊滿 20 個
啟用中的 connector，不會因為前 20 筆剛好都停用就只印 0 筆。

### `connectors show <id>`

```bash
osint-cli connectors show 01a091e3-b905-71c1-81fe-1fa9d1b84d17
```

`credential_reference` / `proxy_reference` 顯示的是 SecretRef 字串（例如 `env:NVD_TOKEN`），
不是明文密鑰——Core 本來就只存 SecretRef。

### `raw list`

```bash
osint-cli raw list --limit 3
osint-cli raw list --source 01a091e3-b8d5-708a-90c5-7ad3a83b274b
```

```text
┌──────────────────────────────────────┬──────────────────────┬────────────────────────────────┬──────────────┬────────┬──────┬───────────────┐
│ ID                                   ┆ 取得時間             ┆ 來源 URL                       ┆ Content-Type ┆ 位元組 ┆ HTTP ┆ SHA256(前 12) │
╞══════════════════════════════════════╪══════════════════════╪════════════════════════════════╪══════════════╪════════╪══════╪═══════════════╡
│ 01a091e3-bf1a-7594-91d9-a92d941ea661 ┆ 2026-09-11T19:13:37Z ┆ http://127.0.0.1:46347/rss.xml ┆ text/plain   ┆ 375    ┆ 200  ┆ d4f48e6a3ddd  │
└──────────────────────────────────────┴──────────────────────┴────────────────────────────────┴──────────────┴────────┴──────┴───────────────┘
```

### `raw show <id>`

只看 metadata：

```bash
osint-cli raw show 01a091e3-bf1a-7594-91d9-a92d941ea661
```

加 `--body` 會從 MinIO 取回實際內容：

```bash
osint-cli raw show <id> --body --max-body-bytes 300
```

```text
--- body（375 bytes，SHA256 d4f48e6a3ddde56f3f89768f824a8d41a9b60bb9e043e81e0e7930071cb1b540）---
<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Local Fixture</title>
...
--- 已截斷：只顯示前 300 bytes，完整內容共 375 bytes。用 --max-body-bytes 調整。---
```

**二進位內容不會噴到終端機。** 內容不是合法 UTF-8（PDF、圖片等）時只印大小與 SHA256：

```text
--- body（27 bytes，SHA256 e872b437fb7811fa76966169be8a95765d0a6347c908516959e97d3b86da3105）---
內容不是合法 UTF-8（可能是 PDF／圖片等二進位），不印出來以免弄亂終端機。
要取原始內容，請直接從物件儲存讀 key `raw/<source_id>/2026/09/12/<id>`（bucket `raw-evidence`）。
```

`--max-body-bytes` 預設 65536。截斷一律切在 UTF-8 字元邊界，不會印出半個中文字。

`--body --json` 會在輸出多一個 `_body` 物件：

```json
{ "bytes": 27, "sha256": "e872b4...", "text": null, "truncated": false, "utf8": false }
```

### `documents list`

```bash
osint-cli documents list --limit 5
```

```text
┌──────────────────────────────────────┬──────────┬───────────────┬──────┬──────────────────────┬──────┬──────┐
│ ID                                   ┆ 類型     ┆ 標題          ┆ 語言 ┆ 觀察時間             ┆ 信心 ┆ 標籤 │
╞══════════════════════════════════════╪══════════╪═══════════════╪══════╪══════════════════════╪══════╪══════╡
│ 01a091e3-bf9f-778c-a3be-dde5171e9dc0 ┆ article  ┆ CVE-2026-0001 ┆ -    ┆ 2026-09-11T19:13:37Z ┆ 0.80 ┆      │
└──────────────────────────────────────┴──────────┴───────────────┴──────┴──────────────────────┴──────┴──────┘
```

沒有 Document 時會提示：RawEvidence 要先經 `osint-normalizer` 正規化才會產生 Document。

### `documents show <id>`

**這是 provenance 鏈的入口**：

```bash
osint-cli documents show 01a091e3-bf9f-778c-a3be-dde5171e9dc0
```

除了 Document 本身的欄位，還會印兩張表：

```text
provenance 鏈（由舊到新）：
┌──────────────────────┬──────────────┬──────────────────┬──────────────────────────────────────┬──────────────────────────────────────┐
│ 時間                 ┆ 動作         ┆ processor        ┆ raw_evidence_id                      ┆ parent_id                            │
╞══════════════════════╪══════════════╪══════════════════╪══════════════════════════════════════╪══════════════════════════════════════╡
│ 2026-09-11T19:13:37Z ┆ derived_from ┆ normalizer 0.1.0 ┆ 01a091e3-bf1a-7594-91d9-a92d941ea661 ┆ 01a091e3-bf1a-7594-91d9-a92d941ea661 │
├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┤
│ 2026-09-11T19:13:37Z ┆ normalized   ┆ normalizer 0.1.0 ┆ 01a091e3-bf1a-7594-91d9-a92d941ea661 ┆ 01a091e3-bf1a-7594-91d9-a92d941ea661 │
└──────────────────────┴──────────────┴──────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
共 2 筆

來源 RawEvidence：
┌──────────────────────────────────────┬──────────────────────┬────────────────────────────────┬───────────────┬─────────────────────────────┐
│ ID                                   ┆ 取得時間             ┆ 來源 URL                       ┆ SHA256(前 12) ┆ 物件 key                    │
╞══════════════════════════════════════╪══════════════════════╪════════════════════════════════╪═══════════════╪═════════════════════════════╡
│ 01a091e3-bf1a-7594-91d9-a92d941ea661 ┆ 2026-09-11T19:13:37Z ┆ http://127.0.0.1:46347/rss.xml ┆ d4f48e6a3ddd  ┆ raw/01a091e3-.../01a091e3-… │
└──────────────────────────────────────┴──────────────────────┴────────────────────────────────┴───────────────┴─────────────────────────────┘
```

接著就能用上面的 `raw_evidence_id` 去 `osint-cli raw show <id> --body` 看原始內容——
這就是完整的「Document → Provenance → RawEvidence → 原始位元組」追溯路徑。

`documents show --json` 會輸出一份包含三段的物件：

```bash
osint-cli documents show <id> --json | jq '.provenance[].processor'
```

```json
{ "document": { ... }, "provenance": [ ... ], "raw_evidence": [ ... ] }
```

如果 provenance 表是空的，代表這筆 Document 是繞過 normalizer 塞進來的——那是異常。

### `jobs list`

```bash
osint-cli jobs list --limit 10
```

欄位：ID、類型、狀態、建立時間、完成時間、重試次數、錯誤訊息（截斷）。

### `health`

對五個後端**各做一次真實的 health check**，平行執行，單項逾時 10 秒：

```bash
osint-cli health
```

```text
┌────────────┬──────┬────────────────────────┬──────────────────────────┐
│ 服務       ┆ 狀態 ┆ 位址                   ┆ 訊息                     │
╞════════════╪══════╪════════════════════════╪══════════════════════════╡
│ PostgreSQL ┆ OK   ┆ env:DATABASE_URL       ┆ SELECT 1 成功            │
│ MinIO/S3   ┆ OK   ┆ http://127.0.0.1:19000 ┆ bucket 存在              │
│ Redis      ┆ OK   ┆ env:REDIS_URL          ┆ PING PONG                │
│ OpenSearch ┆ OK   ┆ http://127.0.0.1:19200 ┆ cluster status=green     │
│ Redpanda   ┆ OK   ┆ 127.0.0.1:9092         ┆ metadata OK，1 個 broker │
└────────────┴──────┴────────────────────────┴──────────────────────────┘
共 5 筆
```

| 服務 | 實際做的事 |
|---|---|
| PostgreSQL | `SELECT 1` |
| MinIO/S3 | 檢查 bucket 存在（不會建立 bucket） |
| Redis | `PING` |
| OpenSearch | `_cluster/health`（green/yellow 算健康） |
| Redpanda | 抓 cluster metadata，回報 broker 數 |

**一項失敗不會中斷其他項**——PostgreSQL 掛掉時你最需要知道的正是其他四個還活著沒有。
有任一項 DOWN 時 exit code 是 2。

PostgreSQL 與 Redis 的「位址」欄顯示的是 SecretRef 名稱而不是連線字串，
因為 DSN 含密碼，不該印到終端機或 log 裡。

---

## 用 `--json` 接腳本

```bash
# 最近 5 筆 Document 的標題
osint-cli documents list -n 5 --json | jq -r '.[].title'

# 某個 source 底下所有 RawEvidence 的總位元組數
osint-cli raw list --source <source_id> -n 1000 --json \
  | jq '[.[].content_length] | add'

# CI 裡等服務起來
until osint-cli health --json | jq -e 'all(.healthy)' >/dev/null; do sleep 2; done
```

---

## 排錯

| 症狀 | 原因與處理 |
|---|---|
| `連不上 PostgreSQL` | `.env` 的 `DATABASE_URL` 不對，或沒跑 `make compose-up`。錯誤訊息裡有三步檢查清單。 |
| `relation "documents" does not exist` | migration 沒跑。CLI 會在錯誤訊息後面附上 `make migrate-postgres` 的提示。**CLI 刻意不自己跑 migration**——唯讀工具不該偷改 schema。 |
| `讀取設定失敗` | 不在 repo 根目錄執行（它會往上找 `config/default.toml`），或 `OSINT_CONFIG_FILE` 指到不存在的檔。 |
| `找不到 Document ...` | id 打錯或被截斷。訊息會告訴你用哪個 list 子命令查。 |
| `health` 顯示 OpenSearch/MinIO 連到奇怪的東西 | 本機若同時跑 OpenCTI，9200/9000 是它的。`.env` 要用 19200/19000，並設 `OSINT_STRICT_PORT_ISOLATION=1`。 |

---

## 相關文件

- `docs/developer/storage-adapters.md` — CLI 用到的 `RelationalStore` cursor 分頁契約
- `docs/developer/import-api.md` — 要**寫入**資料時走的路徑
- `docs/developer/collector-normalizer.md` — RawEvidence 與 Document 是怎麼產生的
- `docs/operations/OPERATIONS.md` — 服務層級的運維程序
