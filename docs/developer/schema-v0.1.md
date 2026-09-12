# V0.1 schema notes

欄位來源：內部規格 V0.1（規格文件本身未隨原始碼公開）。

## 規格沒寫、資料表必須補的欄位

- `relationship_evidence.id`
- `entity_extractions.id`
- `duplicate_groups.id`
- `duplicate_groups.member_object_id` / `member_raw_evidence_id`（規格只寫 member）

Collection 的 sources / connectors / objects 做成關聯表，不內嵌在 `collections` 列。

`relationships.source_object_id` / `target_object_id` 可指向 document 或 entity，因此不設單一 FK。

## PostgreSQL vs SQLite

- PG：`UUID`、`TIMESTAMPTZ`、`JSONB`
- SQLite：`TEXT` UUID / RFC3339 時間 / JSON 字串；`BOOLEAN` 用 `INTEGER`

不要假設兩套 schema 可以共用同一份 SQL。

## `source_network_rules`（Phase 3a）

ADR-001 的 per-`Source` 白名單。不塞進 `sources.collection_policy`，獨立表方便 expiry 查詢與 CASCADE 刪除。

| 欄位 | 說明 |
|---|---|
| `id` | UUID v7 |
| `source_id` | FK → `sources` |
| `cidr_or_host` | 精確 CIDR 或 hostname，禁止 `*`／`?` |
| `ports` | PG JSONB／SQLite TEXT JSON 陣列；NULL = 該目標所有埠 |
| `reason` | 必填 |
| `approved_by` | 核准者身分 |
| `expires_at` | 可空；到期視為規則不存在 |
| `created_at` / `updated_at` | |

Hard-deny 驗證在應用層（`connector-sdk::validate_network_rule`），不用 CHECK constraint 重做 IP 分類。

## `idx_provenance_normalized_raw`（Phase 3 後半）

同一筆 RawEvidence 只能有一列 `action='normalized'` 的 provenance。多份 Document 的個別溯源用 `action='derived_from'`，不在此 unique 範圍。

```sql
CREATE UNIQUE INDEX idx_provenance_normalized_raw
    ON provenance (raw_evidence_id)
    WHERE action = 'normalized' AND raw_evidence_id IS NOT NULL;
```

PG／SQLite 都在 `migrations/*/0003_provenance_normalized_unique.sql`。衝突視為已做過。

**V0.2 Phase 0e 起，normalizer 把 Document 與這一列寫在同一個交易裡**
（`storage_core::TransactionalStore`），不再是 write-then-claim：
中途 crash 就整批回滾，不會留下「Document 在但 claim 不在」的重複來源。
衝突時（別人先佔到）整個交易回滾並回報 `AlreadyDone`。
交易化之前的取捨與兩種順序的代價比較見 `docs/developer/collector-normalizer.md`
「落地原子性」。

## `documents` 的三個 dedup 欄位（Phase 4a）

`migrations/*/0004_dedup_stage_columns.sql`。SPEC §15 只描述「怎麼判斷重複」，
沒有說判斷依據存在哪；這三欄就是那些依據。全部可為 NULL，既有資料不需回填。

| 欄位 | PG | SQLite | 用途 |
|---|---|---|---|
| `external_key` | `TEXT` | `TEXT` | Stage 1 的鍵，`"{platform}\|{external_id}"` |
| `simhash` | `BIGINT` | `INTEGER` | Stage 4 的 64-bit 指紋 |
| `duplicate_of` | `UUID` FK → `documents.id` | `TEXT` FK | 指向 canonical；NULL = 自己是 canonical 或尚未去重 |

`simhash` 存的是 `u64 as i64` 的**位元重解讀**，不是數值轉換——兩個 backend 都沒有
無號 64-bit。比較時只做 XOR／popcount，不做大小比較。adapter 若把它當數值處理
（例如經過浮點）會靜默改值，conformance 的 `document_dedup_fields` 斷言就是在擋這件事。

標記重複用**獨立欄位而不是 `labels`**：`labels` 是自由字串陣列，沒有外鍵語意，
也無法回答「這份的 canonical 是誰」。

三個索引：`idx_documents_external_key`、`idx_documents_duplicate_of`（皆為部分索引），
以及 `idx_documents_simhash_recent ON documents (id DESC) WHERE simhash IS NOT NULL`。
最後一個建在 `id DESC` 而不是 `simhash` 值上——SimHash 比對的是 Hamming 距離，
對指紋本身建 B-tree 沒有任何幫助；Stage 4 掃描的是「最近 N 筆有指紋的 Document」。

## `idx_duplicate_groups_member_object` 與 `idx_provenance_dedup_subject`（Phase 4a）

```sql
CREATE UNIQUE INDEX idx_duplicate_groups_member_object
    ON duplicate_groups (member_object_id)
    WHERE member_object_id IS NOT NULL;

CREATE UNIQUE INDEX idx_provenance_dedup_subject
    ON provenance (subject_id)
    WHERE action = 'deduplicated';
```

前者落實 SPEC §16「一份 Document 最多屬於一個 duplicate group」。deduplicator 另外把
group id 算成 UUID v5（namespace + member document id），所以重複消費同一事件會 upsert
同一列；這個 index 是第二道保險。

後者的語意與 `idx_provenance_normalized_raw` 相同，只是主體從 RawEvidence 換成 Document。
**deduplicator 的落地順序仍是 write-then-claim**（只有 normalizer 在 V0.2 Phase 0e
改成交易，其餘 worker 留到 Phase 1）。

## Entity／Relationship 的自然鍵（Phase 4b）

`migrations/*/0005_entity_natural_key.sql`。

```sql
DROP INDEX idx_entities_normalized_name;            -- 原本是「非唯一」索引

CREATE UNIQUE INDEX idx_entities_natural_key
    ON entities (entity_type, normalized_name);

CREATE UNIQUE INDEX idx_relationships_natural_key
    ON relationships (source_object_id, relationship_type, target_object_id);

CREATE UNIQUE INDEX idx_provenance_entity_subject
    ON provenance (subject_id)
    WHERE action = 'entity_extracted';

CREATE INDEX idx_relationship_evidence_relationship
    ON relationship_evidence (relationship_id, created_at);
```

`idx_entities_normalized_name` 原本是**非唯一**索引：它讓查詢變快，卻不阻止兩個並發的
worker 各建一個 `CVE-2026-0001`。那種重複不會報錯，只會讓「這個 CVE 出現在哪些文章」
的查詢永遠少算。entity-worker 另外把 Entity id 算成
UUID v5(namespace, `entity_type|normalized_name`)，正常路徑寫的是同一列；
unique index 是第二道保險。Relationship 同理，自然鍵是 `(source, type, target)`。

`idx_provenance_entity_subject` 與 0004 的 `idx_provenance_dedup_subject` 是**兩個互不衝突
的部分索引**（後者沒有把 action 寫進鍵、只寫在 `WHERE` 裡），所以同一個 `subject_id`
可以同時有 `deduplicated` 與 `entity_extracted` 各一列。

> ⚠️ `'entity_extracted'` 這個字串同時寫在這個 migration、
> `crates/entity-worker/src/service.rs` 與 `crates/osint-cli/src/commands/documents.rs`。
> **只改一處不會報錯**，只會讓唯一性保證靜默失效。

### 這個 migration 會改資料，不只是加索引

既有資料庫可能已經有重複的自然鍵（因為原本的索引是非唯一的），直接 `CREATE UNIQUE INDEX`
會失敗、直接 `DELETE` 會撞 `entity_extractions` 的外鍵。所以 0005 的前半段做**合併**：
每組保留 `id` 最小的當存活者，把 `entity_extractions` 與 `relationships` 的參照改指過去，
再刪掉輸家；Relationship 也做同樣的事。

**會丟失**輸家那幾列的 `attributes` 與 `description`；抽取紀錄與關聯全部保留。
`relationship_evidence` 是 `ON DELETE CASCADE`，所以必須先改指再刪，否則證據會靜默消失。

實測（2026-09-12）：開發用 Postgres 的 35 列 `(vulnerability, cve-2026-0001)`
合併成 1 列，35 筆 extraction 全部保留、0 筆孤兒。細節見 `docs/developer/entity-worker.md`。

## `documents.normalized_content_hash` 的定義改過（Phase 4a）

Phase 3 的定義是對**原文**直接 `SHA256("{title}|{summary}|{body}")`。Phase 4a 改成先做
空白正規化（trim + 連續空白壓成單一半形空格），實作是 `core_model::content::normalize_content`
與 `core_model::content::content_hash`，**normalizer 與 deduplicator 共用同一份**。

⚠️ **這是既有資料的破壞性變更**：舊資料算出的 hash 與新定義不一致，Stage 3 對新舊混合
的資料會比不中。V0.1 尚無生產資料，不提供回填腳本；要回填就重跑一次 normalizer。
改動理由（舊定義為何太脆）見 `docs/developer/deduplicator.md`。ADR-007 的決策：Stage 3 內容雜湊在算 SHA256 前只正規化空白字元，**刻意不做大小寫正規化**——Stage 3 宣稱「內容完全相同」（confidence 1.0），大小寫被改過代表內容被編輯過，不該算完全相同；那類差異留給 Stage 4（SimHash）接住。

## `raw_evidence.metadata["import"]`（Phase 3d，push 路徑）

沒有 schema 變更：`metadata` 本來就是 JSONB／TEXT JSON。`POST /api/v1/import` 會在裡面寫入
一份 `ImportSpec`（`kind`、`mapping`、`limits`、`object_type`），normalizer 靠它決定怎麼把
JSON／CSV 拆成 Document。

沒有這個鍵的 JSON／CSV 一律不正規化。**這個鍵的存在與否就是「能不能拆」的判準**，
不要改成用 `content_type` 判斷。欄位定義見 `docs/developer/import-api.md`。

## Migration 0006：`audit_log` 與 `api_tokens`（Phase 6a）

兩張表都在 `migrations/postgres/0006_audit_log_api_tokens.sql` 與
`migrations/sqlite/0006_audit_log_api_tokens.sql`。它們是**認證／稽核平面**，
不是情報資料，所以不掛任何 `sources`／`collections` 外鍵。

在這之前 `AuditLog` 與 `ApiTokenStore` 都只有記憶體實作。那代表重啟 `osint-api` 就
失去全部稽核紀錄（`CLAUDE.md` §10 要求的 audit 形同沒有），而且發出去的 API token
在重啟後全部失效、也沒有任何地方查得到發過哪些。

```sql
CREATE TABLE audit_log (
    id              UUID PRIMARY KEY,   -- UUID v7，cursor 分頁靠它排序
    timestamp       TIMESTAMPTZ NOT NULL,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_id     TEXT,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    ip              TEXT,
    outcome         TEXT NOT NULL
);

CREATE INDEX idx_audit_log_resource ON audit_log (resource_type, resource_id, id DESC);
CREATE INDEX idx_audit_log_actor    ON audit_log (actor, id DESC);
CREATE INDEX idx_audit_log_action   ON audit_log (action, id DESC);

CREATE TABLE api_tokens (
    id              UUID PRIMARY KEY,
    name            TEXT NOT NULL,
    token_hash      TEXT NOT NULL,      -- argon2id PHC 字串
    role            TEXT NOT NULL,      -- viewer / operator / admin
    created_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    expires_at      TIMESTAMPTZ,        -- NULL = 不自動到期
    revoked_at      TIMESTAMPTZ,        -- NULL = 未撤銷；撤銷不刪列
    last_used_at    TIMESTAMPTZ
);

CREATE INDEX idx_api_tokens_live ON api_tokens (created_at DESC) WHERE revoked_at IS NULL;
```

### 幾個刻意的決定

- **`resource` 拆成 `resource_type` + `resource_id`。** 最常見的查詢是「這個 job／token
  身上發生過什麼」；單一字串欄位只能 `LIKE 'job/%'`，走不到索引。
- **⚠️ 欄位叫 `details`，Rust 欄位叫 `metadata`。** 名字不同是刻意的（`metadata` 在情報
  資料表裡已經有別的意思），但對照關係**只存在於**
  `crates/storage-postgres/src/security.rs`，改任何一邊都要同步改。
- **`api_tokens` 沒有 `UNIQUE(token_hash)`。** argon2 每次帶新 salt，同一把 secret
  兩次雜湊結果不同——建唯一鍵既擋不住重複也會誤導讀 schema 的人。
- **撤銷不刪列。** 誰在什麼時候撤的，本身就是要保留的事實。重複撤銷不覆寫
  `revoked_at`（第一次才是事實）。
- **`audit_log` 的不可變性靠約定。** adapter 只有 INSERT/SELECT；資料庫層沒有強制。
  部署時應讓 `osint-api` 的 DB 帳號對這張表只有 `INSERT` + `SELECT` 權限。

### SQLite 端只有 schema，沒有 adapter

`migrations/sqlite/0006` 建了同構的兩張表（型別依 0001 慣例：UUID→TEXT、
TIMESTAMPTZ→TEXT RFC3339、JSONB→TEXT），但 V0.1 **沒有 SQLite 的 `AuditLog` /
`ApiTokenStore` 實作**。建起來是為了維持「embedded schema 與 canonical schema 同構」
的既有慣例，之後做嵌入式部署時不必再改 migration 編號。

**對 SQLite 檔跑完 migration 後這兩張表會是空的，而且不會有任何程式去寫它們。
看到空表不代表稽核壞了。**

## `RelationalStore` 新增的四個 list 方法（Phase 6a）

沒有 schema 變更，全部走既有主鍵索引：

| 方法 | 排序 | 備註 |
|---|---|---|
| `list_collections(after, limit)` | `id DESC` | id 是 UUID v7，等同最新在前 |
| `list_events(after, limit)` | `id DESC` | 這裡的 Event 是 SPEC §13 的**領域事件物件**，不是 Redpanda 的 `EventEnvelope` |
| `list_relationships(after, limit)` | `id DESC` | ⚠️ Relationship id 是 UUID v5，**沒有時間序**，不能說「最新在前」 |
| `list_jobs_by_status(status, after, limit)` | `id DESC` | 過濾在 SQL 裡做 |

`list_jobs_by_status` 的過濾**必須在 SQL 裡**。先 `list_jobs` 再在程式端 filter 是錯的：
一頁只有 100 筆，佇列裡有一萬筆 queued 時，取回最新 100 筆再過濾出 running 的可能是
空頁——而「沒有 running 的 job」與「最新 100 筆裡沒有 running 的 job」是完全不同的
兩件事，前者會讓運維以為佇列空了。conformance 對此有雙向斷言（用自己的 status 查得到、
用別的 status 查不到），少了後者的話一個完全忽略 `status` 參數的實作也會通過測試。
