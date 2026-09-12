# Security baseline（V0.1 Phase 2，Phase 6a 更新）

## 測試用 JWT 密鑰不在生產產物裡（Phase 6a 修）

Phase 6a 之前 `crates/core-api/src/lib.rs` 有 `test_jwt()` / `test_app()` /
`issue_test_jwt()`，用一個「32 個相同位元組」的字面量當 JWT 密鑰，而且**沒有
`#[cfg(test)]` gate**——那把密鑰會被編進 `libcore_api` 與 `osint-api` 的 release binary。

它們已經搬到 `crates/core-api/tests/common/mod.rs`。**不要搬回 `src/`。**

沒有改用 `#[cfg(feature = "test-util")]` 的理由：那需要 crate 自己 dev-depend 自己
才能在整合測試裡打開 feature，會出現「同一個 crate 兩份」的型別不相容問題。
放在 `tests/common/` 讓「生產 build 不含測試密鑰」變成**結構上必然**，不是靠設定正確。

兩道防線：

- `crates/core-api/tests/no_test_secret_in_src.rs` 掃 `src/` 找位元組陣列字面量。
  這支測試自己也有一個正向對照（`detector_actually_detects`）——
  否則「掃不到東西」與「掃描壞掉」長得一模一樣。
- 交付前對 release binary 做字串檢查。2026-09-12 實測：
  `target/release/osint-api` 中該位元組序列出現 **0** 次，
  對照組 `target/debug/deps/tokens_api-*`（測試 binary）出現 **1** 次——
  也就是說這個檢查方法本身有效，不是「到處都找不到」。

## JWT

HS256（`jsonwebtoken` 11 + `rust_crypto`，不用 aws-lc／OpenSSL）。secret 來自 `auth.jwt_secret_ref`（預設 `env:JWT_SECRET`），至少 32 bytes。

Claims：`sub`、`role`、`iss`、`iat`、`exp`、`jti`。

## API token（Phase 6a 起落地到 Postgres）

格式：`osint_<uuid>.<hex-secret>`。儲存只留 argon2id PHC 字串。明文只在發行當下回傳。

`ApiTokenStore` 是 trait，兩個實作：

| 實作 | 位置 | 用途 |
|---|---|---|
| `MemoryApiTokenStore` | `core-security` | 測試、以及 Postgres 沒接上時的降級 |
| `PostgresApiTokenStore` | `storage-postgres::security` | 生產。表是 `api_tokens`（migration 0006） |

`osint-api` 連上 Postgres 就用 PG 版；**連不上時會退回記憶體版並發一條 warn**——
那代表這次發出去的 token 重啟後全部失效。看到「重新發的 token 隔天不能用」先查那條 log。

### 管理端點（admin-only）

| 方法 | 路徑 | 說明 |
|---|---|---|
| POST | `/api/v1/tokens` | 發行。**回應是唯一能拿到明文的地方** |
| GET | `/api/v1/tokens` | 列出（含已撤銷）。不含明文，也不含 hash |
| DELETE | `/api/v1/tokens/{id}` | 撤銷。204；id 不存在回 404 |

`POST` body：

```json
{"name": "ci-indexer", "role": "operator", "expires_in_days": 30}
```

- `name`：1..=64 字元、不可含控制字元。會成為 `Principal.subject`（`token:<name>`）並寫進稽核。
- `expires_in_days`：1..=365。**省略或 `null` 代表不自動到期**。上限存在的理由是不讓
  「永不過期的 operator token」變成預設用法；真要長期憑證得明確省略這個欄位。
- 撤銷與到期是兩件不同的事，錯誤訊息也不同（`已撤銷` vs `已過期`）——使用者的下一步不一樣。
- `verify` 成功會更新 `last_used_at`，這是判斷「哪些 token 還在用、可以撤了」的唯一依據。
  更新失敗只寫 warn，不擋請求。

### 認證路徑上的兩個刻意決定

1. **不存在的 token id 回 401，不是 404。**
   `SecurityError::TokenNotFound` 本身會被映成 404（給 admin 查詢用），但**認證中介層
   刻意不用它**：401/404 的差別會變成 token id 的枚舉 oracle。
2. **argon2 驗證跑在 `spawn_blocking`。**
   argon2 是刻意設計成 CPU-heavy 的（本機約數十毫秒）。放在 async context 裡會佔住
   Tokio executor 執行緒，違反 `CLAUDE.md` §5——高併發下**所有**路由的延遲會一起變差，
   而不只是認證。發行（`issue_api_token`）同理。

## RBAC（SPEC §23 沒寫矩陣，這是實作時定的）

三個角色，嚴格超集，沒有可關閉的繼承：

| 權限 | viewer | operator | admin |
|---|---|---|---|
| Read（含 jobs list／health 需認證的讀） | yes | yes | yes |
| Write（建立／轉換／dispatch job、import） | no | yes | yes |
| Admin（`/api/v1/tokens` 三個端點） | no | no | yes |

Phase 6a 之前 admin 那一欄沒有任何守護對象——`Permission::Admin` 存在但沒有路由用它。
`/api/v1/tokens` 是第一組。

## AuditLog（Phase 6a 起落地到 Postgres）

`AuditLog` trait 有三個方法：`append`、`list(after, limit)`、`list_by_resource(type, id)`。

**查詢方法不是裝飾。** 只能寫不能讀的稽核等於沒有稽核——沒有人能在事後回答
「誰撤銷了那把 token」。

| 實作 | 位置 | 用途 |
|---|---|---|
| `MemoryAuditLog` | `core-security` | 測試、connector-sdk 的 SSRF 稽核、降級 |
| `PostgresAuditLog` | `storage-postgres::security` | 生產。表是 `audit_log`（migration 0006） |

### 為什麼 PG 實作放在 `storage-postgres`

`core-security` **不相依任何 storage crate**，這個方向不能破壞：它被 `connector-sdk`、
`collector`、`osint-cli` 等一整排不碰資料庫的 crate 相依，加 sqlx 進去等於讓每支 binary
都背上連線池與 TLS 堆疊。另開一個 `security-postgres` crate 會為了兩個 impl 多一份
Cargo.toml 與 deny/audit 面，而且要各自維護一份連線池與 `map_sqlx`。

放進 `storage-postgres` 正好是 `CLAUDE.md` §13 的
「capability interface → concrete adapter」形狀，相依方向是
`storage-postgres → core-security`，**沒有環**。完整取捨見
`crates/storage-postgres/src/security.rs` 的模組註解。

### 稽核欄位

`AuditEntry` 的 `resource` 在 Phase 6a 拆成 `resource_type` + `resource_id`——
單一字串欄位只能 `LIKE 'job/%'`，走不到索引。要顯示成 `job/<id>` 用 `AuditEntry::resource()`。

⚠️ 資料表欄位叫 `details`，Rust 欄位叫 `metadata`。對照關係只存在於
`crates/storage-postgres/src/security.rs`，改任何一邊都要同步改。

### 會寫稽核的動作

| action | resource_type | 何時寫 |
|---|---|---|
| `import.upload` | `source` | 成功與失敗都寫 |
| `job.create` / `job.transition` / `job.dispatch` / `job.retry` | `job` | 成功與失敗都寫 |
| `source.create` / `source.update` | `source` | 成功與失敗都寫 |
| `connector.create` / `connector.update` | `connector` | 成功與失敗都寫 |
| `collection.create` | `collection` | 成功與失敗都寫 |
| `object.create` | `object` | 每次（V0.1 一律是 `rejected` + 501，見 ADR-006） |
| `token.issue` / `token.list` / `token.revoke` | `api_token` | 每次 |
| `auth.failed` | `auth` | 每一次 401 |
| `authz.denied` | `auth` | 每一次 403 |
| `connector.ssrf.allowlist` | `source` | 每次因白名單放行的請求 |

**失敗也寫**：只記成功的話，「誰一直試圖把已完成的 job 轉回 running」「有人拿撤銷的
token 敲了三千次」這類訊號完全看不到。

**成功的唯讀請求不寫**（`crates/core-api/tests/audit_api.rs` 有斷言擋住）：
每次輪詢 `GET /api/v1/jobs` 都長一列的話，稽核表會被正常流量灌爆，
真正要看的寫入動作反而被埋掉。

### 稽核不會記什麼

出示的憑證（JWT、API token、Basic 字串）**一律不寫進稽核**，連前綴都不寫——
稽核表不該存任何可以拿去重放的東西。`audit_api.rs` 有測試擋住這一點。

### `ip` 欄位只認 TCP peer

`osint-api` 用 `into_make_service_with_connect_info::<SocketAddr>()` 啟動，稽核的 `ip`
來自 TCP peer。**刻意不讀 `X-Forwarded-For`**：在沒有可信任的 reverse proxy 覆寫它之前，
那是呼叫端完全可控的字串，採信它等於讓任何人都能往稽核表寫任意 IP——
那比沒有 IP 更糟，因為它看起來像證據。要支援 proxy 佈署時必須連同「哪些 proxy 可信」
一起設計。整合測試用 `oneshot` 直接呼叫 router，沒有 `ConnectInfo`，所以 `ip` 預設是 `NULL`
（要驗 IP 有寫進去的測試會自己把 `ConnectInfo` 塞進 request 的 extensions）。

⚠️ **handler 拿 IP 要用 `ClientIp` extractor**（`crates/core-api/src/extractors.rs`）。
Phase 6a 的 `token.issue` / `token.revoke` 就是漏了這一步——middleware 寫的
`auth.failed` / `authz.denied` 有 IP，但 handler 自己寫的那幾列整欄是 NULL，
而且完全不會報錯：要到查「那把 token 是誰從哪裡發的」時才會發現沒有資料。
Phase 6b 補上，`tests/resources_api.rs::token_audit_records_the_client_ip` 釘住它。

`connector.create` 的稽核**不記 `credential_reference`**：就算它是合法的 SecretRef，
也沒有必要在稽核裡多留一份「密鑰放在哪」的指引。

### 已知限制（V0.1）

- **沒有保留策略。** `auth.failed` 的寫入速率等同於外部可控的請求速率（全域 rate limit
  之內），長期跑會讓 `audit_log` 一直長。分區／清除排程要在 V0.2 處理。
- **不可變性靠約定。** adapter 只有 INSERT 與 SELECT，沒有 UPDATE/DELETE 路徑，但
  資料庫層沒有強制。要真正防竄改，部署時應讓 `osint-api` 的 DB 帳號對 `audit_log`
  只有 `INSERT` + `SELECT` 權限。
- **稽核寫入失敗不擋請求**（只寫 `tracing::error!`）。DB 抖一下不該讓 API 全面 503，
  代價是那段時間的動作沒有留痕——log 是唯一線索。

## `/metrics` 維持公開（Phase 6a 的決定）

`GET /metrics` 不需要認證。理由：

1. Prometheus 的預設 scrape 設定不帶憑證。要求 JWT 等於讓每個佈署都得先處理
   「誰來簽一把永不過期的 scraper token」——那會生出一把比 `/metrics` 本身更值得保護的
   長期憑證。
2. 輸出的是**聚合計數器**：收了幾筆、重複率、佇列深度、各類錯誤數、延遲總和。
   沒有 source 名稱、沒有 URL、沒有實體內容，也沒有任何識別碼
   （全部欄位見 `crates/core-observability/src/metrics.rs` 的 `render_prometheus`）。
3. 預設 `[http].bind` 是 `127.0.0.1:18080`，不對外。

**但量體本身仍是情報**（「今天收集量突然掉到零」對觀察者有意義），所以部署時
**必須**靠網路層限制：綁 loopback 或內網介面，或放在只允許 Prometheus 來源 IP 的
reverse proxy 後面。不要靠「反正沒人知道路徑」。

之後若要加認證，Prometheus 支援 `authorization.credentials_file`，屆時應做成設定開關
而不是硬性要求——不要讓本機開發也得先簽 token。

## SSRF IP 分類

`core_security::classify_ip` / `classify_host`：

- **Hard deny**：`169.254.169.254`、`169.254.169.253`、`168.63.129.16`（Azure IMDS）、`100.100.100.200`（Aliyun IMDS）、`fd00:ec2::254`、`metadata.google.internal` 等。不可被白名單覆寫。
- **Soft deny**：RFC1918、loopback、link-local、CGNAT `100.64.0.0/10`、IPv6 ULA／link-local。
- **Public**：其餘。

## NetworkRule／SSRF Guard（Phase 3a）

`connector-sdk` 在 `classify_ip` 之上實作 ADR-001：

- Hard deny（cloud metadata）永遠不能被 `NetworkRule` 覆寫；寫入時 `validate_network_rule` 也會拒絕。
- Soft deny（RFC1918／loopback／link-local）預設拒絕；`Source` 上未過期的 `NetworkRule` 可放行。
- 每次因白名單放行的請求寫 `AuditLog`（action `connector.ssrf.allowlist`）。
- 建立／編輯規則需要 operator 或 admin。Viewer 會得到 `NetworkRuleForbidden`。
- 每次 hop（含 redirect）重跑同一套檢查。連線釘在 resolve 一次後的 IP（`reqwest` `ClientBuilder::resolve`），關閉 DNS rebinding TOCTOU。

資料表：`source_network_rules`（Postgres + SQLite migration `0002`）。`RelationalStore` 新增 `put/get/list/delete_network_rule`。
