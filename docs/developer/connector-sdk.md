# Connector SDK（V0.1 Phase 3a）

對應內部規格 V0.1 §6／§8（規格文件本身未隨原始碼公開）。安全規範對應內部架構文件 CONNECTOR_SECURITY.md（未隨原始碼公開）。SSRF 防護的例外機制決策（ADR-001）：以**每個 Source** 的 `NetworkRule` 允許清單實作對私有網路的存取例外——預設仍是全 deny，allow-list 建立需要 operator 以上 RBAC 且有 `reason` 與 `approved_by`；cloud metadata 端點是**獨立的 hard-deny 層，不受 allow-list 覆蓋**。

## Crates

```text
crates/connector-sdk           ConnectorTrait、SSRF Guard、SourcePolicy、GuardedFetcher、EvidenceSink
crates/connector-rss           RSS 2.0／Atom 1.0（feed-rs）
crates/connector-static-web    Static Web（scraper 抽 title／description／正文）
crates/connector-rest-api      REST JSON API（整份 response 存 RawEvidence）
```

尚未實作：Manual／JSON／CSV import connector。排程服務見 `docs/developer/collector-normalizer.md`（`crates/collector`、`crates/normalizer`）。

`GuardedFetcher::get` 是 GET wrapper；REST 走 `GuardedFetcher::request(method, url, headers, body)`，每次 hop 仍重跑 SSRF Guard。不要自己開 `reqwest::Client`。

Checkpoint 除了 `etag`／`last_modified`／`last_retrieved_at`，還有 `content_sha256`：Static Web 與 REST 在伺服器沒給條件標頭時，用 body hash 判斷有沒有新內容。

## ConnectorTrait

規格只列資料欄位，沒有方法簽名。實作契約：

| 方法 | 語意 |
|---|---|
| `discover` | 從 Source 找出要抓的 URL。RSS 就是 `base_url` |
| `collect` | 抓取（SSRF／rate limit／ETag／Last-Modified／content SHA-256）。304 或內容沒變不產生新 RawEvidence |
| `parse` | body → 結構化項目。不寫 Document |
| `create_raw_evidence` | body → ObjectStore，metadata → RelationalStore |
| `update_checkpoint` | 回寫 `connectors.checkpoint`（可重試） |
| `health` | connector 自身是否可跑。**不探活遠端** |

## SSRF

IP 分類沿用 `core_security::classify_ip`／`classify_host`。`connector-sdk` 加上：

1. scheme 只允許 http／https
2. domain allowlist／denylist（`SourcePolicy`）
3. hard deny → 無條件拒絕
4. public → 允許
5. soft deny → 未過期 `NetworkRule` 才允許，並寫稽核
6. `reqwest` 關閉自動 redirect；每個 hop 重跑 1–5
7. `ClientBuilder::resolve(host, pinned_ip)`，connect 不再 DNS

`validate_network_rule`：operator／admin、必填 `reason`／`approved_by`、禁止萬用字元、拒絕涵蓋 IMDS 的規則（含 `169.254.0.0/16` 這種會蓋到 metadata 的 CIDR）。

## RawEvidence 路徑

```text
raw/{source_id}/{yyyy}/{mm}/{dd}/{evidence_id}
```

SHA256 用 `sha2` 0.10。同一 `id` 再寫仍是 `StorageError::Conflict`。物件寫成功、metadata 失敗時會嘗試刪掉剛寫的 blob。

## 測試

- 單元：IP 分類、規則驗證、rate limit、retry、feed-rs parse
- `connector-sdk/tests/ssrf_http.rs`：本機 axum 假 server（ephemeral 埠），涵蓋 default deny、白名單、過期規則、IMDS、redirect、DNS pin、oversized、304、timeout
- `connector-rss/tests/e2e.rs`：假 RSS → MinIO + Postgres 讀回核對。需要本機 compose 與 `.env`
- `connector-static-web/tests/e2e.rs`：假 HTML → RawEvidence；ETag 304 不寫新證據
- `connector-rest-api/tests/e2e.rs`：假 JSON API + SecretRef bearer → RawEvidence；configuration 含 Authorization 會被拒
- `normalizer/tests/e2e.rs`：假 RSS／HTML／JSON → collector → Document 或 `SkippedUnsupported`。見 `docs/developer/collector-normalizer.md`

測試**不**連真實外網，也**不**打 8080／9200／9000。
