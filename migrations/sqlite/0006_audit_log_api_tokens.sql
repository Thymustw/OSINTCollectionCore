-- V0.1 Phase 6a：稽核與 API token 落地（SQLite 對應 schema）。
-- 語意對齊 migrations/postgres/0006_audit_log_api_tokens.sql，
-- 型別依 0001 的慣例轉換：UUID → TEXT、TIMESTAMPTZ → TEXT（RFC3339）、JSONB → TEXT。
--
-- ⚠️ V0.1 **只有 schema，沒有 SQLite adapter**。
-- `AuditLog` / `ApiTokenStore` 的具體實作目前只有
-- `core_security::Memory*`（記憶體）與 `storage_postgres::Postgres*`（生產）兩種。
-- 這兩張表先建起來是為了維持「SQLite embedded schema 與 canonical schema 同構」
-- 的既有慣例——之後要做嵌入式部署的 audit 時不必再改 migration 編號。
--
-- 也就是說：**現在對這個 SQLite 檔跑完 migration，這兩張表會是空的，
-- 而且不會有任何程式去寫它們。** 看到空表不代表稽核壞了。

CREATE TABLE audit_log (
    id              TEXT PRIMARY KEY,
    timestamp       TEXT NOT NULL,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_id     TEXT,
    details         TEXT NOT NULL DEFAULT '{}',
    ip              TEXT,
    outcome         TEXT NOT NULL
);

CREATE INDEX idx_audit_log_resource
    ON audit_log (resource_type, resource_id, id DESC);

CREATE INDEX idx_audit_log_actor
    ON audit_log (actor, id DESC);

CREATE INDEX idx_audit_log_action
    ON audit_log (action, id DESC);

CREATE TABLE api_tokens (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    token_hash      TEXT NOT NULL,
    role            TEXT NOT NULL,
    created_by      TEXT,
    created_at      TEXT NOT NULL,
    expires_at      TEXT,
    revoked_at      TEXT,
    last_used_at    TEXT
);

CREATE INDEX idx_api_tokens_live
    ON api_tokens (created_at DESC)
    WHERE revoked_at IS NULL;
