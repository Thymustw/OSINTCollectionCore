-- V0.1 Phase 6a：稽核與 API token 落地。
--
-- 在這之前 AuditLog 與 ApiTokenStore 都只有記憶體實作。那代表：
--   * 重啟 osint-api 就失去全部稽核紀錄——CLAUDE.md §10 要求的 audit 形同沒有。
--   * 發出去的 API token 在重啟後全部失效，而且沒有任何地方查得到發過哪些。
-- 兩張表都是**認證／稽核平面**，不是情報資料，所以不掛任何 collections/sources 外鍵。

-- ---------------------------------------------------------------------------
-- audit_log
-- ---------------------------------------------------------------------------
-- append-only。adapter（storage_postgres::PostgresAuditLog）只有 INSERT 與 SELECT，
-- 沒有 UPDATE/DELETE 路徑。
--
-- ⚠️ 資料庫層**沒有**強制不可變（那需要 trigger 或另開一個只有 INSERT 權限的角色，
-- 屬於部署層的事）。要真正防竄改，部署時應讓 osint-api 的 DB 帳號對這張表
-- 只有 INSERT + SELECT 權限。這一點寫在 docs/developer/security.md。
--
-- ⚠️ 欄位名 `details` 對應 Rust 的 `AuditEntry.metadata`。名字不同是刻意的
-- （`metadata` 在情報資料表裡已經是「這筆資料自己的中繼資料」的意思，
-- 稽核這裡指的是「這個動作的細節」）；對照關係只存在於
-- crates/storage-postgres/src/security.rs，改任何一邊都要同步改。
CREATE TABLE audit_log (
    -- UUID v7。cursor 分頁靠它排序，所以必須有時間序，不可改用 v4。
    id              UUID PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL,
    actor           TEXT NOT NULL,
    action          TEXT NOT NULL,
    resource_type   TEXT NOT NULL,
    resource_id     TEXT,
    details         JSONB NOT NULL DEFAULT '{}'::jsonb,
    ip              TEXT,
    outcome         TEXT NOT NULL
);

-- 「這個 job／token 身上發生過什麼」是最主要的查詢。id DESC 一起放進索引，
-- 讓「由新到舊」不必額外排序。
CREATE INDEX idx_audit_log_resource
    ON audit_log (resource_type, resource_id, id DESC);

-- 「這個人做過什麼」。事件調查的第二種問法。
CREATE INDEX idx_audit_log_actor
    ON audit_log (actor, id DESC);

-- 「認證失敗有沒有在短時間內暴增」。action 是低基數欄位，單獨建索引才查得動。
CREATE INDEX idx_audit_log_action
    ON audit_log (action, id DESC);

-- ---------------------------------------------------------------------------
-- api_tokens
-- ---------------------------------------------------------------------------
-- token_hash 是 argon2id 的 PHC 字串，**不是**明文也不是可逆編碼。
-- 明文只在 POST /api/v1/tokens 的回應裡出現一次，之後任何地方都拿不到。
--
-- 沒有 UNIQUE(token_hash)：argon2 每次都帶新的 salt，同一把 secret 兩次雜湊
-- 結果不同，建唯一鍵既擋不住重複也會誤導讀 schema 的人。
CREATE TABLE api_tokens (
    id              UUID PRIMARY KEY,
    name            TEXT NOT NULL,
    token_hash      TEXT NOT NULL,
    role            TEXT NOT NULL,
    created_by      TEXT,
    created_at      TIMESTAMPTZ NOT NULL,
    -- NULL = 不自動到期，只能靠撤銷。
    expires_at      TIMESTAMPTZ,
    -- NULL = 未撤銷。撤銷後不刪列：誰在什麼時候撤的，本身就是要保留的事實。
    revoked_at      TIMESTAMPTZ,
    last_used_at    TIMESTAMPTZ
);

-- 認證路徑是主鍵查詢（token 裡就帶著 id），不需要額外索引。
-- 這個部分索引是給運維用的：「現在還有幾把活的 token」。
CREATE INDEX idx_api_tokens_live
    ON api_tokens (created_at DESC)
    WHERE revoked_at IS NULL;
