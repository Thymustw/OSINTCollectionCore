-- V0.3 階段 4-2：transactional outbox（SQLite 版）。
--
-- UUID／時間／JSON 欄位走 TEXT，與既有 sqlite migration 同一套慣例。
--
-- ⚠️ SQLite 沒有 `FOR UPDATE SKIP LOCKED`。claim 靠單一寫入者前提：
-- SQLite 同一時間只有一個寫入者，兩個郵差會在寫鎖上序列化，不會重複送。
-- 本機／測試用；生產 canonical 是 PostgreSQL。
CREATE TABLE event_outbox (
    id              TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    partition_key   TEXT NOT NULL,
    correlation_id  TEXT,
    payload         TEXT NOT NULL DEFAULT '{}',
    source_service  TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TEXT NOT NULL,
    published_at    TEXT
);

CREATE INDEX idx_event_outbox_due
    ON event_outbox (next_attempt_at, created_at)
    WHERE published_at IS NULL;

CREATE INDEX idx_event_outbox_published
    ON event_outbox (published_at)
    WHERE published_at IS NOT NULL;
