-- V0.3 階段 4-2：transactional outbox。
--
-- 業務寫入與事件列必須在同一個交易裡：業務 rollback 時 outbox 列也不會留下。
-- 郵差另外 claim／publish／標記，不在這張表的寫入交易裡打 broker。
--
-- published_at IS NULL = 待送（含退避中）。郵差取「到期且未送」的列。
CREATE TABLE event_outbox (
    id              UUID PRIMARY KEY,
    topic           TEXT NOT NULL,
    partition_key   TEXT NOT NULL,
    correlation_id  UUID,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- 原始發送服務名，寫進 envelope.source_service。郵差在 core-api 裡跑，
    -- 但不能把所有事件都標成 core-api——discovery-worker／resolver 發出的
    -- 要保留真正的來源。
    source_service  TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    published_at    TIMESTAMPTZ
);

-- 郵差查詢：WHERE published_at IS NULL AND next_attempt_at <= $now
-- ORDER BY created_at。partial index 排除已送列，避免表長大之後掃到歷史。
CREATE INDEX idx_event_outbox_due
    ON event_outbox (next_attempt_at, created_at)
    WHERE published_at IS NULL;

-- 已送列的保留期限清除：DELETE WHERE published_at < $cutoff。
CREATE INDEX idx_event_outbox_published
    ON event_outbox (published_at)
    WHERE published_at IS NOT NULL;
