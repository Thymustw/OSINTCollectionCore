-- 對應 migrations/postgres/0015_v0_3_budget.sql。**每個欄位「為什麼長這樣」
-- 的完整理由寫在 PostgreSQL 那一份，不在這裡重複。**
--
-- 型別依 0001 的既有慣例轉換：UUID → TEXT、TIMESTAMPTZ → TEXT（RFC3339）、
-- DATE → TEXT（YYYY-MM-DD）、BIGINT → INTEGER（SQLite 整數本來就是 64-bit）。

CREATE TABLE collection_budgets (
    collection_id           TEXT PRIMARY KEY REFERENCES collections (id),
    max_candidates_per_run  INTEGER NOT NULL,
    max_requests_per_run    INTEGER NOT NULL,
    max_ai_calls_per_run    INTEGER NOT NULL,
    max_depth               INTEGER NOT NULL,
    daily_request_budget    INTEGER NOT NULL,
    daily_ai_budget         INTEGER NOT NULL,
    created_at              TEXT NOT NULL,
    updated_at              TEXT NOT NULL
);

CREATE TABLE discovery_daily_usage (
    collection_id   TEXT NOT NULL REFERENCES collections (id),
    usage_date      TEXT NOT NULL,
    requests_used   INTEGER NOT NULL DEFAULT 0,
    ai_calls_used   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (collection_id, usage_date)
);