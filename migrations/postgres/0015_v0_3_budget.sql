-- V0.3 Discovery Budget（SPEC_V0.3 §10／§11）。
--
-- 兩張表：
-- - collection_budgets：每個 collection 的配額設定，1:1 對 collections，
--   collection_id 本身就是主鍵——這是設定，不是可列表資源，不需要獨立 id。
-- - discovery_daily_usage：每個 collection 每天的用量計數器，自然鍵
--   (collection_id, usage_date)，跟 failed_events 的 (topic, partition, offset)
--   自然鍵 upsert 同一種設計：DB 端原子累加，不是「先讀再寫」。
--
-- daily_request_budget／daily_ai_budget 用 BIGINT（比 max_*_per_run 的
-- INTEGER 大一級）：per-run 上限是單次執行範圍，量級跟一次 API 呼叫次數
-- 相當；daily 配額跨多次 run 累加，量級可能大得多。

CREATE TABLE collection_budgets (
    collection_id           UUID PRIMARY KEY REFERENCES collections (id),
    max_candidates_per_run  INTEGER NOT NULL,
    max_requests_per_run    INTEGER NOT NULL,
    max_ai_calls_per_run    INTEGER NOT NULL,
    max_depth               INTEGER NOT NULL,
    daily_request_budget    BIGINT NOT NULL,
    daily_ai_budget         BIGINT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL
);

CREATE TABLE discovery_daily_usage (
    collection_id   UUID NOT NULL REFERENCES collections (id),
    usage_date      DATE NOT NULL,
    requests_used   BIGINT NOT NULL DEFAULT 0,
    ai_calls_used   BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (collection_id, usage_date)
);