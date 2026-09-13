-- V0.2 Phase 3 Step 1：Embedding canonical record（SQLite 對應 schema）。
--
-- 語意對齊 migrations/postgres/0010_v0_2_embeddings.sql，
-- 型別依 0001 的慣例轉換：UUID → TEXT、TIMESTAMPTZ → TEXT（RFC3339）。
-- 「為什麼不存向量、為什麼 target_id 沒有 FK」的完整理由寫在 PostgreSQL 那一份。

CREATE TABLE embeddings (
    id              TEXT PRIMARY KEY,
    target_id       TEXT NOT NULL,
    target_type     TEXT NOT NULL,
    model           TEXT NOT NULL,
    model_version   TEXT NOT NULL,
    dimensions      INTEGER NOT NULL,
    content_hash    TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_embeddings_target_model_hash
    ON embeddings (target_id, target_type, model, content_hash);

CREATE INDEX idx_embeddings_target ON embeddings (target_id, target_type, id);
