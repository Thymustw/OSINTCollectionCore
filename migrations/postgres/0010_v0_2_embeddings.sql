-- V0.2 Phase 3 Step 1：Embedding canonical record（SPEC §11）。
--
-- **不存向量本體**——向量只投影進 OpenSearch k-NN index（Step 2）。
-- PostgreSQL 只存「這個目標、這個模型、這個內容雜湊算過了」的事實，
-- 給 embedding-worker 判斷要不要重算（re-generate）。
--
-- `target_id` 刻意**沒有** REFERENCES：目標可以是 Document／Entity／Event，
-- 三張表共用一個 UUID 欄位。FK 綁任何一張都會讓另外兩種寫不進去。
-- 完整性由呼叫端（Step 3 的 embedding-worker）保證。

CREATE TABLE embeddings (
    id              UUID PRIMARY KEY,
    target_id       UUID NOT NULL,
    target_type     TEXT NOT NULL,
    model           TEXT NOT NULL,
    model_version   TEXT NOT NULL,
    dimensions      INTEGER NOT NULL,
    content_hash    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

-- 同一目標、同一模型、同一內容雜湊只算一次——這是 re-generate 判斷「要不要
-- 重算」的依據：查得到就跳過，查不到才重新呼叫 EmbeddingProvider。
CREATE UNIQUE INDEX idx_embeddings_target_model_hash
    ON embeddings (target_id, target_type, model, content_hash);

-- 查「這個目標有哪些 embedding」（例如 Step 4 的 similar objects 用）。
-- id 一起放進索引，讓 ORDER BY id 不必額外排序。
CREATE INDEX idx_embeddings_target ON embeddings (target_id, target_type, id);
