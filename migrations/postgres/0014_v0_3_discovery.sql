-- V0.3 Discovery Foundation（SPEC_V0.3 §2／§4／§6／§7）。
--
-- 四張表：seeds、candidates、candidate_evidence、ai_runs。
-- 這次**只建資料模型**，不建 RelationalStore trait／storage 實作／core-api handler
-- （那些是後續步驟）。App 一律走 Core API，不直接讀這幾張表。
--
-- 型別對映：UUID、TIMESTAMPTZ、JSONB、DOUBLE PRECISION、BIGINT
-- 都比照既有 0001 的慣例。
--
-- 刻意不加 CHECK constraint 限制 seed_type／origin／candidate_type／status 的
-- 值——比照 entities.entity_type／relationships.relationship_type 既有慣例
-- （TEXT 欄位無 CHECK constraint，靠 Rust enum 的 serde 邊界把關，
-- 之後加新 variant 不需要 migration）。
--
-- candidate_evidence.object_id 刻意不加 REFERENCES documents (id)
-- 外鍵：跟 relationship_evidence.object_id 同一個理由，ObjectId 是「可能指向
-- Document 或其他物件種類」的通用別名，不綁單一 FK（見 relationship.rs 檔頭）。

CREATE TABLE seeds (
    id              UUID PRIMARY KEY,
    collection_id   UUID REFERENCES collections (id),
    seed_type       TEXT NOT NULL,
    value           TEXT NOT NULL,
    entity_id       UUID REFERENCES entities (id),
    priority        INTEGER NOT NULL,
    confidence      DOUBLE PRECISION NOT NULL,
    origin          TEXT NOT NULL,
    status          TEXT NOT NULL,
    depth           INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_seeds_collection ON seeds (collection_id, id DESC);
CREATE INDEX idx_seeds_status ON seeds (status, id DESC);

CREATE TABLE candidates (
    id                  UUID PRIMARY KEY,
    candidate_type      TEXT NOT NULL,
    value               TEXT NOT NULL,
    normalized_value    TEXT NOT NULL,
    collection_id       UUID REFERENCES collections (id),
    discovered_by       TEXT NOT NULL,
    discovery_method    TEXT NOT NULL,
    confidence          DOUBLE PRECISION NOT NULL,
    score               DOUBLE PRECISION NOT NULL,
    status              TEXT NOT NULL,
    depth               INTEGER NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    reviewed_at         TIMESTAMPTZ
);

CREATE INDEX idx_candidates_status ON candidates (status, id DESC);
CREATE INDEX idx_candidates_collection ON candidates (collection_id, id DESC);
-- Candidate Review Queue（Console，之後才會做，但 index 現在先建）常見查法
-- 是「這個 collection 有哪些 pending」，複合索引比對照 status 再過濾 collection 快。

CREATE TABLE candidate_evidence (
    id              UUID PRIMARY KEY,
    candidate_id    UUID NOT NULL REFERENCES candidates (id),
    object_id       UUID,
    entity_id       UUID REFERENCES entities (id),
    relationship_id UUID REFERENCES relationships (id),
    raw_evidence_id UUID REFERENCES raw_evidence (id),
    reason          TEXT NOT NULL,
    weight          DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_candidate_evidence_candidate ON candidate_evidence (candidate_id);

CREATE TABLE ai_runs (
    id                  UUID PRIMARY KEY,
    task_type           TEXT NOT NULL,
    provider            TEXT NOT NULL,
    model               TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    input_reference     JSONB NOT NULL DEFAULT '{}'::jsonb,
    output              JSONB NOT NULL DEFAULT '{}'::jsonb,
    confidence          DOUBLE PRECISION NOT NULL,
    tokens              BIGINT NOT NULL,
    estimated_cost      DOUBLE PRECISION NOT NULL,
    duration_ms         BIGINT NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_ai_runs_task_type ON ai_runs (task_type, id DESC);