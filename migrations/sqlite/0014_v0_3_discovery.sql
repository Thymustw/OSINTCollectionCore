-- V0.3 Discovery Foundation（SPEC_V0.3 §2／§4／§6／§7）（SQLite 對應 schema）。
--
-- 語意對齊 migrations/postgres/0014_v0_3_discovery.sql，
-- 型別依 0001 的慣例轉換：UUID → TEXT、TIMESTAMPTZ → TEXT（RFC3339）、
-- JSONB → TEXT、DOUBLE PRECISION → REAL、BIGINT → INTEGER（SQLite 整數就是 64-bit）。
--
-- **每個欄位「為什麼長這樣」的完整理由寫在 PostgreSQL 那一份，不在這裡重複。**
-- 這個檔只記 SQLite 專屬的差異。

CREATE TABLE seeds (
    id              TEXT PRIMARY KEY,
    collection_id   TEXT REFERENCES collections (id),
    seed_type       TEXT NOT NULL,
    value           TEXT NOT NULL,
    entity_id       TEXT REFERENCES entities (id),
    priority        INTEGER NOT NULL,
    confidence      REAL NOT NULL,
    origin          TEXT NOT NULL,
    status          TEXT NOT NULL,
    depth           INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_seeds_collection ON seeds (collection_id, id DESC);
CREATE INDEX idx_seeds_status ON seeds (status, id DESC);

CREATE TABLE candidates (
    id                  TEXT PRIMARY KEY,
    candidate_type      TEXT NOT NULL,
    value               TEXT NOT NULL,
    normalized_value    TEXT NOT NULL,
    collection_id       TEXT REFERENCES collections (id),
    discovered_by       TEXT NOT NULL,
    discovery_method    TEXT NOT NULL,
    confidence          REAL NOT NULL,
    score               REAL NOT NULL,
    status              TEXT NOT NULL,
    depth               INTEGER NOT NULL,
    created_at          TEXT NOT NULL,
    reviewed_at         TEXT
);

CREATE INDEX idx_candidates_status ON candidates (status, id DESC);
CREATE INDEX idx_candidates_collection ON candidates (collection_id, id DESC);
-- Candidate Review Queue（Console，之後才會做，但 index 現在先建）常見查法
-- 是「這個 collection 有哪些 pending」，複合索引比對照 status 再過濾 collection 快。

CREATE TABLE candidate_evidence (
    id              TEXT PRIMARY KEY,
    candidate_id    TEXT NOT NULL REFERENCES candidates (id),
    object_id       TEXT,
    entity_id       TEXT REFERENCES entities (id),
    relationship_id TEXT REFERENCES relationships (id),
    raw_evidence_id TEXT REFERENCES raw_evidence (id),
    reason          TEXT NOT NULL,
    weight          REAL NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE INDEX idx_candidate_evidence_candidate ON candidate_evidence (candidate_id);

CREATE TABLE ai_runs (
    id                  TEXT PRIMARY KEY,
    task_type           TEXT NOT NULL,
    provider            TEXT NOT NULL,
    model               TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    prompt_version      TEXT NOT NULL,
    input_reference     TEXT NOT NULL DEFAULT '{}',
    output              TEXT NOT NULL DEFAULT '{}',
    confidence          REAL NOT NULL,
    tokens              INTEGER NOT NULL,
    estimated_cost      REAL NOT NULL,
    duration_ms         INTEGER NOT NULL,
    created_at          TEXT NOT NULL
);

CREATE INDEX idx_ai_runs_task_type ON ai_runs (task_type, id DESC);