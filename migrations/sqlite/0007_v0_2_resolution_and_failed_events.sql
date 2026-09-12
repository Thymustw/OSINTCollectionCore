-- V0.2 Phase 0c：Entity Resolution 四張表 + failed_events（SQLite 對應 schema）。
--
-- 語意對齊 migrations/postgres/0007_v0_2_resolution_and_failed_events.sql，
-- 型別依 0001 的慣例轉換：UUID → TEXT、TIMESTAMPTZ → TEXT（RFC3339）、
-- JSONB → TEXT、DOUBLE PRECISION → REAL、BIGINT → INTEGER（SQLite 整數就是 64-bit）。
--
-- **每個欄位「為什麼長這樣」的完整理由寫在 PostgreSQL 那一份，不在這裡重複。**
-- 這個檔只記 SQLite 專屬的差異。

-- ---------------------------------------------------------------------------
-- entity_aliases（SPEC §3）
-- ---------------------------------------------------------------------------
CREATE TABLE entity_aliases (
    id              TEXT PRIMARY KEY,
    entity_id       TEXT NOT NULL REFERENCES entities (id),
    alias           TEXT NOT NULL,
    alias_type      TEXT NOT NULL,
    source_id       TEXT REFERENCES sources (id),
    confidence      REAL NOT NULL,
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL
);

CREATE INDEX idx_entity_aliases_entity ON entity_aliases (entity_id, id);
CREATE INDEX idx_entity_aliases_alias ON entity_aliases (alias);

-- ---------------------------------------------------------------------------
-- entity_identifiers（SPEC §4）
-- ---------------------------------------------------------------------------
CREATE TABLE entity_identifiers (
    id                  TEXT PRIMARY KEY,
    entity_id           TEXT NOT NULL REFERENCES entities (id),
    namespace           TEXT NOT NULL,
    value               TEXT NOT NULL,
    normalized_value    TEXT NOT NULL,
    confidence          REAL NOT NULL,
    source_id           TEXT REFERENCES sources (id),
    first_seen          TEXT NOT NULL,
    last_seen           TEXT NOT NULL
);

-- ⚠️ 同 PG：撞到這個唯一鍵是 resolution 的訊號，不是可以吞掉的錯誤。
CREATE UNIQUE INDEX idx_entity_identifiers_natural
    ON entity_identifiers (namespace, normalized_value);

CREATE INDEX idx_entity_identifiers_entity ON entity_identifiers (entity_id, id);

-- ---------------------------------------------------------------------------
-- resolution_candidates（SPEC §5）
-- ---------------------------------------------------------------------------
CREATE TABLE resolution_candidates (
    id              TEXT PRIMARY KEY,
    entity_a_id     TEXT NOT NULL REFERENCES entities (id),
    entity_b_id     TEXT NOT NULL REFERENCES entities (id),
    score           REAL NOT NULL,
    method          TEXT NOT NULL,
    evidence        TEXT NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    reviewed_at     TEXT,

    -- 這裡比的是 UUID 的**文字**形式，PG 比的是 16 個位元組。兩者順序相同：
    -- canonical 形式是固定長度的小寫十六進位、連字號在固定位置，
    -- ASCII 字典序與位元組序一致（'0'..'9' < 'a'..'f'）。
    -- ⚠️ 前提是寫入端一律用小寫 canonical 形式（uuid::Uuid::to_string 就是）。
    -- 寫成大寫會讓兩個 backend 的排序分岔。
    CONSTRAINT resolution_candidates_pair_ordered CHECK (entity_a_id < entity_b_id)
);

CREATE UNIQUE INDEX idx_resolution_candidates_pair_method
    ON resolution_candidates (entity_a_id, entity_b_id, method);

CREATE INDEX idx_resolution_candidates_status
    ON resolution_candidates (status, id DESC);

-- ---------------------------------------------------------------------------
-- merge_history（SPEC §7 / Acceptance C）
-- ---------------------------------------------------------------------------
CREATE TABLE merge_history (
    id                      TEXT PRIMARY KEY,
    survivor_id             TEXT NOT NULL REFERENCES entities (id),
    -- 刻意不加外鍵，理由見 PG 版。
    merged_id               TEXT NOT NULL,
    reason                  TEXT NOT NULL,
    operator                TEXT NOT NULL,
    timestamp               TEXT NOT NULL,
    repointed_references    TEXT NOT NULL DEFAULT '[]',
    undone_at               TEXT
);

CREATE INDEX idx_merge_history_survivor ON merge_history (survivor_id, id DESC);
CREATE INDEX idx_merge_history_merged ON merge_history (merged_id, id DESC);

-- ---------------------------------------------------------------------------
-- failed_events（ADR-008）
-- ---------------------------------------------------------------------------
CREATE TABLE failed_events (
    id              TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    partition       INTEGER NOT NULL,
    -- `offset` 在 SQLite 也是保留字，同樣要加引號。
    "offset"        INTEGER NOT NULL,
    consumer_group  TEXT NOT NULL,
    failure_reason  TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 1,
    envelope        TEXT NOT NULL DEFAULT '{}',
    first_seen      TEXT NOT NULL,
    last_seen       TEXT NOT NULL,
    replayed_at     TEXT
);

CREATE UNIQUE INDEX idx_failed_events_coordinates
    ON failed_events (topic, partition, "offset");

-- SQLite 從 3.8.0 起支援 partial index，語法與 PG 相同。
CREATE INDEX idx_failed_events_unreplayed
    ON failed_events (id DESC)
    WHERE replayed_at IS NULL;
