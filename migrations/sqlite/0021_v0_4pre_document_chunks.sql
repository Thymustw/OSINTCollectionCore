-- 階段 5 M1（ADR-016 D5）：段落表（SQLite 版）。
--
-- UUID／時間欄位走 TEXT，與既有 sqlite migration 同一套慣例。語意見 postgres 版 0021。
CREATE TABLE document_chunks (
    id                TEXT PRIMARY KEY,
    document_id       TEXT NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    seq               INTEGER NOT NULL CHECK (seq >= 0),
    start_char        INTEGER NOT NULL CHECK (start_char >= 0),
    end_char          INTEGER NOT NULL CHECK (end_char >= start_char),
    text              TEXT NOT NULL,
    header            TEXT,
    token_count       INTEGER NOT NULL CHECK (token_count >= 0),
    strategy          TEXT NOT NULL,
    strategy_version  INTEGER NOT NULL CHECK (strategy_version >= 1),
    simhash           INTEGER,
    group_id          TEXT,
    created_at        TEXT NOT NULL,
    UNIQUE (document_id, seq)
);

CREATE INDEX idx_document_chunks_group
    ON document_chunks (group_id)
    WHERE group_id IS NOT NULL;
