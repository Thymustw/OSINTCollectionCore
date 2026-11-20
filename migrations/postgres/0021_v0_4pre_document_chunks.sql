-- 階段 5 M1（ADR-016 D5）：段落表。
--
-- 段落是衍生資料：從文件的串接文字（storage_core::chunk_source_text）切出來，可整批刪掉重切。
-- 向量是 OpenSearch 投影（M2），不在這裡。
--
-- id = UUID v5(CHUNK_NAMESPACE, "{document_id}|{strategy}@{strategy_version}|{seq}")，重切冪等。
-- start_char／end_char 是字元（Unicode scalar）偏移，不是 byte。
-- simhash／group_id 給 M4（段落層級近似去重）用，M1 一律 NULL；先建欄位是為了不必再改 schema。
CREATE TABLE document_chunks (
    id                UUID PRIMARY KEY,
    document_id       UUID NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    seq               INTEGER NOT NULL CHECK (seq >= 0),
    start_char        INTEGER NOT NULL CHECK (start_char >= 0),
    end_char          INTEGER NOT NULL CHECK (end_char >= start_char),
    text              TEXT NOT NULL,
    header            TEXT,
    token_count       INTEGER NOT NULL CHECK (token_count >= 0),
    strategy          TEXT NOT NULL,
    strategy_version  INTEGER NOT NULL CHECK (strategy_version >= 1),
    simhash           BIGINT,
    group_id          UUID,
    created_at        TIMESTAMPTZ NOT NULL,
    UNIQUE (document_id, seq)
);

-- M4 段落群組查詢（同群組的其他段落）。partial：M1 全是 NULL，不佔空間。
CREATE INDEX idx_document_chunks_group
    ON document_chunks (group_id)
    WHERE group_id IS NOT NULL;
