-- V0.1 Phase 4a：deduplicator（SPEC §15 五階段 + §16 duplicate group）。
-- 對應 migrations/postgres/0004_dedup_stage_columns.sql，說明見該檔。
--
-- SQLite 差異：
--   * UUID 一律存 TEXT（本 schema 的既有慣例）。
--   * BIGINT → INTEGER（SQLite 的 INTEGER 本來就是有號 64-bit）。
--   * `ALTER TABLE ... ADD COLUMN` 不能加 REFERENCES 並帶 NOT NULL 預設值，
--     但可以加可空的外鍵欄位。

ALTER TABLE documents ADD COLUMN external_key TEXT;
ALTER TABLE documents ADD COLUMN simhash INTEGER;
ALTER TABLE documents ADD COLUMN duplicate_of TEXT REFERENCES documents (id);

CREATE INDEX idx_documents_external_key ON documents (external_key)
    WHERE external_key IS NOT NULL;

CREATE INDEX idx_documents_duplicate_of ON documents (duplicate_of)
    WHERE duplicate_of IS NOT NULL;

CREATE INDEX idx_documents_simhash_recent ON documents (id DESC)
    WHERE simhash IS NOT NULL;

CREATE UNIQUE INDEX idx_duplicate_groups_member_object
    ON duplicate_groups (member_object_id)
    WHERE member_object_id IS NOT NULL;

CREATE UNIQUE INDEX idx_provenance_dedup_subject
    ON provenance (subject_id)
    WHERE action = 'deduplicated';
