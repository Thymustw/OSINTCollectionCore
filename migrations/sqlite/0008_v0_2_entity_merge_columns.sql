-- V0.2 Phase 1e：Entity Merge 兩個欄位擴充（SQLite 對應）。
--
-- 語意對齊 migrations/postgres/0008_v0_2_entity_merge_columns.sql。
-- 型別依 0001 的慣例：UUID → TEXT、JSONB → TEXT。
-- 「為什麼要有這兩個欄位」的完整理由寫在 PostgreSQL 那一份，這裡只記 SQLite 差異。
--
-- SQLite 的 ALTER TABLE ADD COLUMN：
--   * 可空的 REFERENCES 可以加（同 0004 的 documents.duplicate_of）。
--   * NOT NULL 欄位必須給常數預設值；`'[]'` 是常數，可以用。

ALTER TABLE entities ADD COLUMN merged_into TEXT REFERENCES entities (id);

ALTER TABLE merge_history ADD COLUMN merged_relationships TEXT NOT NULL DEFAULT '[]';
