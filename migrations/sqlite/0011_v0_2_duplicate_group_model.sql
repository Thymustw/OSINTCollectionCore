-- V0.2 Phase 3 Step 1：DuplicateGroup.model（SQLite 對應）。
--
-- 語意對齊 migrations/postgres/0011_v0_2_duplicate_group_model.sql。
-- SQLite 的 ALTER TABLE ADD COLUMN 可空欄位可以加（同 0004 的 documents.duplicate_of）。

ALTER TABLE duplicate_groups ADD COLUMN model TEXT;
