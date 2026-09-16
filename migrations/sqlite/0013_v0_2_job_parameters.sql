-- V0.2 STIX Phase 4 Step 2：Job 加 parameters 欄位（SQLite 對應）。
--
-- 語意對齊 migrations/postgres/0013_v0_2_job_parameters.sql。
-- SQLite 沒有 JSONB，用 TEXT 存 JSON 字串，NULL 語意相同。
ALTER TABLE jobs ADD COLUMN parameters TEXT DEFAULT NULL;
