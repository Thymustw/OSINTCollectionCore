-- V0.2 Phase 1 / ADR-012：AI 輔助自動核准的稽核欄位（SQLite 對應）。
--
-- 語意對齊 migrations/postgres/0012_v0_2_merge_history_auto_approval_audit.sql。
-- SQLite 沒有 JSONB，用 TEXT 存 JSON 字串，NULL 語意相同。
ALTER TABLE merge_history ADD COLUMN auto_approval_audit TEXT DEFAULT NULL;
