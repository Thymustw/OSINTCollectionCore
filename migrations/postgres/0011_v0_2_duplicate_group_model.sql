-- V0.2 Phase 3 Step 1：Semantic Dedup 的模型名稱寫在 DuplicateGroup.model。
--
-- Stage 1-4 的方法不靠模型，欄位是 NULL。Stage 5（語意判定）才填模型名稱。
-- SPEC §17「method/model」。

ALTER TABLE duplicate_groups ADD COLUMN model TEXT NULL;
