-- V0.2 STIX Phase 4 Step 2：Job 加 parameters 欄位。
--
-- 既有 9 個欄位沒有地方放「這個 job 要用什麼參數執行」——graph_rebuild 一直
-- 被迫永遠 drop_graph=false 正是這個缺口的直接後果（沒有欄位可以安全傳 flag）。
-- STIX import/export 需要傳 raw_evidence_id、source_id、export filter 等參數，
-- 這是通用的補齊，不是 STIX 專屬——未來任何需要參數的 Job type（embedding
-- backfill filter、graph partial rebuild）都受益。
--
-- NULL = 沒有參數（既有 job type 如 graph_rebuild／collect 不受影響，繼續
-- 讀 config 或用固定行為）。
ALTER TABLE jobs ADD COLUMN parameters JSONB DEFAULT NULL;

COMMENT ON COLUMN jobs.parameters IS
  'Job 執行參數，JSON 物件，語意依 jobs.type 而定。NULL = 無參數（沿用舊行為）。';
