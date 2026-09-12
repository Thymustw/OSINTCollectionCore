-- V0.2 Phase 1e：Entity Merge 需要的兩個欄位擴充。
--
-- entities.merged_into：merge 時標記被併掉的 Entity，不刪除該列
-- （resolution_candidates／entity_extractions 的 FK 沒有 ON DELETE CASCADE，
-- 刪除會違反外鍵；undo 需要這一列存在才能把參照寫回去）。
ALTER TABLE entities ADD COLUMN merged_into UUID NULL REFERENCES entities (id);

-- merge_history.merged_relationships：記錄因 relationships 表的
-- (source_object_id, relationship_type, target_object_id) UNIQUE 撞號而被
-- 吸收合併或自迴圈刪除的 relationship。RepointedReference 只能表達「單欄位
-- 從 A 改成 B」，無法表達「整列被刪除、evidence 搬去別的列」，所以另開欄位。
ALTER TABLE merge_history ADD COLUMN merged_relationships JSONB NOT NULL DEFAULT '[]'::jsonb;
