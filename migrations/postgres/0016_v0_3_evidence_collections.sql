-- V0.3：原始證據 ↔ 集合改成多對多；collection_objects.object_id 補 FK。
--
-- 一個來源可屬於多個調查集合，但 raw_evidence.collection_id 只有一格，
-- 收集／匯入時被迫寫 None。改成關聯表後，一筆原始證據可以同時屬於多個集合。
-- 既有非空 collection_id 先搬進新表再刪欄位。

CREATE TABLE raw_evidence_collections (
    raw_evidence_id UUID NOT NULL REFERENCES raw_evidence (id) ON DELETE CASCADE,
    collection_id   UUID NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    PRIMARY KEY (raw_evidence_id, collection_id)
);

-- PK 前綴是 raw_evidence_id，從集合反查證據需要另一邊的索引。
CREATE INDEX idx_raw_evidence_collections_collection
    ON raw_evidence_collections (collection_id);

INSERT INTO raw_evidence_collections (raw_evidence_id, collection_id)
SELECT id, collection_id
FROM raw_evidence
WHERE collection_id IS NOT NULL;

ALTER TABLE raw_evidence DROP COLUMN collection_id;

-- collection_objects.object_id 原本沒有 FK：刪文件不會清掉關聯。
-- 若有 object_id 對不到 documents，這條會失敗——那是資料問題，不要默默刪。
ALTER TABLE collection_objects
    ADD CONSTRAINT collection_objects_object_id_fkey
    FOREIGN KEY (object_id) REFERENCES documents (id) ON DELETE CASCADE;
