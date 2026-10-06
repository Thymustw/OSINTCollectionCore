-- V0.3：原始證據 ↔ 集合改成多對多；collection_objects.object_id 補 FK。
--
-- SQLite 版本與做法（實測）：
--   * sqlx 0.9 / libsqlite3-sys 0.37.0 內建 SQLite 3.51.3。
--   * 3.35+ 的 ALTER TABLE DROP COLUMN 可以刪「自己帶 REFERENCES」的欄位
--     （限制是「被其他表／索引參照」的欄位不能刪；raw_evidence.collection_id
--     沒有被任何索引或其他表引用）。系統 Python sqlite 3.45.1 實測通過，
--     刪完 sqlite_master 裡的 REFERENCES 一併消失。因此 raw_evidence 走
--     DROP COLUMN，不必整表重建。
--   * SQLite 不能 ALTER TABLE ADD CONSTRAINT，collection_objects 要加
--     documents FK 必須走「建新表 → 複製 → DROP 舊表 → RENAME」。
--     原表沒有 PK 以外的索引或觸發器要重建。

CREATE TABLE raw_evidence_collections (
    raw_evidence_id TEXT NOT NULL REFERENCES raw_evidence (id) ON DELETE CASCADE,
    collection_id   TEXT NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    PRIMARY KEY (raw_evidence_id, collection_id)
);

CREATE INDEX idx_raw_evidence_collections_collection
    ON raw_evidence_collections (collection_id);

INSERT INTO raw_evidence_collections (raw_evidence_id, collection_id)
SELECT id, collection_id
FROM raw_evidence
WHERE collection_id IS NOT NULL;

ALTER TABLE raw_evidence DROP COLUMN collection_id;

CREATE TABLE collection_objects__new (
    collection_id   TEXT NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    object_id       TEXT NOT NULL REFERENCES documents (id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, object_id)
);

INSERT INTO collection_objects__new (collection_id, object_id)
SELECT collection_id, object_id FROM collection_objects;

DROP TABLE collection_objects;

ALTER TABLE collection_objects__new RENAME TO collection_objects;
