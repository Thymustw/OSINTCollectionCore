-- V0.3：collection_objects.object_id 補反向索引。
--
-- 0016 幫 object_id 加上 REFERENCES documents (id) ON DELETE CASCADE。
-- 主鍵是 (collection_id, object_id)，刪文件時用 object_id 找關聯列
-- 走不到 PK，沒有這條索引會變成 Seq Scan。

CREATE INDEX IF NOT EXISTS collection_objects_object_id_idx
    ON collection_objects (object_id);
