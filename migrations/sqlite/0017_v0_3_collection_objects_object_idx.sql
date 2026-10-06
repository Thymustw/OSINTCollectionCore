-- V0.3：collection_objects.object_id 補反向索引。
--
-- 0016 重建 collection_objects 時加上 documents FK ON DELETE CASCADE，
-- 但主鍵仍是 (collection_id, object_id)。刪文件時用 object_id 找關聯列
-- 走不到 PK，沒有這條索引會變成全表掃描。

CREATE INDEX IF NOT EXISTS collection_objects_object_id_idx
    ON collection_objects (object_id);
