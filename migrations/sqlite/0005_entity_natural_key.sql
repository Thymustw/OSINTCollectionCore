-- V0.1 Phase 4b：entity-worker。
-- 對應 migrations/postgres/0005_entity_natural_key.sql，完整說明見該檔。
--
-- SQLite 差異（三處）：
--   1. 沒有 `UPDATE ... FROM`（3.33 之後才有，不假設本機版本），改用相關子查詢。
--   2. UUID 存 TEXT，所以 MIN(id) 直接就是字串比較，不需要 ::text 轉換。
--   3. 沒有 TEMP TABLE 的必要——相關子查詢已足夠，少一個要清理的物件。
--
-- 順序與 PostgreSQL 版相同且同樣不可對調：
-- 先合併重複 Entity → 再合併重複 Relationship → 才建 UNIQUE。

-- ---------------------------------------------------------------------------
-- 1. 合併重複 Entity
-- ---------------------------------------------------------------------------
-- 每組 (entity_type, normalized_name) 保留 id 最小的那一列，其餘的參照改指過去。

UPDATE entity_extractions
SET entity_id = (
    SELECT MIN(keep.id)
    FROM entities keep
    JOIN entities loser ON loser.id = entity_extractions.entity_id
    WHERE keep.entity_type = loser.entity_type
      AND keep.normalized_name = loser.normalized_name
)
WHERE EXISTS (SELECT 1 FROM entities WHERE id = entity_extractions.entity_id);

UPDATE relationships
SET source_object_id = (
    SELECT MIN(keep.id)
    FROM entities keep
    JOIN entities loser ON loser.id = relationships.source_object_id
    WHERE keep.entity_type = loser.entity_type
      AND keep.normalized_name = loser.normalized_name
)
WHERE EXISTS (SELECT 1 FROM entities WHERE id = relationships.source_object_id);

UPDATE relationships
SET target_object_id = (
    SELECT MIN(keep.id)
    FROM entities keep
    JOIN entities loser ON loser.id = relationships.target_object_id
    WHERE keep.entity_type = loser.entity_type
      AND keep.normalized_name = loser.normalized_name
)
WHERE EXISTS (SELECT 1 FROM entities WHERE id = relationships.target_object_id);

DELETE FROM entities
WHERE id NOT IN (
    SELECT MIN(id) FROM entities GROUP BY entity_type, normalized_name
);

-- ---------------------------------------------------------------------------
-- 2. 合併重複 Relationship
-- ---------------------------------------------------------------------------
-- ⚠️ relationship_evidence 對 relationships 是 ON DELETE CASCADE，
-- 必須先把 evidence 改指存活者再刪，否則證據會跟著被刪掉（靜默資料遺失）。

UPDATE relationship_evidence
SET relationship_id = (
    SELECT MIN(keep.id)
    FROM relationships keep
    JOIN relationships loser ON loser.id = relationship_evidence.relationship_id
    WHERE keep.source_object_id = loser.source_object_id
      AND keep.relationship_type = loser.relationship_type
      AND keep.target_object_id = loser.target_object_id
)
WHERE EXISTS (SELECT 1 FROM relationships WHERE id = relationship_evidence.relationship_id);

DELETE FROM relationships
WHERE id NOT IN (
    SELECT MIN(id) FROM relationships
    GROUP BY source_object_id, relationship_type, target_object_id
);

-- ---------------------------------------------------------------------------
-- 3. 自然鍵 UNIQUE 與冪等 claim
-- ---------------------------------------------------------------------------

DROP INDEX idx_entities_normalized_name;

CREATE UNIQUE INDEX idx_entities_natural_key
    ON entities (entity_type, normalized_name);

CREATE UNIQUE INDEX idx_relationships_natural_key
    ON relationships (source_object_id, relationship_type, target_object_id);

CREATE UNIQUE INDEX idx_provenance_entity_subject
    ON provenance (subject_id)
    WHERE action = 'entity_extracted';

-- ---------------------------------------------------------------------------
-- 4. RelationshipEvidence 反查
-- ---------------------------------------------------------------------------

CREATE INDEX idx_relationship_evidence_relationship
    ON relationship_evidence (relationship_id, created_at);
