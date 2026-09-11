-- V0.1 Phase 4b：entity-worker（SPEC §17 entity extraction + §11／§12 relationship/evidence）。
--
-- 四件事，順序不可對調：
--   1. 合併既有的重複 Entity（否則第 3 步的 UNIQUE 建不起來）
--   2. 合併既有的重複 Relationship（同上）
--   3. 建立自然鍵 UNIQUE 與 entity-worker 的冪等 claim
--   4. RelationshipEvidence 的反查索引

-- ---------------------------------------------------------------------------
-- 1. 合併重複 Entity
-- ---------------------------------------------------------------------------
-- 原始 schema 的 idx_entities_normalized_name 是**非唯一**索引，所以既有資料庫裡
-- 可能已經有多列共用同一組 (entity_type, normalized_name)。實測：本專案的開發用
-- Postgres 有 35 列 (vulnerability, cve-2026-0001)——全部來自
-- storage-core conformance 的固定字串 fixture（該 fixture 已在本次改動中改成含 UUID）。
--
-- 直接 CREATE UNIQUE INDEX 會失敗；直接 DELETE 會撞 entity_extractions 的外鍵。
-- 所以做**合併**：每組保留 id 最小的那一列當存活者，把指向其他列的參照改指過去，
-- 再刪掉輸家。
--
-- 合併而不是報錯要求人工處理，是因為這些列**在語意上本來就是同一個實體**——
-- 自然鍵相同就是同一個。合併不會丟失任何抽取紀錄或關聯，只是把它們接到同一個 id 上。
-- 唯一會丟的是輸家那幾列的 `attributes`／`description`，這一點寫在
-- docs/developer/entity-worker.md 的「migration 0005」一節。

CREATE TEMP TABLE entity_merge_map AS
SELECT
    e.id          AS loser_id,
    survivor.id   AS survivor_id
FROM entities e
JOIN (
    SELECT entity_type, normalized_name, MIN(id::text) AS keep_id
    FROM entities
    GROUP BY entity_type, normalized_name
    HAVING COUNT(*) > 1
) survivor_key
  ON e.entity_type = survivor_key.entity_type
 AND e.normalized_name = survivor_key.normalized_name
JOIN entities survivor
  ON survivor.id = survivor_key.keep_id::uuid
WHERE e.id <> survivor.id;

-- 抽取紀錄改指存活者。
UPDATE entity_extractions ex
SET entity_id = m.survivor_id
FROM entity_merge_map m
WHERE ex.entity_id = m.loser_id;

-- 關聯的兩端都可能指向被合併的 Entity。
UPDATE relationships r
SET source_object_id = m.survivor_id
FROM entity_merge_map m
WHERE r.source_object_id = m.loser_id;

UPDATE relationships r
SET target_object_id = m.survivor_id
FROM entity_merge_map m
WHERE r.target_object_id = m.loser_id;

DELETE FROM entities e
USING entity_merge_map m
WHERE e.id = m.loser_id;

-- ---------------------------------------------------------------------------
-- 2. 合併重複 Relationship
-- ---------------------------------------------------------------------------
-- 第 1 步把兩端改指存活者之後，原本不同的邊有可能塌成同一組
-- (source, type, target)。先合併再建 UNIQUE。
--
-- ⚠️ relationship_evidence 對 relationships 是 ON DELETE CASCADE，
-- **必須先把 evidence 改指存活者再刪**，否則證據會跟著被刪掉——
-- 那是靜默的資料遺失（migration 會成功，只是證據少了）。

CREATE TEMP TABLE relationship_merge_map AS
SELECT
    r.id        AS loser_id,
    survivor.id AS survivor_id
FROM relationships r
JOIN (
    SELECT source_object_id, relationship_type, target_object_id,
           MIN(id::text) AS keep_id
    FROM relationships
    GROUP BY source_object_id, relationship_type, target_object_id
    HAVING COUNT(*) > 1
) survivor_key
  ON r.source_object_id = survivor_key.source_object_id
 AND r.relationship_type = survivor_key.relationship_type
 AND r.target_object_id = survivor_key.target_object_id
JOIN relationships survivor
  ON survivor.id = survivor_key.keep_id::uuid
WHERE r.id <> survivor.id;

UPDATE relationship_evidence ev
SET relationship_id = m.survivor_id
FROM relationship_merge_map m
WHERE ev.relationship_id = m.loser_id;

DELETE FROM relationships r
USING relationship_merge_map m
WHERE r.id = m.loser_id;

DROP TABLE entity_merge_map;
DROP TABLE relationship_merge_map;

-- ---------------------------------------------------------------------------
-- 3. 自然鍵 UNIQUE 與冪等 claim
-- ---------------------------------------------------------------------------
-- 「同一個 CVE 在不同文章出現要對到同一個 Entity」是 SPEC §10 的隱含要求，但原始
-- schema 只有非唯一索引——它讓查詢變快，卻不阻止兩個並發的 worker 各建一個
-- `CVE-2026-0001`。那種重複不會報錯，只會讓之後每一次「這個 CVE 出現在哪些文章」
-- 的查詢都少算一半。
--
-- entity-worker 另外把 Entity id 算成 UUID v5(namespace, entity_type|normalized_name)，
-- 所以正常路徑寫的是同一列 id、走 upsert；這個 unique index 是第二道保險。
DROP INDEX idx_entities_normalized_name;

CREATE UNIQUE INDEX idx_entities_natural_key
    ON entities (entity_type, normalized_name);

-- SPEC §11 的 Relationship 是一級物件，(source, type, target) 就是它的身分。
-- 同一份 Document 提到同一個 Entity 兩次不該產生兩條 mentions——那會讓
-- evidence_count 失去意義（數的是「有幾條邊」而不是「有幾筆證據」）。
CREATE UNIQUE INDEX idx_relationships_natural_key
    ON relationships (source_object_id, relationship_type, target_object_id);

-- entity-worker idempotency：同一份 Document 只能有一列 action='entity_extracted'。
-- 語意與 0003 的 idx_provenance_normalized_raw、0004 的 idx_provenance_dedup_subject 相同，
-- 只是主體換成「被抽取的 Document」。
--
-- ⚠️ 這個 `entity_extracted` 字串同時寫在
-- crates/entity-worker/src/service.rs 的 ACTION_ENTITY_EXTRACTED 與
-- crates/osint-cli/src/commands/documents.rs 的 ENTITY_EXTRACTED_ACTION。
-- 三處必須一致；只改一處不會報錯，只會讓唯一性保證靜默失效。
--
-- 0004 的 idx_provenance_dedup_subject 沒有把 action 寫進鍵、只寫在 WHERE 裡，
-- 所以它與這一個是**兩個互不衝突的部分索引**：同一個 subject_id 可以同時有
-- deduplicated 與 entity_extracted 各一列。
CREATE UNIQUE INDEX idx_provenance_entity_subject
    ON provenance (subject_id)
    WHERE action = 'entity_extracted';

-- ---------------------------------------------------------------------------
-- 4. RelationshipEvidence 反查
-- ---------------------------------------------------------------------------
-- SPEC §12：任何 relationship 必須能回查 evidence。反查一定是
-- 「給我這條 relationship 的所有 evidence」，所以索引建在 relationship_id 上。
-- created_at 一起放進索引，讓「依時間升序」不必額外排序。
CREATE INDEX idx_relationship_evidence_relationship
    ON relationship_evidence (relationship_id, created_at);
