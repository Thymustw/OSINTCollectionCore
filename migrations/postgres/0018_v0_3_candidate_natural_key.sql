-- V0.3：Candidate 自然鍵唯一索引（Graph Expansion 重跑產生重複候選的修正，2026-10-15）。
--
-- 自然鍵 = (collection_id, candidate_type, normalized_value)：
-- 「同一個調查集合裡、同一類、正規化後同值」就是同一個候選。
--
-- 為什麼包含 collection_id：同一個實體在兩個集合被發現，各自需要一筆候選——
-- 核准／拒絕是 per-collection 的判斷（A 調查要、B 調查不要是合理的）。
-- 為什麼不包含 discovery_method：graph_expansion 與 source_expansion 對同一值
-- 的發現是同一個候選的兩條證據，不是兩個候選（candidate_type 已經把 Entity／
-- Source 分開了）。
--
-- Partial index（WHERE collection_id IS NOT NULL）：沒有集合的候選不查重。
-- PostgreSQL 的 UNIQUE 本來就把 NULL 視為互不相等，partial 只是把意圖寫明。
--
-- 建索引前先合併既有重複（開發 DB 已清空，但這份 migration 必須在有重複時也能跑）：
-- 1. 每組自然鍵保留一筆 survivor：優先保留已被審過的（非 pending），
--    其次最舊的（created_at，再以 id 決勝）——人工判斷過的結論不能被合併丟掉。
-- 2. 其他列的 candidate_evidence 改指 survivor（證據一筆都不丟）。
-- 3. 刪掉非 survivor 的候選列。

CREATE TEMP TABLE candidate_dedup_map ON COMMIT DROP AS
SELECT id AS loser_id, survivor_id
FROM (
    SELECT
        id,
        first_value(id) OVER (
            PARTITION BY collection_id, candidate_type, normalized_value
            ORDER BY (status = 'pending') ASC, created_at ASC, id ASC
        ) AS survivor_id
    FROM candidates
    WHERE collection_id IS NOT NULL
) ranked
WHERE id <> survivor_id;

UPDATE candidate_evidence AS ce
SET candidate_id = m.survivor_id
FROM candidate_dedup_map AS m
WHERE ce.candidate_id = m.loser_id;

DELETE FROM candidates AS c
USING candidate_dedup_map AS m
WHERE c.id = m.loser_id;

CREATE UNIQUE INDEX uq_candidates_natural_key
    ON candidates (collection_id, candidate_type, normalized_value)
    WHERE collection_id IS NOT NULL;
