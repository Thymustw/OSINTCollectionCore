-- V0.3：Candidate 自然鍵唯一索引（SQLite 版，語意同 migrations/postgres/0018）。
--
-- 自然鍵 = (collection_id, candidate_type, normalized_value)，只對有集合的候選。
-- 為什麼這樣選鍵、survivor 怎麼挑，見 Postgres 版檔頭。
--
-- 建索引前先合併既有重複：survivor 優先保留已審過的（非 pending），其次最舊的
-- （created_at 是 RFC3339 毫秒字串，字典序 = 時間序；再以 id 決勝）。
-- 用相關子查詢而不是 UPDATE ... FROM／window function，避免綁定 SQLite 最低版本。

CREATE TEMP TABLE candidate_dedup_map AS
SELECT c.id AS loser_id,
       (
           SELECT s.id FROM candidates AS s
           WHERE s.collection_id = c.collection_id
             AND s.candidate_type = c.candidate_type
             AND s.normalized_value = c.normalized_value
           ORDER BY (s.status = 'pending') ASC, s.created_at ASC, s.id ASC
           LIMIT 1
       ) AS survivor_id
FROM candidates AS c
WHERE c.collection_id IS NOT NULL;

DELETE FROM candidate_dedup_map WHERE loser_id = survivor_id;

UPDATE candidate_evidence
SET candidate_id = (
    SELECT m.survivor_id FROM candidate_dedup_map AS m
    WHERE m.loser_id = candidate_evidence.candidate_id
)
WHERE candidate_id IN (SELECT loser_id FROM candidate_dedup_map);

DELETE FROM candidates WHERE id IN (SELECT loser_id FROM candidate_dedup_map);

DROP TABLE candidate_dedup_map;

CREATE UNIQUE INDEX uq_candidates_natural_key
    ON candidates (collection_id, candidate_type, normalized_value)
    WHERE collection_id IS NOT NULL;
