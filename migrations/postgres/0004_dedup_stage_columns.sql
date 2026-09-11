-- V0.1 Phase 4a：deduplicator（SPEC §15 五階段 + §16 duplicate group）。
--
-- SPEC §15 只描述判斷方式，沒有說判斷依據存在哪。這裡把 Stage 1／4 的鍵與 Stage 1~5 的
-- 判定結果放在 documents 上，理由見 core-model/src/document.rs 的註解。
-- 三個欄位都可為 NULL：既有資料不需要回填，deduplicator 處理到誰才寫誰。

ALTER TABLE documents ADD COLUMN external_key TEXT;
ALTER TABLE documents ADD COLUMN simhash BIGINT;
ALTER TABLE documents ADD COLUMN duplicate_of UUID REFERENCES documents (id);

-- Stage 1 候選查詢。
CREATE INDEX idx_documents_external_key ON documents (external_key)
    WHERE external_key IS NOT NULL;

-- 「這份 canonical 底下有哪些 duplicate」。
CREATE INDEX idx_documents_duplicate_of ON documents (duplicate_of)
    WHERE duplicate_of IS NOT NULL;

-- Stage 4 掃描的是「最近 N 筆有 fingerprint 的 Document」（見 docs/developer/deduplicator.md），
-- 所以索引建在 id DESC 上，不是建在 simhash 值上——SimHash 比對的是 Hamming 距離，
-- 對 fingerprint 本身建 B-tree 沒有任何幫助。
CREATE INDEX idx_documents_simhash_recent ON documents (id DESC)
    WHERE simhash IS NOT NULL;

-- SPEC §16：一份 Document 最多屬於一個 duplicate group。
-- deduplicator 另外把 group id 算成 UUID v5（namespace + member document id），
-- 所以重複消費同一個事件會 upsert 同一列；這個 unique index 是第二道保險，
-- 擋掉任何繞過 v5 規則、想幫同一個 member 建第二個 group 的寫入。
CREATE UNIQUE INDEX idx_duplicate_groups_member_object
    ON duplicate_groups (member_object_id)
    WHERE member_object_id IS NOT NULL;

-- deduplicator idempotency：同一份 Document 只能有一列 action='deduplicated' 的 provenance。
-- 語意與 0003 的 idx_provenance_normalized_raw 相同，只是主體從 RawEvidence 換成 Document。
CREATE UNIQUE INDEX idx_provenance_dedup_subject
    ON provenance (subject_id)
    WHERE action = 'deduplicated';
