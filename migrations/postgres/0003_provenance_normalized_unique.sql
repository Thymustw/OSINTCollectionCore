-- normalizer idempotency：同一筆 RawEvidence 只能寫一列 action='normalized' 的 provenance。
-- 多筆 Document 的個別溯源另用 action='derived_from'（不在此 unique 範圍）。

CREATE UNIQUE INDEX idx_provenance_normalized_raw
    ON provenance (raw_evidence_id)
    WHERE action = 'normalized' AND raw_evidence_id IS NOT NULL;
