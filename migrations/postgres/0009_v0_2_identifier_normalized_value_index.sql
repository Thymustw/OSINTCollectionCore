-- V0.2：`entity_identifiers.normalized_value` 的單欄索引。
--
-- `check_account_handle` 要找「這個 handle 不管掛在哪個 namespace 下」
-- （`github_handle` 與 `twitter_handle` 的 UNIQUE 不會撞，因為 namespace 不同）。
-- 既有 `idx_entity_identifiers_natural (namespace, normalized_value)` 的
-- 前導欄是 namespace，只查 normalized_value 走不到那條索引。
--
-- `(normalized_value, id)` 把排序欄也蓋進去，讓
-- `ORDER BY id ASC LIMIT n` 不必再回表排序。

CREATE INDEX idx_entity_identifiers_normalized_value
    ON entity_identifiers (normalized_value, id);
