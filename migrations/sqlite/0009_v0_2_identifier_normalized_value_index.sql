-- V0.2：`entity_identifiers.normalized_value` 的單欄索引（SQLite 對應）。
--
-- 語意對齊 migrations/postgres/0009_v0_2_identifier_normalized_value_index.sql。
-- 為什麼要有這條索引，完整理由寫在 PostgreSQL 那一份。

CREATE INDEX idx_entity_identifiers_normalized_value
    ON entity_identifiers (normalized_value, id);
