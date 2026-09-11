-- Per-Source SSRF 白名單（語意對齊 postgres/0002）。

CREATE TABLE source_network_rules (
    id              TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    cidr_or_host    TEXT NOT NULL,
    ports           TEXT,
    reason          TEXT NOT NULL,
    approved_by     TEXT NOT NULL,
    expires_at      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE INDEX source_network_rules_source_id_idx
    ON source_network_rules (source_id);
