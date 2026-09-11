-- Per-Source SSRF 白名單（CONNECTOR_SECURITY §3a／ADR-001）。
-- hard-deny 驗證在應用層；資料表不嘗試用 CHECK 重做 IP 分類。

CREATE TABLE source_network_rules (
    id              UUID PRIMARY KEY,
    source_id       UUID NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    cidr_or_host    TEXT NOT NULL,
    ports           JSONB,
    reason          TEXT NOT NULL,
    approved_by     TEXT NOT NULL,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX source_network_rules_source_id_idx
    ON source_network_rules (source_id);
