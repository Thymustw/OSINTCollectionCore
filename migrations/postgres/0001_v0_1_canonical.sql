-- PostgreSQL canonical schema（V0.1）
-- 對齊 docs/specs/SPEC_V0.1.md 與 crates/core-model。
-- UUID 由應用層產生 v7；JSONB 用於規格裡結構未定的巢狀欄位。
-- RawEvidence 沒有 updated_at：寫入後不可變。

CREATE TABLE sources (
    id              UUID PRIMARY KEY,
    name            TEXT NOT NULL,
    source_type     TEXT NOT NULL,
    platform        TEXT,
    base_url        TEXT,
    description     TEXT,
    language        TEXT,
    country         TEXT,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    collection_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL,
    last_seen       TIMESTAMPTZ
);

CREATE TABLE connectors (
    id                      UUID PRIMARY KEY,
    source_id               UUID NOT NULL REFERENCES sources (id),
    name                    TEXT NOT NULL,
    type                    TEXT NOT NULL,
    version                 TEXT NOT NULL,
    enabled                 BOOLEAN NOT NULL DEFAULT TRUE,
    configuration           JSONB NOT NULL DEFAULT '{}'::jsonb,
    credential_reference    TEXT,
    schedule                TEXT,
    rate_limit              JSONB NOT NULL DEFAULT '{}'::jsonb,
    timeout                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    proxy_reference         TEXT,
    checkpoint              JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_run                TIMESTAMPTZ,
    last_success            TIMESTAMPTZ,
    status                  TEXT NOT NULL,
    error_count             INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE collections (
    id              UUID PRIMARY KEY,
    workspace_id    UUID,
    name            TEXT NOT NULL,
    description     TEXT,
    status          TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE collection_sources (
    collection_id   UUID NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    source_id       UUID NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, source_id)
);

CREATE TABLE collection_connectors (
    collection_id   UUID NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    connector_id    UUID NOT NULL REFERENCES connectors (id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, connector_id)
);

CREATE TABLE collection_objects (
    collection_id   UUID NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    object_id       UUID NOT NULL,
    PRIMARY KEY (collection_id, object_id)
);

CREATE TABLE raw_evidence (
    id                  UUID PRIMARY KEY,
    source_id           UUID NOT NULL REFERENCES sources (id),
    connector_id        UUID NOT NULL REFERENCES connectors (id),
    collection_id       UUID REFERENCES collections (id),
    external_id         TEXT,
    source_url          TEXT NOT NULL,
    retrieved_at        TIMESTAMPTZ NOT NULL,
    content_type        TEXT,
    mime_type           TEXT,
    content_length      BIGINT,
    sha256              TEXT NOT NULL,
    storage_path        TEXT NOT NULL,
    http_status         INTEGER,
    http_headers        JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb,
    collector_version   TEXT NOT NULL
);

CREATE TABLE documents (
    id                          UUID PRIMARY KEY,
    object_type                 TEXT NOT NULL,
    schema_version              TEXT NOT NULL,
    title                       TEXT,
    body                        TEXT,
    summary                     TEXT,
    language                    TEXT,
    author                      TEXT,
    published_at                TIMESTAMPTZ,
    modified_at                 TIMESTAMPTZ,
    observed_at                 TIMESTAMPTZ NOT NULL,
    collected_at                TIMESTAMPTZ NOT NULL,
    source_url                  TEXT,
    canonical_url               TEXT,
    normalized_content_hash     TEXT,
    confidence                  DOUBLE PRECISION NOT NULL,
    labels                      JSONB NOT NULL DEFAULT '[]'::jsonb,
    attributes                  JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE entities (
    id                  UUID PRIMARY KEY,
    entity_type         TEXT NOT NULL,
    name                TEXT NOT NULL,
    normalized_name     TEXT NOT NULL,
    description         TEXT,
    confidence          DOUBLE PRECISION NOT NULL,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,
    attributes          JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE relationships (
    id                  UUID PRIMARY KEY,
    source_object_id    UUID NOT NULL,
    relationship_type   TEXT NOT NULL,
    target_object_id    UUID NOT NULL,
    confidence          DOUBLE PRECISION NOT NULL,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,
    evidence_count      INTEGER NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE events (
    id              UUID PRIMARY KEY,
    event_type      TEXT NOT NULL,
    title           TEXT NOT NULL,
    description     TEXT,
    start_time      TIMESTAMPTZ,
    end_time        TIMESTAMPTZ,
    confidence      DOUBLE PRECISION NOT NULL,
    status          TEXT NOT NULL,
    attributes      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE TABLE provenance (
    id                  UUID PRIMARY KEY,
    subject_id          UUID NOT NULL,
    action              TEXT NOT NULL,
    parent_id           UUID,
    raw_evidence_id     UUID REFERENCES raw_evidence (id),
    processor           TEXT NOT NULL,
    processor_version   TEXT NOT NULL,
    timestamp           TIMESTAMPTZ NOT NULL,
    metadata            JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE jobs (
    id                  UUID PRIMARY KEY,
    type                TEXT NOT NULL,
    status              TEXT NOT NULL,
    correlation_id      UUID,
    created_at          TIMESTAMPTZ NOT NULL,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    error               TEXT
);

CREATE TABLE duplicate_groups (
    id                          UUID PRIMARY KEY,
    canonical_object_id         UUID NOT NULL,
    member_object_id            UUID,
    member_raw_evidence_id      UUID REFERENCES raw_evidence (id),
    method                      TEXT NOT NULL,
    similarity                  DOUBLE PRECISION NOT NULL,
    first_seen                  TIMESTAMPTZ NOT NULL,
    CONSTRAINT duplicate_groups_member_present CHECK (
        member_object_id IS NOT NULL OR member_raw_evidence_id IS NOT NULL
    )
);

CREATE TABLE relationship_evidence (
    id                  UUID PRIMARY KEY,
    relationship_id     UUID NOT NULL REFERENCES relationships (id) ON DELETE CASCADE,
    object_id           UUID NOT NULL,
    raw_evidence_id     UUID REFERENCES raw_evidence (id),
    excerpt             TEXT,
    confidence          DOUBLE PRECISION NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE entity_extractions (
    id                  UUID PRIMARY KEY,
    object_id           UUID NOT NULL,
    entity_id           UUID NOT NULL REFERENCES entities (id),
    extractor           TEXT NOT NULL,
    extractor_version   TEXT NOT NULL,
    confidence          DOUBLE PRECISION NOT NULL,
    text_offset         INTEGER,
    excerpt             TEXT
);

CREATE INDEX idx_connectors_source_id ON connectors (source_id);
CREATE INDEX idx_raw_evidence_source_id ON raw_evidence (source_id);
CREATE INDEX idx_raw_evidence_sha256 ON raw_evidence (sha256);
CREATE INDEX idx_raw_evidence_external_id ON raw_evidence (external_id);
CREATE INDEX idx_documents_canonical_url ON documents (canonical_url);
CREATE INDEX idx_documents_normalized_content_hash ON documents (normalized_content_hash);
CREATE INDEX idx_entities_normalized_name ON entities (entity_type, normalized_name);
CREATE INDEX idx_relationships_source ON relationships (source_object_id);
CREATE INDEX idx_relationships_target ON relationships (target_object_id);
CREATE INDEX idx_jobs_status ON jobs (status);
CREATE INDEX idx_provenance_subject ON provenance (subject_id);
CREATE INDEX idx_duplicate_groups_canonical ON duplicate_groups (canonical_object_id);
CREATE INDEX idx_entity_extractions_object ON entity_extractions (object_id);
CREATE INDEX idx_entity_extractions_entity ON entity_extractions (entity_id);
