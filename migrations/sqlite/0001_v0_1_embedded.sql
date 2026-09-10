-- SQLite embedded / App / projection schema（V0.1）
-- 語意對齊 PostgreSQL canonical，但排除 PG-specific 特性：
-- JSONB → TEXT、TIMESTAMPTZ → TEXT（RFC3339）、BOOLEAN → INTEGER 0/1。
-- SQLite 不是 Core 高併發 canonical store。

CREATE TABLE sources (
    id                  TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    source_type         TEXT NOT NULL,
    platform            TEXT,
    base_url            TEXT,
    description         TEXT,
    language            TEXT,
    country             TEXT,
    enabled             INTEGER NOT NULL DEFAULT 1,
    collection_policy   TEXT NOT NULL DEFAULT '{}',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    last_seen           TEXT
);

CREATE TABLE connectors (
    id                      TEXT PRIMARY KEY,
    source_id               TEXT NOT NULL REFERENCES sources (id),
    name                    TEXT NOT NULL,
    type                    TEXT NOT NULL,
    version                 TEXT NOT NULL,
    enabled                 INTEGER NOT NULL DEFAULT 1,
    configuration           TEXT NOT NULL DEFAULT '{}',
    credential_reference    TEXT,
    schedule                TEXT,
    rate_limit              TEXT NOT NULL DEFAULT '{}',
    timeout                 TEXT NOT NULL DEFAULT '{}',
    proxy_reference         TEXT,
    checkpoint              TEXT NOT NULL DEFAULT '{}',
    last_run                TEXT,
    last_success            TEXT,
    status                  TEXT NOT NULL,
    error_count             INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE collections (
    id              TEXT PRIMARY KEY,
    workspace_id    TEXT,
    name            TEXT NOT NULL,
    description     TEXT,
    status          TEXT NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE collection_sources (
    collection_id   TEXT NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    source_id       TEXT NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, source_id)
);

CREATE TABLE collection_connectors (
    collection_id   TEXT NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    connector_id    TEXT NOT NULL REFERENCES connectors (id) ON DELETE CASCADE,
    PRIMARY KEY (collection_id, connector_id)
);

CREATE TABLE collection_objects (
    collection_id   TEXT NOT NULL REFERENCES collections (id) ON DELETE CASCADE,
    object_id       TEXT NOT NULL,
    PRIMARY KEY (collection_id, object_id)
);

CREATE TABLE raw_evidence (
    id                  TEXT PRIMARY KEY,
    source_id           TEXT NOT NULL REFERENCES sources (id),
    connector_id        TEXT NOT NULL REFERENCES connectors (id),
    collection_id       TEXT REFERENCES collections (id),
    external_id         TEXT,
    source_url          TEXT NOT NULL,
    retrieved_at        TEXT NOT NULL,
    content_type        TEXT,
    mime_type           TEXT,
    content_length      INTEGER,
    sha256              TEXT NOT NULL,
    storage_path        TEXT NOT NULL,
    http_status         INTEGER,
    http_headers        TEXT NOT NULL DEFAULT '{}',
    metadata            TEXT NOT NULL DEFAULT '{}',
    collector_version   TEXT NOT NULL
);

CREATE TABLE documents (
    id                          TEXT PRIMARY KEY,
    object_type                 TEXT NOT NULL,
    schema_version              TEXT NOT NULL,
    title                       TEXT,
    body                        TEXT,
    summary                     TEXT,
    language                    TEXT,
    author                      TEXT,
    published_at                TEXT,
    modified_at                 TEXT,
    observed_at                 TEXT NOT NULL,
    collected_at                TEXT NOT NULL,
    source_url                  TEXT,
    canonical_url               TEXT,
    normalized_content_hash     TEXT,
    confidence                  REAL NOT NULL,
    labels                      TEXT NOT NULL DEFAULT '[]',
    attributes                  TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE entities (
    id                  TEXT PRIMARY KEY,
    entity_type         TEXT NOT NULL,
    name                TEXT NOT NULL,
    normalized_name     TEXT NOT NULL,
    description         TEXT,
    confidence          REAL NOT NULL,
    first_seen          TEXT NOT NULL,
    last_seen           TEXT NOT NULL,
    attributes          TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE relationships (
    id                  TEXT PRIMARY KEY,
    source_object_id    TEXT NOT NULL,
    relationship_type   TEXT NOT NULL,
    target_object_id    TEXT NOT NULL,
    confidence          REAL NOT NULL,
    first_seen          TEXT NOT NULL,
    last_seen           TEXT NOT NULL,
    evidence_count      INTEGER NOT NULL DEFAULT 0,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE TABLE events (
    id              TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    title           TEXT NOT NULL,
    description     TEXT,
    start_time      TEXT,
    end_time        TEXT,
    confidence      REAL NOT NULL,
    status          TEXT NOT NULL,
    attributes      TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);

CREATE TABLE provenance (
    id                  TEXT PRIMARY KEY,
    subject_id          TEXT NOT NULL,
    action              TEXT NOT NULL,
    parent_id           TEXT,
    raw_evidence_id     TEXT REFERENCES raw_evidence (id),
    processor           TEXT NOT NULL,
    processor_version   TEXT NOT NULL,
    timestamp           TEXT NOT NULL,
    metadata            TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE jobs (
    id                  TEXT PRIMARY KEY,
    type                TEXT NOT NULL,
    status              TEXT NOT NULL,
    correlation_id      TEXT,
    created_at          TEXT NOT NULL,
    started_at          TEXT,
    completed_at        TEXT,
    retry_count         INTEGER NOT NULL DEFAULT 0,
    error               TEXT
);

CREATE TABLE duplicate_groups (
    id                          TEXT PRIMARY KEY,
    canonical_object_id         TEXT NOT NULL,
    member_object_id            TEXT,
    member_raw_evidence_id      TEXT REFERENCES raw_evidence (id),
    method                      TEXT NOT NULL,
    similarity                  REAL NOT NULL,
    first_seen                  TEXT NOT NULL,
    CONSTRAINT duplicate_groups_member_present CHECK (
        member_object_id IS NOT NULL OR member_raw_evidence_id IS NOT NULL
    )
);

CREATE TABLE relationship_evidence (
    id                  TEXT PRIMARY KEY,
    relationship_id     TEXT NOT NULL REFERENCES relationships (id) ON DELETE CASCADE,
    object_id           TEXT NOT NULL,
    raw_evidence_id     TEXT REFERENCES raw_evidence (id),
    excerpt             TEXT,
    confidence          REAL NOT NULL,
    created_at          TEXT NOT NULL
);

CREATE TABLE entity_extractions (
    id                  TEXT PRIMARY KEY,
    object_id           TEXT NOT NULL,
    entity_id           TEXT NOT NULL REFERENCES entities (id),
    extractor           TEXT NOT NULL,
    extractor_version   TEXT NOT NULL,
    confidence          REAL NOT NULL,
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
