-- Content-keyed parse cache for declared security policies (files + linked pages),
-- and the assembled per-repo reporting protocol. See ADR-0010 addendum.
CREATE TABLE IF NOT EXISTS security_policy_parses (
    blob_oid       TEXT PRIMARY KEY, -- git blob oid for files; sha256 of extracted text for linked pages
    source_kind    TEXT NOT NULL,    -- 'security-file' | 'linked-page'
    url            TEXT,             -- linked-page rows only: the fetched URL (join key from linked_urls)
    parser         TEXT NOT NULL,    -- 'deterministic' | 'llm'
    parser_version INT  NOT NULL,
    status         TEXT NOT NULL,    -- 'ok' | 'template' | 'degraded'
    parsed         JSONB NOT NULL DEFAULT '{}',
    linked_urls    TEXT[] NOT NULL DEFAULT '{}',
    parsed_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS security_policy_parses_linked_page_url_idx
    ON security_policy_parses (url)
    WHERE source_kind = 'linked-page';

CREATE TABLE IF NOT EXISTS repo_reporting_protocols (
    repo_id      BIGINT PRIMARY KEY REFERENCES repos(id) ON DELETE CASCADE,
    declared     BOOLEAN NOT NULL,
    methods      JSONB NOT NULL DEFAULT '[]',
    guidelines   JSONB,
    sources      JSONB NOT NULL DEFAULT '[]',
    assembled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
