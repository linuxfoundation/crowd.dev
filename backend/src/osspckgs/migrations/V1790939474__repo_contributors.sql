CREATE TABLE IF NOT EXISTS repo_contributors (
    id              BIGSERIAL PRIMARY KEY,
    repo_id         BIGINT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    source          TEXT NOT NULL,
    role            TEXT NOT NULL,
    role_kind       TEXT NOT NULL,
    identity_type   TEXT NOT NULL,
    identity_value  TEXT NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS repo_contributors_repo_source_identity_role_uq
    ON repo_contributors (repo_id, source, identity_type, identity_value, role);

CREATE INDEX IF NOT EXISTS repo_contributors_identity_idx
    ON repo_contributors (identity_type, identity_value);
