CREATE TABLE IF NOT EXISTS security_contacts (
    id              BIGSERIAL PRIMARY KEY,
    repo_id         BIGINT NOT NULL REFERENCES repos(id) ON DELETE CASCADE,
    channel         TEXT NOT NULL,
    value           TEXT NOT NULL,
    role            TEXT NOT NULL,
    name            TEXT,
    score           NUMERIC(4,3) NOT NULL,
    confidence      TEXT NOT NULL,
    provenance      JSONB NOT NULL DEFAULT '[]',
    last_refreshed  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS security_contacts_repo_channel_value_uq
    ON security_contacts (repo_id, channel, value);

CREATE INDEX IF NOT EXISTS security_contacts_repo_confidence_idx
    ON security_contacts (repo_id, confidence);

ALTER TABLE repos ADD COLUMN IF NOT EXISTS pvr_enabled                 BOOL;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS security_policy_url         TEXT;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS vulnerability_reporting_url TEXT;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS bug_bounty_url              TEXT;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS security_txt_url            TEXT;
ALTER TABLE repos ADD COLUMN IF NOT EXISTS contacts_last_refreshed     TIMESTAMPTZ;
