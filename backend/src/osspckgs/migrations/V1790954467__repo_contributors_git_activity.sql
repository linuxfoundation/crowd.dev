ALTER TABLE repo_contributors
    ADD COLUMN IF NOT EXISTS cdp_member_id UUID,
    ADD COLUMN IF NOT EXISTS commit_count INTEGER;

CREATE TABLE IF NOT EXISTS repo_contributors_sync_state (
    source      TEXT PRIMARY KEY,
    watermark   TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
