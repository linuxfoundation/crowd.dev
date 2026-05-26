ALTER TABLE repos
    ALTER COLUMN created_at SET DEFAULT NOW();

ALTER TABLE repos
    ALTER COLUMN last_synced_at DROP NOT NULL,
    ALTER COLUMN last_synced_at DROP DEFAULT;
