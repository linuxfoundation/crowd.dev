ALTER TABLE stewardships
    ADD COLUMN IF NOT EXISTS resolution_path TEXT,
    ADD COLUMN IF NOT EXISTS status_note     TEXT;
