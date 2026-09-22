ALTER TABLE stewardship_activity
  ADD COLUMN IF NOT EXISTS actor_username     TEXT,
  ADD COLUMN IF NOT EXISTS actor_display_name TEXT,
  ADD COLUMN IF NOT EXISTS actor_avatar_url   TEXT;
