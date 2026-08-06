-- Store the stewardship status as it was at the moment each activity was logged.
-- Existing rows get NULL (no history to reconstruct); queries fall back to s.status for them.
ALTER TABLE stewardship_activity
  ADD COLUMN IF NOT EXISTS status_at_time TEXT;
