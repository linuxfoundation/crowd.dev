ALTER TABLE advisory_affected_ranges ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS advisory_affected_ranges_live_idx
   ON advisory_affected_ranges (advisory_package_id)
   WHERE deleted_at IS NULL;
