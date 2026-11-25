CREATE INDEX IF NOT EXISTS ix_sync_units_running
  ON integration.sync_units (platform)
  WHERE "lockedAt" IS NOT NULL;
