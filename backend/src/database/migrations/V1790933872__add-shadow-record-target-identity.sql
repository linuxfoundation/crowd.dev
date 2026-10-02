ALTER TABLE integration.sync_shadow_records
  ADD COLUMN IF NOT EXISTS "segmentId" UUID,
  ADD COLUMN IF NOT EXISTS "integrationId" UUID;
