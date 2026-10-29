ALTER TABLE integration.sync_unit_validations
  ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT true;
