CREATE INDEX IF NOT EXISTS ix_sync_shadow_records_unit_occurredAt
  ON integration.sync_shadow_records ("unitId", "occurredAt");

CREATE INDEX IF NOT EXISTS ix_nango_mapping_owner_repoName
  ON integration.nango_mapping (owner, "repoName");
