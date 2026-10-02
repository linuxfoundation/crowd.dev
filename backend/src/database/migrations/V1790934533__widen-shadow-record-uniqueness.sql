ALTER TABLE integration.sync_shadow_records
  DROP CONSTRAINT sync_shadow_records_pkey,
  ADD CONSTRAINT sync_shadow_records_identity_key
    UNIQUE ("unitId", type, "sourceId", "segmentId", "integrationId");
