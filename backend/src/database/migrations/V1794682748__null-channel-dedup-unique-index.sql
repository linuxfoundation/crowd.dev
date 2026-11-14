-- Must exist before createOrUpdateRelations upserts with ON CONFLICT ... WHERE channel IS NULL.
-- A leftover NULL-channel duplicate leaves it INVALID, which IF NOT EXISTS skips: check indisvalid.
create unique index concurrently if not exists ix_unique_activity_relations_dedup_key_null_channel
  on "activityRelations" ("timestamp", platform, type, "sourceId", "segmentId")
  where channel is null;
