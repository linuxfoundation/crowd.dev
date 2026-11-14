-- One row per dedup key when the channel is NULL. ix_unique_activity_relations_dedup_key treats
-- NULL channels as distinct, so it never stopped a second NULL-channel copy.
-- The index must exist before data-sink-worker's NULL-channel upsert (ON CONFLICT ... WHERE channel
-- IS NULL), which fails without it. It was built on production on 2026-11-14, after the duplicates
-- were removed (linuxfoundation/lfx-cdp-insights-ops#852).
-- Building it fails if any NULL-channel key still has two rows and leaves an INVALID index behind,
-- which IF NOT EXISTS would then skip: check pg_index.indisvalid after it runs.
create unique index concurrently if not exists ix_unique_activity_relations_dedup_key_null_channel
  on "activityRelations" ("timestamp", platform, type, "sourceId", "segmentId")
  where channel is null;
