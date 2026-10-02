-- Backfill segmentId/integrationId on legacy shadow rows (predating these columns) from their
-- owning sync unit's integration, the same values emit() would have computed at write time.
UPDATE integration.sync_shadow_records ssr
SET "integrationId" = su."integrationId",
    "segmentId" = i."segmentId"
FROM integration.sync_units su
JOIN public.integrations i ON i.id = su."integrationId"
WHERE ssr."unitId" = su.id
  AND (ssr."integrationId" IS NULL OR ssr."segmentId" IS NULL)
  AND i."segmentId" IS NOT NULL;

-- Any row that still can't be backfilled (its integration has no segment) is a stale diff-cache
-- entry with no resolvable identity; safe to drop, since diffUnit() only uses this table to
-- re-derive comparisons against the live source each run, never as a system of record.
DELETE FROM integration.sync_shadow_records
WHERE "integrationId" IS NULL OR "segmentId" IS NULL;

ALTER TABLE integration.sync_shadow_records
  ALTER COLUMN "integrationId" SET NOT NULL,
  ALTER COLUMN "segmentId" SET NOT NULL;
