UPDATE integration.sync_shadow_records ssr
SET "integrationId" = su."integrationId",
    "segmentId" = i."segmentId"
FROM integration.sync_units su
JOIN public.integrations i ON i.id = su."integrationId"
WHERE ssr."unitId" = su.id
  AND (ssr."integrationId" IS NULL OR ssr."segmentId" IS NULL)
  AND i."segmentId" IS NOT NULL;

-- Safe to drop: this table is a rebuildable diff cache (see diffUnit()), not a system of record.
DELETE FROM integration.sync_shadow_records
WHERE "integrationId" IS NULL OR "segmentId" IS NULL;

ALTER TABLE integration.sync_shadow_records
  ALTER COLUMN "integrationId" SET NOT NULL,
  ALTER COLUMN "segmentId" SET NOT NULL;
