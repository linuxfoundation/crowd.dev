ALTER TABLE integration.sync_units
  ADD COLUMN IF NOT EXISTS "validationEnabled" BOOLEAN NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS "validationCounterKeys" TEXT[];

CREATE INDEX IF NOT EXISTS ix_sync_units_validation_enabled
  ON integration.sync_units (id) WHERE "validationEnabled";

CREATE TABLE IF NOT EXISTS integration.sync_validation_results (
  "unitId"        UUID NOT NULL REFERENCES integration.sync_units(id) ON DELETE CASCADE,
  "counterKey"    TEXT NOT NULL,
  "activityTypes" TEXT[] NOT NULL,
  "windowStart"   TIMESTAMPTZ NOT NULL,
  "windowEnd"     TIMESTAMPTZ NOT NULL,
  "sourceCount"   BIGINT NOT NULL,
  "tinybirdCount" BIGINT NOT NULL,
  gap             BIGINT GENERATED ALWAYS AS ("sourceCount" - "tinybirdCount") STORED,
  "checkedAt"     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY ("unitId", "counterKey", "windowStart", "windowEnd")
);
