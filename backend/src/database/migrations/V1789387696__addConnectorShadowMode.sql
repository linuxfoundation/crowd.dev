ALTER TABLE integration.sync_units
  ADD COLUMN IF NOT EXISTS "emitEnabled" BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS integration.sync_shadow_records (
    "unitId"      UUID NOT NULL REFERENCES integration.sync_units(id) ON DELETE CASCADE,
    type          TEXT NOT NULL,
    "sourceId"    TEXT NOT NULL,
    "occurredAt"  TIMESTAMPTZ NOT NULL,
    data          JSONB NOT NULL,
    "firstSeenAt" TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("unitId", type, "sourceId")
);
