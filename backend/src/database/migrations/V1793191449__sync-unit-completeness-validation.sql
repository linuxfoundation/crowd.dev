CREATE TABLE IF NOT EXISTS integration.sync_unit_validations (
  "unitId"    UUID PRIMARY KEY REFERENCES integration.sync_units(id) ON DELETE CASCADE,
  "createdAt" TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS integration.sync_unit_validation_counters (
  "unitId"     UUID NOT NULL REFERENCES integration.sync_unit_validations("unitId") ON DELETE CASCADE,
  "counterKey" TEXT NOT NULL CHECK ("counterKey" <> ''),
  PRIMARY KEY ("unitId", "counterKey")
);

CREATE TABLE IF NOT EXISTS integration.sync_validation_results (
  "unitId"        UUID NOT NULL REFERENCES integration.sync_units(id) ON DELETE CASCADE,
  "counterKey"    TEXT NOT NULL CHECK ("counterKey" <> ''),
  "windowStart"   TIMESTAMPTZ NOT NULL,
  "windowEnd"     TIMESTAMPTZ NOT NULL,
  "sourceCount"   BIGINT NOT NULL,
  "tinybirdCount" BIGINT NOT NULL,
  gap             BIGINT GENERATED ALWAYS AS ("sourceCount" - "tinybirdCount") STORED,
  "checkedAt"     TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY ("unitId", "counterKey", "windowStart", "windowEnd"),
  CHECK ("windowStart" < "windowEnd")
);
