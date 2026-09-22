CREATE TABLE IF NOT EXISTS integration.sync_diff_summary (
    "unitId"               UUID NOT NULL REFERENCES integration.sync_units(id) ON DELETE CASCADE,
    day                    DATE NOT NULL,
    "integrationId"        UUID NOT NULL,
    "channelName"          TEXT NOT NULL,
    "missingInNangoCount"  INT NOT NULL DEFAULT 0,
    "missingInShadowCount" INT NOT NULL DEFAULT 0,
    "fieldMismatchCount"   INT NOT NULL DEFAULT 0,
    "unsupportedSyncCount" INT NOT NULL DEFAULT 0,
    "highSeverityCount"    INT NOT NULL DEFAULT 0,
    "hasMismatch"          BOOLEAN NOT NULL DEFAULT false,
    "lastCheckedAt"        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("unitId", day)
);

CREATE INDEX IF NOT EXISTS ix_sync_diff_summary_day_hasMismatch
    ON integration.sync_diff_summary (day, "hasMismatch");
