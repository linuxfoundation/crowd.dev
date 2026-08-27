CREATE TABLE IF NOT EXISTS integration.sync_units (
    id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    "integrationId"       UUID NOT NULL REFERENCES public.integrations(id),
    platform              TEXT NOT NULL,
    "channelId"           TEXT NOT NULL,
    "channelName"         TEXT NOT NULL,
    "syncName"            TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active','paused','dead_letter','decommissioned')),
    "nextRunAt"           TIMESTAMPTZ NOT NULL DEFAULT now(),
    "lockedAt"            TIMESTAMPTZ,
    "lastRunAt"           TIMESTAMPTZ,
    "lastSuccessAt"       TIMESTAMPTZ,
    "consecutiveFailures" INT NOT NULL DEFAULT 0,
    "lastErrorClass"      TEXT,
    watermark             JSONB,
    "emittedCount"        INT,
    "createdAt"           TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE ("integrationId", "channelId", "syncName")
);

CREATE INDEX IF NOT EXISTS "ix_sync_units_due"
    ON integration.sync_units ("nextRunAt")
    WHERE status = 'active';
