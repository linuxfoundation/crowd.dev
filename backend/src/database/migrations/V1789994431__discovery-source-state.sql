-- One row per discovery source; forward-only watermark, enforced in the DAL upsert (GREATEST).
CREATE TABLE public."discoverySourceState" (
    "source"    VARCHAR(64) PRIMARY KEY,
    "watermark" TIMESTAMP WITH TIME ZONE,
    "lastRunAt" TIMESTAMP WITH TIME ZONE,
    "createdAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    "updatedAt" TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
