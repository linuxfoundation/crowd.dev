-- Backup full integration rows (including deprecated columns and nangoMapping in settings) before any modifications
CREATE TABLE integration.integrations_backup_02_24_2026 AS
SELECT id, settings, "limitCount", "limitLastResetAt", "emailSentAt", "importHash" FROM integrations;

-- Part A: Drop deprecated columns from integrations table
ALTER TABLE public.integrations
  DROP COLUMN IF EXISTS "limitCount",
  DROP COLUMN IF EXISTS "limitLastResetAt",
  DROP COLUMN IF EXISTS "emailSentAt",
  DROP COLUMN IF EXISTS "importHash";

-- Drop same columns from integrationsBackup table (created as SELECT * FROM integrations WHERE 1=2)
ALTER TABLE public."integrationsBackup"
  DROP COLUMN IF EXISTS "limitCount",
  DROP COLUMN IF EXISTS "limitLastResetAt",
  DROP COLUMN IF EXISTS "emailSentAt",
  DROP COLUMN IF EXISTS "importHash";

-- Part B: Create nango_mapping table and migrate data from settings JSONB
CREATE TABLE integration.nango_mapping (
    "integrationId"  UUID   NOT NULL,
    "connectionId"   TEXT   NOT NULL,
    "repositoryId"   UUID,
    owner            TEXT   NOT NULL,
    "repoName"       TEXT   NOT NULL,
    "createdAt"      TIMESTAMPTZ NOT NULL DEFAULT now(),
    "updatedAt"      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY ("integrationId", "connectionId"),
    FOREIGN KEY ("integrationId") REFERENCES integrations(id) ON DELETE CASCADE,
    FOREIGN KEY ("repositoryId") REFERENCES repositories(id) ON DELETE SET NULL
);

CREATE UNIQUE INDEX ix_nango_mapping_connectionId ON integration.nango_mapping ("connectionId");
CREATE INDEX ix_nango_mapping_repositoryId ON integration.nango_mapping ("repositoryId");

-- Migrate existing data from settings.nangoMapping
INSERT INTO integration.nango_mapping ("integrationId", "connectionId", "repositoryId", owner, "repoName")
SELECT
    i.id,
    nm.key,
    r.id,
    nm.value->>'owner',
    nm.value->>'repoName'
FROM integrations i
CROSS JOIN LATERAL jsonb_each(i.settings->'nangoMapping') nm
LEFT JOIN repositories r
    ON lower(r.url) = lower('https://github.com/' || (nm.value->>'owner') || '/' || (nm.value->>'repoName'))
    AND r."sourceIntegrationId" = i.id
    AND r."deletedAt" IS NULL
WHERE i.platform = 'github-nango'
  AND i."deletedAt" IS NULL
  AND i.settings->'nangoMapping' IS NOT NULL
ON CONFLICT DO NOTHING;

-- Remove nangoMapping from settings (only github-nango integrations ever stored this key)
UPDATE integrations
SET settings = settings - 'nangoMapping'
WHERE platform = 'github-nango'
  AND settings->'nangoMapping' IS NOT NULL;
