-- Backup settings before any modifications
CREATE TABLE integration.integrations_settings_backup_02_13_2026 AS
SELECT id, settings FROM integrations;

CREATE TABLE integration.nango_cursors (
    "integrationId"  UUID         NOT NULL,
    "connectionId"   TEXT         NOT NULL,
    platform         TEXT         NOT NULL,
    model            TEXT         NOT NULL,
    cursor           TEXT         NOT NULL,
    "lastCheckedAt"  TIMESTAMPTZ,
    "createdAt"      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    "updatedAt"      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY ("integrationId", "connectionId", model),
    FOREIGN KEY ("integrationId") REFERENCES integrations(id) ON DELETE CASCADE
);

CREATE INDEX ix_nango_cursors_lastCheckedAt ON integration.nango_cursors ("lastCheckedAt" NULLS FIRST);
CREATE INDEX ix_nango_cursors_connectionId ON integration.nango_cursors ("connectionId");

-- GitHub-nango: unnest nangoMapping keys x cursor models
INSERT INTO integration.nango_cursors ("integrationId", "connectionId", platform, model, cursor)
SELECT
    i.id,
    nm.key,
    'github',
    cm.key,
    cm.value #>> '{}'
FROM integrations i,
     jsonb_each(i.settings->'nangoMapping') nm,
     jsonb_each(COALESCE(i.settings->'cursors'->nm.key, '{}'::jsonb)) cm
WHERE i.platform = 'github-nango'
  AND i."deletedAt" IS NULL
  AND i.settings->'nangoMapping' IS NOT NULL
ON CONFLICT DO NOTHING;

-- Non-GitHub nango: connectionId = integrationId, unnest cursor models
INSERT INTO integration.nango_cursors ("integrationId", "connectionId", platform, model, cursor)
SELECT
    i.id,
    i.id::text,
    CASE
        WHEN i.platform = 'gerrit' THEN 'gerrit'
        WHEN i.platform = 'jira' THEN COALESCE(i.settings->>'nangoIntegrationName', 'jira-basic')
        WHEN i.platform = 'confluence' THEN COALESCE(i.settings->>'nangoIntegrationName', 'confluence')
        ELSE i.platform
    END,
    cm.key,
    cm.value #>> '{}'
FROM integrations i,
     jsonb_each(COALESCE(i.settings->'cursors'->i.id::text, '{}'::jsonb)) cm
WHERE i.platform IN ('gerrit', 'jira', 'confluence')
  AND i."deletedAt" IS NULL
ON CONFLICT DO NOTHING;

-- Clean up settings.cursors
UPDATE integrations
SET settings = settings - 'cursors'
WHERE settings->'cursors' IS NOT NULL;
