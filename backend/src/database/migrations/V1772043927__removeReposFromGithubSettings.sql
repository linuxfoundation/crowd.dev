-- Backup settings before any modifications
CREATE TABLE integration.integrations_settings_backup_02_28_2026 AS
SELECT id, settings FROM integrations;

-- Strip repos from orgs in settings for github and github-nango integrations
-- Repos now live in public.repositories table and are populated into API responses
-- via the compatibility layer in integrationRepository._populateRelations
UPDATE integrations
SET settings = jsonb_set(
  settings,
  '{orgs}',
  (
    SELECT coalesce(jsonb_agg(
      org - 'repos'
    ), '[]'::jsonb)
    FROM jsonb_array_elements(settings->'orgs') org
  )
)
WHERE platform IN ('github', 'github-nango')
  AND settings->'orgs' IS NOT NULL
  AND "deletedAt" IS NULL
  AND status != 'mapping';

-- Also clean up top-level repos/unavailableRepos if present
UPDATE integrations
SET settings = settings - 'repos' - 'unavailableRepos'
WHERE platform IN ('github', 'github-nango')
  AND (settings ? 'repos' OR settings ? 'unavailableRepos')
  AND "deletedAt" IS NULL
  AND status != 'mapping';
