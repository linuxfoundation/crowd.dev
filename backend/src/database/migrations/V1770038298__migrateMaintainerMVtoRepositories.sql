-- Migrate mv_maintainer_roles from githubRepos to public.repositories
-- This must run before dropping the githubRepos table

DROP MATERIALIZED VIEW IF EXISTS mv_maintainer_roles;

CREATE MATERIALIZED VIEW mv_maintainer_roles AS
SELECT
    mai.id,
    mei."memberId",
    r."segmentId",
    mai."createdAt" AS "dateStart",
    NULL as "dateEnd",
    r.url,
    CASE WHEN i.platform = 'github-nango' THEN 'github' ELSE i.platform END AS "repoType",
    mai.role
FROM "maintainersInternal" mai
JOIN "memberIdentities" mei ON mai."identityId" = mei.id
JOIN public.repositories r ON mai."repoId" = r.id
JOIN integrations i ON r."sourceIntegrationId" = i.id
WHERE r."deletedAt" IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS mv_maintainer_roles_id ON mv_maintainer_roles (id);
