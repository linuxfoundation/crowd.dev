export function buildReposSql(snapshotDate: string, systems: string): string {
  return `
WITH raw AS (
  SELECT DISTINCT
    p.Type AS raw_project_type,
    p.Name AS raw_project_name,
    CASE p.Type
      WHEN 'GITHUB'    THEN 'github.com'
      WHEN 'GITLAB'    THEN 'gitlab.com'
      WHEN 'BITBUCKET' THEN 'bitbucket.org'
    END AS host,
    CASE p.Type
      WHEN 'GITHUB' THEN LOWER(
        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(p.Name), r'^https?://[^/]+/', ''), r'\\.git$', ''), r'/$', '')
      )
      ELSE
        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(p.Name), r'^https?://[^/]+/', ''), r'\\.git$', ''), r'/$', '')
    END AS path,
    p.OpenIssuesCount AS open_issues,
    p.StarsCount      AS stars,
    p.ForksCount      AS forks,
    p.Description     AS description,
    p.Homepage        AS homepage
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionToProject\` pvp
  JOIN \`bigquery-public-data.deps_dev_v1.ProjectsLatest\` p
    ON p.Type = pvp.ProjectType AND p.Name = pvp.ProjectName
  WHERE pvp.SnapshotAt >= TIMESTAMP('${snapshotDate}')
    AND pvp.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${snapshotDate}', INTERVAL 1 DAY))
    AND pvp.System IN (${systems})
    AND pvp.RelationType = 'SOURCE_REPO_TYPE'
    AND pvp.ProjectType IN ('GITHUB', 'GITLAB', 'BITBUCKET')
    AND p.Type IN ('GITHUB', 'GITLAB', 'BITBUCKET')
)
SELECT
  CONCAT('https://', host, '/', path) AS canonical_url,
  host,
  REGEXP_EXTRACT(path, r'^([^/]+)/') AS owner,
  REGEXP_EXTRACT(path, r'^[^/]+/(.+)$') AS name,
  raw_project_type,
  raw_project_name,
  description,
  homepage,
  stars,
  forks,
  open_issues
FROM raw
WHERE REGEXP_CONTAINS(path, r'^[a-zA-Z0-9._-]+/[a-zA-Z0-9._/-]+$')
`
}
