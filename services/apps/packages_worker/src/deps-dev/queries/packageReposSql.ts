export function buildPackageReposSql(snapshotDate: string, systems: string): string {
  return `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(REGEXP_REPLACE(Purl, r'@[^@]+$', '')) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN (${systems})
    AND Purl IS NOT NULL
  GROUP BY System, Name
),
path_computed AS (
  SELECT
    pm.purl,
    CASE pvp.ProjectType
      WHEN 'GITHUB'    THEN 'github'
      WHEN 'GITLAB'    THEN 'gitlab'
      WHEN 'BITBUCKET' THEN 'bitbucket'
    END AS host,
    CASE pvp.ProjectType
      WHEN 'GITHUB' THEN LOWER(
        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(pvp.ProjectName), r'^https?://[^/]+/', ''), r'\\.git$', ''), r'/$', '')
      )
      ELSE
        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(pvp.ProjectName), r'^https?://[^/]+/', ''), r'\\.git$', ''), r'/$', '')
    END AS path,
    CASE pvp.RelationProvenance
      WHEN 'SLSA_ATTESTATION'              THEN 0.99
      WHEN 'RUBYGEMS_PUBLISH_ATTESTATION'  THEN 0.95
      WHEN 'PYPI_PUBLISH_ATTESTATION'      THEN 0.95
      WHEN 'GO_ORIGIN'                     THEN 0.9
      WHEN 'UNVERIFIED_METADATA'           THEN 0.5
      ELSE 0.4
    END AS confidence
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionToProject\` pvp
  JOIN purl_map pm ON pm.System = pvp.System AND pm.Name = pvp.Name
  WHERE pvp.SnapshotAt >= TIMESTAMP('${snapshotDate}')
    AND pvp.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${snapshotDate}', INTERVAL 1 DAY))
    AND pvp.System IN (${systems})
    AND pvp.RelationType = 'SOURCE_REPO_TYPE'
    AND pvp.ProjectType IN ('GITHUB', 'GITLAB', 'BITBUCKET')
)
SELECT
  purl,
  CONCAT('https://', CASE host WHEN 'github' THEN 'github.com' WHEN 'gitlab' THEN 'gitlab.com' WHEN 'bitbucket' THEN 'bitbucket.org' END, '/', path) AS canonical_url,
  confidence
FROM path_computed
WHERE REGEXP_CONTAINS(path, r'^[a-zA-Z0-9._-]+/[a-zA-Z0-9._/-]+$')
QUALIFY ROW_NUMBER() OVER (PARTITION BY purl, host, path ORDER BY confidence DESC) = 1
`
}
