// DependentsLatest scans all historical snapshots (~77 TB, ~$303). Use one partition instead.
// No ecosystem filter on Dependents — counts dependents from all ecosystems pointing at our packages.
export function buildDependentCountsSql(snapshotDate: string): string {
  return `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(REGEXP_REPLACE(Purl, r'@[^@]+$', '')) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
    AND Purl IS NOT NULL
  GROUP BY System, Name
)
SELECT
  pm.purl                                                                      AS purl,
  COUNT(DISTINCT CONCAT(d.Dependent.System, ':', d.Dependent.Name))            AS dependent_packages_count,
  COUNT(DISTINCT pvp.ProjectName)                                               AS dependent_repos_count
FROM \`bigquery-public-data.deps_dev_v1.Dependents\` d
JOIN purl_map pm ON pm.System = d.System AND pm.Name = d.Name
LEFT JOIN (
  SELECT System, Name, ANY_VALUE(ProjectName) AS ProjectName
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionToProject\`
  WHERE SnapshotAt >= TIMESTAMP('${snapshotDate}')
    AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${snapshotDate}', INTERVAL 1 DAY))
    AND RelationType = 'SOURCE_REPO_TYPE'
  GROUP BY System, Name
) pvp ON pvp.System = d.Dependent.System AND pvp.Name = d.Dependent.Name
WHERE d.SnapshotAt >= TIMESTAMP('${snapshotDate}')
  AND d.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${snapshotDate}', INTERVAL 1 DAY))
  AND d.System IN ('NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO')
  AND d.MinimumDepth = 1
  AND d.DependentIsHighestReleaseWithResolution = TRUE
GROUP BY pm.purl
`
}
