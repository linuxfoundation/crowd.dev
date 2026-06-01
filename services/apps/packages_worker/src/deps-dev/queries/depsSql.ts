// ADR-0003 Option A: DependencyGraphEdgesLatest — has version_constraint; no NUGET support.
export function buildDepsFullSqlA(systems: string): string {
  return `
SELECT
  LOWER(e.System) AS ecosystem,
  e.Name          AS root_name,
  e.Version       AS root_version,
  e.To.Name       AS to_name,
  e.To.Version    AS to_version,
  e.Requirement   AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdgesLatest\` e
WHERE e.System IN (${systems})
  AND e.From.Name = e.Name
  AND e.From.Version = e.Version
`
}

export function buildDepsIncrementalSqlA(today: string, watermark: string, systems: string): string {
  return `
WITH today AS (
  SELECT
    LOWER(e.System) AS ecosystem,
    e.Name          AS root_name,
    e.Version       AS root_version,
    e.To.Name       AS to_name,
    e.To.Version    AS to_version,
    e.Requirement   AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdges\` e
  WHERE e.SnapshotAt >= TIMESTAMP('${today}')
    AND e.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
    AND e.System IN (${systems})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
),
last_watermark AS (
  SELECT e.System, e.Name, e.Version, e.To.Name AS to_name, e.To.Version AS to_version
  FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdges\` e
  WHERE e.SnapshotAt >= TIMESTAMP('${watermark}')
    AND e.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
    AND e.System IN (${systems})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
  GROUP BY e.System, e.Name, e.Version, e.To.Name, e.To.Version
)
SELECT t.*
FROM today t
LEFT JOIN last_watermark l
  ON LOWER(l.System) = t.ecosystem AND l.Name = t.root_name AND l.Version = t.root_version
 AND l.to_name = t.to_name AND l.to_version = t.to_version
WHERE l.to_name IS NULL
`
}

// ADR-0003 Option B: DependenciesLatest — cheaper (~65%), covers NUGET, but no version_constraint.
export function buildDepsFullSqlB(systems: string): string {
  return `
SELECT
  LOWER(d.System)           AS ecosystem,
  d.Name                    AS root_name,
  d.Version                 AS root_version,
  d.Dependency.Name         AS to_name,
  d.Dependency.Version      AS to_version,
  CAST(NULL AS STRING)      AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.DependenciesLatest\` d
WHERE d.System IN (${systems})
  AND d.MinimumDepth = 1
`
}

export function buildDepsIncrementalSqlB(today: string, watermark: string, systems: string): string {
  return `
WITH today AS (
  SELECT
    LOWER(d.System)           AS ecosystem,
    d.Name                    AS root_name,
    d.Version                 AS root_version,
    d.Dependency.Name         AS to_name,
    d.Dependency.Version      AS to_version,
    CAST(NULL AS STRING)      AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.Dependencies\` d
  WHERE d.SnapshotAt >= TIMESTAMP('${today}')
    AND d.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
    AND d.System IN (${systems})
    AND d.MinimumDepth = 1
),
last_watermark AS (
  SELECT d.System, d.Name, d.Version, d.Dependency.Name AS to_name, d.Dependency.Version AS to_version
  FROM \`bigquery-public-data.deps_dev_v1.Dependencies\` d
  WHERE d.SnapshotAt >= TIMESTAMP('${watermark}')
    AND d.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
    AND d.System IN (${systems})
    AND d.MinimumDepth = 1
  GROUP BY d.System, d.Name, d.Version, d.Dependency.Name, d.Dependency.Version
)
SELECT t.*
FROM today t
LEFT JOIN last_watermark l
  ON LOWER(l.System) = t.ecosystem AND l.Name = t.root_name AND l.Version = t.root_version
 AND l.to_name = t.to_name AND l.to_version = t.to_version
WHERE l.to_name IS NULL
`
}

export function buildDepsFullSql(systems: string, tableOption: 'A' | 'B' = 'A'): string {
  return tableOption === 'B' ? buildDepsFullSqlB(systems) : buildDepsFullSqlA(systems)
}

export function buildDepsIncrementalSql(today: string, watermark: string, systems: string, tableOption: 'A' | 'B' = 'A'): string {
  return tableOption === 'B'
    ? buildDepsIncrementalSqlB(today, watermark, systems)
    : buildDepsIncrementalSqlA(today, watermark, systems)
}
