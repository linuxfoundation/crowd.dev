// NPM, MAVEN, PYPI, CARGO are in DependencyGraphEdgesLatest / DependenciesLatest.
// GO uses GoRequirementsLatest (DirectDependencies, no resolved to_version).
// NUGET uses NuGetRequirementsLatest (DependencyGroups → Dependencies, no resolved to_version).
// Confirmed via BQ query 2026-06-17: DependencyGraphEdgesLatest and DependenciesLatest
// both contain exactly {NPM, MAVEN, PYPI, CARGO} — GO and NUGET absent from both.

const EDGE_SYSTEMS = new Set(['NPM', 'MAVEN', 'PYPI', 'CARGO'])

export const DEPS_DEFAULT_ECOSYSTEMS = ['NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO']

// --- Full SQL helpers ---

const GO_FULL_PART = `
SELECT
  'go'                   AS ecosystem,
  g.Name                 AS root_name,
  g.Version              AS root_version,
  d.Name                 AS to_name,
  CAST(NULL AS STRING)   AS to_version,
  d.Requirement          AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.GoRequirementsLatest\` g,
UNNEST(g.DirectDependencies) AS d`

// NuGet groups deps by TargetFramework — flatten all groups, dedup handled downstream
// by DISTINCT ON in MERGE_SQL_FULL and ON CONFLICT in MERGE_SQL.
const NUGET_FULL_PART = `
SELECT
  'nuget'                AS ecosystem,
  n.Name                 AS root_name,
  n.Version              AS root_version,
  dep.Name               AS to_name,
  CAST(NULL AS STRING)   AS to_version,
  dep.Requirement        AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.NuGetRequirementsLatest\` n,
UNNEST(n.DependencyGroups) AS grp,
UNNEST(grp.Dependencies) AS dep`

// ADR-0003 Option A: DependencyGraphEdgesLatest for NPM/MAVEN/PYPI/CARGO — has version_constraint.
// GO + NUGET always come from their ecosystem-specific tables regardless of option.
export function buildDepsFullSqlA(ecosystems: string[]): string {
  const parts: string[] = []

  const edgeSystems = ecosystems.filter((s) => EDGE_SYSTEMS.has(s))
  if (edgeSystems.length > 0) {
    const filter = edgeSystems.map((s) => `'${s}'`).join(', ')
    parts.push(`
SELECT
  LOWER(e.System) AS ecosystem,
  e.Name          AS root_name,
  e.Version       AS root_version,
  e.To.Name       AS to_name,
  e.To.Version    AS to_version,
  e.Requirement   AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdgesLatest\` e
WHERE e.System IN (${filter})
  AND e.From.Name = e.Name
  AND e.From.Version = e.Version`)
  }

  if (ecosystems.includes('GO')) parts.push(GO_FULL_PART)
  if (ecosystems.includes('NUGET')) parts.push(NUGET_FULL_PART)

  return parts.join('\nUNION ALL\n')
}

// ADR-0003 Option B: DependenciesLatest for NPM/MAVEN/PYPI/CARGO — cheaper, no version_constraint.
// GO + NUGET same as Option A (ecosystem-specific tables, version_constraint available).
export function buildDepsFullSqlB(ecosystems: string[]): string {
  const parts: string[] = []

  const depsSystems = ecosystems.filter((s) => EDGE_SYSTEMS.has(s))
  if (depsSystems.length > 0) {
    const filter = depsSystems.map((s) => `'${s}'`).join(', ')
    parts.push(`
SELECT
  LOWER(d.System)           AS ecosystem,
  d.Name                    AS root_name,
  d.Version                 AS root_version,
  d.Dependency.Name         AS to_name,
  d.Dependency.Version      AS to_version,
  CAST(NULL AS STRING)      AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.DependenciesLatest\` d
WHERE d.System IN (${filter})
  AND d.MinimumDepth = 1`)
  }

  if (ecosystems.includes('GO')) parts.push(GO_FULL_PART)
  if (ecosystems.includes('NUGET')) parts.push(NUGET_FULL_PART)

  return parts.join('\nUNION ALL\n')
}

// --- Incremental SQL helpers ---
// Uses base tables (GoRequirements, NuGetRequirements, DependencyGraphEdges) with SnapshotAt filter.
// All CTEs combined in one WITH clause so UNION ALL can reference them freely.

export function buildDepsIncrementalSqlA(
  today: string,
  watermark: string,
  ecosystems: string[],
): string {
  const edgeSystems = ecosystems.filter((s) => EDGE_SYSTEMS.has(s))
  const includeGo = ecosystems.includes('GO')
  const includeNuget = ecosystems.includes('NUGET')

  const ctes: string[] = []
  const selects: string[] = []

  if (edgeSystems.length > 0) {
    const filter = edgeSystems.map((s) => `'${s}'`).join(', ')
    ctes.push(
      `today_edges AS (
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
    AND e.System IN (${filter})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
)`,
      `watermark_edges AS (
  SELECT e.System, e.Name, e.Version, e.To.Name AS to_name, e.To.Version AS to_version
  FROM \`bigquery-public-data.deps_dev_v1.DependencyGraphEdges\` e
  WHERE e.SnapshotAt >= TIMESTAMP('${watermark}')
    AND e.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
    AND e.System IN (${filter})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
  GROUP BY e.System, e.Name, e.Version, e.To.Name, e.To.Version
)`,
    )
    selects.push(
      `SELECT t.*
FROM today_edges t
LEFT JOIN watermark_edges l
  ON LOWER(l.System) = t.ecosystem AND l.Name = t.root_name AND l.Version = t.root_version
 AND l.to_name = t.to_name AND l.to_version = t.to_version
WHERE l.to_name IS NULL`,
    )
  }

  if (includeGo) {
    ctes.push(
      `today_go AS (
  SELECT
    'go'                   AS ecosystem,
    g.Name                 AS root_name,
    g.Version              AS root_version,
    d.Name                 AS to_name,
    CAST(NULL AS STRING)   AS to_version,
    d.Requirement          AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.GoRequirements\` g,
  UNNEST(g.DirectDependencies) AS d
  WHERE g.SnapshotAt >= TIMESTAMP('${today}')
    AND g.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
)`,
      `watermark_go AS (
  SELECT g.Name, g.Version, d.Name AS to_name
  FROM \`bigquery-public-data.deps_dev_v1.GoRequirements\` g,
  UNNEST(g.DirectDependencies) AS d
  WHERE g.SnapshotAt >= TIMESTAMP('${watermark}')
    AND g.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
  GROUP BY g.Name, g.Version, d.Name
)`,
    )
    selects.push(
      `SELECT t.*
FROM today_go t
LEFT JOIN watermark_go l
  ON l.Name = t.root_name AND l.Version = t.root_version AND l.to_name = t.to_name
WHERE l.to_name IS NULL`,
    )
  }

  if (includeNuget) {
    ctes.push(
      `today_nuget AS (
  SELECT
    'nuget'                AS ecosystem,
    n.Name                 AS root_name,
    n.Version              AS root_version,
    dep.Name               AS to_name,
    CAST(NULL AS STRING)   AS to_version,
    dep.Requirement        AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.NuGetRequirements\` n,
  UNNEST(n.DependencyGroups) AS grp,
  UNNEST(grp.Dependencies) AS dep
  WHERE n.SnapshotAt >= TIMESTAMP('${today}')
    AND n.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
)`,
      `watermark_nuget AS (
  SELECT n.Name, n.Version, dep.Name AS to_name
  FROM \`bigquery-public-data.deps_dev_v1.NuGetRequirements\` n,
  UNNEST(n.DependencyGroups) AS grp,
  UNNEST(grp.Dependencies) AS dep
  WHERE n.SnapshotAt >= TIMESTAMP('${watermark}')
    AND n.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
  GROUP BY n.Name, n.Version, dep.Name
)`,
    )
    selects.push(
      `SELECT t.*
FROM today_nuget t
LEFT JOIN watermark_nuget l
  ON l.Name = t.root_name AND l.Version = t.root_version AND l.to_name = t.to_name
WHERE l.to_name IS NULL`,
    )
  }

  return `WITH\n${ctes.join(',\n')}\n${selects.join('\nUNION ALL\n')}`
}

export function buildDepsIncrementalSqlB(
  today: string,
  watermark: string,
  ecosystems: string[],
): string {
  const depsSystems = ecosystems.filter((s) => EDGE_SYSTEMS.has(s))
  const includeGo = ecosystems.includes('GO')
  const includeNuget = ecosystems.includes('NUGET')

  const ctes: string[] = []
  const selects: string[] = []

  if (depsSystems.length > 0) {
    const filter = depsSystems.map((s) => `'${s}'`).join(', ')
    ctes.push(
      `today_deps AS (
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
    AND d.System IN (${filter})
    AND d.MinimumDepth = 1
)`,
      `watermark_deps AS (
  SELECT d.System, d.Name, d.Version, d.Dependency.Name AS to_name, d.Dependency.Version AS to_version
  FROM \`bigquery-public-data.deps_dev_v1.Dependencies\` d
  WHERE d.SnapshotAt >= TIMESTAMP('${watermark}')
    AND d.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
    AND d.System IN (${filter})
    AND d.MinimumDepth = 1
  GROUP BY d.System, d.Name, d.Version, d.Dependency.Name, d.Dependency.Version
)`,
    )
    selects.push(
      `SELECT t.*
FROM today_deps t
LEFT JOIN watermark_deps l
  ON LOWER(l.System) = t.ecosystem AND l.Name = t.root_name AND l.Version = t.root_version
 AND l.to_name = t.to_name AND l.to_version = t.to_version
WHERE l.to_name IS NULL`,
    )
  }

  if (includeGo) {
    ctes.push(
      `today_go AS (
  SELECT
    'go'                   AS ecosystem,
    g.Name                 AS root_name,
    g.Version              AS root_version,
    d.Name                 AS to_name,
    CAST(NULL AS STRING)   AS to_version,
    d.Requirement          AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.GoRequirements\` g,
  UNNEST(g.DirectDependencies) AS d
  WHERE g.SnapshotAt >= TIMESTAMP('${today}')
    AND g.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
)`,
      `watermark_go AS (
  SELECT g.Name, g.Version, d.Name AS to_name
  FROM \`bigquery-public-data.deps_dev_v1.GoRequirements\` g,
  UNNEST(g.DirectDependencies) AS d
  WHERE g.SnapshotAt >= TIMESTAMP('${watermark}')
    AND g.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
  GROUP BY g.Name, g.Version, d.Name
)`,
    )
    selects.push(
      `SELECT t.*
FROM today_go t
LEFT JOIN watermark_go l
  ON l.Name = t.root_name AND l.Version = t.root_version AND l.to_name = t.to_name
WHERE l.to_name IS NULL`,
    )
  }

  if (includeNuget) {
    ctes.push(
      `today_nuget AS (
  SELECT
    'nuget'                AS ecosystem,
    n.Name                 AS root_name,
    n.Version              AS root_version,
    dep.Name               AS to_name,
    CAST(NULL AS STRING)   AS to_version,
    dep.Requirement        AS version_constraint
  FROM \`bigquery-public-data.deps_dev_v1.NuGetRequirements\` n,
  UNNEST(n.DependencyGroups) AS grp,
  UNNEST(grp.Dependencies) AS dep
  WHERE n.SnapshotAt >= TIMESTAMP('${today}')
    AND n.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${today}', INTERVAL 1 DAY))
)`,
      `watermark_nuget AS (
  SELECT n.Name, n.Version, dep.Name AS to_name
  FROM \`bigquery-public-data.deps_dev_v1.NuGetRequirements\` n,
  UNNEST(n.DependencyGroups) AS grp,
  UNNEST(grp.Dependencies) AS dep
  WHERE n.SnapshotAt >= TIMESTAMP('${watermark}')
    AND n.SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${watermark}', INTERVAL 1 DAY))
  GROUP BY n.Name, n.Version, dep.Name
)`,
    )
    selects.push(
      `SELECT t.*
FROM today_nuget t
LEFT JOIN watermark_nuget l
  ON l.Name = t.root_name AND l.Version = t.root_version AND l.to_name = t.to_name
WHERE l.to_name IS NULL`,
    )
  }

  return `WITH\n${ctes.join(',\n')}\n${selects.join('\nUNION ALL\n')}`
}

export function buildDepsFullSql(ecosystems: string[], tableOption: 'A' | 'B' = 'A'): string {
  return tableOption === 'B' ? buildDepsFullSqlB(ecosystems) : buildDepsFullSqlA(ecosystems)
}

export function buildDepsIncrementalSql(
  today: string,
  watermark: string,
  ecosystems: string[],
  tableOption: 'A' | 'B' = 'A',
): string {
  return tableOption === 'B'
    ? buildDepsIncrementalSqlB(today, watermark, ecosystems)
    : buildDepsIncrementalSqlA(today, watermark, ecosystems)
}
