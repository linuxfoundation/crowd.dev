// NPM, MAVEN, PYPI, CARGO are in DependencyGraphEdgesLatest / DependenciesLatest.
// GO uses GoRequirementsLatest (DirectDependencies, no resolved to_version).
// NUGET uses NuGetRequirementsLatest (DependencyGroups → Dependencies, no resolved to_version).
// RUBYGEMS uses RubyGemsRequirementsLatest (RuntimeDependencies, no resolved to_version).
// Confirmed via BQ query: DependencyGraphEdgesLatest and DependenciesLatest contain exactly
// {NPM, MAVEN, PYPI, CARGO} — GO, NUGET and RUBYGEMS absent from both (rubygems verified 2026-07-03).

const EDGE_SYSTEMS = new Set(['NPM', 'MAVEN', 'PYPI', 'CARGO'])

export const DEPS_DEFAULT_ECOSYSTEMS = ['NPM', 'GO', 'MAVEN', 'PYPI', 'NUGET', 'CARGO', 'RUBYGEMS']

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

// NuGet groups deps by TargetFramework — flatten all groups, duplicate (root, dep) pairs are
// deduped downstream by DISTINCT ON + ON CONFLICT in MERGE_SQL (and the fill-constraints variant).
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

// RubyGems: RuntimeDependencies only (the runtime graph — dev deps excluded, matching GO/NUGET).
const RUBYGEMS_FULL_PART = `
SELECT
  'rubygems'             AS ecosystem,
  r.Name                 AS root_name,
  r.Version              AS root_version,
  d.Name                 AS to_name,
  CAST(NULL AS STRING)   AS to_version,
  d.Requirement          AS version_constraint
FROM \`bigquery-public-data.deps_dev_v1.RubyGemsRequirementsLatest\` r,
UNNEST(r.RuntimeDependencies) AS d`

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
  if (ecosystems.includes('RUBYGEMS')) parts.push(RUBYGEMS_FULL_PART)

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
  if (ecosystems.includes('RUBYGEMS')) parts.push(RUBYGEMS_FULL_PART)

  return parts.join('\nUNION ALL\n')
}

// --- Incremental SQL helpers (snapshot edge-diff) ---
//
// Diff today's direct edges against the watermark (last successfully ingested) snapshot, matched on
// the edge identity (ecosystem, root_name, root_version, to_name) — deliberately EXCLUDING the
// resolved dependency version (to_version) and the requirement string from the match key.
//
// Why exclude to_version: a version's declared direct dependencies are immutable (manifest), but
// deps.dev RE-RESOLVES each dependency's concrete version every snapshot (a range like "^4.17.0"
// resolves to whatever patch is newest that week). Including to_version in the match makes every
// stable edge look "new" whenever any dependency ships a patch — the re-resolution churn that
// produced the ~555M-row exports. Matching on (root, to_name) only:
//   - drops that churn, AND
//   - still catches genuinely-new edges: brand-new versions' edges AND edges deps.dev resolved
//     LATE for already-published versions. (A version lands in PackageVersions on publish, but its
//     resolved graph can appear a snapshot or more later — measured ~14% of edge-bearing versions.
//     A version-level diff misses those; an edge-level diff catches them, because the edge is
//     simply new vs the watermark.)
//
// Consistent with how PG stores the data: the unique key is (version_id, depends_on_id,
// dependency_kind) — depends_on_version_id is NOT in it and the merge is ON CONFLICT DO NOTHING, so
// depends_on_version_id is never updated. to_version / version_constraint are still SELECTed (to
// populate the columns on first insert) but are not part of the diff key; the today CTEs collapse
// to one row per edge identity via GROUP BY + MAX (also defuses ×100 duplication if a corrupt
// snapshot ever slips past the edge-quality guard).
//
// All CTEs go into one WITH clause so each branch's UNION ALL SELECT can reference its CTE pair.

const DEPS_DEV = 'bigquery-public-data.deps_dev_v1'

// Snapshot dates always arrive as YYYY-MM-DD (resolveSnapshotDate slices the timestamp;
// bootstrap uses toISOString().slice(0,10)). Validate before embedding in SQL so a malformed or
// operator-supplied value fails loudly instead of producing broken — or injectable — SQL.
export function assertSnapshotDate(date: string): void {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    throw new Error(`Invalid snapshot date '${date}' — expected YYYY-MM-DD`)
  }
}

// SnapshotAt is not exactly midnight, so filter by a [date, date+1day) range, never `= TIMESTAMP(date)`.
function snapshotRange(col: string, date: string): string {
  assertSnapshotDate(date)
  return `${col} >= TIMESTAMP('${date}')
    AND ${col} <  TIMESTAMP(DATE_ADD(DATE '${date}', INTERVAL 1 DAY))`
}

// Anti-join: today edges not present in the watermark snapshot, matched on the edge identity.
function antiJoinSelect(todayCte: string, watermarkCte: string): string {
  return `SELECT t.ecosystem, t.root_name, t.root_version, t.to_name, t.to_version, t.version_constraint
FROM ${todayCte} t
LEFT JOIN ${watermarkCte} l
  ON l.ecosystem = t.ecosystem AND l.root_name = t.root_name
 AND l.root_version = t.root_version AND l.to_name = t.to_name
WHERE l.to_name IS NULL`
}

// GO + NUGET come from manifest tables (GoRequirements / NuGetRequirements) regardless of Option
// A/B, so their incremental branches are shared. Each returns its today + watermark CTEs + select.
function goIncrementalBranch(today: string, watermark: string): { ctes: string[]; select: string } {
  return {
    ctes: [
      `today_go AS (
  SELECT
    'go'                 AS ecosystem,
    g.Name               AS root_name,
    g.Version            AS root_version,
    d.Name               AS to_name,
    CAST(NULL AS STRING) AS to_version,
    MAX(d.Requirement)   AS version_constraint
  FROM \`${DEPS_DEV}.GoRequirements\` g,
  UNNEST(g.DirectDependencies) AS d
  WHERE ${snapshotRange('g.SnapshotAt', today)}
  GROUP BY 1, 2, 3, 4
)`,
      `watermark_go AS (
  SELECT 'go' AS ecosystem, g.Name AS root_name, g.Version AS root_version, d.Name AS to_name
  FROM \`${DEPS_DEV}.GoRequirements\` g,
  UNNEST(g.DirectDependencies) AS d
  WHERE ${snapshotRange('g.SnapshotAt', watermark)}
  GROUP BY 1, 2, 3, 4
)`,
    ],
    select: antiJoinSelect('today_go', 'watermark_go'),
  }
}

function nugetIncrementalBranch(
  today: string,
  watermark: string,
): { ctes: string[]; select: string } {
  return {
    ctes: [
      `today_nuget AS (
  SELECT
    'nuget'              AS ecosystem,
    n.Name               AS root_name,
    n.Version            AS root_version,
    dep.Name             AS to_name,
    CAST(NULL AS STRING) AS to_version,
    MAX(dep.Requirement) AS version_constraint
  FROM \`${DEPS_DEV}.NuGetRequirements\` n,
  UNNEST(n.DependencyGroups) AS grp,
  UNNEST(grp.Dependencies) AS dep
  WHERE ${snapshotRange('n.SnapshotAt', today)}
  GROUP BY 1, 2, 3, 4
)`,
      `watermark_nuget AS (
  SELECT 'nuget' AS ecosystem, n.Name AS root_name, n.Version AS root_version, dep.Name AS to_name
  FROM \`${DEPS_DEV}.NuGetRequirements\` n,
  UNNEST(n.DependencyGroups) AS grp,
  UNNEST(grp.Dependencies) AS dep
  WHERE ${snapshotRange('n.SnapshotAt', watermark)}
  GROUP BY 1, 2, 3, 4
)`,
    ],
    select: antiJoinSelect('today_nuget', 'watermark_nuget'),
  }
}

// RubyGems manifest — RuntimeDependencies only (runtime graph; dev deps excluded, matching GO/NUGET).
function rubygemsIncrementalBranch(
  today: string,
  watermark: string,
): { ctes: string[]; select: string } {
  return {
    ctes: [
      `today_rubygems AS (
  SELECT
    'rubygems'           AS ecosystem,
    r.Name               AS root_name,
    r.Version            AS root_version,
    d.Name               AS to_name,
    CAST(NULL AS STRING) AS to_version,
    MAX(d.Requirement)   AS version_constraint
  FROM \`${DEPS_DEV}.RubyGemsRequirements\` r,
  UNNEST(r.RuntimeDependencies) AS d
  WHERE ${snapshotRange('r.SnapshotAt', today)}
  GROUP BY 1, 2, 3, 4
)`,
      `watermark_rubygems AS (
  SELECT 'rubygems' AS ecosystem, r.Name AS root_name, r.Version AS root_version, d.Name AS to_name
  FROM \`${DEPS_DEV}.RubyGemsRequirements\` r,
  UNNEST(r.RuntimeDependencies) AS d
  WHERE ${snapshotRange('r.SnapshotAt', watermark)}
  GROUP BY 1, 2, 3, 4
)`,
    ],
    select: antiJoinSelect('today_rubygems', 'watermark_rubygems'),
  }
}

export function buildDepsIncrementalSqlA(
  today: string,
  watermark: string,
  ecosystems: string[],
): string {
  const edgeSystems = ecosystems.filter((s) => EDGE_SYSTEMS.has(s))
  const includeGo = ecosystems.includes('GO')
  const includeNuget = ecosystems.includes('NUGET')
  const includeRubygems = ecosystems.includes('RUBYGEMS')

  const ctes: string[] = []
  const selects: string[] = []

  if (edgeSystems.length > 0) {
    const filter = edgeSystems.map((s) => `'${s}'`).join(', ')
    ctes.push(
      `today_edges AS (
  SELECT
    LOWER(e.System)    AS ecosystem,
    e.Name             AS root_name,
    e.Version          AS root_version,
    e.To.Name          AS to_name,
    MAX(e.To.Version)  AS to_version,
    MAX(e.Requirement) AS version_constraint
  FROM \`${DEPS_DEV}.DependencyGraphEdges\` e
  WHERE ${snapshotRange('e.SnapshotAt', today)}
    AND e.System IN (${filter})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
  GROUP BY 1, 2, 3, 4
)`,
      `watermark_edges AS (
  SELECT LOWER(e.System) AS ecosystem, e.Name AS root_name, e.Version AS root_version, e.To.Name AS to_name
  FROM \`${DEPS_DEV}.DependencyGraphEdges\` e
  WHERE ${snapshotRange('e.SnapshotAt', watermark)}
    AND e.System IN (${filter})
    AND e.From.Name = e.Name AND e.From.Version = e.Version
  GROUP BY 1, 2, 3, 4
)`,
    )
    selects.push(antiJoinSelect('today_edges', 'watermark_edges'))
  }

  if (includeGo) {
    const { ctes: goCtes, select } = goIncrementalBranch(today, watermark)
    ctes.push(...goCtes)
    selects.push(select)
  }

  if (includeNuget) {
    const { ctes: nugetCtes, select } = nugetIncrementalBranch(today, watermark)
    ctes.push(...nugetCtes)
    selects.push(select)
  }

  if (includeRubygems) {
    const { ctes: rubygemsCtes, select } = rubygemsIncrementalBranch(today, watermark)
    ctes.push(...rubygemsCtes)
    selects.push(select)
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
  const includeRubygems = ecosystems.includes('RUBYGEMS')

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
    MAX(d.Dependency.Version) AS to_version,
    CAST(NULL AS STRING)      AS version_constraint
  FROM \`${DEPS_DEV}.Dependencies\` d
  WHERE ${snapshotRange('d.SnapshotAt', today)}
    AND d.System IN (${filter})
    AND d.MinimumDepth = 1
  GROUP BY 1, 2, 3, 4
)`,
      `watermark_deps AS (
  SELECT LOWER(d.System) AS ecosystem, d.Name AS root_name, d.Version AS root_version, d.Dependency.Name AS to_name
  FROM \`${DEPS_DEV}.Dependencies\` d
  WHERE ${snapshotRange('d.SnapshotAt', watermark)}
    AND d.System IN (${filter})
    AND d.MinimumDepth = 1
  GROUP BY 1, 2, 3, 4
)`,
    )
    selects.push(antiJoinSelect('today_deps', 'watermark_deps'))
  }

  if (includeGo) {
    const { ctes: goCtes, select } = goIncrementalBranch(today, watermark)
    ctes.push(...goCtes)
    selects.push(select)
  }

  if (includeNuget) {
    const { ctes: nugetCtes, select } = nugetIncrementalBranch(today, watermark)
    ctes.push(...nugetCtes)
    selects.push(select)
  }

  if (includeRubygems) {
    const { ctes: rubygemsCtes, select } = rubygemsIncrementalBranch(today, watermark)
    ctes.push(...rubygemsCtes)
    selects.push(select)
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
