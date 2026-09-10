import { assertSnapshotDate } from './depsSql'

// The deps.dev `Dependents` reverse index only covers the resolved-graph ecosystems
// {NPM, MAVEN, PYPI, CARGO} — GO and NUGET are entirely absent from the table (verified via BQ
// 2026-06-17). GO/NUGET dependent counts are produced separately by inverting their manifest tables
// (see buildGoDependentCountsSql / buildNugetDependentCountsSql). This builder is therefore scoped
// to the four edge systems; the job kind stays `dependent_counts` so existing job history, the
// monitor, and the row-count guard baseline remain continuous.
//
// DependentsLatest scans all historical snapshots (~77 TB, ~$303). Use one partition instead.
// MinimumDepth=1 = direct dependents (package declares an explicit dependency edge).
// MinimumDepth>1 = transitive dependents (only reachable via intermediaries).
const EDGE_DEPENDENT_SYSTEMS = `('NPM', 'MAVEN', 'PYPI', 'CARGO')`

export function buildDependentCountsSql(snapshotDate: string): string {
  assertSnapshotDate(snapshotDate)
  return `
WITH purl_map AS (
  SELECT System, Name, ANY_VALUE(REGEXP_REPLACE(Purl, r'@[^@]+$', '')) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System IN ${EDGE_DEPENDENT_SYSTEMS}
    AND Purl IS NOT NULL
    AND Name NOT LIKE '%>%'
  GROUP BY System, Name
)
SELECT
  pm.purl                                                                                                  AS purl,
  COUNT(DISTINCT IF(d.MinimumDepth = 1 AND d.Dependent.Name NOT LIKE '%>%', CONCAT(d.Dependent.System, ':', d.Dependent.Name), NULL))         AS dependent_count,
  COUNT(DISTINCT IF(d.MinimumDepth > 1 AND d.Dependent.Name NOT LIKE '%>%', CONCAT(d.Dependent.System, ':', d.Dependent.Name), NULL))         AS transitive_dependent_count,
  COUNT(DISTINCT pvp.ProjectName)                                                                          AS dependent_repos_count
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
  AND d.System IN ${EDGE_DEPENDENT_SYSTEMS}
  AND d.MinimumDepth >= 1
  AND d.DependentIsHighestReleaseWithResolution = TRUE
GROUP BY pm.purl
`
}

// GO/NUGET/RUBYGEMS reverse-dependent counts. deps.dev publishes NO reverse-dependent or transitive
// graph for these three ecosystems (absent from Dependents/Dependencies/DependencyGraphEdges — see
// ADR-0004), so we compute the EXACT reverse transitive closure ourselves from the direct manifests
// it DOES publish (GoRequirementsLatest / NuGetRequirementsLatest / RubyGemsRequirementsLatest). The
// "Latest" tables hold the highest release per package, matching the
// DependentIsHighestReleaseWithResolution semantics of the edge query.
//
// Unlike the edge query (a single SELECT), this is a multi-statement BQ SCRIPT: a semi-naive fixpoint
// over session-scoped TEMP tables, run via bqExportToGcs's isScript mode. It ends by creating
// `_export_data` with all three count columns, which the activity exports. Names are hashed to INT64
// via FARM_FINGERPRINT for the loop (~5× cheaper scans; collisions ~1e-6), then mapped back to purls.
//
// Edge orientation matches the v1 direct-only builder: dep = requirer (g/n.Name), subj = depended-upon
// (d/dep.Name), so dependent_count(subj) = distinct requirers — purl keys line up with the merge.

// GO needs 31 hops to converge, NUGET 19 (measured 2026-06-23). The cap is a deterministic runaway
// guard (e.g. a corrupt snapshot with a version cycle that never saturates), set well above both.
const MAX_CLOSURE_ITERATIONS = 60

// Builds the full closure script for one ecosystem. `edgesSql` must SELECT DISTINCT a `dep` (requirer)
// and `subj` (depended-upon) name column from that ecosystem's manifest table.
function buildClosureScript(
  system: 'GO' | 'NUGET' | 'RUBYGEMS',
  edgesSql: string,
  snapshotDate: string,
): string {
  assertSnapshotDate(snapshotDate)
  return `
DECLARE new_pairs INT64 DEFAULT 1;
DECLARE iter INT64 DEFAULT 0;

-- 1. module-level direct edges (dep depends on subj); versions collapsed, self-loops + malformed names dropped.
CREATE TEMP TABLE edges_raw CLUSTER BY subj AS
${edgesSql};

-- 2. fingerprint names -> INT64 (≈5× cheaper scans; collisions ~1e-6, see ADR-0004).
CREATE TEMP TABLE fe CLUSTER BY subj, dep AS
SELECT DISTINCT FARM_FINGERPRINT(dep) AS dep, FARM_FINGERPRINT(subj) AS subj FROM edges_raw;

-- 3. fingerprint -> name lookup (both endpoints) for mapping results back to purls/repos.
CREATE TEMP TABLE names AS
SELECT DISTINCT FARM_FINGERPRINT(name) AS fp, name FROM (
  SELECT dep AS name FROM edges_raw UNION DISTINCT SELECT subj AS name FROM edges_raw
);

-- 4. seed reach (full closure, incl. direct) and frontier with the direct edges.
CREATE TEMP TABLE reach CLUSTER BY subj, dep AS SELECT dep, subj FROM fe;
CREATE TEMP TABLE frontier CLUSTER BY subj AS SELECT dep, subj FROM fe;

-- 5. semi-naive fixpoint: extend the frontier one base edge deeper, keep only pairs not already in reach.
--    Terminates when no new pairs survive (finite pair space). iter cap = runaway guard.
WHILE new_pairs > 0 AND iter < ${MAX_CLOSURE_ITERATIONS} DO
  CREATE OR REPLACE TEMP TABLE next_frontier CLUSTER BY subj AS
    SELECT DISTINCT f.dep AS dep, e.subj AS subj
    FROM frontier f
    JOIN fe e ON e.dep = f.subj
    LEFT JOIN reach r ON r.dep = f.dep AND r.subj = e.subj
    WHERE r.dep IS NULL;
  SET new_pairs = (SELECT COUNT(*) FROM next_frontier);
  INSERT INTO reach SELECT dep, subj FROM next_frontier;
  CREATE OR REPLACE TEMP TABLE frontier CLUSTER BY subj AS SELECT dep, subj FROM next_frontier;
  SET iter = iter + 1;
END WHILE;

-- 5b. The loop exits on convergence (new_pairs = 0) OR the iteration cap. If it stopped on the cap
--     with pairs still pending, the closure is INCOMPLETE: transitive_dependent_count would be
--     silently undercounted and the row-count guard wouldn't catch it (row volume stays ~flat).
--     Fail the job loudly instead of exporting partial counts. Raising the cap is the fix only if
--     the graph legitimately deepened past it.
IF new_pairs > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    '${system} reverse-dependent closure did not converge within ${MAX_CLOSURE_ITERATIONS} iterations (new_pairs=%d still pending) - aborting to avoid undercounting transitive dependents',
    new_pairs);
END IF;

-- 6. repo mapping for the all-depth dependent_repos_count: latest source repo per package within a
--    60-day window anchored on the run date. Literal date bounds prune partitions (a subquery-derived
--    SnapshotAt = (SELECT MAX ...) would NOT prune and scans the whole table).
CREATE TEMP TABLE dep_repos AS
SELECT Name, repo FROM (
  SELECT Name, ProjectName AS repo,
         ROW_NUMBER() OVER (PARTITION BY Name ORDER BY SnapshotAt DESC) AS rn
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionToProject\`
  WHERE System = '${system}' AND RelationType = 'SOURCE_REPO_TYPE'
    AND SnapshotAt >= TIMESTAMP(DATE_SUB(DATE '${snapshotDate}', INTERVAL 60 DAY))
    AND SnapshotAt <  TIMESTAMP(DATE_ADD(DATE '${snapshotDate}', INTERVAL 1 DAY))
)
WHERE rn = 1;

-- collapse fingerprint -> repo once (~millions) so the multi-billion-row reach join hits one small table.
CREATE TEMP TABLE fp_repo CLUSTER BY fp AS
SELECT n.fp AS fp, dr.repo AS repo
FROM names n JOIN dep_repos dr ON dr.Name = n.name;

-- 7. per-subject counts -> purl. Self-pairs (cycles) excluded so a package is never its own dependent.
CREATE TEMP TABLE _export_data AS
WITH
purl_map AS (
  SELECT Name, ANY_VALUE(REGEXP_REPLACE(Purl, r'@[^@]+$', '')) AS purl
  FROM \`bigquery-public-data.deps_dev_v1.PackageVersionsLatest\`
  WHERE System = '${system}' AND Purl IS NOT NULL AND Name NOT LIKE '%>%'
  GROUP BY Name
),
direct AS (
  SELECT subj, COUNT(DISTINCT dep) AS dependent_count
  FROM fe WHERE dep != subj GROUP BY subj
),
total AS (
  SELECT subj, COUNT(DISTINCT dep) AS reach_count
  FROM reach WHERE dep != subj GROUP BY subj
),
repos AS (
  SELECT r.subj, COUNT(DISTINCT fr.repo) AS dependent_repos_count
  FROM reach r JOIN fp_repo fr ON fr.fp = r.dep
  WHERE r.dep != r.subj
  GROUP BY r.subj
)
SELECT
  pm.purl                            AS purl,
  d.dependent_count                  AS dependent_count,
  t.reach_count - d.dependent_count  AS transitive_dependent_count,
  COALESCE(rp.dependent_repos_count, 0) AS dependent_repos_count
FROM total t
JOIN names nm      ON nm.fp = t.subj
JOIN purl_map pm   ON pm.Name = nm.name
JOIN direct d      ON d.subj = t.subj
LEFT JOIN repos rp ON rp.subj = t.subj;
`
}

export function buildGoDependentCountsSql(snapshotDate: string): string {
  const edgesSql = `
SELECT DISTINCT g.Name AS dep, d.Name AS subj
FROM \`bigquery-public-data.deps_dev_v1.GoRequirementsLatest\` g,
UNNEST(g.DirectDependencies) AS d
WHERE d.Name NOT LIKE '%>%' AND g.Name NOT LIKE '%>%' AND g.Name != d.Name`
  return buildClosureScript('GO', edgesSql, snapshotDate)
}

export function buildNugetDependentCountsSql(snapshotDate: string): string {
  const edgesSql = `
SELECT DISTINCT n.Name AS dep, dp.Name AS subj
FROM \`bigquery-public-data.deps_dev_v1.NuGetRequirementsLatest\` n,
UNNEST(n.DependencyGroups) AS grp,
UNNEST(grp.Dependencies) AS dp
WHERE dp.Name NOT LIKE '%>%' AND n.Name NOT LIKE '%>%' AND n.Name != dp.Name`
  return buildClosureScript('NUGET', edgesSql, snapshotDate)
}

// RubyGems has no reverse-dependent/transitive graph in deps.dev either (Dependents contains only
// {NPM, MAVEN, PYPI, CARGO} — verified via BQ 2026-07-13), so — like GO/NUGET — the exact reverse
// transitive closure is computed from the runtime-dependency manifest (RubyGemsRequirementsLatest),
// matching the RuntimeDependencies-only scope used for the forward package_dependencies ingest.
export function buildRubygemsDependentCountsSql(snapshotDate: string): string {
  const edgesSql = `
SELECT DISTINCT r.Name AS dep, d.Name AS subj
FROM \`bigquery-public-data.deps_dev_v1.RubyGemsRequirementsLatest\` r,
UNNEST(r.RuntimeDependencies) AS d
WHERE d.Name NOT LIKE '%>%' AND r.Name NOT LIKE '%>%' AND r.Name != d.Name`
  return buildClosureScript('RUBYGEMS', edgesSql, snapshotDate)
}
