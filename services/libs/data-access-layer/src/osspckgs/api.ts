import { QueryExecutor } from '../queryExecutor'

export interface PackageMetrics {
  totalPackages: number
  criticalPackages: number
}

export async function getPackageMetrics(qx: QueryExecutor): Promise<PackageMetrics> {
  const row: { total: string; critical: string } = await qx.selectOne(`
    SELECT
      COUNT(*) AS total,
      -- TODO: confirm with product whether "critical" here means health=critical, not has_critical_vulnerability
      COUNT(*) FILTER (WHERE has_critical_vulnerability = true) AS critical
    FROM packages
    WHERE is_critical = true
  `)
  return {
    totalPackages: parseInt(row.total, 10),
    criticalPackages: parseInt(row.critical, 10),
  }
}

export interface PackageStewardshipRow {
  purl: string
  name: string
  ecosystem: string
  criticalityScore: number | null
  stewardshipStatus: string | null
}

export async function getPackagesByStewardshipPurls(
  qx: QueryExecutor,
  purls: string[],
): Promise<PackageStewardshipRow[]> {
  if (purls.length === 0) return []
  return qx.select(
    `
    SELECT
      p.purl,
      p.name,
      p.ecosystem,
      p.impact AS "criticalityScore",
      s.status AS "stewardshipStatus"
    FROM packages p
    LEFT JOIN stewardships s ON s.package_id = p.id
    WHERE p.purl = ANY($(purls))
    `,
    { purls },
  )
}

export interface PackageListRow {
  purl: string
  name: string
  ecosystem: string
  criticalityScore: number | null
  stewardshipStatus: string | null
  openVulns: number
  maintainerCount: number
  total: string
}

export interface ListPackagesOptions {
  page: number
  pageSize: number
  ecosystem?: string
  staleOnly: boolean
  unstewardedOnly: boolean
  sortBy: 'name' | 'impact' | 'openVulns'
  sortDir: 'asc' | 'desc'
}

const STALE_MONTHS = 18

export async function listPackagesForApi(
  qx: QueryExecutor,
  opts: ListPackagesOptions,
): Promise<{ rows: PackageListRow[]; total: number }> {
  const conditions: string[] = ['p.is_critical = true']
  const params: Record<string, unknown> = {}

  if (opts.ecosystem) {
    conditions.push('p.ecosystem = $(ecosystem)')
    params.ecosystem = opts.ecosystem
  }

  if (opts.staleOnly) {
    conditions.push(
      `(p.latest_release_at IS NULL OR p.latest_release_at < NOW() - INTERVAL '${STALE_MONTHS} months')`,
    )
  }

  if (opts.unstewardedOnly) {
    conditions.push(`(s.status = 'unassigned' OR s.id IS NULL)`)
  }

  if (opts.busFactor1Only) {
    // References pm_counts LATERAL join below — computed once, used in both WHERE and SELECT
    conditions.push(`pm_counts.cnt = 1`)
  }

  const where = `WHERE ${conditions.join(' AND ')}`

  // health is a v2 field — fall back to name sort
  let sortExpr: string
  if (opts.sortBy === 'impact') sortExpr = 'p.impact'
  else if (opts.sortBy === 'openVulns') sortExpr = '"openVulns"'
  else sortExpr = 'LOWER(p.name)'
  const sortDir = opts.sortDir === 'desc' ? 'DESC' : 'ASC'

  // Separate paginated params from filter-only params used by the fallback COUNT query
  const queryParams = { ...params, limit: opts.pageSize, offset: (opts.page - 1) * opts.pageSize }

  const rows: PackageListRow[] = await qx.select(
    `
    SELECT
      p.purl,
      p.name,
      p.ecosystem,
      p.impact AS "criticalityScore",
      s.status AS "stewardshipStatus",
      COALESCE(ap_counts.cnt, 0) AS "openVulns",
      pm_counts.cnt AS "maintainerCount",
      COUNT(*) OVER() AS total
    FROM packages p
    LEFT JOIN stewardships s ON s.package_id = p.id
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM advisory_packages WHERE package_id = p.id
    ) ap_counts ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM package_maintainers pm WHERE pm.package_id = p.id
    ) pm_counts ON true
    ${where}
    ORDER BY ${sortExpr} ${sortDir} NULLS LAST, p.purl ${sortDir}
    LIMIT $(limit) OFFSET $(offset)
    `,
    queryParams,
  )

  let total: number
  if (rows.length > 0) {
    total = parseInt(rows[0].total, 10)
  } else {
    // Window function returns no rows when the page is beyond the result set.
    // Fall back to a separate COUNT so the caller always gets the real total.
    const countRow: { count: string } = await qx.selectOne(
      `SELECT COUNT(*)::text AS count
       FROM packages p
       LEFT JOIN stewardships s ON s.package_id = p.id
       LEFT JOIN LATERAL (
         SELECT COUNT(*)::int AS cnt FROM package_maintainers pm WHERE pm.package_id = p.id
       ) pm_counts ON true
       ${where}`,
      params,
    )
    total = parseInt(countRow.count, 10)
  }

  return { rows, total }
}

export interface PackageDetailRow {
  id: string
  purl: string
  name: string
  ecosystem: string
  criticalityScore: number | null
  dependentPackagesCount: number | null
  dependentReposCount: number | null
  latestVersion: string | null
  versionsCount: number | null
  latestReleaseAt: Date | null
  declaredRepositoryUrl: string | null
  repositoryUrl: string | null
  hasCriticalVulnerability: boolean
  stewardshipStatus: string | null
  stewardshipLastStatusAt: Date | null
  // from package_repos + repos
  repoUrl: string | null
  repoMappingConfidence: number | null
  repoLastCommitAt: Date | null
  scorecardScore: number | null
  hasSecurityFile: boolean | null
  hasSecurityPolicy: boolean | null
  branchProtectionEnabled: boolean | null
  // from downloads_last_30d
  downloadsLast30d: string | null
  maintainerCount: number
  transitiveReach: number | null
}

export interface AdvisoryRow {
  osvId: string
  severity: string
  resolution: 'open' | 'patched' | null
}

export async function getPackageDetailByPurl(
  qx: QueryExecutor,
  purl: string,
): Promise<PackageDetailRow | null> {
  return qx.selectOneOrNone(
    `
    SELECT
      p.id::text AS id,
      p.purl,
      p.name,
      p.ecosystem,
      p.impact AS "criticalityScore",
      p.dependent_count AS "dependentPackagesCount",
      p.dependent_repos_count AS "dependentReposCount",
      p.latest_version AS "latestVersion",
      p.versions_count AS "versionsCount",
      p.latest_release_at AS "latestReleaseAt",
      p.declared_repository_url AS "declaredRepositoryUrl",
      p.repository_url AS "repositoryUrl",
      p.has_critical_vulnerability AS "hasCriticalVulnerability",
      s.status AS "stewardshipStatus",
      s.last_status_at AS "stewardshipLastStatusAt",
      -- best repo link (highest confidence, prefer declared)
      r.url AS "repoUrl",
      pr.confidence AS "repoMappingConfidence",
      r.last_commit_at AS "repoLastCommitAt",
      r.scorecard_score AS "scorecardScore",
      r.security_file_enabled AS "hasSecurityFile",
      r.security_policy_enabled AS "hasSecurityPolicy",
      r.branch_protection_enabled AS "branchProtectionEnabled",
      -- latest 30-day download count
      (
        SELECT d.count::text
        FROM downloads_last_30d d
        WHERE d.purl = p.purl
        ORDER BY d.end_date DESC
        LIMIT 1
      ) AS "downloadsLast30d",
      (SELECT COUNT(*)::int FROM package_maintainers pm WHERE pm.package_id = p.id) AS "maintainerCount",
      -- TODO: precompute and store in packages.transitive_reach_prank; full window scan is too slow at npm scale (~24s for npm)
      -- (
      --   SELECT r.prank
      --   FROM (
      --     SELECT purl, PERCENT_RANK() OVER (PARTITION BY ecosystem ORDER BY transitive_dependent_count ASC NULLS FIRST) AS prank
      --     FROM packages
      --     WHERE ecosystem = p.ecosystem
      --   ) r
      --   WHERE r.purl = p.purl
      -- ) AS "transitiveReach"
      NULL::float AS "transitiveReach"
    FROM packages p
    LEFT JOIN stewardships s ON s.package_id = p.id
    LEFT JOIN LATERAL (
      SELECT pr2.repo_id, pr2.confidence
      FROM package_repos pr2
      WHERE pr2.package_id = p.id
      ORDER BY pr2.confidence DESC, (pr2.source = 'declared') DESC
      LIMIT 1
    ) pr ON true
    LEFT JOIN repos r ON r.id = pr.repo_id
    WHERE p.purl = $(purl)
    `,
    { purl },
  )
}

export async function getAdvisoriesByPackageId(
  qx: QueryExecutor,
  packageId: string,
): Promise<AdvisoryRow[]> {
  return qx.select(
    `
    SELECT
      a.osv_id AS "osvId",
      LOWER(a.severity) AS severity,
      CASE
        WHEN p.latest_version IS NULL THEN NULL
        WHEN COUNT(ar.id) = 0 THEN NULL
        -- TODO: text comparison is lexicographic, not semver — '1.9.0' >= '1.10.0' is TRUE here.
        -- Replace with a proper semver comparison function when one is available in the DB.
        WHEN BOOL_AND(
          CASE
            WHEN ar.fixed_version IS NULL AND ar.last_affected IS NULL THEN FALSE
            WHEN ar.fixed_version IS NOT NULL AND p.latest_version >= ar.fixed_version THEN TRUE
            WHEN ar.fixed_version IS NOT NULL THEN FALSE
            WHEN ar.last_affected IS NOT NULL AND p.latest_version > ar.last_affected THEN TRUE
            ELSE FALSE
          END
        ) THEN 'patched'
        ELSE 'open'
      END AS resolution
    FROM advisory_packages ap
    JOIN advisories a ON a.id = ap.advisory_id
    LEFT JOIN advisory_affected_ranges ar ON ar.advisory_package_id = ap.id
    JOIN packages p ON p.id = ap.package_id
    WHERE ap.package_id = $(packageId)::bigint
    GROUP BY a.osv_id, a.severity, p.latest_version
    `,
    { packageId },
  )
}
