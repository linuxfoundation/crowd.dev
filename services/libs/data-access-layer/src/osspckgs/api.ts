import { QueryExecutor } from '../queryExecutor'

import {
  SEVERITY_RANK_EXPR,
  STEWARD_DISPLAY_NAME_METADATA,
  STEWARD_MENTIONED_JOIN,
} from './sqlFragments'

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

// TODO[deprecate]: rename to AkritesMetrics once /v1/ossprey is removed
export interface OsspreyMetrics {
  totalPackages: number
  criticalPackages: number
  coveragePercent: number
  coverageTrend: number | null
  activeStewards: number
  unassignedCritical: number
  needsAttention: number
  escalated: number
}

// TODO[deprecate]: rename to getAkritesMetrics once /v1/ossprey is removed
export async function getOsspreyMetrics(qx: QueryExecutor): Promise<OsspreyMetrics> {
  const [counts, stewardRow]: [
    {
      totalPackages: string
      criticalPackages: string
      covered: string
      needsAttention: string
      escalated: string
      unassignedCritical: string
    },
    { count: string },
  ] = await Promise.all([
    qx.selectOne(`
      SELECT
        (SELECT reltuples::bigint::text FROM pg_class WHERE relname = 'packages')            AS "totalPackages",
        COUNT(*)::text                                                                     AS "criticalPackages",
        COUNT(*) FILTER (WHERE s.status IN ('assessing','active','needs_attention'))::text AS covered,
        COUNT(*) FILTER (WHERE s.status = 'needs_attention')::text                        AS "needsAttention",
        COUNT(*) FILTER (WHERE s.status = 'escalated')::text                              AS escalated,
        COUNT(*) FILTER (WHERE s.id IS NULL OR s.status IS NULL OR s.status IN ('unassigned','open','blocked','inactive'))::text AS "unassignedCritical"
      FROM packages p
      LEFT JOIN stewardships s ON s.package_id = p.id
      WHERE p.is_critical = true
    `),
    qx.selectOne(`
      SELECT COUNT(DISTINCT ss.user_id)::text AS count
      FROM stewardship_stewards ss
      JOIN stewardships s ON s.id = ss.stewardship_id
      WHERE ss.deleted_at IS NULL
        AND s.status != 'inactive'
    `),
  ])

  const total = parseInt(counts.criticalPackages, 10)
  const covered = parseInt(counts.covered, 10)

  return {
    totalPackages: parseInt(counts.totalPackages, 10),
    criticalPackages: total,
    coveragePercent: total > 0 ? Math.round((covered / total) * 1000) / 10 : 0,
    coverageTrend: null, // TODO: requires snapshot mechanism or stewardship_activity timestamp analysis
    activeStewards: parseInt(stewardRow.count, 10),
    unassignedCritical: parseInt(counts.unassignedCritical, 10),
    needsAttention: parseInt(counts.needsAttention, 10),
    escalated: parseInt(counts.escalated, 10),
  }
}

export interface StewardEntry {
  userId: string
  username: string | null
  displayName: string | null
  role: string
  assignedAt: string
}

export interface PackageListRow {
  purl: string
  name: string
  ecosystem: string
  criticalityScore: number | null
  stewardshipId: string | null
  stewardshipStatus: string | null
  openVulns: number
  maxVulnSeverity: 'critical' | 'high' | 'medium' | 'low' | null
  maintainerCount: number
  scorecardScore: number | null
  latestReleaseAt: Date | null
  lastActivityType?: string | null
  lastActivityContent?: string | null
  lastActivityMetadata?: Record<string, unknown> | null
  lastActivityAt?: Date | null
  stewards?: StewardEntry[]
  total: string
}

export type HealthBand = 'healthy' | 'fair' | 'concerning' | 'critical'
export type VulnSeverityFilter = 'any' | 'high' | 'critical' | 'none'

export function computeHealthBand(scorecardScore: number | null): HealthBand {
  if (scorecardScore === null || scorecardScore < 3.0) return 'critical'
  if (scorecardScore < 5.0) return 'concerning'
  if (scorecardScore < 7.0) return 'fair'
  return 'healthy'
}

export function buildHealthBandCondition(scoreColumn: string, band: HealthBand): string {
  if (band === 'healthy') return `${scoreColumn} >= 7.0`
  if (band === 'fair') return `${scoreColumn} >= 5.0 AND ${scoreColumn} < 7.0`
  if (band === 'concerning') return `${scoreColumn} >= 3.0 AND ${scoreColumn} < 5.0`
  return `(${scoreColumn} IS NULL OR ${scoreColumn} < 3.0)`
}

export interface ListPackagesOptions {
  page: number
  pageSize: number
  ecosystem?: string
  lifecycle?: string
  name?: string
  purl?: string
  status?: string
  healthBand?: HealthBand
  vulnSeverity?: VulnSeverityFilter
  staleOnly: boolean
  unstewardedOnly: boolean
  busFactor1Only: boolean
  includeStewards?: boolean
  includeLastActivity?: boolean
  sortBy: 'name' | 'impact' | 'openVulns' | 'health' | 'risk'
  sortDir: 'asc' | 'desc'
}

export { SEVERITY_RANK_EXPR } from './sqlFragments'

const STALE_MONTHS = 18

export interface PackageStatusCounts {
  all: number
  unassigned: number
  open: number
  assessing: number
  active: number
  needs_attention: number
  escalated: number
  blocked: number
  inactive: number
}

export type StatusCountsOptions = Omit<
  ListPackagesOptions,
  'status' | 'page' | 'pageSize' | 'sortBy' | 'sortDir'
>

const ALL_STEWARDSHIP_STATUSES = [
  'unassigned',
  'open',
  'assessing',
  'active',
  'needs_attention',
  'escalated',
  'blocked',
  'inactive',
] as const

/**
 * Returns per-status counts for the same filter set used by listPackagesForApi, but without
 * the status filter so the tab bar always shows all status breakdowns for the active filters.
 */
export async function getPackageStatusCounts(
  qx: QueryExecutor,
  opts: StatusCountsOptions,
): Promise<PackageStatusCounts> {
  const conditions: string[] = ['p.is_critical = true']
  const params: Record<string, unknown> = {}

  if (opts.ecosystem) {
    conditions.push('p.ecosystem = $(ecosystem)')
    params.ecosystem = opts.ecosystem
  }

  if (opts.name) {
    conditions.push('p.name ILIKE $(name)')
    params.name = `%${opts.name}%`
  }

  if (opts.purl) {
    conditions.push('p.purl ILIKE $(purl)')
    params.purl = `%${opts.purl}%`
  }

  if (opts.lifecycle) {
    conditions.push('p.status IS NOT NULL')
  }

  if (opts.healthBand) {
    if (opts.healthBand === 'healthy') {
      conditions.push('r_sc.scorecard_score >= 7.0')
    } else if (opts.healthBand === 'fair') {
      conditions.push('r_sc.scorecard_score >= 5.0 AND r_sc.scorecard_score < 7.0')
    } else if (opts.healthBand === 'concerning') {
      conditions.push('r_sc.scorecard_score >= 3.0 AND r_sc.scorecard_score < 5.0')
    } else {
      conditions.push('(r_sc.scorecard_score IS NULL OR r_sc.scorecard_score < 3.0)')
    }
  }

  if (opts.vulnSeverity) {
    if (opts.vulnSeverity === 'any') {
      conditions.push('ap_counts.cnt > 0')
    } else if (opts.vulnSeverity === 'none') {
      conditions.push('ap_counts.cnt = 0')
    } else if (opts.vulnSeverity === 'high') {
      conditions.push('ap_severity.max_rank >= 3')
    } else {
      conditions.push('ap_severity.max_rank >= 4')
    }
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
    conditions.push(`pm_counts.cnt = 1`)
  }

  const where = `WHERE ${conditions.join(' AND ')}`

  const rows: { status: string; count: number }[] = await qx.select(
    `
    SELECT
      COALESCE(s.status, 'unassigned') AS status,
      COUNT(*)::int AS count
    FROM packages p
    LEFT JOIN stewardships s ON s.package_id = p.id
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM advisory_packages WHERE package_id = p.id
    ) ap_counts ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM package_maintainers pm WHERE pm.package_id = p.id
    ) pm_counts ON true
    LEFT JOIN LATERAL (
      SELECT ${SEVERITY_RANK_EXPR} AS max_rank
      FROM advisory_packages ap
      JOIN advisories a ON a.id = ap.advisory_id
      WHERE ap.package_id = p.id
    ) ap_severity ON true
    LEFT JOIN LATERAL (
      SELECT r.scorecard_score
      FROM package_repos pr
      JOIN repos r ON r.id = pr.repo_id
      WHERE pr.package_id = p.id
      ORDER BY pr.confidence DESC
      LIMIT 1
    ) r_sc ON true
    ${where}
    GROUP BY COALESCE(s.status, 'unassigned')
    `,
    params,
  )

  const countsMap: Record<string, number> = {}
  let all = 0
  for (const row of rows) {
    countsMap[row.status] = row.count
    all += row.count
  }

  const result: PackageStatusCounts = {
    all,
    unassigned: 0,
    open: 0,
    assessing: 0,
    active: 0,
    needs_attention: 0,
    escalated: 0,
    blocked: 0,
    inactive: 0,
  }
  for (const status of ALL_STEWARDSHIP_STATUSES) {
    result[status] = countsMap[status] ?? 0
  }
  return result
}

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

  if (opts.name) {
    conditions.push('p.name ILIKE $(name)')
    params.name = `%${opts.name}%`
  }

  if (opts.purl) {
    conditions.push('p.purl ILIKE $(purl)')
    params.purl = `%${opts.purl}%`
  }

  // Exclude packages with no registry status when a lifecycle filter is active.
  // Full lifecycle column support is pending; this prevents null-lifecycle rows
  // from leaking into filtered results.
  if (opts.lifecycle) {
    conditions.push('p.status IS NOT NULL')
  }

  if (opts.status) {
    // 'unassigned' includes packages that have no stewardship row yet
    if (opts.status === 'unassigned') {
      conditions.push(`(s.status = 'unassigned' OR s.id IS NULL)`)
    } else {
      conditions.push('s.status = $(status)')
      params.status = opts.status
    }
  }

  if (opts.healthBand) {
    // scorecard_score is 0–10; multiply by 10 to get 0–100 health score.
    // Packages with no linked repo (scorecard_score IS NULL) fall into 'critical'.
    if (opts.healthBand === 'healthy') {
      conditions.push('r_sc.scorecard_score >= 7.0')
    } else if (opts.healthBand === 'fair') {
      conditions.push('r_sc.scorecard_score >= 5.0 AND r_sc.scorecard_score < 7.0')
    } else if (opts.healthBand === 'concerning') {
      conditions.push('r_sc.scorecard_score >= 3.0 AND r_sc.scorecard_score < 5.0')
    } else {
      // critical band includes no-repo packages (NULL scorecard)
      conditions.push('(r_sc.scorecard_score IS NULL OR r_sc.scorecard_score < 3.0)')
    }
  }

  if (opts.vulnSeverity) {
    if (opts.vulnSeverity === 'any') {
      conditions.push('ap_counts.cnt > 0')
    } else if (opts.vulnSeverity === 'none') {
      conditions.push('ap_counts.cnt = 0')
    } else if (opts.vulnSeverity === 'high') {
      conditions.push('ap_severity.max_rank >= 3')
    } else {
      // critical: worst severity is CRITICAL only
      conditions.push('ap_severity.max_rank >= 4')
    }
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

  let sortExpr: string
  if (opts.sortBy === 'impact') {
    sortExpr = 'p.impact'
  } else if (opts.sortBy === 'openVulns') {
    sortExpr = '"openVulns"'
  } else if (opts.sortBy === 'health') {
    sortExpr = 'r_sc.scorecard_score'
  } else if (opts.sortBy === 'risk') {
    // Composite risk score: impact + health deficit + vuln exposure + bus factor + staleness
    sortExpr = `(
      COALESCE(p.impact, 0) * 100
      + (100.0 - COALESCE(r_sc.scorecard_score, 0) * 10) * 0.8
      + COALESCE(ap_severity.max_rank, 0) * 15
      + COALESCE(ap_counts.cnt, 0) * 4
      + CASE WHEN pm_counts.cnt = 1 THEN 20 ELSE 0 END
      + CASE WHEN (p.latest_release_at IS NULL OR p.latest_release_at < NOW() - INTERVAL '${STALE_MONTHS} months') THEN 15 ELSE 0 END
    )`
  } else {
    sortExpr = 'LOWER(p.name)'
  }
  const sortDir = opts.sortDir === 'desc' ? 'DESC' : 'ASC'

  // Separate paginated params from filter-only params used by the fallback COUNT query
  const queryParams: Record<string, unknown> = {
    ...params,
    limit: opts.pageSize,
    offset: (opts.page - 1) * opts.pageSize,
  }

  // Float exact name/purl matches to the top while still returning all partial matches via ILIKE.
  const exactParts: string[] = []
  if (opts.name) {
    exactParts.push('p.name ILIKE $(name_exact)')
    queryParams.name_exact = opts.name
  }
  if (opts.purl) {
    exactParts.push('p.purl ILIKE $(purl_exact)')
    queryParams.purl_exact = opts.purl
  }
  const exactSort =
    exactParts.length > 0 ? `CASE WHEN ${exactParts.join(' OR ')} THEN 0 ELSE 1 END` : ''

  // Laterals needed for WHERE filter conditions — included in both the main query and the COUNT fallback.
  const filterLaterals = `
    LEFT JOIN stewardships s ON s.package_id = p.id
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM advisory_packages WHERE package_id = p.id
    ) ap_counts ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM package_maintainers pm WHERE pm.package_id = p.id
    ) pm_counts ON true
    LEFT JOIN LATERAL (
      SELECT ${SEVERITY_RANK_EXPR} AS max_rank
      FROM advisory_packages ap
      JOIN advisories a ON a.id = ap.advisory_id
      WHERE ap.package_id = p.id
    ) ap_severity ON true
    LEFT JOIN LATERAL (
      SELECT r.scorecard_score
      FROM package_repos pr
      JOIN repos r ON r.id = pr.repo_id
      WHERE pr.package_id = p.id
      ORDER BY pr.confidence DESC
      LIMIT 1
    ) r_sc ON true`

  // Additional laterals for SELECT output only — not needed in the COUNT fallback.
  const stewardsLateral =
    opts.includeStewards === true
      ? `
    LEFT JOIN LATERAL (
      SELECT COALESCE(
        json_agg(
          json_build_object(
            'userId', ss.user_id,
            'username', st.username,
            'displayName', st.display_name,
            'role', ss.role,
            'assignedAt', ss.assigned_at
          )
          ORDER BY ss.assigned_at ASC
        ) FILTER (WHERE ss.id IS NOT NULL),
        '[]'::json
      ) AS stewards
      FROM stewardship_stewards ss
      LEFT JOIN stewards st ON st.user_id = ss.user_id
      WHERE ss.stewardship_id = s.id
        AND ss.deleted_at IS NULL
    ) ss_agg ON true`
      : ''

  const lastActLateral =
    opts.includeLastActivity === true
      ? `
    LEFT JOIN LATERAL (
      SELECT sa.activity_type, sa.content, sa.created_at,
        ${STEWARD_DISPLAY_NAME_METADATA} AS metadata
      FROM stewardship_activity sa
      ${STEWARD_MENTIONED_JOIN}
      WHERE sa.stewardship_id = s.id
      ORDER BY sa.created_at DESC
      LIMIT 1
    ) last_act ON true`
      : ''

  const laterals = `${filterLaterals}
    ${stewardsLateral}
    ${lastActLateral}`

  const rows: PackageListRow[] = await qx.select(
    `
    SELECT
      p.purl,
      p.name,
      p.ecosystem,
      p.impact AS "criticalityScore",
      s.id::text AS "stewardshipId",
      s.status AS "stewardshipStatus",
      COALESCE(ap_counts.cnt, 0) AS "openVulns",
      CASE ap_severity.max_rank
        WHEN 4 THEN 'critical'
        WHEN 3 THEN 'high'
        WHEN 2 THEN 'medium'
        WHEN 1 THEN 'low'
        ELSE NULL
      END AS "maxVulnSeverity",
      pm_counts.cnt AS "maintainerCount",
      r_sc.scorecard_score AS "scorecardScore",
      p.latest_release_at AS "latestReleaseAt",
      ${opts.includeLastActivity === true ? `last_act.activity_type AS "lastActivityType", last_act.content AS "lastActivityContent", last_act.metadata AS "lastActivityMetadata", last_act.created_at AS "lastActivityAt",` : ''}
      ${opts.includeStewards === true ? 'ss_agg.stewards AS stewards,' : ''}
      COUNT(*) OVER() AS total
    FROM packages p
    ${laterals}
    ${where}
    ORDER BY ${[exactSort, `${sortExpr} ${sortDir} NULLS LAST`, `p.purl ${sortDir}`].filter(Boolean).join(', ')}
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
    // Use filterLaterals (not laterals) — stewards/last_act laterals are SELECT-only and not needed here.
    const countRow: { count: string } = await qx.selectOne(
      `SELECT COUNT(*)::text AS count
       FROM packages p
       ${filterLaterals}
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
  stewardshipId: string | null
  stewardshipStatus: string | null
  stewardshipLastStatusAt: Date | null
  stewardshipResolutionPath: string | null
  stewardshipStatusNote: string | null
  stewardshipOrigin: string | null
  stewardshipVersion: number | null
  stewardshipOpenedAt: Date | null
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
      s.id::text AS "stewardshipId",
      s.status AS "stewardshipStatus",
      s.last_status_at AS "stewardshipLastStatusAt",
      s.resolution_path AS "stewardshipResolutionPath",
      s.status_note AS "stewardshipStatusNote",
      s.origin AS "stewardshipOrigin",
      s.version AS "stewardshipVersion",
      s.opened_at AS "stewardshipOpenedAt",
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

export interface ScatterPoint {
  purl: string
  name: string
  criticalityScore: number
  healthScore: number
  healthBand: HealthBand
  stewardshipStatus: string | null
  stewardshipId: string | null
  openVulns: number
  advisoryCount: number
}

export async function listPackagesForScatter(
  qx: QueryExecutor,
  options: { status?: string[]; ecosystem?: string } = {},
): Promise<ScatterPoint[]> {
  const { status, ecosystem } = options

  // 'unassigned' covers packages with no stewardship row (s.id IS NULL) in addition
  // to rows explicitly marked unassigned. All other statuses filter via s.status = ANY(...).
  // The query always uses LEFT JOIN — the filter is applied in the WHERE clause, not the join.
  const includesUnassigned = status?.includes('unassigned') ?? false
  const statusFilter = status?.length
    ? `AND (s.status = ANY($(status)::text[])${includesUnassigned ? ' OR s.id IS NULL' : ''})`
    : ''
  const ecosystemFilter = ecosystem ? `AND p.ecosystem = $(ecosystem)` : ''

  const rows: Array<{
    purl: string
    name: string
    criticalityScore: number
    healthScore: number
    scorecardScoreRaw: number | null
    stewardshipId: string | null
    stewardshipStatus: string | null
    openVulns: number
  }> = await qx.select(
    `
    SELECT
      p.purl,
      p.name,
      ROUND(COALESCE(p.impact, 0) * 100)::int             AS "criticalityScore",
      ROUND(COALESCE(r_sc.scorecard_score, 0) * 10)::int  AS "healthScore",
      r_sc.scorecard_score                                 AS "scorecardScoreRaw",
      s.id::text                                           AS "stewardshipId",
      s.status                                             AS "stewardshipStatus",
      COALESCE(ap_counts.cnt, 0)                           AS "openVulns"
    FROM packages p
    LEFT JOIN stewardships s ON s.package_id = p.id
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM advisory_packages WHERE package_id = p.id
    ) ap_counts ON true
    LEFT JOIN LATERAL (
      SELECT r.scorecard_score
      FROM package_repos pr
      JOIN repos r ON r.id = pr.repo_id
      WHERE pr.package_id = p.id
      ORDER BY pr.confidence DESC
      LIMIT 1
    ) r_sc ON true
    WHERE p.is_critical = true
    ${statusFilter}
    ${ecosystemFilter}
    ORDER BY p.impact DESC NULLS LAST, p.purl ASC
    LIMIT 2000
    `,
    { status, ecosystem },
  )

  return rows.map((r) => ({
    purl: r.purl,
    name: r.name,
    criticalityScore: r.criticalityScore,
    healthScore: r.healthScore,
    healthBand: computeHealthBand(r.scorecardScoreRaw),
    stewardshipStatus: r.stewardshipStatus ?? null,
    stewardshipId: r.stewardshipId ?? null,
    openVulns: r.openVulns,
    advisoryCount: r.openVulns,
  }))
}

export async function getAdvisoriesByPackageId(
  qx: QueryExecutor,
  packageId: string,
  opts?: { page: number; pageSize: number },
): Promise<{ rows: AdvisoryRow[]; total: number }> {
  const cte = `
    WITH advisory_data AS (
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
    )
  `

  const paginationClause = opts ? `LIMIT $(limit) OFFSET $(offset)` : ''

  const rows = (await qx.select(
    `${cte} SELECT * FROM advisory_data ORDER BY "osvId" ${paginationClause}`,
    { packageId, limit: opts?.pageSize, offset: opts ? (opts.page - 1) * opts.pageSize : 0 },
  )) as AdvisoryRow[]

  if (!opts) {
    return { rows, total: rows.length }
  }

  const countResult = (await qx.selectOne(`${cte} SELECT COUNT(*) AS total FROM advisory_data`, {
    packageId,
  })) as { total: string }

  return { rows, total: Number(countResult.total) }
}
