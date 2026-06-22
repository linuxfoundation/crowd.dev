import { QueryExecutor } from '../queryExecutor'

import { SEVERITY_RANK_EXPR } from './api'

export interface StewardshipRecord {
  id: string
  packageId: string
  status: string
  origin: string
  version: number
  openedAt: string | null
  lastStatusAt: string | null
  inactiveReason: string | null
  resolutionPath: string | null
  statusNote: string | null
  createdAt: string
  updatedAt: string
}

export interface StewardshipStewardRecord {
  id: string
  stewardshipId: string
  userId: string
  username: string | null
  displayName: string | null
  role: string
  assignedAt: string
  assignedBy: string | null
}

/**
 * Returns a page of critical package ids that do not yet have a stewardship row,
 * ordered by id ascending. Used as the cursor-based pagination source for the
 * stewardship backfill.
 */
export async function listCriticalPackagesWithoutStewardship(
  qx: QueryExecutor,
  options: { afterId: number; limit: number },
): Promise<number[]> {
  // pg returns BIGINT columns as strings; Number() is safe here because
  // package ids are well within JS safe-integer range.
  const rows: Array<{ id: string | number }> = await qx.select(
    `
    SELECT p.id
    FROM packages p
    LEFT JOIN stewardships s ON s.package_id = p.id
    WHERE p.is_critical = true
      AND p.id > $(afterId)
      AND s.package_id IS NULL
    ORDER BY p.id ASC
    LIMIT $(limit)
    `,
    options,
  )
  return rows.map((r) => Number(r.id))
}

/**
 * Inserts one unassigned stewardship row per package id. Idempotent:
 * ON CONFLICT DO NOTHING skips ids that already have a row.
 * Returns the number of rows actually inserted.
 *
 * Re-checks is_critical at insert time to guard against concurrent criticality
 * changes between the SELECT and INSERT phases.
 */
export async function insertUnassignedStewardships(
  qx: QueryExecutor,
  packageIds: number[],
): Promise<number> {
  if (packageIds.length === 0) return 0
  const result: { count: string } = await qx.selectOne(
    `
    WITH ins AS (
      INSERT INTO stewardships (package_id, status, origin, opened_at, last_status_at)
      SELECT p.id, 'unassigned', 'auto_imported', NOW(), NOW()
      FROM packages p
      WHERE p.id = ANY($(packageIds)::bigint[])
        AND p.is_critical = true
      ON CONFLICT (package_id) DO NOTHING
      RETURNING 1
    )
    SELECT COUNT(*) AS count FROM ins
    `,
    { packageIds },
  )
  return parseInt(result.count, 10)
}

function toIso(v: unknown): string {
  return v instanceof Date ? v.toISOString() : String(v)
}

async function fetchActiveStewards(
  qx: QueryExecutor,
  stewardshipId: number,
): Promise<StewardshipStewardRecord[]> {
  const rows: Array<Record<string, unknown>> = await qx.select(
    `SELECT ss.id, ss.stewardship_id, ss.user_id, ss.role, ss.assigned_at, ss.assigned_by,
            st.username, st.display_name
     FROM stewardship_stewards ss
     LEFT JOIN stewards st ON st.user_id = ss.user_id
     WHERE ss.stewardship_id = $(stewardshipId)
       AND ss.deleted_at IS NULL
     ORDER BY ss.assigned_at ASC`,
    { stewardshipId },
  )
  return rows.map(mapStewardStewardRow)
}

function mapStewardStewardRow(row: Record<string, unknown>): StewardshipStewardRecord {
  return {
    id: String(row.id),
    stewardshipId: String(row.stewardship_id),
    userId: String(row.user_id),
    username: (row.username as string) ?? null,
    displayName: (row.display_name as string) ?? null,
    role: String(row.role),
    assignedAt: toIso(row.assigned_at),
    assignedBy: (row.assigned_by as string) ?? null,
  }
}

function mapStewardshipRow(row: Record<string, unknown>): StewardshipRecord {
  return {
    id: String(row.id),
    packageId: String(row.package_id),
    status: row.status as string,
    origin: row.origin as string,
    version: Number(row.version),
    openedAt: row.opened_at ? toIso(row.opened_at) : null,
    lastStatusAt: row.last_status_at ? toIso(row.last_status_at) : null,
    inactiveReason: row.inactive_reason ? String(row.inactive_reason) : null,
    resolutionPath: row.resolution_path ? String(row.resolution_path) : null,
    statusNote: row.status_note ? String(row.status_note) : null,
    createdAt: toIso(row.created_at),
    updatedAt: toIso(row.updated_at),
  }
}

export async function getStewardshipById(
  qx: QueryExecutor,
  id: number,
): Promise<StewardshipRecord | null> {
  const row: Record<string, unknown> | null = await qx.selectOneOrNone(
    `SELECT id, package_id, status, origin, version, opened_at, last_status_at,
            inactive_reason, resolution_path, status_note, created_at, updated_at
     FROM stewardships
     WHERE id = $(id)`,
    { id },
  )
  return row ? mapStewardshipRow(row) : null
}

/**
 * Opens a package for stewardship (status → 'open').
 * If a stewardship row already exists, updates the status to 'open'.
 * If none exists, creates one with origin 'opened_for_claim'.
 * Returns the upserted stewardship row, or null if the package purl is not found.
 */
export async function openStewardshipByPurl(
  qx: QueryExecutor,
  purl: string,
  actorUserId: string,
): Promise<StewardshipRecord | null> {
  const row: Record<string, unknown> | null = await qx.selectOneOrNone(
    `
    WITH pkg AS (
      SELECT id FROM packages WHERE purl = $(purl) LIMIT 1
    ),
    prev AS (
      SELECT s.status AS old_status
      FROM stewardships s
      WHERE s.package_id = (SELECT id FROM pkg)
    ),
    upserted AS (
      INSERT INTO stewardships (package_id, status, origin, opened_at, last_status_at)
      SELECT id, 'open', 'opened_for_claim', NOW(), NOW()
      FROM pkg
      ON CONFLICT (package_id) DO UPDATE
        SET status          = 'open',
            opened_at       = NOW(),
            last_status_at  = NOW(),
            inactive_reason = NULL,
            resolution_path = NULL,
            status_note     = NULL,
            updated_at      = NOW()
      RETURNING id, package_id, status, origin, version, opened_at,
                last_status_at, inactive_reason, resolution_path, status_note, created_at, updated_at
    ),
    _log AS (
      INSERT INTO stewardship_activity (stewardship_id, actor_user_id, actor_type, activity_type, content)
      SELECT upserted.id, $(actorUserId), 'user', 'state_changed', 'Opened for stewardship'
      FROM upserted
      WHERE NOT EXISTS (SELECT 1 FROM prev WHERE prev.old_status = 'open')
    )
    SELECT * FROM upserted
    `,
    { purl, actorUserId },
  )
  return row ? mapStewardshipRow(row) : null
}

/**
 * Assigns a steward to a stewardship. Soft-deletes any existing active entry
 * for the same user (allowing role changes). Logs a steward_added activity.
 * Returns the stewardship row (unchanged by this operation) and the full
 * active stewards list as of the end of the transaction.
 */
export async function assignSteward(
  qx: QueryExecutor,
  stewardshipId: number,
  data: {
    userId: string
    username?: string | null
    displayName?: string | null
    role: 'lead' | 'co_steward'
    assignedBy: string
    note?: string
    moveToAssessing?: boolean
  },
): Promise<{ stewardship: StewardshipRecord; stewards: StewardshipStewardRecord[] } | null> {
  return qx.tx(async (tx) => {
    const stewardship = await getStewardshipById(tx, stewardshipId)
    if (!stewardship) return null

    if (data.username != null && data.displayName != null) {
      await tx.result(
        `INSERT INTO stewards (user_id, username, display_name, updated_at)
         VALUES ($(userId), $(username), $(displayName), NOW())
         ON CONFLICT (user_id) DO UPDATE
           SET username     = EXCLUDED.username,
               display_name = EXCLUDED.display_name,
               updated_at   = NOW()`,
        { userId: data.userId, username: data.username, displayName: data.displayName },
      )
    }

    // Soft-delete existing active entry for this user (handles role changes).
    await tx.result(
      `UPDATE stewardship_stewards
       SET deleted_at = NOW()
       WHERE stewardship_id = $(stewardshipId)
         AND user_id = $(userId)
         AND deleted_at IS NULL`,
      { stewardshipId, userId: data.userId },
    )

    await tx.result(
      `INSERT INTO stewardship_stewards (stewardship_id, user_id, role, assigned_by)
       VALUES ($(stewardshipId), $(userId), $(role), $(assignedBy))`,
      { stewardshipId, userId: data.userId, role: data.role, assignedBy: data.assignedBy },
    )

    await tx.result(
      `INSERT INTO stewardship_activity (stewardship_id, actor_user_id, actor_type, activity_type, content, metadata)
       VALUES ($(stewardshipId), $(actorUserId), 'user', 'steward_added', $(content), $(metadata)::jsonb)`,
      {
        stewardshipId,
        actorUserId: data.assignedBy,
        content: `Assigned steward ${data.userId} as ${data.role}`,
        metadata: JSON.stringify({
          userId: data.userId,
          role: data.role,
          ...(data.note ? { note: data.note } : {}),
        }),
      },
    )

    let finalStewardship = stewardship

    if (data.moveToAssessing) {
      const updated: Record<string, unknown> | null = await tx.selectOneOrNone(
        `
        WITH upd AS (
          UPDATE stewardships
          SET status         = 'assessing',
              last_status_at = NOW(),
              resolution_path = NULL,
              status_note     = NULL,
              updated_at     = NOW()
          WHERE id = $(stewardshipId)
            AND status IN ('unassigned', 'open')
          RETURNING id, package_id, status, origin, version, opened_at,
                    last_status_at, inactive_reason, resolution_path, status_note, created_at, updated_at
        ),
        _log AS (
          INSERT INTO stewardship_activity
            (stewardship_id, actor_user_id, actor_type, activity_type, content, metadata)
          SELECT id, $(actorUserId), 'user', 'state_changed',
                 'Status updated to assessing', $(metadata)::jsonb
          FROM upd
        )
        SELECT * FROM upd
        `,
        {
          stewardshipId,
          actorUserId: data.assignedBy,
          metadata: JSON.stringify({ status: 'assessing' }),
        },
      )
      if (updated) finalStewardship = mapStewardshipRow(updated)
    }

    return {
      stewardship: finalStewardship,
      stewards: await fetchActiveStewards(tx, stewardshipId),
    }
  })
}

export interface StewardshipSummary {
  stewards: StewardshipStewardRecord[]
  lastActivityAt: string | null
}

export async function getStewardshipSummary(
  qx: QueryExecutor,
  stewardshipId: number,
): Promise<StewardshipSummary> {
  const [stewards, activityRow] = await Promise.all([
    fetchActiveStewards(qx, stewardshipId),
    qx.selectOneOrNone(
      `SELECT MAX(created_at) AS last_activity_at
       FROM stewardship_activity
       WHERE stewardship_id = $(stewardshipId)`,
      { stewardshipId },
    ) as Promise<Record<string, unknown> | null>,
  ])

  return {
    stewards,
    lastActivityAt: activityRow?.last_activity_at ? toIso(activityRow.last_activity_at) : null,
  }
}

export interface ActivityFeedRow {
  id: string
  stewardshipId: string
  packagePurl: string
  packageName: string
  packageEcosystem: string
  actorUserId: string | null
  // TODO: join actor display name from crowd.dev users/members table (actor_user_id is an Auth0 ID stored in packages DB)
  actorType: string
  activityType: string
  content: string | null
  metadata: Record<string, unknown> | null
  stewardshipStatus: string
  createdAt: string
  total: string
}

export async function listStewardshipActivity(
  qx: QueryExecutor,
  opts: { page: number; pageSize: number },
): Promise<{ rows: Omit<ActivityFeedRow, 'total'>[]; total: number }> {
  const rows: ActivityFeedRow[] = await qx.select(
    `
    SELECT
      sa.id::text                        AS id,
      sa.stewardship_id::text            AS "stewardshipId",
      p.purl                             AS "packagePurl",
      p.name                             AS "packageName",
      p.ecosystem                        AS "packageEcosystem",
      sa.actor_user_id                   AS "actorUserId",
      sa.actor_type                      AS "actorType",
      sa.activity_type                   AS "activityType",
      sa.content                         AS content,
      sa.metadata                        AS metadata,
      s.status                           AS "stewardshipStatus",
      sa.created_at                      AS "createdAt",
      COUNT(*) OVER()::text              AS total
    FROM stewardship_activity sa
    JOIN stewardships s ON s.id = sa.stewardship_id
    JOIN packages p ON p.id = s.package_id
    ORDER BY sa.created_at DESC, sa.id DESC
    LIMIT $(limit) OFFSET $(offset)
    `,
    { limit: opts.pageSize, offset: (opts.page - 1) * opts.pageSize },
  )

  let total: number
  if (rows.length > 0) {
    total = parseInt(rows[0].total, 10)
  } else {
    const countRow: { count: string } = await qx.selectOne(
      `SELECT COUNT(*)::text AS count
       FROM stewardship_activity sa
       JOIN stewardships s ON s.id = sa.stewardship_id
       JOIN packages p ON p.id = s.package_id`,
    )
    total = parseInt(countRow.count, 10)
  }

  return {
    rows: rows.map((row) => ({
      id: row.id,
      stewardshipId: row.stewardshipId,
      packagePurl: row.packagePurl,
      packageName: row.packageName,
      packageEcosystem: row.packageEcosystem,
      actorUserId: row.actorUserId,
      actorType: row.actorType,
      activityType: row.activityType,
      content: translateActivityContent(
        row.content,
        row.activityType,
        row.metadata as Record<string, unknown> | null,
      ),
      metadata: row.metadata as Record<string, unknown> | null,
      stewardshipStatus: row.stewardshipStatus,
      createdAt: toIso(row.createdAt),
    })),
    total,
  }
}

export interface PackageHistoryEvent {
  id: string
  actorUserId: string | null
  actorType: string
  activityType: string
  content: string | null
  metadata: Record<string, unknown> | null
  createdAt: string
}

export async function listPackageHistory(
  qx: QueryExecutor,
  stewardshipId: string,
): Promise<PackageHistoryEvent[]> {
  const rows: Array<Record<string, unknown>> = await qx.select(
    `SELECT id::text             AS id,
            actor_user_id        AS "actorUserId",
            actor_type           AS "actorType",
            activity_type        AS "activityType",
            content,
            metadata,
            created_at           AS "createdAt"
     FROM stewardship_activity
     WHERE stewardship_id = $(stewardshipId)::bigint
     ORDER BY created_at DESC`,
    { stewardshipId },
  )
  return rows.map((r) => ({
    id: r.id as string,
    actorUserId: r.actorUserId ? String(r.actorUserId) : null,
    actorType: String(r.actorType),
    activityType: String(r.activityType),
    content: translateActivityContent(
      r.content ? String(r.content) : null,
      String(r.activityType),
      r.metadata as Record<string, unknown> | null,
    ),
    metadata: r.metadata as Record<string, unknown> | null,
    createdAt: toIso(r.createdAt),
  }))
}

export interface MyPackageRow {
  purl: string
  name: string
  ecosystem: string
  lifecycle: string | null
  scorecardScore: number | null
  openVulns: number
  maxVulnSeverity: 'critical' | 'high' | 'medium' | 'low' | null
  lastActivityContent: string | null
  lastActivityType: string | null
  lastActivityMetadata: Record<string, unknown> | null
  lastActivityAt: Date | null
  stewardshipId: string
  stewardshipStatus: string
  myRole: string
  total: string
}

export interface MyPackageStatusCounts {
  assessing: number
  active: number
  needs_attention: number
  escalated: number
  blocked: number
}

export interface ListMyPackagesOptions {
  userId: string
  page: number
  pageSize: number
  status?: 'assessing' | 'active' | 'needs_attention' | 'escalated' | 'blocked'
  search?: string
  ecosystem?: string
  healthBand?: 'healthy' | 'fair' | 'concerning' | 'critical'
  vulnSeverity?: 'high' | 'critical'
  sortBy: 'risk' | 'health' | 'vulns' | 'name' | 'last_activity'
  sortDir: 'asc' | 'desc'
}

export async function listMyPackages(
  qx: QueryExecutor,
  opts: ListMyPackagesOptions,
): Promise<{
  rows: Omit<MyPackageRow, 'total'>[]
  total: number
  statusCounts: MyPackageStatusCounts
}> {
  const conditions: string[] = ['ss.user_id = $(userId)', 'ss.deleted_at IS NULL']
  const params: Record<string, unknown> = { userId: opts.userId }

  if (opts.status) {
    conditions.push('s.status = $(status)')
    params.status = opts.status
  } else {
    conditions.push(
      "s.status IN ('assessing', 'active', 'needs_attention', 'escalated', 'blocked')",
    )
  }

  if (opts.search) {
    conditions.push('(p.name ILIKE $(search) OR p.purl ILIKE $(search))')
    params.search = `%${opts.search}%`
  }

  if (opts.ecosystem) {
    conditions.push('p.ecosystem = $(ecosystem)')
    params.ecosystem = opts.ecosystem
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
    if (opts.vulnSeverity === 'high') {
      conditions.push('ap_severity.max_rank >= 3')
    } else {
      conditions.push('ap_severity.max_rank >= 4')
    }
  }

  const where = `WHERE ${conditions.join(' AND ')}`

  let sortExpr: string
  if (opts.sortBy === 'health') {
    sortExpr = 'r_sc.scorecard_score'
  } else if (opts.sortBy === 'vulns') {
    sortExpr = 'ap_counts.cnt'
  } else if (opts.sortBy === 'name') {
    sortExpr = 'LOWER(p.name)'
  } else if (opts.sortBy === 'last_activity') {
    sortExpr = 'last_act.created_at'
  } else {
    // risk: composite of impact + health deficit + vuln severity + vuln count
    sortExpr = `(
      COALESCE(p.impact, 0) * 100
      + (100.0 - COALESCE(r_sc.scorecard_score, 0) * 10) * 0.8
      + COALESCE(ap_severity.max_rank, 0) * 15
      + COALESCE(ap_counts.cnt, 0) * 4
    )`
  }
  const sortDir = opts.sortDir === 'asc' ? 'ASC' : 'DESC'

  const queryParams = { ...params, limit: opts.pageSize, offset: (opts.page - 1) * opts.pageSize }

  // filterLaterals affect WHERE clauses — included in both main query and fallback COUNT.
  // displayLaterals are SELECT-only — excluded from COUNT to avoid unnecessary subqueries.
  const filterLaterals = `
    LEFT JOIN LATERAL (
      SELECT r.scorecard_score
      FROM package_repos pr
      JOIN repos r ON r.id = pr.repo_id
      WHERE pr.package_id = p.id
      ORDER BY pr.confidence DESC
      LIMIT 1
    ) r_sc ON true
    LEFT JOIN LATERAL (
      SELECT COUNT(*)::int AS cnt FROM advisory_packages WHERE package_id = p.id
    ) ap_counts ON true
    LEFT JOIN LATERAL (
      SELECT ${SEVERITY_RANK_EXPR} AS max_rank
      FROM advisory_packages ap
      JOIN advisories a ON a.id = ap.advisory_id
      WHERE ap.package_id = p.id
    ) ap_severity ON true`

  const displayLaterals = `
    LEFT JOIN LATERAL (
      SELECT sa.content, sa.activity_type, sa.metadata, sa.created_at
      FROM stewardship_activity sa
      WHERE sa.stewardship_id = s.id
      ORDER BY sa.created_at DESC
      LIMIT 1
    ) last_act ON true`

  const [rows, statusRows] = await Promise.all([
    qx.select(
      `
      SELECT
        p.purl,
        p.name,
        p.ecosystem,
        p.status                       AS lifecycle,
        r_sc.scorecard_score           AS "scorecardScore",
        COALESCE(ap_counts.cnt, 0)     AS "openVulns",
        CASE ap_severity.max_rank
          WHEN 4 THEN 'critical'
          WHEN 3 THEN 'high'
          WHEN 2 THEN 'medium'
          WHEN 1 THEN 'low'
          ELSE NULL
        END                            AS "maxVulnSeverity",
        last_act.content               AS "lastActivityContent",
        last_act.activity_type         AS "lastActivityType",
        last_act.metadata              AS "lastActivityMetadata",
        last_act.created_at            AS "lastActivityAt",
        s.id::text                     AS "stewardshipId",
        s.status                       AS "stewardshipStatus",
        ss.role                        AS "myRole",
        COUNT(*) OVER()::text          AS total
      FROM stewardship_stewards ss
      JOIN stewardships s ON s.id = ss.stewardship_id
      JOIN packages p ON p.id = s.package_id
      ${filterLaterals}
      ${displayLaterals}
      ${where}
      ORDER BY ${sortExpr} ${sortDir} NULLS LAST, p.purl ASC
      LIMIT $(limit) OFFSET $(offset)
      `,
      queryParams,
    ) as Promise<MyPackageRow[]>,
    qx.select(
      `
      SELECT s.status, COUNT(*)::int AS count
      FROM stewardship_stewards ss
      JOIN stewardships s ON s.id = ss.stewardship_id
      WHERE ss.user_id = $(userId) AND ss.deleted_at IS NULL
        AND s.status IN ('assessing', 'active', 'needs_attention', 'escalated', 'blocked')
      GROUP BY s.status
      `,
      { userId: opts.userId },
    ) as Promise<{ status: string; count: number }[]>,
  ])

  let total: number
  if (rows.length > 0) {
    total = parseInt(rows[0].total, 10)
  } else {
    const countRow: { count: string } = await qx.selectOne(
      `
      SELECT COUNT(*)::text AS count
      FROM stewardship_stewards ss
      JOIN stewardships s ON s.id = ss.stewardship_id
      JOIN packages p ON p.id = s.package_id
      ${filterLaterals}
      ${where}
      `,
      params,
    )
    total = parseInt(countRow.count, 10)
  }

  const countsMap: Record<string, number> = {}
  for (const r of statusRows) {
    countsMap[r.status] = r.count
  }
  const statusCounts: MyPackageStatusCounts = {
    assessing: countsMap.assessing ?? 0,
    active: countsMap.active ?? 0,
    needs_attention: countsMap.needs_attention ?? 0,
    escalated: countsMap.escalated ?? 0,
    blocked: countsMap.blocked ?? 0,
  }

  return {
    rows: rows.map((row) => ({
      purl: row.purl,
      name: row.name,
      ecosystem: row.ecosystem,
      lifecycle: row.lifecycle ?? null,
      scorecardScore: row.scorecardScore != null ? Number(row.scorecardScore) : null,
      openVulns: Number(row.openVulns),
      maxVulnSeverity: row.maxVulnSeverity ?? null,
      lastActivityContent: row.lastActivityContent ?? null,
      lastActivityType: row.lastActivityType ?? null,
      lastActivityMetadata: row.lastActivityMetadata ?? null,
      lastActivityAt: row.lastActivityAt ?? null,
      stewardshipId: row.stewardshipId,
      stewardshipStatus: row.stewardshipStatus,
      myRole: row.myRole,
    })),
    total,
    statusCounts,
  }
}

export interface ListMyActivityOptions {
  userId: string
  page: number
  pageSize: number
  status?: string[]
}

export async function listMyActivity(
  qx: QueryExecutor,
  opts: ListMyActivityOptions,
): Promise<{ rows: Omit<ActivityFeedRow, 'total'>[]; total: number }> {
  const params: Record<string, unknown> = {
    userId: opts.userId,
    limit: opts.pageSize,
    offset: (opts.page - 1) * opts.pageSize,
  }

  const statusFilter =
    opts.status && opts.status.length > 0 ? 'AND s.status = ANY($(statusFilter))' : ''
  if (opts.status && opts.status.length > 0) {
    params.statusFilter = opts.status
  }

  // DISTINCT ON keeps the most recent event per stewardship; the outer query
  // re-sorts newest-first after deduplication and applies pagination.
  const rows: ActivityFeedRow[] = await qx.select(
    `
    SELECT
      sub.id,
      sub."stewardshipId",
      sub."packagePurl",
      sub."packageName",
      sub."packageEcosystem",
      sub."actorUserId",
      sub."actorType",
      sub."activityType",
      sub.content,
      sub.metadata,
      sub."stewardshipStatus",
      sub."createdAt",
      COUNT(*) OVER()::text AS total
    FROM (
      SELECT DISTINCT ON (sa.stewardship_id)
        sa.id::text                        AS id,
        sa.stewardship_id::text            AS "stewardshipId",
        p.purl                             AS "packagePurl",
        p.name                             AS "packageName",
        p.ecosystem                        AS "packageEcosystem",
        sa.actor_user_id                   AS "actorUserId",
        sa.actor_type                      AS "actorType",
        sa.activity_type                   AS "activityType",
        sa.content                         AS content,
        sa.metadata                        AS metadata,
        s.status                           AS "stewardshipStatus",
        sa.created_at                      AS "createdAt"
      FROM stewardship_activity sa
      JOIN stewardships s ON s.id = sa.stewardship_id
      JOIN packages p ON p.id = s.package_id
      WHERE s.id IN (
        SELECT stewardship_id FROM stewardship_stewards
        WHERE user_id = $(userId) AND deleted_at IS NULL
      )
      ${statusFilter}
      ORDER BY sa.stewardship_id, sa.created_at DESC, sa.id DESC
    ) sub
    ORDER BY sub."createdAt" DESC, sub.id::bigint DESC
    LIMIT $(limit) OFFSET $(offset)
    `,
    params,
  )

  let total: number
  if (rows.length > 0) {
    total = parseInt(rows[0].total, 10)
  } else {
    const countParams: Record<string, unknown> = { userId: opts.userId }
    if (opts.status && opts.status.length > 0) {
      countParams.statusFilter = opts.status
    }
    const countRow: { count: string } = await qx.selectOne(
      `
      SELECT COUNT(DISTINCT sa.stewardship_id)::text AS count
      FROM stewardship_activity sa
      JOIN stewardships s ON s.id = sa.stewardship_id
      WHERE s.id IN (
        SELECT stewardship_id FROM stewardship_stewards
        WHERE user_id = $(userId) AND deleted_at IS NULL
      )
      ${statusFilter}
      `,
      countParams,
    )
    total = parseInt(countRow.count, 10)
  }

  return {
    rows: rows.map((row) => ({
      id: row.id,
      stewardshipId: row.stewardshipId,
      packagePurl: row.packagePurl,
      packageName: row.packageName,
      packageEcosystem: row.packageEcosystem,
      actorUserId: row.actorUserId,
      actorType: row.actorType,
      activityType: row.activityType,
      content: translateActivityContent(
        row.content,
        row.activityType,
        row.metadata as Record<string, unknown> | null,
      ),
      metadata: row.metadata as Record<string, unknown> | null,
      stewardshipStatus: row.stewardshipStatus,
      createdAt: toIso(row.createdAt),
    })),
    total,
  }
}

export const ESCALATION_RESOLUTION_PATHS = [
  'right_of_first_refusal',
  'replace_the_dependency',
  'find_vendor_for_lts',
  'consortium_adopts_maintainership',
  'compensating_controls_monitor',
  'namespace_takeover',
] as const

export type EscalationResolutionPath = (typeof ESCALATION_RESOLUTION_PATHS)[number]

export const ESCALATION_RESOLUTION_PATH_LABELS: Record<EscalationResolutionPath, string> = {
  right_of_first_refusal: 'Right of First Refusal',
  replace_the_dependency: 'Replace the Dependency',
  find_vendor_for_lts: 'Find Vendor for LTS',
  consortium_adopts_maintainership: 'Consortium Adopts Maintainership',
  compensating_controls_monitor: 'Compensating Controls / Monitor',
  namespace_takeover: 'Namespace Takeover',
}

export function translateActivityContent(
  content: string | null,
  activityType?: string | null,
  metadata?: Record<string, unknown> | null,
): string | null {
  if (!content) return content
  if (activityType === 'escalation' && metadata?.resolutionPath) {
    const label =
      ESCALATION_RESOLUTION_PATH_LABELS[metadata.resolutionPath as EscalationResolutionPath]
    if (label) return `Escalated with resolution path: ${label}`
  }
  return content.replace(/^(Escalated with resolution path: )(\S+)$/, (_, prefix, key) => {
    const label = ESCALATION_RESOLUTION_PATH_LABELS[key as EscalationResolutionPath]
    return label ? `${prefix}${label}` : content
  })
}

/**
 * Escalates a stewardship. Updates status to 'escalated' and logs the
 * chosen resolution path in stewardship_activity metadata.
 */
export async function escalateStewardship(
  qx: QueryExecutor,
  stewardshipId: number,
  data: { resolutionPath: EscalationResolutionPath; notes?: string; actorUserId: string },
): Promise<StewardshipRecord | null> {
  const row: Record<string, unknown> | null = await qx.selectOneOrNone(
    `
    WITH upd AS (
      UPDATE stewardships
      SET status          = 'escalated',
          last_status_at  = NOW(),
          inactive_reason = NULL,
          resolution_path = $(resolutionPath),
          status_note     = $(statusNote),
          updated_at      = NOW()
      WHERE id = $(stewardshipId)
      RETURNING id, package_id, status, origin, version, opened_at,
                last_status_at, inactive_reason, resolution_path, status_note, created_at, updated_at
    ),
    _log AS (
      INSERT INTO stewardship_activity
        (stewardship_id, actor_user_id, actor_type, activity_type, content, metadata)
      SELECT id, $(actorUserId), 'user', 'escalation',
             $(content), $(metadata)::jsonb
      FROM upd
    )
    SELECT * FROM upd
    `,
    {
      stewardshipId,
      resolutionPath: data.resolutionPath,
      statusNote: data.notes ?? null,
      actorUserId: data.actorUserId,
      content: `Escalated with resolution path: ${data.resolutionPath}`,
      metadata: JSON.stringify({
        resolutionPath: data.resolutionPath,
        ...(data.notes ? { notes: data.notes } : {}),
      }),
    },
  )
  return row ? mapStewardshipRow(row) : null
}

export const INACTIVE_REASONS = [
  'quarterly_cadence_missed',
  'stepped_down',
  'no_longer_critical',
] as const

export type InactiveReason = (typeof INACTIVE_REASONS)[number]

export const STEWARDSHIP_UPDATABLE_STATUSES = [
  'assessing',
  'active',
  'needs_attention',
  'blocked',
  'inactive',
] as const

export type UpdatableStewardshipStatus = (typeof STEWARDSHIP_UPDATABLE_STATUSES)[number]

/**
 * Updates the status of a stewardship and logs the change.
 * For 'inactive', an inactiveReason must be provided.
 */
export async function updateStewardshipStatus(
  qx: QueryExecutor,
  stewardshipId: number,
  data: {
    status: UpdatableStewardshipStatus
    inactiveReason?: InactiveReason
    notes?: string
    actorUserId: string
  },
): Promise<StewardshipRecord | null> {
  const row: Record<string, unknown> | null = await qx.selectOneOrNone(
    `
    WITH upd AS (
      UPDATE stewardships
      SET status          = $(status),
          last_status_at  = NOW(),
          inactive_reason = CASE WHEN $(status) = 'inactive' THEN $(inactiveReason) ELSE inactive_reason END,
          resolution_path = NULL,
          status_note     = $(statusNote),
          updated_at      = NOW()
      WHERE id = $(stewardshipId)
      RETURNING id, package_id, status, origin, version, opened_at,
                last_status_at, inactive_reason, resolution_path, status_note, created_at, updated_at
    ),
    _log AS (
      INSERT INTO stewardship_activity
        (stewardship_id, actor_user_id, actor_type, activity_type, content, metadata)
      SELECT id, $(actorUserId), 'user', 'state_changed',
             $(content), $(metadata)::jsonb
      FROM upd
    )
    SELECT * FROM upd
    `,
    {
      stewardshipId,
      status: data.status,
      inactiveReason: data.inactiveReason ?? null,
      statusNote: data.notes ?? null,
      actorUserId: data.actorUserId,
      content: `Status updated to ${data.status}`,
      metadata: JSON.stringify({
        status: data.status,
        ...(data.inactiveReason ? { inactiveReason: data.inactiveReason } : {}),
        ...(data.notes ? { notes: data.notes } : {}),
      }),
    },
  )
  return row ? mapStewardshipRow(row) : null
}
