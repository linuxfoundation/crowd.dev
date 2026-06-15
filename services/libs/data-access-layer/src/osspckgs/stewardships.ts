import { QueryExecutor } from '../queryExecutor'

export interface StewardshipRecord {
  id: string
  packageId: string
  status: string
  origin: string
  version: number
  openedAt: string | null
  lastStatusAt: string | null
  inactiveReason: string | null
  createdAt: string
  updatedAt: string
}

export interface StewardshipStewardRecord {
  id: string
  stewardshipId: string
  userId: string
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

function mapStewardshipRow(row: Record<string, unknown>): StewardshipRecord {
  return {
    id: String(row.id),
    packageId: String(row.package_id),
    status: row.status as string,
    origin: row.origin as string,
    version: Number(row.version),
    openedAt: row.opened_at ? String(row.opened_at) : null,
    lastStatusAt: row.last_status_at ? String(row.last_status_at) : null,
    inactiveReason: row.inactive_reason ? String(row.inactive_reason) : null,
    createdAt: String(row.created_at),
    updatedAt: String(row.updated_at),
  }
}

export async function getStewardshipById(
  qx: QueryExecutor,
  id: number,
): Promise<StewardshipRecord | null> {
  const row: Record<string, unknown> | null = await qx.selectOneOrNone(
    `SELECT id, package_id, status, origin, version, opened_at, last_status_at,
            inactive_reason, created_at, updated_at
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
    upserted AS (
      INSERT INTO stewardships (package_id, status, origin, opened_at, last_status_at)
      SELECT id, 'open', 'opened_for_claim', NOW(), NOW()
      FROM pkg
      ON CONFLICT (package_id) DO UPDATE
        SET status        = 'open',
            opened_at     = COALESCE(stewardships.opened_at, NOW()),
            last_status_at = NOW(),
            updated_at    = NOW()
      RETURNING id, package_id, status, origin, version, opened_at,
                last_status_at, inactive_reason, created_at, updated_at
    ),
    _log AS (
      INSERT INTO stewardship_activity (stewardship_id, actor_user_id, actor_type, activity_type, content)
      SELECT id, $(actorUserId), 'user', 'state_changed', 'Opened for stewardship'
      FROM upserted
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
 * Returns the updated stewardship row and the full active stewards list.
 */
export async function assignSteward(
  qx: QueryExecutor,
  stewardshipId: number,
  data: { userId: string; role: 'lead' | 'co_steward'; assignedBy: string },
): Promise<{ stewardship: StewardshipRecord; stewards: StewardshipStewardRecord[] } | null> {
  return qx.tx(async (tx) => {
    const stewardship = await getStewardshipById(tx, stewardshipId)
    if (!stewardship) return null

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
        metadata: JSON.stringify({ userId: data.userId, role: data.role }),
      },
    )

    const stewards: Array<Record<string, unknown>> = await tx.select(
      `SELECT id, stewardship_id, user_id, role, assigned_at, assigned_by
       FROM stewardship_stewards
       WHERE stewardship_id = $(stewardshipId)
         AND deleted_at IS NULL
       ORDER BY assigned_at ASC`,
      { stewardshipId },
    )

    return {
      stewardship,
      stewards: stewards.map((s) => ({
        id: String(s.id),
        stewardshipId: String(s.stewardship_id),
        userId: String(s.user_id),
        role: String(s.role),
        assignedAt: String(s.assigned_at),
        assignedBy: s.assigned_by ? String(s.assigned_by) : null,
      })),
    }
  })
}

export const ESCALATION_RESOLUTION_PATHS = [
  'lf_staff_review',
  'community_outreach',
  'corporate_adoption',
  'transfer_ownership',
  'fork_and_maintain',
  'deprecate',
] as const

export type EscalationResolutionPath = (typeof ESCALATION_RESOLUTION_PATHS)[number]

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
      SET status         = 'escalated',
          last_status_at = NOW(),
          updated_at     = NOW()
      WHERE id = $(stewardshipId)
      RETURNING id, package_id, status, origin, version, opened_at,
                last_status_at, inactive_reason, created_at, updated_at
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
    inactiveReason?: string
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
          updated_at      = NOW()
      WHERE id = $(stewardshipId)
      RETURNING id, package_id, status, origin, version, opened_at,
                last_status_at, inactive_reason, created_at, updated_at
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
