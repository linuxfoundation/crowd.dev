import { QueryExecutor } from '../queryExecutor'

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
