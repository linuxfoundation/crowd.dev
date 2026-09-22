import {
  QueryExecutor,
  insertUnassignedStewardships,
  listCriticalPackagesWithoutStewardship,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('stewardship-backfill')

export interface BackfillResult {
  inserted: number
  skipped: number
  batches: number
}

interface BackfillOptions {
  batchSize: number
}

/**
 * Seeds one `stewardships` row (status=unassigned, origin=auto_imported) for
 * every critical package that doesn't already have one. Idempotent: ON CONFLICT
 * DO NOTHING means re-running is safe and will just report 0 inserts.
 *
 * Designed to be called from a Temporal activity or directly from the bin script.
 * The `isStopping` callback lets the caller signal a graceful shutdown between
 * batches — the function returns the totals collected so far.
 */
export async function runStewardshipBackfill(
  qx: QueryExecutor,
  options: BackfillOptions,
  isStopping: () => boolean = () => false,
): Promise<BackfillResult> {
  const { batchSize } = options
  let lastId = 0
  let inserted = 0
  let skipped = 0
  let batches = 0

  while (!isStopping()) {
    const ids = await listCriticalPackagesWithoutStewardship(qx, {
      afterId: lastId,
      limit: batchSize,
    })

    if (ids.length === 0) break

    const batchInserted = await insertUnassignedStewardships(qx, ids)
    const batchSkipped = ids.length - batchInserted

    inserted += batchInserted
    skipped += batchSkipped
    batches++
    lastId = ids[ids.length - 1]

    log.info({ batches, inserted, skipped, lastId, batchInserted, batchSkipped }, 'Batch complete.')
  }

  return { inserted, skipped, batches }
}
