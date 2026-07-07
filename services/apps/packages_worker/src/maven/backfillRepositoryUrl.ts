import {
  listMavenPackagesForRepoUrlRecompute,
  updateMavenRepositoryUrls,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { normalizeScmUrl } from './extract'

const log = getServiceChildLogger('maven-repo-url-backfill')

export type RepoUrlBackfillTotals = {
  scanned: number
  filled: number // Gap B: NULL → canonical value
  cleared: number // Gap C: non-repo value → NULL
  rewritten: number // non-canonical value → different canonical value
  unchanged: number
}

/**
 * Recomputes `repository_url` for every Maven row directly from the stored
 * `declared_repository_url`, applying the current `normalizeScmUrl`. No POMs are
 * fetched — the raw SCM value is already in the DB. Fills recoverable NULLs
 * (Gap B) and clears non-repository values (Gap C) via direct UPDATE.
 *
 * Idempotent and resumable: the id cursor is derived from the scan, so a
 * re-run after an interrupt simply reprocesses from the start and skips rows
 * that already match.
 */
export async function backfillMavenRepositoryUrls(
  qx: QueryExecutor,
  options: { batchSize: number; dryRun: boolean; isShuttingDown: () => boolean },
): Promise<RepoUrlBackfillTotals> {
  const { batchSize, dryRun, isShuttingDown } = options
  const totals: RepoUrlBackfillTotals = {
    scanned: 0,
    filled: 0,
    cleared: 0,
    rewritten: 0,
    unchanged: 0,
  }

  let afterId = 0
  for (;;) {
    if (isShuttingDown()) {
      log.info('Shutdown requested — stopping after the current batch.')
      break
    }

    const rows = await listMavenPackagesForRepoUrlRecompute(qx, { afterId, limit: batchSize })
    if (rows.length === 0) break

    const updates: { id: number; repositoryUrl: string | null }[] = []
    for (const row of rows) {
      totals.scanned++
      const desired = normalizeScmUrl(row.declaredRepositoryUrl)
      if (desired === row.repositoryUrl) {
        totals.unchanged++
        continue
      }
      if (row.repositoryUrl === null) totals.filled++
      else if (desired === null) totals.cleared++
      else totals.rewritten++
      updates.push({ id: row.id, repositoryUrl: desired })
    }

    if (updates.length > 0 && !dryRun) {
      await updateMavenRepositoryUrls(qx, updates)
    }

    afterId = rows[rows.length - 1].id
    log.info({ afterId, changes: updates.length, dryRun, ...totals }, 'Backfill progress')
  }

  return totals
}
