import {
  deleteMavenPackageRepoLinks,
  listMavenPackagesForRepoUrlRecompute,
  updateMavenRepositoryUrls,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { normalizeScmUrl } from './extract'
import { withDeadlockRetry, writeRepoLink } from './runMavenEnrichmentLoop'

const log = getServiceChildLogger('maven-repo-url-backfill')

export type RepoUrlBackfillTotals = {
  scanned: number
  filled: number // Gap B: NULL → canonical value
  cleared: number // Gap C: non-repo value → NULL
  rewritten: number // non-canonical value → different canonical value
  unchanged: number
  linked: number // repos/package_repos link (re)written for a fill or rewrite
  pruned: number // stale 'declared' link removed for a clear or rewrite
}

/**
 * Recomputes `repository_url` for every Maven row directly from the stored
 * `declared_repository_url`, applying the current `normalizeScmUrl`. No POMs are
 * fetched — the raw SCM value is already in the DB. Fills recoverable NULLs
 * (Gap B) and clears non-repository values (Gap C) via direct UPDATE.
 *
 * Link tables: package_repos is kept consistent with the recomputed
 * repository_url. For rows that had a value and now change (rewrites) or are
 * cleared, the stale source='declared' link is deleted; for rows that gain a
 * canonical URL (fills and rewrites) the correct link is (re)written via the
 * same `writeRepoLink` the enrichment loop uses. This matters because consumers
 * such as security-contacts read the repo through repos ⋈ package_repos, not
 * packages.repository_url. Note this is stricter than the incremental
 * enrichment path, which is upsert-only and never prunes.
 *
 * Idempotent and resumable: the id cursor is derived from the scan, so a
 * re-run after an interrupt simply reprocesses from the start and skips rows
 * that already match.
 */
export async function backfillMavenRepositoryUrls(
  qx: QueryExecutor,
  options: {
    batchSize: number
    dryRun: boolean
    criticalOnly: boolean
    isShuttingDown: () => boolean
  },
): Promise<RepoUrlBackfillTotals> {
  const { batchSize, dryRun, criticalOnly, isShuttingDown } = options
  const totals: RepoUrlBackfillTotals = {
    scanned: 0,
    filled: 0,
    cleared: 0,
    rewritten: 0,
    unchanged: 0,
    linked: 0,
    pruned: 0,
  }

  let afterId = 0
  for (;;) {
    if (isShuttingDown()) {
      log.info('Shutdown requested — stopping after the current batch.')
      break
    }

    const rows = await listMavenPackagesForRepoUrlRecompute(qx, {
      afterId,
      limit: batchSize,
      criticalOnly,
    })
    if (rows.length === 0) break

    const updates: { id: number; repositoryUrl: string | null }[] = []
    // Rows that had a link and now change/clear — their stale 'declared' link is pruned.
    const pruneTargets: number[] = []
    // Rows that gained a canonical URL — their repo link is (re)written after the update.
    const linkTargets: { id: number; repositoryUrl: string }[] = []
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
      // A 'declared' link only exists when the row already had a value.
      if (row.repositoryUrl !== null) pruneTargets.push(row.id)
      if (desired !== null) linkTargets.push({ id: row.id, repositoryUrl: desired })
    }

    if (updates.length > 0 && !dryRun) {
      // Atomic per batch: the repository_url UPDATE, the stale-link prune, and the
      // relink must commit together. Otherwise an interrupt between them leaves
      // packages.repository_url updated but package_repos out of sync — and on a
      // re-run the row is skipped (its repository_url already matches `desired`),
      // so the inconsistency would never be repaired. On rollback the row stays
      // unchanged and is reprocessed on the next run.
      await withDeadlockRetry(() =>
        qx.tx(async (t) => {
          await updateMavenRepositoryUrls(t, updates)
          await deleteMavenPackageRepoLinks(t, pruneTargets)
          for (const target of linkTargets) {
            await writeRepoLink(t, target.id, target.repositoryUrl)
          }
        }),
      )
      totals.pruned += pruneTargets.length
      totals.linked += linkTargets.length
    }

    afterId = rows[rows.length - 1].id
    log.info(
      { afterId, changes: updates.length, dryRun, criticalOnly, ...totals },
      'Backfill progress',
    )
  }

  return totals
}
