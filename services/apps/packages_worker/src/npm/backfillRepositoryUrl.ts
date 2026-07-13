import { deleteMavenPackageRepoLinks } from '@crowd/data-access-layer'
import {
  NpmRepoUrlRow,
  getOrCreateRepoByUrl,
  listNpmPackagesForRepoUrlRecompute,
  updateNpmRepositoryUrls,
  upsertPackageRepo,
} from '@crowd/data-access-layer/src/packages'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { canonicalizeRepoUrl } from '../utils/canonicalizeRepoUrl'

const log = getServiceChildLogger('npm-repo-url-backfill')

// Postgres deadlock (40P01) is transient: concurrent transactions upserting the same shared
// rows (e.g. the same repo linked from many packages) can form a lock cycle. Re-running the
// whole transaction resolves it — the upserts are idempotent. Mirrors Maven's
// withDeadlockRetry (src/maven/runMavenEnrichmentLoop.ts).
async function withDeadlockRetry<T>(fn: () => Promise<T>, maxAttempts = 4): Promise<T> {
  for (let attempt = 1; ; attempt++) {
    try {
      return await fn()
    } catch (err) {
      const code = (err as { code?: string }).code
      const isDeadlock =
        code === '40P01' || /deadlock detected/i.test(String((err as Error)?.message))
      if (isDeadlock && attempt < maxAttempts) {
        await new Promise((r) => setTimeout(r, 50 * attempt + Math.random() * 100))
        log.debug({ attempt }, 'Deadlock detected — retrying transaction')
        continue
      }
      throw err
    }
  }
}

export type RepoUrlBackfillTotals = {
  scanned: number
  filled: number // NULL → canonical value
  cleared: number // canonical value → NULL (e.g. a stricter normalizer no longer accepts it)
  rewritten: number // non-canonical value → different canonical value
  unchanged: number
  linked: number // repos/package_repos link (re)written for a fill or rewrite
  pruned: number // stale 'declared' link removed for a clear or rewrite
}

/**
 * Recomputes `repository_url` for every npm row directly from the stored
 * `declared_repository_url`, applying the current `canonicalizeRepoUrl`. No
 * packument is re-fetched — the raw declared value is already in the DB. Fills
 * recoverable NULLs (e.g. scp-form ssh:// URLs the previous normalizer dropped).
 *
 * `cleared` only accounts for values a *future* normalizer change stops
 * accepting for a row that already has a repository_url — the current
 * canonicalizeRepoUrl only ever adds recognized cases, so this backfill
 * doesn't clear any values on its own today.
 *
 * Link table: package_repos is kept consistent with the recomputed
 * repository_url. For a fill (previously null) or a rewrite (value changes) the
 * canonical repo link is (re)written via getOrCreateRepoByUrl/upsertPackageRepo —
 * the same pair the ingest path uses. A rewrite first prunes the stale
 * source='declared' link so a package never carries two 'declared' links to
 * different repos.
 *
 * Idempotent and resumable: the id cursor is derived from the scan, so a
 * re-run after an interrupt simply reprocesses from the start and skips rows
 * that already match.
 */
export async function backfillNpmRepositoryUrls(
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

  let afterId = '0'
  for (;;) {
    if (isShuttingDown()) {
      log.info('Shutdown requested — stopping after the current batch.')
      break
    }

    const rows: NpmRepoUrlRow[] = await listNpmPackagesForRepoUrlRecompute(qx, {
      afterId,
      limit: batchSize,
      criticalOnly,
    })
    if (rows.length === 0) break

    const updates: { id: string; repositoryUrl: string | null }[] = []
    // Rows that had a link and now change to a different one — their stale 'declared' link is pruned.
    const pruneTargets: number[] = []
    // Rows that gained/changed a canonical URL — their repo link is (re)written after the update.
    const linkTargets: { id: string; repositoryUrl: string; host: string }[] = []
    for (const row of rows) {
      totals.scanned++
      // declaredRepositoryUrl is guaranteed non-null by the query's IS NOT NULL filter.
      const canonical = canonicalizeRepoUrl(row.declaredRepositoryUrl as string)
      const desired = canonical?.url ?? null
      if (desired === row.repositoryUrl) {
        totals.unchanged++
        continue
      }
      if (row.repositoryUrl === null) totals.filled++
      else if (desired === null) totals.cleared++
      else totals.rewritten++
      updates.push({ id: row.id, repositoryUrl: desired })
      if (row.repositoryUrl !== null && desired !== row.repositoryUrl) {
        pruneTargets.push(Number(row.id))
      }
      if (canonical)
        linkTargets.push({ id: row.id, repositoryUrl: canonical.url, host: canonical.host })
    }

    if (updates.length > 0 && !dryRun) {
      // Atomic per batch: the repository_url UPDATE, the stale-link prune, and the
      // relink must commit together — otherwise an interrupt between them leaves
      // packages.repository_url updated but package_repos out of sync, and a
      // re-run would skip the row (its repository_url already matches `desired`),
      // so the inconsistency would never be repaired.
      await withDeadlockRetry(() =>
        qx.tx(async (t: QueryExecutor) => {
          await updateNpmRepositoryUrls(t, updates)
          if (pruneTargets.length > 0) await deleteMavenPackageRepoLinks(t, pruneTargets)
          for (const target of linkTargets) {
            const { id: repoId } = await getOrCreateRepoByUrl(t, target.repositoryUrl, target.host)
            await upsertPackageRepo(t, target.id, repoId, 'declared', 0.8)
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
