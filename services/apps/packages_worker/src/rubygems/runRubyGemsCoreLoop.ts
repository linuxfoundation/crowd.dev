import {
  QueryExecutor,
  RubyGemsPackageToSync,
  getOrCreateRepoByUrl,
  listRubyGemsPackagesToSync,
  logAuditFieldChange,
  recordDownloadSnapshot,
  removeDeclaredPackageRepo,
  setPackageDeclaredRepositoryUrl,
  setPackageRepositoryUrl,
  upsertPackage,
  upsertPackageRepo,
} from '@crowd/data-access-layer'
import type { PackageRepoOwnershipMatch } from '@crowd/data-access-layer/src/packages/repoConfidence'
import { getServiceChildLogger } from '@crowd/logging'

import {
  bumpDeclaredOwnershipCounts,
  emptyDeclaredOwnershipCounts,
  matchOwnership,
  repoOwnerFromCanonical,
} from '../utils/ownershipMatch'

import { fetchGem } from './client'
import { normalizeRubyGemsPackage } from './normalize'
import { BatchResult, isRubyGemsFetchError } from './types'

const log = getServiceChildLogger('rubygems-core')

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

function rubyGemsRegistryUrl(name: string): string {
  return `https://rubygems.org/gems/${name}`
}

async function markPackageError(qx: QueryExecutor, pkg: RubyGemsPackageToSync): Promise<void> {
  await upsertPackage(qx, {
    purl: pkg.purl,
    ecosystem: 'rubygems',
    namespace: null,
    name: pkg.name,
    description: null,
    homepage: null,
    declaredRepositoryUrl: null,
    licenses: null,
    licensesRaw: null,
    latestVersion: pkg.latestVersion,
    ingestionSource: 'rubygems_error',
  })
}

type PackageStatus = 'processed' | 'skipped' | 'error' | 'unchanged'

export type RubyGemsCoreConfig = { batchSize: number; concurrency: number }

interface ProcessPackageResult {
  status: PackageStatus
  ownershipMatch: PackageRepoOwnershipMatch | null
}

async function processPackage(
  qx: QueryExecutor,
  pkg: RubyGemsPackageToSync,
  today: string,
  signal?: AbortSignal,
): Promise<ProcessPackageResult> {
  const gemResult = await fetchGem(pkg.name, signal)

  if (isRubyGemsFetchError(gemResult)) {
    if (gemResult.kind === 'NOT_FOUND') {
      await upsertPackage(qx, {
        purl: pkg.purl,
        ecosystem: 'rubygems',
        namespace: null,
        name: pkg.name,
        description: null,
        homepage: null,
        declaredRepositoryUrl: null,
        licenses: null,
        licensesRaw: null,
        latestVersion: pkg.latestVersion,
        registryUrl: rubyGemsRegistryUrl(pkg.name),
        ingestionSource: 'rubygems_not_found',
      })
      log.warn(
        { purl: pkg.purl },
        'Package not found on RubyGems registry — writing minimal record',
      )
      return { status: 'skipped', ownershipMatch: null }
    }
    if (gemResult.kind === 'RATE_LIMIT') {
      log.warn({ purl: pkg.purl }, 'Rate limited by RubyGems registry — will retry next pass')
      return { status: 'error', ownershipMatch: null }
    }
    throw new Error(`Transient error fetching ${pkg.purl}: ${gemResult.message}`)
  }

  const normalized = normalizeRubyGemsPackage(gemResult)

  let ownershipMatch: PackageRepoOwnershipMatch | null = null

  await withDeadlockRetry(() =>
    qx.tx(async (t) => {
      const changed = new Set<string>()

      const { id: packageDbId, changedFields: pkgChanged } = await upsertPackage(t, {
        purl: pkg.purl,
        ecosystem: 'rubygems',
        namespace: null,
        name: pkg.name,
        description: normalized.description,
        homepage: normalized.homepage,
        declaredRepositoryUrl: normalized.declaredRepositoryUrl,
        repositoryUrl: normalized.resolvedRepo?.repo.url ?? null,
        licenses: normalized.licenses,
        licensesRaw: normalized.licensesRaw,
        latestVersion: normalized.latestVersion,
        registryUrl: rubyGemsRegistryUrl(pkg.name),
        ingestionSource: 'rubygems-registry',
      })
      pkgChanged.forEach((f) => changed.add(f))

      // upsertPackage's COALESCE can't distinguish a dropped source_code_uri from "unknown" —
      // this registry fetch just succeeded, so clear declaredRepositoryUrl explicitly when gone.
      const declaredClearedFields = await setPackageDeclaredRepositoryUrl(
        t,
        packageDbId.toString(),
        normalized.declaredRepositoryUrl,
      )
      declaredClearedFields.forEach((f) => changed.add(f))

      if (normalized.resolvedRepo) {
        const { id: repoId, changedFields: repoChanged } = await getOrCreateRepoByUrl(
          t,
          normalized.resolvedRepo.repo.url,
          normalized.resolvedRepo.repo.host,
        )
        repoChanged.forEach((f) => changed.add(f))

        // Gem authors are free-text display names, not GitHub identity handles, so they
        // can't be used as maintainer evidence — per ADR-0022, this loop stays no_evidence
        // until owners are fetched in the critical loop.
        ownershipMatch = matchOwnership({
          repoOwner: repoOwnerFromCanonical(normalized.resolvedRepo.repo),
        })

        const linkChanged = await upsertPackageRepo(t, packageDbId.toString(), repoId, {
          source: 'declared',
          signal: normalized.resolvedRepo.signal,
          ownershipMatch,
        })
        linkChanged.forEach((f) => changed.add(f))

        const removedFields = await removeDeclaredPackageRepo(t, packageDbId.toString(), repoId)
        removedFields.forEach((f) => changed.add(f))
      } else {
        const removedFields = await removeDeclaredPackageRepo(t, packageDbId.toString())
        removedFields.forEach((f) => changed.add(f))
        const clearedFields = await setPackageRepositoryUrl(t, packageDbId.toString(), null)
        clearedFields.forEach((f) => changed.add(f))
      }

      if (normalized.totalDownloads > 0) {
        const dlChanged = await recordDownloadSnapshot(t, {
          packageId: packageDbId,
          purl: pkg.purl,
          totalDownloads: normalized.totalDownloads,
          today,
        })
        dlChanged.forEach((f) => changed.add(f))
      }

      await logAuditFieldChange(t, 'rubygems', pkg.purl, Array.from(changed))

      log.debug({ purl: pkg.purl, totalDownloads: normalized.totalDownloads }, 'ok')
    }),
  )

  return { status: 'processed', ownershipMatch }
}

export async function processBatch(
  qx: QueryExecutor,
  config: RubyGemsCoreConfig,
  today: string,
  signal?: AbortSignal,
): Promise<BatchResult> {
  const packages = await listRubyGemsPackagesToSync(qx, { limit: config.batchSize })

  if (packages.length === 0)
    return { processed: 0, skipped: 0, error: 0, unchanged: 0, ...emptyDeclaredOwnershipCounts() }

  log.info({ count: packages.length }, 'Batch started')

  const counts: BatchResult = {
    processed: 0,
    skipped: 0,
    error: 0,
    unchanged: 0,
    ...emptyDeclaredOwnershipCounts(),
  }

  for (let batchStart = 0; batchStart < packages.length; batchStart += config.concurrency) {
    signal?.throwIfAborted()
    const group = packages.slice(batchStart, batchStart + config.concurrency)

    await Promise.all(
      group.map(async (pkg) => {
        try {
          const { status, ownershipMatch } = await processPackage(qx, pkg, today, signal)
          counts[status]++
          if (ownershipMatch) bumpDeclaredOwnershipCounts(counts, ownershipMatch)
        } catch (err) {
          signal?.throwIfAborted()
          const message = err instanceof Error ? err.message : String(err)
          log.error({ purl: pkg.purl, error: message }, 'Unexpected error processing package')
          try {
            await markPackageError(qx, pkg)
          } catch {
            // best-effort — don't let a failed mark crash the batch
          }
          counts.error++
        }
      }),
    )
    signal?.throwIfAborted()

    const done = batchStart + group.length
    if (done % 1000 === 0 || done === packages.length) {
      log.info({ done, total: packages.length, ...counts }, 'Progress')
    }
  }

  if (counts.processed === 0 && counts.skipped === 0 && counts.error > 0) {
    throw new Error(
      `RubyGems core batch made no progress (${counts.error} errors, likely rate-limited) — failing to trigger backoff`,
    )
  }

  return counts
}
