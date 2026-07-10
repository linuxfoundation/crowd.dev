import {
  IDbVersionUpsert,
  QueryExecutor,
  RubyGemsCriticalPackageToSync,
  listRubyGemsCriticalPackagesToSync,
  logAuditFieldChange,
  replacePackageMaintainers,
  upsertMaintainer,
  upsertPackage,
  upsertVersionsBatch,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { fetchOwners, fetchVersions } from './client'
import {
  normalizeRubyGemsOwners,
  normalizeRubyGemsVersions,
  pickLatestRubyGemsVersion,
} from './normalize'
import { BatchResult, isRubyGemsFetchError } from './types'

const log = getServiceChildLogger('rubygems-critical')

export type RubyGemsCriticalConfig = {
  batchSize: number
  concurrency: number
}

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

type PackageStatus = 'processed' | 'skipped' | 'error' | 'unchanged'

async function processPackage(
  qx: QueryExecutor,
  pkg: RubyGemsCriticalPackageToSync,
): Promise<PackageStatus> {
  const [versionsResult, ownersResult] = await Promise.all([
    fetchVersions(pkg.name),
    fetchOwners(pkg.name),
  ])

  if (isRubyGemsFetchError(versionsResult)) {
    if (versionsResult.kind === 'NOT_FOUND') {
      log.warn({ purl: pkg.purl }, 'Package not found on RubyGems versions endpoint — skipping')
      return 'skipped'
    }
    if (versionsResult.kind === 'RATE_LIMIT') {
      log.warn({ purl: pkg.purl }, 'Rate limited fetching RubyGems versions — will retry next pass')
      return 'error'
    }
    throw new Error(`Transient error fetching versions for ${pkg.purl}: ${versionsResult.message}`)
  }

  const normalizedVersions = normalizeRubyGemsVersions(versionsResult)
  const latest = pickLatestRubyGemsVersion(normalizedVersions)
  const ownersFetchFailed = isRubyGemsFetchError(ownersResult)
  const owners = ownersFetchFailed ? [] : normalizeRubyGemsOwners(ownersResult)

  await withDeadlockRetry(() =>
    qx.tx(async (t) => {
      const changed = new Set<string>()

      const { id: packageDbId, changedFields: pkgChanged } = await upsertPackage(t, {
        purl: pkg.purl,
        ecosystem: 'rubygems',
        namespace: null,
        name: pkg.name,
        description: null,
        homepage: null,
        declaredRepositoryUrl: null,
        licenses: null,
        licensesRaw: null,
        latestVersion: latest?.number ?? null,
        versionsCount: normalizedVersions.length > 0 ? normalizedVersions.length : null,
        latestReleaseAt: latest?.publishedAt ?? null,
        ingestionSource: 'rubygems-registry',
      })
      pkgChanged.forEach((f) => changed.add(f))

      if (normalizedVersions.length > 0) {
        const versionRows: IDbVersionUpsert[] = normalizedVersions.map((v) => ({
          packageId: packageDbId,
          ecosystem: 'rubygems',
          namespace: null,
          name: pkg.name,
          number: v.number,
          isLatest: v.number === latest?.number,
          isPrerelease: v.isPrerelease,
          license: v.licenses && v.licenses.length > 0 ? v.licenses[0] : null,
          publishedAt: v.publishedAt,
        }))
        const verChanged = await upsertVersionsBatch(t, versionRows)
        verChanged.forEach((f) => changed.add(f))
      }

      const maintainerLinks: Array<{ maintainerId: number; role: 'maintainer' }> = []
      for (const owner of owners) {
        const { id: maintainerId, changedFields: mChanged } = await upsertMaintainer(t, {
          ecosystem: 'rubygems',
          username: owner.username,
          displayName: owner.username,
          url: null,
          email: owner.email,
        })
        mChanged.forEach((f) => changed.add(f))
        maintainerLinks.push({ maintainerId, role: 'maintainer' })
      }

      if (!ownersFetchFailed) {
        const pmChanged = await replacePackageMaintainers(t, packageDbId, maintainerLinks)
        pmChanged.forEach((f) => changed.add(f))
      }

      await logAuditFieldChange(t, 'rubygems', pkg.purl, Array.from(changed))

      log.debug(
        {
          purl: pkg.purl,
          versions: normalizedVersions.length,
          maintainers: maintainerLinks.length,
        },
        'ok',
      )
    }),
  )

  return 'processed'
}

export async function processBatch(
  qx: QueryExecutor,
  config: RubyGemsCriticalConfig,
  afterId: number,
): Promise<BatchResult & { lastId: number | null }> {
  const packages = await listRubyGemsCriticalPackagesToSync(qx, {
    limit: config.batchSize,
    afterId,
  })

  if (packages.length === 0) {
    return { processed: 0, skipped: 0, error: 0, unchanged: 0, lastId: null }
  }

  log.info({ count: packages.length, afterId }, 'Critical batch started')

  const counts = { processed: 0, skipped: 0, error: 0, unchanged: 0 }

  for (let batchStart = 0; batchStart < packages.length; batchStart += config.concurrency) {
    const group = packages.slice(batchStart, batchStart + config.concurrency)

    await Promise.all(
      group.map(async (pkg) => {
        try {
          const status = await processPackage(qx, pkg)
          counts[status]++
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          log.error(
            { purl: pkg.purl, error: message },
            'Unexpected error processing critical package',
          )
          counts.error++
        }
      }),
    )

    const done = batchStart + group.length
    if (done % 1000 === 0 || done === packages.length) {
      log.info({ done, total: packages.length, ...counts }, 'Critical progress')
    }
  }

  return { ...counts, lastId: packages[packages.length - 1].id }
}
