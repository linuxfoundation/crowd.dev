import {
  IDbNuGetVersionUpsert,
  NuGetPackageToSync,
  QueryExecutor,
  listNuGetPackagesToSync,
  logAuditFieldChange,
  recordNuGetDownloadSnapshot,
  replacePackageMaintainers,
  upsertMaintainer,
  upsertNuGetPackage,
  upsertNuGetVersionsBatch,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getNuGetConfig } from '../config'

import { fetchRegistration, fetchSearch } from './client'
import { normalizeNuGetPackage } from './normalize'
import { BatchResult, isNuGetFetchError } from './types'

const log = getServiceChildLogger('nuget')

type NuGetConfig = ReturnType<typeof getNuGetConfig>
type PackageRow = NuGetPackageToSync

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

function nugetRegistryUrl(packageId: string): string {
  return `https://www.nuget.org/packages/${packageId}`
}

type PackageStatus = 'processed' | 'skipped' | 'error' | 'unchanged'

async function processPackage(
  qx: QueryExecutor,
  pkg: PackageRow,
  config: NuGetConfig,
  today: string,
): Promise<PackageStatus> {
  const packageId = pkg.name

  const [searchResult, registrationResult] = await Promise.all([
    fetchSearch(packageId),
    fetchRegistration(packageId),
  ])

  if (isNuGetFetchError(registrationResult)) {
    if (registrationResult.kind === 'NOT_FOUND') {
      await upsertNuGetPackage(qx, {
        purl: pkg.purl,
        name: pkg.name,
        description: null,
        homepage: null,
        declaredRepositoryUrl: null,
        repositoryUrl: null,
        licenses: null,
        licensesRaw: null,
        keywords: null,
        status: null,
        latestVersion: pkg.latestVersion,
        versionsCount: null,
        firstReleaseAt: null,
        latestReleaseAt: null,
        registryUrl: nugetRegistryUrl(packageId),
        ingestionSource: 'nuget_not_found',
        dependentPackagesCount: pkg.dependentPackagesCount,
        dependentReposCount: pkg.dependentReposCount,
      })
      log.warn({ purl: pkg.purl }, 'Package not found on NuGet registry — writing minimal record')
      return 'skipped'
    }
    if (registrationResult.kind === 'RATE_LIMIT') {
      log.warn({ purl: pkg.purl }, 'Rate limited by NuGet registry — will retry next pass')
      return 'error'
    }
    throw new Error(
      `Transient error fetching registration for ${pkg.purl}: ${registrationResult.message}`,
    )
  }

  const searchItem = isNuGetFetchError(searchResult) ? null : searchResult

  const normalized = normalizeNuGetPackage(packageId, searchItem, registrationResult)

  await withDeadlockRetry(() =>
    qx.tx(async (t) => {
      const changed = new Set<string>()

      const { id: packageDbId, changedFields: pkgChanged } = await upsertNuGetPackage(t, {
        purl: pkg.purl,
        name: pkg.name,
        description: normalized.description,
        homepage: normalized.homepage,
        declaredRepositoryUrl: normalized.declaredRepositoryUrl,
        repositoryUrl: normalized.repositoryUrl,
        licenses: normalized.licenses,
        licensesRaw: normalized.licensesRaw,
        keywords: normalized.keywords,
        status: normalized.status,
        latestVersion: normalized.latestVersion,
        versionsCount: normalized.versionsCount > 0 ? normalized.versionsCount : null,
        firstReleaseAt: normalized.firstReleaseAt,
        latestReleaseAt: normalized.latestReleaseAt,
        registryUrl: nugetRegistryUrl(packageId),
        ingestionSource: 'nuget-registry',
        dependentPackagesCount: pkg.dependentPackagesCount,
        dependentReposCount: pkg.dependentReposCount,
      })
      pkgChanged.forEach((f) => changed.add(f))

      if (normalized.versions.length > 0) {
        const versionRows: IDbNuGetVersionUpsert[] = normalized.versions.map((v) => ({
          packageId: packageDbId,
          name: pkg.name,
          number: v.number,
          publishedAt: v.publishedAt,
          isLatest: v.isLatest,
          isPrerelease: v.isPrerelease,
          isYanked: v.isYanked,
          licenses: v.licenses,
          downloadCount: v.downloadCount !== null ? BigInt(v.downloadCount) : null,
        }))
        const verChanged = await upsertNuGetVersionsBatch(t, versionRows)
        verChanged.forEach((f) => changed.add(f))
      }

      const allPeople = [
        ...normalized.owners.map((username) => ({ username, role: 'maintainer' as const })),
        ...normalized.authors
          .filter((a) => !normalized.owners.includes(a))
          .map((username) => ({ username, role: 'author' as const })),
      ].sort((a, b) => a.username.localeCompare(b.username))

      const maintainerLinks: Array<{ maintainerId: number; role: 'author' | 'maintainer' }> = []
      for (const person of allPeople) {
        if (!person.username) continue
        const { id: maintainerId, changedFields: mChanged } = await upsertMaintainer(t, {
          ecosystem: 'nuget',
          username: person.username,
          displayName: person.username,
          url: null,
          email: null,
        })
        mChanged.forEach((f) => changed.add(f))
        maintainerLinks.push({ maintainerId, role: person.role })
      }

      if (maintainerLinks.length > 0) {
        const pmChanged = await replacePackageMaintainers(t, packageDbId, maintainerLinks)
        pmChanged.forEach((f) => changed.add(f))
      }

      if (normalized.totalDownloads > 0) {
        const dlChanged = await recordNuGetDownloadSnapshot(t, {
          packageId: packageDbId,
          purl: pkg.purl,
          totalDownloads: normalized.totalDownloads,
          today,
        })
        dlChanged.forEach((f) => changed.add(f))
      }

      await logAuditFieldChange(t, 'nuget', pkg.purl, Array.from(changed))

      log.debug(
        {
          purl: pkg.purl,
          versions: normalized.versions.length,
          maintainers: maintainerLinks.length,
          totalDownloads: normalized.totalDownloads,
        },
        'ok',
      )
    }),
  )

  return 'processed'
}

export async function processBatch(
  qx: QueryExecutor,
  config: NuGetConfig,
  today: string,
): Promise<BatchResult> {
  const packages = await listNuGetPackagesToSync(qx, {
    limit: config.batchSize,
    isCritical: config.isCritical,
  })

  if (packages.length === 0) return { processed: 0, skipped: 0, error: 0, unchanged: 0 }

  log.info({ count: packages.length }, 'Batch started')

  const counts = { processed: 0, skipped: 0, error: 0, unchanged: 0 }

  for (let batchStart = 0; batchStart < packages.length; batchStart += config.concurrency) {
    const group = packages.slice(batchStart, batchStart + config.concurrency)

    if (config.groupDelayMs > 0 && batchStart > 0) {
      await new Promise((r) => setTimeout(r, config.groupDelayMs))
    }

    await Promise.all(
      group.map(async (pkg) => {
        try {
          const status = await processPackage(qx, pkg, config, today)
          counts[status]++
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          log.error({ purl: pkg.purl, error: message }, 'Unexpected error processing package')
          counts.error++
        }
      }),
    )

    const done = batchStart + group.length
    if (done % 1000 === 0 || done === packages.length) {
      log.info({ done, total: packages.length, ...counts }, 'Progress')
    }
  }

  return counts
}
