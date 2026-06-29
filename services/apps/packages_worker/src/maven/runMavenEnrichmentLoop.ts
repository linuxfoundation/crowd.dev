import {
  MavenPackageToSync,
  QueryExecutor,
  listMavenPackagesToSync,
  logAuditFieldChange,
  replacePackageMaintainers,
  touchPackageSyncedAt,
  upsertMaintainer,
  upsertMavenPackageRepo,
  upsertPackage,
  upsertRepo,
  upsertVersionsBatch,
} from '@crowd/data-access-layer'
import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'

import { extractArtifact, getPomCacheStats, normalizeScmUrl } from './extract'
import { isMavenFetchError, resolveVersionsList } from './metadata'
import { isPrerelease, parseRepoUrl } from './normalize'
import { resolveRegistryBaseUrl, resolveRegistryPageUrl } from './registry'

const log = getServiceChildLogger('maven')

// ─── Types ────────────────────────────────────────────────────────────────────

export interface BatchResult {
  processed: number
  skipped: number
  error: number
  unchanged: number
}

type CriticalStatus = 'processed' | 'skipped' | 'unchanged' | 'error'

interface CriticalPackageResult {
  status: CriticalStatus
}

// prettier-ignore
type MavenConfig = ReturnType<typeof getMavenConfig>
type PackageRow = MavenPackageToSync

// ─── Helpers ──────────────────────────────────────────────────────────────────

// prettier-ignore
async function writeRepoLink(qx: QueryExecutor, packageId: number, repositoryUrl: string | null, changed: Set<string>): Promise<void> {
  if (!repositoryUrl) return
  const parsed = parseRepoUrl(repositoryUrl)
  if (!parsed) return
  const repoId = await upsertRepo(qx, { url: repositoryUrl, ...parsed })
  const repoChanged = await upsertMavenPackageRepo(qx, {
    packageId,
    repoId,
    source: 'declared',
    confidence: 0.8,
  })
  repoChanged.forEach((f) => changed.add(f))
}

// Postgres deadlock (40P01) is transient: concurrent transactions upserting the same shared
// rows (e.g. maintainer 'hboutemy' across many org.apache packages, or the shared apache repo)
// can form a lock cycle. Re-running the whole transaction resolves it — the upserts are idempotent.
// prettier-ignore
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

// ─── Non-critical: copy universe stats into packages ─────────────────────────

// prettier-ignore
async function processNonCriticalPackage(qx: QueryExecutor, pkg: PackageRow): Promise<void> {
  await upsertPackage(qx, {
    purl: pkg.purl,
    ecosystem: 'maven',
    namespace: pkg.namespace,
    name: pkg.name,
    description: null,
    homepage: null,
    registryUrl: pkg.namespace ? resolveRegistryPageUrl(pkg.namespace, pkg.name) : null,
    declaredRepositoryUrl: null,
    repositoryUrl: null,
    licenses: null,
    licensesRaw: null,
    latestVersion: null,
    ingestionSource: 'packages_universe',
    dependentPackagesCount: pkg.dependentPackagesCount,
    dependentReposCount: pkg.dependentReposCount,
  })
}

// ─── Critical: full POM extraction ───────────────────────────────────────────

// prettier-ignore
async function processCriticalPackage(qx: QueryExecutor, pkg: PackageRow, forceFullExtraction: boolean): Promise<CriticalPackageResult> {
  const groupId = pkg.namespace
  const artifactId = pkg.name

  if (!groupId) {
    log.warn({ purl: pkg.purl }, 'Skipping: null namespace (groupId)')
    return { status: 'skipped' }
  }

  const baseUrl = resolveRegistryBaseUrl(groupId)

  // Phase 1: lightweight metadata fetch to get the current upstream version.
  const metadata = await resolveVersionsList(groupId, artifactId, baseUrl)

  if (isMavenFetchError(metadata)) {
    if (metadata.kind === 'NOT_FOUND') {
      await upsertPackage(qx, {
        purl: pkg.purl,
        ecosystem: 'maven',
        namespace: groupId,
        name: artifactId,
        description: null,
        homepage: null,
        registryUrl: resolveRegistryPageUrl(groupId, artifactId),
        declaredRepositoryUrl: null,
        repositoryUrl: null,
        licenses: null,
        licensesRaw: null,
        latestVersion: pkg.latestVersion ?? null,
        ingestionSource: 'maven_not_on_central',
        dependentPackagesCount: pkg.dependentPackagesCount,
        dependentReposCount: pkg.dependentReposCount,
      })
      log.warn({ groupId, artifactId, baseUrl }, 'Not found in registry — writing minimal record')
      return { status: 'skipped' }
    }
    if (metadata.kind === 'RATE_LIMIT') {
      log.warn(
        { groupId, artifactId, status: metadata.status },
        'Rate limited — will retry next pass',
      )
      return { status: 'error' }
    }
    throw new Error(
      `Transient error fetching metadata for ${groupId}:${artifactId} — ${metadata.message}`,
    )
  }

  const version = metadata.releaseVersion

  if (!version) {
    await upsertPackage(qx, {
      purl: pkg.purl,
      ecosystem: 'maven',
      namespace: groupId,
      name: artifactId,
      description: null,
      homepage: null,
      registryUrl: resolveRegistryPageUrl(groupId, artifactId),
      declaredRepositoryUrl: null,
      repositoryUrl: null,
      licenses: null,
      licensesRaw: null,
      latestVersion: null,
      ingestionSource: 'maven_no_version',
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
    })
    log.warn({ groupId, artifactId }, 'No release version in metadata — writing minimal record')
    return { status: 'skipped' }
  }

  // Phase 2: skip full POM extraction when upstream version matches what we already have.
  if (!forceFullExtraction && version === pkg.latestVersion) {
    await touchPackageSyncedAt(qx, pkg.purl, {
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
    })
    log.debug({ groupId, artifactId, version }, 'Version unchanged — skipping POM extraction')
    return { status: 'unchanged' }
  }

  // Phase 3: full POM extraction with parent-chain resolution — wrapped in a
  // transaction so partial writes never leave the package in an inconsistent state.
  const result = await extractArtifact(groupId, artifactId, version, baseUrl)

  if (result.error) {
    log.warn({ groupId, artifactId, version, error: result.error }, 'POM extraction failed')
    await upsertPackage(qx, {
      purl: pkg.purl,
      ecosystem: 'maven',
      namespace: groupId,
      name: artifactId,
      description: null,
      homepage: null,
      registryUrl: resolveRegistryPageUrl(groupId, artifactId),
      declaredRepositoryUrl: null,
      repositoryUrl: null,
      licenses: null,
      licensesRaw: null,
      latestVersion: version,
      ingestionSource: 'maven_error',
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
    })
    return { status: 'error' }
  }

  const repositoryUrl = normalizeScmUrl(result.scmUrl)

  await withDeadlockRetry(() =>
    qx.tx(async (t) => {
      const changed = new Set<string>()

      const { id: packageId, changedFields: pkgChanged } = await upsertPackage(t, {
        purl: pkg.purl,
        ecosystem: 'maven',
        namespace: groupId,
        name: artifactId,
        description: result.description,
        homepage: result.homepageUrl,
        registryUrl: resolveRegistryPageUrl(groupId, artifactId),
        declaredRepositoryUrl: result.scmUrl,
        repositoryUrl,
        licenses: result.licenses.length > 0 ? result.licenses : null,
        licensesRaw: result.licensesRaw,
        latestVersion: version,
        versionsCount: metadata.versions.length > 0 ? metadata.versions.length : null,
        latestReleaseAt: metadata.lastUpdated,
        ingestionSource: 'maven-registry',
        dependentPackagesCount: pkg.dependentPackagesCount,
        dependentReposCount: pkg.dependentReposCount,
      })
      pkgChanged.forEach((f) => changed.add(f))

      const allVersions = metadata.versions.length > 0 ? metadata.versions : [version]
      const verChanged = await upsertVersionsBatch(
        t,
        allVersions.map((v) => ({
          packageId,
          ecosystem: 'maven',
          namespace: groupId,
          name: artifactId,
          number: v,
          isLatest: v === metadata.releaseVersion,
          isPrerelease: isPrerelease(v),
          license: result.licenses[0] ?? null,
        })),
      )
      verChanged.forEach((f) => changed.add(f))

      const allPeople = [
        ...result.developers.map((d) => ({ ...d, role: 'author' as const })),
        ...result.contributors.map((c) => ({ ...c, role: 'maintainer' as const })),
      ].sort((a, b) => {
        // Stable order on the shared maintainers table so concurrent transactions acquire
        // row locks in the same order → no deadlock cycles.
        const ka = a.username ?? a.email ?? a.displayName ?? ''
        const kb = b.username ?? b.email ?? b.displayName ?? ''
        return ka < kb ? -1 : ka > kb ? 1 : 0
      })

      const maintainerLinks: Array<{ maintainerId: number; role: 'author' | 'maintainer' }> = []
      for (const person of allPeople) {
        const username = person.username ?? person.email ?? person.displayName
        if (!username) continue
        const { id: maintainerId, changedFields: mChanged } = await upsertMaintainer(t, {
          ecosystem: 'maven',
          username,
          displayName: person.displayName,
          url: person.url,
          email: person.email ?? null,
        })
        mChanged.forEach((f) => changed.add(f))
        maintainerLinks.push({ maintainerId, role: person.role })
      }

      if (maintainerLinks.length > 0) {
        const pmChanged = await replacePackageMaintainers(t, packageId, maintainerLinks)
        pmChanged.forEach((f) => changed.add(f))
      }

      await writeRepoLink(t, packageId, repositoryUrl, changed)

      await logAuditFieldChange(t, 'maven', pkg.purl, Array.from(changed))

      log.debug(
        {
          groupId,
          artifactId,
          version,
          parentHops: result.parentHops,
          licenses: result.licenses.length,
          maintainers: maintainerLinks.length,
          versions: allVersions.length,
        },
        'ok',
      )
    }),
  )

  return { status: 'processed' }
}

// ─── Batch processing ─────────────────────────────────────────────────────────

// prettier-ignore
export async function processBatch(qx: QueryExecutor, config: MavenConfig, isCritical: boolean, forceFullExtraction: boolean): Promise<BatchResult> {
  const batchSize = isCritical ? config.batchSize : config.nonCriticalBatchSize
  const refreshDays = config.refreshDays

  const packages = await listMavenPackagesToSync(qx, { limit: batchSize, refreshDays, isCritical })

  return processPackages(qx, config, packages, isCritical, forceFullExtraction)
}

// Runs a concrete list of packages through the enrichment pipeline.
// prettier-ignore
async function processPackages(qx: QueryExecutor, config: MavenConfig, packages: PackageRow[], isCritical: boolean, forceFullExtraction: boolean): Promise<BatchResult> {
  const concurrency = isCritical ? config.concurrency : config.nonCriticalConcurrency

  if (packages.length === 0) return { processed: 0, skipped: 0, error: 0, unchanged: 0 }

  // Cluster the batch by namespace so artifacts sharing a parent POM are processed
  // adjacently — this is what makes the parent-POM cache effective. The criticality
  // ordering only decides *which* packages are in the batch (via the SQL LIMIT);
  // it does not group same-namespace siblings, so we reorder here. Only matters on
  // the critical path (the non-critical path issues no POM/parent HTTP).
  if (isCritical) {
    packages.sort(
      (a, b) =>
        (a.namespace ?? '').localeCompare(b.namespace ?? '') || a.name.localeCompare(b.name),
    )
  }

  log.info({ count: packages.length, isCritical }, 'Batch started')

  const counts = { processed: 0, skipped: 0, error: 0, unchanged: 0 }

  for (let batchStart = 0; batchStart < packages.length; batchStart += concurrency) {
    const group = packages.slice(batchStart, batchStart + concurrency)

    if (isCritical && config.groupDelayMs > 0 && batchStart > 0) {
      await new Promise((r) => setTimeout(r, config.groupDelayMs))
    }

    await Promise.all(
      group.map(async (pkg) => {
        try {
          if (!isCritical) {
            await processNonCriticalPackage(qx, pkg)
            counts.processed++
            return
          }

          const res = await processCriticalPackage(qx, pkg, forceFullExtraction)
          counts[res.status]++
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

  if (isCritical) {
    // POM cache only fills on the critical path (parent-chain resolution).
    log.info(getPomCacheStats(), 'POM cache')
  }

  return counts
}

// ─── Phase runner ─────────────────────────────────────────────────────────────

// prettier-ignore
async function runPhase(qx: QueryExecutor, config: MavenConfig, isCritical: boolean, isShuttingDown: () => boolean): Promise<BatchResult> {
  const label = isCritical ? 'critical' : 'non-critical'
  const total: BatchResult = {
    processed: 0,
    skipped: 0,
    error: 0,
    unchanged: 0,
  }
  let batchNum = 0
  const phaseStartedAt = Date.now()

  log.info({ phase: label }, 'Phase started')

  while (!isShuttingDown()) {
    // The standalone loop is the backfill entry point → always full extraction.
    const result = await processBatch(qx, config, isCritical, true)

    if (result.processed + result.skipped + result.error + result.unchanged === 0) {
      const durationSec = Math.round((Date.now() - phaseStartedAt) / 1000)
      log.info({ phase: label, ...total, durationSec }, 'Phase complete')
      return total
    }

    batchNum++
    total.processed += result.processed
    total.skipped += result.skipped
    total.error += result.error
    total.unchanged += result.unchanged

    log.info(
      {
        phase: label,
        batch: batchNum,
        totalProcessed: total.processed,
        totalSkipped: total.skipped,
        totalUnchanged: total.unchanged,
        totalErrors: total.error,
        elapsedSec: Math.round((Date.now() - phaseStartedAt) / 1000),
      },
      'Batch done',
    )
  }

  return total
}

// ─── One-shot backfill ──────────────────────────────────────────────────────────

/**
 * Drains the Tier 2 critical queue once, with full POM extraction, and returns
 * the totals. It does NOT idle-loop — it runs until a batch comes back empty (or
 * shutdown is requested) and then returns, so the caller can exit. Meant to be
 * triggered manually (e.g. `pnpm backfill:maven` execed into the packages-worker
 * container).
 */
// prettier-ignore
export async function runMavenCriticalBackfill(qx: QueryExecutor, config: MavenConfig, isShuttingDown: () => boolean): Promise<BatchResult> {
  return runPhase(qx, config, true, isShuttingDown)
}
