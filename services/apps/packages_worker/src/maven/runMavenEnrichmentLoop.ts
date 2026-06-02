import crypto from 'crypto'

import {
  listMavenPackagesToSync,
  logAuditFieldChange,
  replacePackageMaintainers,
  touchPackageSyncedAt,
  upsertMaintainer,
  upsertPackage,
  upsertPackageRepo,
  upsertRepo,
  upsertVersionsBatch,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { MAX_PARENT_HOPS, extractArtifact, normalizeScmUrl } from './extract'
import { isMavenFetchError, resolveVersionsList } from './metadata'
import { isPrerelease, parseRepoUrl } from './normalize'

const log = getServiceChildLogger('maven')

// ─── Types ────────────────────────────────────────────────────────────────────

export interface BatchResult {
  processed: number
  skipped: number
  error: number
  unchanged: number
}

type MavenConfig = ReturnType<typeof getMavenConfig>
type PackageRow = Awaited<ReturnType<typeof listMavenPackagesToSync>>[number]

// ─── Helpers ──────────────────────────────────────────────────────────────────

function mavenRegistryUrl(groupId: string, artifactId: string): string {
  return `https://central.sonatype.com/artifact/${groupId}/${artifactId}`
}

async function writeRepoLink(
  qx: QueryExecutor,
  packageId: number,
  repositoryUrl: string | null,
  changed: Set<string>,
): Promise<void> {
  if (!repositoryUrl) return
  const parsed = parseRepoUrl(repositoryUrl)
  if (!parsed) return
  const repoId = await upsertRepo(qx, { url: repositoryUrl, ...parsed })
  const repoChanged = await upsertPackageRepo(qx, { packageId, repoId, source: 'declared', confidence: 0.8 })
  repoChanged.forEach((f) => changed.add(f))
}

// ─── Non-critical: copy universe stats into packages ─────────────────────────

async function processNonCriticalPackage(qx: QueryExecutor, pkg: PackageRow): Promise<void> {
  await upsertPackage(qx, {
    purl: pkg.purl,
    ecosystem: 'maven',
    namespace: pkg.namespace,
    name: pkg.name,
    description: null,
    homepage: null,
    registryUrl: pkg.namespace ? mavenRegistryUrl(pkg.namespace, pkg.name) : null,
    declaredRepositoryUrl: null,
    repositoryUrl: null,
    licenses: null,
    licensesRaw: null,
    latestVersion: null,
    ingestionSource: 'packages_universe',
    criticalityScore: pkg.criticalityScore,
    dependentPackagesCount: pkg.dependentPackagesCount,
    dependentReposCount: pkg.dependentReposCount,
    downloadsLastMonth: pkg.downloads30d,
  })
}

// ─── Critical: full POM extraction ───────────────────────────────────────────

async function processCriticalPackage(
  qx: QueryExecutor,
  pkg: PackageRow,
  forceFullExtraction: boolean,
): Promise<'processed' | 'skipped' | 'unchanged' | 'error'> {
  const groupId = pkg.namespace
  const artifactId = pkg.name

  if (!groupId) {
    log.warn({ purl: pkg.purl }, 'Skipping: null namespace (groupId)')
    return 'skipped'
  }

  // Phase 1: lightweight metadata fetch to get the current upstream version.
  const metadata = await resolveVersionsList(groupId, artifactId)

  if (isMavenFetchError(metadata)) {
    if (metadata.kind === 'NOT_FOUND') {
      await upsertPackage(qx, {
        purl: pkg.purl,
        ecosystem: 'maven',
        namespace: groupId,
        name: artifactId,
        description: null,
        homepage: null,
        registryUrl: mavenRegistryUrl(groupId, artifactId),
        declaredRepositoryUrl: null,
        repositoryUrl: null,
        licenses: null,
        licensesRaw: null,
        latestVersion: pkg.latestVersion ?? null,
        ingestionSource: 'maven_not_on_central',
        criticalityScore: pkg.criticalityScore,
        dependentPackagesCount: pkg.dependentPackagesCount,
        dependentReposCount: pkg.dependentReposCount,
        downloadsLastMonth: pkg.downloads30d,
      })
      log.warn({ groupId, artifactId }, 'Not on Maven Central — writing minimal record')
      return 'skipped'
    }
    if (metadata.kind === 'RATE_LIMIT') {
      log.warn({ groupId, artifactId, status: metadata.status }, 'Rate limited — will retry next pass')
      return 'error'
    }
    throw new Error(`Transient error fetching metadata for ${groupId}:${artifactId} — ${metadata.message}`)
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
      registryUrl: mavenRegistryUrl(groupId, artifactId),
      declaredRepositoryUrl: null,
      repositoryUrl: null,
      licenses: null,
      licensesRaw: null,
      latestVersion: null,
      ingestionSource: 'maven_no_version',
      criticalityScore: pkg.criticalityScore,
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
      downloadsLastMonth: pkg.downloads30d,
    })
    log.warn({ groupId, artifactId }, 'No release version in metadata — writing minimal record')
    return 'skipped'
  }

  // Phase 2: skip full POM extraction when upstream version matches what we already have.
  if (!forceFullExtraction && version === pkg.latestVersion) {
    await touchPackageSyncedAt(qx, pkg.purl, {
      criticalityScore: pkg.criticalityScore,
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
      downloadsLastMonth: pkg.downloads30d,
    })
    log.debug({ groupId, artifactId, version }, 'Version unchanged — skipping POM extraction')
    return 'unchanged'
  }

  // Phase 3: full POM extraction with parent-chain resolution — wrapped in a
  // transaction so partial writes never leave the package in an inconsistent state.
  const result = await extractArtifact(groupId, artifactId, version)

  if (result.error) {
    log.warn({ groupId, artifactId, version, error: result.error }, 'POM extraction failed')
    await upsertPackage(qx, {
      purl: pkg.purl,
      ecosystem: 'maven',
      namespace: groupId,
      name: artifactId,
      description: null,
      homepage: null,
      registryUrl: mavenRegistryUrl(groupId, artifactId),
      declaredRepositoryUrl: null,
      repositoryUrl: null,
      licenses: null,
      licensesRaw: null,
      latestVersion: version,
      ingestionSource: 'maven_error',
      criticalityScore: pkg.criticalityScore,
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
      downloadsLastMonth: pkg.downloads30d,
    })
    return 'error'
  }

  if (result.parentHops > MAX_PARENT_HOPS) {
    log.warn(
      { groupId, artifactId, parentHops: result.parentHops, missingLicenses: result.licenses.length === 0, missingScm: !result.scmUrl },
      'Parent hop limit reached — data may be incomplete',
    )
  }

  const repositoryUrl = normalizeScmUrl(result.scmUrl)

  await qx.tx(async (t) => {
    const changed = new Set<string>()

    const { id: packageId, changedFields: pkgChanged } = await upsertPackage(t, {
      purl: pkg.purl,
      ecosystem: 'maven',
      namespace: groupId,
      name: artifactId,
      description: result.description,
      homepage: result.homepageUrl,
      registryUrl: mavenRegistryUrl(groupId, artifactId),
      declaredRepositoryUrl: result.scmUrl,
      repositoryUrl,
      licenses: result.licenses.length > 0 ? result.licenses : null,
      licensesRaw: result.licensesRaw,
      latestVersion: version,
      ingestionSource: 'maven-registry',
      criticalityScore: pkg.criticalityScore,
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
      downloadsLastMonth: pkg.downloads30d,
    })
    pkgChanged.forEach((f) => changed.add(f))

    const allVersions = metadata.versions.length > 0 ? metadata.versions : [version]
    const verChanged = await upsertVersionsBatch(
      t,
      allVersions.map((v) => ({
        packageId,
        ecosystem: 'maven',
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
    ]

    const maintainerLinks: Array<{ maintainerId: number; role: 'author' | 'maintainer' }> = []
    for (const person of allPeople) {
      const username = person.username ?? person.email ?? person.displayName
      if (!username) continue
      const emailHash = person.email
        ? crypto.createHash('sha256').update(person.email.toLowerCase().trim()).digest('hex')
        : null
      const { id: maintainerId, changedFields: mChanged } = await upsertMaintainer(t, {
        ecosystem: 'maven',
        username,
        displayName: person.displayName,
        url: person.url,
        emailHash,
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

    log.info(
      { groupId, artifactId, version, parentHops: result.parentHops, licenses: result.licenses.length, maintainers: maintainerLinks.length, versions: allVersions.length },
      'ok',
    )
  })

  return 'processed'
}

// ─── Batch processing ─────────────────────────────────────────────────────────

export async function processBatch(
  qx: QueryExecutor,
  config: MavenConfig,
  isCritical: boolean,
): Promise<BatchResult> {
  const batchSize = isCritical ? config.batchSize : config.nonCriticalBatchSize
  const concurrency = isCritical ? config.concurrency : config.nonCriticalConcurrency
  const refreshDays = config.refreshDays
  const forceFullExtraction = config.forceFullExtraction

  const packages = await listMavenPackagesToSync(qx, { limit: batchSize, refreshDays, isCritical })

  if (packages.length === 0) return { processed: 0, skipped: 0, error: 0, unchanged: 0 }

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

          const status = await processCriticalPackage(qx, pkg, forceFullExtraction)
          counts[status]++
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          log.error({ purl: pkg.purl, error: message }, 'Unexpected error processing package')
          counts.error++
        }
      }),
    )

    const done = batchStart + group.length
    if (done % 25 === 0 || done === packages.length) {
      log.debug({ done, total: packages.length, ...counts }, 'Progress')
    }
  }

  return counts
}

// ─── Phase runner ─────────────────────────────────────────────────────────────

async function runPhase(
  qx: QueryExecutor,
  config: MavenConfig,
  isCritical: boolean,
  isShuttingDown: () => boolean,
): Promise<BatchResult> {
  const label = isCritical ? 'critical' : 'non-critical'
  const total: BatchResult = { processed: 0, skipped: 0, error: 0, unchanged: 0 }
  let batchNum = 0
  const phaseStartedAt = Date.now()

  log.info({ phase: label }, 'Phase started')

  while (!isShuttingDown()) {
    const result = await processBatch(qx, config, isCritical)

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

// ─── Main loop ────────────────────────────────────────────────────────────────

export async function runMavenEnrichmentLoop(
  qx: QueryExecutor,
  config: MavenConfig,
  isShuttingDown: () => boolean,
): Promise<void> {
  log.info(
    {
      batchSize: config.batchSize,
      concurrency: config.concurrency,
      nonCriticalBatchSize: config.nonCriticalBatchSize,
      nonCriticalConcurrency: config.nonCriticalConcurrency,
      refreshDays: config.refreshDays,
      forceFullExtraction: config.forceFullExtraction,
    },
    config.forceFullExtraction
      ? 'POM fetcher started — FORCE FULL EXTRACTION (version-unchanged check disabled)'
      : 'POM fetcher started',
  )

  let passNumber = 0

  while (!isShuttingDown()) {
    passNumber++
    const passStartedAt = Date.now()
    log.info({ pass: passNumber }, 'Pass started')

    const critical = await runPhase(qx, config, true, isShuttingDown)

    const durationSec = Math.round((Date.now() - passStartedAt) / 1000)
    log.info(
      {
        pass: passNumber,
        totalProcessed: critical.processed,
        totalSkipped: critical.skipped,
        totalUnchanged: critical.unchanged,
        totalErrors: critical.error,
        durationSec,
      },
      `Pass complete — sleeping ${config.idleSleepSec}s`,
    )

    await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
  }
}
