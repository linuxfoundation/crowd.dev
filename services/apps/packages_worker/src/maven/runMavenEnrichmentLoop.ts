import crypto from 'crypto'

import {
  listMavenPackagesToSync,
  logAuditFieldChange,
  touchPackageSyncedAt,
  upsertMaintainer,
  upsertPackage,
  upsertPackageMaintainer,
  upsertPackageRepo,
  upsertRepo,
  upsertVersionsBatch,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getMavenConfig } from '../config'
import { MAX_PARENT_HOPS, extractArtifact, normalizeScmUrl } from './extract'
import { resolveVersionsList } from './metadata'

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

function isPrerelease(version: string): boolean {
  return /-(SNAPSHOT|alpha|beta|rc|m\d+)/i.test(version)
}

// Reorders packages so that consecutive items come from different namespaces (e.g. org.apache, com.google).
// This spreads Maven Central requests across different group IDs, avoiding bursts that could hit rate limits
// on the same namespace in a tight loop.
// function interleaveByNamespace(packages: PackageRow[]): PackageRow[] {
//   const byNamespace = new Map<string, PackageRow[]>()
//   for (const pkg of packages) {
//     const ns = pkg.namespace ?? '__unknown__'
//     if (!byNamespace.has(ns)) byNamespace.set(ns, [])
//     byNamespace.get(ns)!.push(pkg)
//   }
//   const queues = [...byNamespace.values()]
//   const result: PackageRow[] = []
//   let i = 0
//   while (result.length < packages.length) {
//     const q = queues[i % queues.length]
//     if (q.length > 0) result.push(q.shift()!)
//     i++
//   }
//   return result
// }

function parseRepoUrl(url: string): { host: string; owner: string | null; name: string | null } | null {
  try {
    const parsed = new URL(url)
    const h = parsed.hostname.toLowerCase()
    let host: string
    if (h === 'github.com' || h.endsWith('.github.com')) host = 'github'
    else if (h === 'gitlab.com' || h.includes('gitlab')) host = 'gitlab'
    else if (h === 'bitbucket.org') host = 'bitbucket'
    else host = 'other'
    const parts = parsed.pathname.split('/').filter(Boolean)
    return { host, owner: parts[0] ?? null, name: parts[1] ?? null }
  } catch {
    return null
  }
}

async function writeRepoLink(qx: QueryExecutor, packageId: number, repositoryUrl: string | null): Promise<void> {
  if (!repositoryUrl) return
  const parsed = parseRepoUrl(repositoryUrl)
  if (!parsed) return
  const repoId = await upsertRepo(qx, { url: repositoryUrl, ...parsed })
  await upsertPackageRepo(qx, { packageId, repoId, source: 'declared', confidence: 0.8 })
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

  if (!metadata) {
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
  // This avoids 1-8 HTTP calls (POM + parent chain) for packages that haven't released
  // a new version since the last sync.
  // Skipped on forceFullExtraction (first run against a fresh/restored DB) because
  // packages.latest_version may carry stale data from the dump.
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

  // Phase 3: full POM extraction with parent-chain resolution.
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

  const packageId = await upsertPackage(qx, {
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
    ingestionSource: 'maven',
    criticalityScore: pkg.criticalityScore,
    dependentPackagesCount: pkg.dependentPackagesCount,
    dependentReposCount: pkg.dependentReposCount,
    downloadsLastMonth: pkg.downloads30d,
  })

  const allPeople = [
    ...result.developers.map((d) => ({ ...d, role: 'author' as const })),
    ...result.contributors.map((c) => ({ ...c, role: 'maintainer' as const })),
  ]

  let maintainerCount = 0
  for (const person of allPeople) {
    const username = person.username ?? person.email ?? person.displayName
    if (!username) continue
    const emailHash = person.email ? crypto.createHash('sha256').update(person.email.toLowerCase().trim()).digest('hex') : null
    const maintainerId = await upsertMaintainer(qx, {
      ecosystem: 'maven',
      username,
      displayName: person.displayName,
      url: person.url,
      emailHash,
    })
    await upsertPackageMaintainer(qx, { packageId, maintainerId, role: person.role })
    maintainerCount++
  }

  const allVersions = metadata.versions.length > 0 ? metadata.versions : [version]
  await upsertVersionsBatch(
    qx,
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

  await writeRepoLink(qx, packageId, repositoryUrl)

  const auditFields = ['latest_version']
  if (result.licenses.length > 0) auditFields.push('licenses')
  if (repositoryUrl) auditFields.push('repository_url')
  if (result.description) auditFields.push('description')
  await logAuditFieldChange(qx, 'maven', pkg.purl, auditFields)

  log.info(
    { groupId, artifactId, version, parentHops: result.parentHops, licenses: result.licenses.length, maintainers: maintainerCount, versions: allVersions.length },
    'ok',
  )

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
  // interleaveByNamespace was introduced as a workaround when the local dev IP was throttled by Maven Central.
  // In production runs are 24h apart so the IP is always cold — leaving packages in their natural order for now.
  // const queue = isCritical ? interleaveByNamespace(packages) : packages
  // const queue = packages

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
          const isRateLimit = message.includes('403') || message.includes('429')
          log.error(
            { purl: pkg.purl, error: message },
            isRateLimit ? 'Rate limited — will retry next pass' : 'Unexpected error processing package',
          )
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

    // Phase 1: non-critical — DB-only, high throughput, no HTTP
    // const nonCritical = await runPhase(qx, config, false, isShuttingDown)

    // Phase 2: critical — HTTP-bound, two-phase version check + POM extraction
    const critical = await runPhase(qx, config, true, isShuttingDown)

    const durationSec = Math.round((Date.now() - passStartedAt) / 1000)
    log.info(
      {
        pass: passNumber,
        // totalProcessed: nonCritical.processed + critical.processed,
        // totalSkipped: nonCritical.skipped + critical.skipped,
        // totalUnchanged: nonCritical.unchanged + critical.unchanged,
        // totalErrors: nonCritical.error + critical.error,
        totalProcessed:  critical.processed,
        totalSkipped:  critical.skipped,
        totalUnchanged:  critical.unchanged,
        totalErrors:  critical.error,
        durationSec,
      },
      `Pass complete — sleeping ${config.idleSleepSec}s`,
    )

    await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
  }
}
