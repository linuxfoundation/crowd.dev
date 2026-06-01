import {
  listCriticalMavenPackagesToSync,
  upsertMaintainer,
  upsertPackage,
  upsertPackageMaintainer,
  upsertPackageRepo,
  upsertRepo,
  upsertVersionsBatch,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getPomFetcherConfig } from '../config'
import { MAX_PARENT_HOPS, extractArtifact, normalizeScmUrl } from './extract'
import { resolveVersionsList } from './metadata'

const log = getServiceChildLogger('pom-fetcher')

export interface BatchResult {
  processed: number
  skipped: number
  errors: number
}

type PomFetcherConfig = ReturnType<typeof getPomFetcherConfig>
type PackageRow = Awaited<ReturnType<typeof listCriticalMavenPackagesToSync>>[number]

// ─── Helpers ──────────────────────────────────────────────────────────────────

function mavenRegistryUrl(groupId: string, artifactId: string): string {
  return `https://central.sonatype.com/artifact/${groupId}/${artifactId}`
}

function isPrerelease(version: string): boolean {
  return /-(SNAPSHOT|alpha|beta|rc|m\d+)/i.test(version)
}

function interleaveByNamespace(packages: PackageRow[]): PackageRow[] {
  const byNamespace = new Map<string, PackageRow[]>()
  for (const pkg of packages) {
    const ns = pkg.namespace ?? '__unknown__'
    if (!byNamespace.has(ns)) byNamespace.set(ns, [])
    byNamespace.get(ns)!.push(pkg)
  }
  const queues = [...byNamespace.values()]
  const result: PackageRow[] = []
  let i = 0
  while (result.length < packages.length) {
    const q = queues[i % queues.length]
    if (q.length > 0) result.push(q.shift()!)
    i++
  }
  return result
}

interface PersonWithRole {
  username: string | null
  displayName: string | null
  email: string | null
  url: string | null
  role: 'author' | 'maintainer'
}

async function writeMaintainers(qx: QueryExecutor, packageId: number, people: PersonWithRole[]): Promise<number> {
  let count = 0
  for (const person of people) {
    const username = person.username ?? person.email ?? person.displayName
    if (!username) continue
    const maintainerId = await upsertMaintainer(qx, {
      ecosystem: 'maven',
      username,
      displayName: person.displayName,
      url: person.url,
      emailHash: person.email,
    })
    await upsertPackageMaintainer(qx, { packageId, maintainerId, role: person.role })
    count++
  }
  return count
}

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

// ─── Package processing ───────────────────────────────────────────────────────

async function processPackage(qx: QueryExecutor, pkg: PackageRow): Promise<'processed' | 'skipped' | 'error'> {
  const groupId = pkg.namespace
  const artifactId = pkg.name

  if (!groupId) {
    log.warn({ purl: pkg.purl }, 'Skipping: null namespace (groupId)')
    return 'skipped'
  }

  const meta = await resolveVersionsList(groupId, artifactId)

  if (!meta) {
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
      ingestionSource: 'pom_fetcher_not_on_central',
      criticalityScore: pkg.criticalityScore,
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
      downloadsLastMonth: pkg.downloads30d,
    })
    log.warn({ groupId, artifactId }, 'Not on Maven Central — writing minimal record')
    return 'skipped'
  }

  const version = meta.releaseVersion

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
      ingestionSource: 'pom_fetcher_no_version',
      criticalityScore: pkg.criticalityScore,
      dependentPackagesCount: pkg.dependentPackagesCount,
      dependentReposCount: pkg.dependentReposCount,
      downloadsLastMonth: pkg.downloads30d,
    })
    log.warn({ groupId, artifactId }, 'No release version in metadata — writing minimal record')
    return 'skipped'
  }

  const result = await extractArtifact(groupId, artifactId, version, (msg) => {
    log.debug({ groupId, artifactId, version }, msg)
  })

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
      ingestionSource: 'pom_fetcher_error',
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
    ingestionSource: 'pom_fetcher',
    criticalityScore: pkg.criticalityScore,
    dependentPackagesCount: pkg.dependentPackagesCount,
    dependentReposCount: pkg.dependentReposCount,
    downloadsLastMonth: pkg.downloads30d,
  })

  const allPeople = [
    ...result.developers.map((d) => ({ ...d, role: 'author' as const })),
    ...result.contributors.map((c) => ({ ...c, role: 'maintainer' as const })),
  ]
  const maintainerCount = await writeMaintainers(qx, packageId, allPeople)

  const allVersions = meta.versions.length > 0 ? meta.versions : [version]
  await upsertVersionsBatch(
    qx,
    allVersions.map((v) => ({
      packageId,
      ecosystem: 'maven',
      number: v,
      isLatest: v === meta.releaseVersion,
      isPrerelease: isPrerelease(v),
      license: result.licenses[0] ?? null,
    })),
  )

  await writeRepoLink(qx, packageId, repositoryUrl)

  log.info(
    { groupId, artifactId, version, parentHops: result.parentHops, licenses: result.licenses.length, maintainers: maintainerCount, versions: allVersions.length },
    'ok',
  )

  return 'processed'
}

// ─── Batch ────────────────────────────────────────────────────────────────────

export async function processBatch(qx: QueryExecutor, config: PomFetcherConfig): Promise<BatchResult> {
  const packages = await listCriticalMavenPackagesToSync(qx, {
    limit: config.batchSize,
    refreshDays: config.refreshDays,
  })

  if (packages.length === 0) return { processed: 0, skipped: 0, errors: 0 }

  log.info({ count: packages.length }, 'Batch started')

  let processed = 0
  let skipped = 0
  let errors = 0
  const queue = interleaveByNamespace(packages)

  for (let i = 0; i < queue.length; i += config.concurrency) {
    const group = queue.slice(i, i + config.concurrency)

    if (config.groupDelayMs > 0 && i > 0) {
      await new Promise((r) => setTimeout(r, config.groupDelayMs))
    }

    await Promise.all(
      group.map(async (pkg) => {
        try {
          const status = await processPackage(qx, pkg)
          if (status === 'processed') processed++
          else if (status === 'skipped') skipped++
          else errors++
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          const isRateLimit = message.includes('403') || message.includes('429')
          log.error(
            { purl: pkg.purl, error: message },
            isRateLimit ? 'Rate limited — will retry next pass' : 'Unexpected error processing package',
          )
          errors++
        }
      }),
    )

    const done = i + group.length
    if (done % 25 === 0 || done === queue.length) {
      log.debug({ done, total: queue.length, processed, skipped, errors }, 'Progress')
    }
  }

  return { processed, skipped, errors }
}

// ─── Main loop ────────────────────────────────────────────────────────────────

export async function runPomEnrichmentLoop(
  qx: QueryExecutor,
  config: PomFetcherConfig,
  isShuttingDown: () => boolean,
): Promise<void> {
  log.info({ batchSize: config.batchSize, concurrency: config.concurrency, refreshDays: config.refreshDays }, 'POM fetcher started')

  let passNumber = 0

  while (!isShuttingDown()) {
    passNumber++
    const passStartedAt = Date.now()
    log.info({ pass: passNumber }, 'Pass started')

    let total = { processed: 0, skipped: 0, errors: 0 }

    while (!isShuttingDown()) {
      const result = await processBatch(qx, config)
      if (result.processed + result.skipped + result.errors === 0) break
      total.processed += result.processed
      total.skipped += result.skipped
      total.errors += result.errors
    }

    const durationSec = Math.round((Date.now() - passStartedAt) / 1000)
    log.info({ pass: passNumber, ...total, durationSec }, `Pass complete — sleeping ${config.idleSleepSec}s`)

    await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
  }
}
