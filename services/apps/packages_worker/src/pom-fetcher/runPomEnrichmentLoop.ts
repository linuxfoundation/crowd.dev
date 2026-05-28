import crypto from 'crypto'

import {
  listMavenPackagesToSync,
  upsertMaintainer,
  upsertPackage,
  upsertPackageMaintainer,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getPomFetcherConfig } from '../config'
import { extractArtifact, normalizeScmUrl } from './extract'
import { resolveLatestVersion } from './metadata'

const log = getServiceChildLogger('pom-fetcher')

// ─── Types ────────────────────────────────────────────────────────────────────

export interface BatchResult {
  processed: number
  skipped: number
  errors: number
}

type PackageToSync = Awaited<ReturnType<typeof listMavenPackagesToSync>>[number]

// ─── Non-critical: copy universe stats into packages ─────────────────────────

function mavenRegistryUrl(groupId: string, artifactId: string): string {
  return `https://central.sonatype.com/artifact/${groupId}/${artifactId}`
}

async function processNonCriticalPackage(qx: QueryExecutor, pkg: PackageToSync): Promise<void> {
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
  pkg: PackageToSync,
): Promise<'processed' | 'skipped' | 'error'> {
  const groupId = pkg.namespace
  const artifactId = pkg.name

  if (!groupId) {
    log.warn({ purl: pkg.purl }, 'Skipping critical package with null namespace (groupId)')
    return 'skipped'
  }

  let version = pkg.latestVersion ?? null
  if (!version) {
    log.debug({ groupId, artifactId }, 'No baseline version — falling back to maven-metadata.xml')
    version = await resolveLatestVersion(groupId, artifactId)
  }

  if (!version) {
    log.warn({ groupId, artifactId }, 'Could not resolve latest version, skipping')
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
    return 'skipped'
  }

  const result = await extractArtifact(groupId, artifactId, version, (msg) => {
    log.debug({ groupId, artifactId, version }, msg)
  })

  if (result.error) {
    log.warn({ groupId, artifactId, version, error: result.error }, 'POM extraction error')
    return 'error'
  }

  const packageId = await upsertPackage(qx, {
    purl: pkg.purl,
    ecosystem: 'maven',
    namespace: groupId,
    name: artifactId,
    description: result.description,
    homepage: result.homepageUrl,
    registryUrl: mavenRegistryUrl(groupId, artifactId),
    declaredRepositoryUrl: result.scmUrl,
    repositoryUrl: normalizeScmUrl(result.scmUrl),
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

  for (const person of allPeople) {
    const username = person.username ?? person.email ?? person.displayName
    if (!username) continue

    const emailHash = person.email
      ? crypto.createHash('sha256').update(person.email.toLowerCase().trim()).digest('hex')
      : null

    const maintainerId = await upsertMaintainer(qx, {
      ecosystem: 'maven',
      username,
      displayName: person.displayName,
      url: person.url,
      emailHash,
    })

    await upsertPackageMaintainer(qx, {
      packageId,
      maintainerId,
      role: person.role,
    })
  }

  return 'processed'
}

// ─── Batch processing ─────────────────────────────────────────────────────────

export async function processBatch(
  qx: QueryExecutor,
  config: ReturnType<typeof getPomFetcherConfig>,
  isCritical: boolean,
): Promise<BatchResult> {
  const batchSize = isCritical ? config.batchSize : config.nonCriticalBatchSize
  const concurrency = isCritical ? config.concurrency : config.nonCriticalConcurrency

  const packages = await listMavenPackagesToSync(qx, {
    limit: batchSize,
    offset: 0,
    fullRefreshDays: config.fullRefreshDays,
    nonCriticalRefreshDays: config.nonCriticalRefreshDays,
    isCritical,
  })

  if (packages.length === 0) {
    return { processed: 0, skipped: 0, errors: 0 }
  }

  log.info({ count: packages.length, isCritical }, 'Processing batch...')

  let processed = 0
  let skipped = 0
  let errors = 0
  const PROGRESS_EVERY = 25

  for (let i = 0; i < packages.length; i += concurrency) {
    const group = packages.slice(i, i + concurrency)

    await Promise.all(
      group.map(async (pkg) => {
        try {
          if (!isCritical) {
            await processNonCriticalPackage(qx, pkg)
            processed++
            return
          }

          const status = await processCriticalPackage(qx, pkg)
          if (status === 'processed') processed++
          else if (status === 'skipped') skipped++
          else errors++
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          log.error({ purl: pkg.purl, error: message }, 'Unexpected error processing package')
          errors++
        }
      }),
    )

    const done = i + group.length
    const prevDone = i
    const crossedBoundary = Math.floor(done / PROGRESS_EVERY) > Math.floor(prevDone / PROGRESS_EVERY)
    if (crossedBoundary || done === packages.length) {
      log.debug(
        { done, total: packages.length, processed, skipped, errors },
        `Progress: ${done}/${packages.length}`,
      )
    }
  }

  return { processed, skipped, errors }
}

// ─── Phase runner ─────────────────────────────────────────────────────────────

async function runPhase(
  qx: QueryExecutor,
  config: ReturnType<typeof getPomFetcherConfig>,
  isCritical: boolean,
  isShuttingDown: () => boolean,
): Promise<{ processed: number; skipped: number; errors: number }> {
  const label = isCritical ? 'critical' : 'non-critical'
  let total = { processed: 0, skipped: 0, errors: 0 }
  let batchNum = 0
  const phaseStartedAt = Date.now()

  log.info({ phase: label }, 'Phase started')

  while (!isShuttingDown()) {
    const result = await processBatch(qx, config, isCritical)

    if (result.processed + result.skipped + result.errors === 0) {
      const durationSec = Math.round((Date.now() - phaseStartedAt) / 1000)
      log.info({ phase: label, ...total, durationSec }, 'Phase complete')
      return total
    }

    batchNum++
    total.processed += result.processed
    total.skipped += result.skipped
    total.errors += result.errors

    log.info(
      {
        phase: label,
        batch: batchNum,
        totalProcessed: total.processed,
        totalSkipped: total.skipped,
        totalErrors: total.errors,
        elapsedSec: Math.round((Date.now() - phaseStartedAt) / 1000),
      },
      'Batch done',
    )
  }

  return total
}

// ─── Main loop ────────────────────────────────────────────────────────────────

export async function runPomEnrichmentLoop(
  qx: QueryExecutor,
  config: ReturnType<typeof getPomFetcherConfig>,
  isShuttingDown: () => boolean,
): Promise<void> {
  let passNumber = 0

  while (!isShuttingDown()) {
    passNumber++
    const passStartedAt = Date.now()
    log.info({ pass: passNumber }, 'Starting pass')

    // Phase 1: non-critical first — DB-only, high throughput
    const nonCritical = await runPhase(qx, config, false, isShuttingDown)

    // Phase 2: critical — HTTP-bound, lower throughput
    const critical = await runPhase(qx, config, true, isShuttingDown)

    const durationMs = Date.now() - passStartedAt
    log.info(
      {
        pass: passNumber,
        totalProcessed: nonCritical.processed + critical.processed,
        totalSkipped: nonCritical.skipped + critical.skipped,
        totalErrors: nonCritical.errors + critical.errors,
        durationSec: Math.round(durationMs / 1000),
      },
      `Pass complete. Sleeping ${config.idleSleepSec}s before next pass.`,
    )

    await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
  }
}
