import crypto from 'crypto'

import {
  listMavenPackagesToEnrich,
  upsertMaintainer,
  upsertPackage,
  upsertPackageMaintainer,
} from '@crowd/data-access-layer'
import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getPomFetcherConfig } from '../config'
import { extractArtifact } from './extract'
import { resolveLatestVersion } from './metadata'

const log = getServiceChildLogger('pom-fetcher')

// ─── Types ────────────────────────────────────────────────────────────────────

interface BatchResult {
  processed: number
  skipped: number
  errors: number
}

// ─── Batch processing ─────────────────────────────────────────────────────────

async function processBatch(
  qx: QueryExecutor,
  config: ReturnType<typeof getPomFetcherConfig>,
): Promise<BatchResult> {
  const packages = await listMavenPackagesToEnrich(qx, {
    limit: config.batchSize,
    offset: 0,
    staleDays: config.staleDays,
  })

  if (packages.length === 0) {
    return { processed: 0, skipped: 0, errors: 0 }
  }

  log.info({ count: packages.length }, 'Processing POM batch...')

  let processed = 0
  let skipped = 0
  let errors = 0
  const PROGRESS_EVERY = 25

  // Process in small concurrent groups to be polite to Maven Central
  for (let i = 0; i < packages.length; i += config.concurrency) {
    const group = packages.slice(i, i + config.concurrency)

    await Promise.all(
      group.map(async (pkg) => {
        const groupId = pkg.namespace
        const artifactId = pkg.name

        if (!groupId) {
          log.warn({ purl: pkg.purl }, 'Skipping package with null namespace (groupId)')
          skipped++
          return
        }

        try {
          // Step 1: resolve latest version from maven-metadata.xml
          const version = await resolveLatestVersion(groupId, artifactId)
          if (!version) {
            log.warn({ groupId, artifactId }, 'Could not resolve latest version, skipping')
            // Upsert a minimal record so last_synced_at is set — prevents this package
            // from re-appearing in every batch within the same pass.
            // ingestionSource 'pom_fetcher_no_version' marks that it was tried but had no
            // resolvable version on Maven Central (404 on maven-metadata.xml).
            await upsertPackage(qx, {
              purl: `pkg:maven/${groupId}/${artifactId}`,
              ecosystem: 'maven',
              namespace: groupId,
              name: artifactId,
              description: null,
              homepage: null,
              declaredRepositoryUrl: null,
              licenses: null,
              licensesRaw: null,
              latestVersion: null,
              ingestionSource: 'pom_fetcher_no_version',
            })
            skipped++
            return
          }

          // Step 2: fetch + resolve POM (follows parent chain)
          const result = await extractArtifact(groupId, artifactId, version, (msg) => {
            log.debug({ groupId, artifactId, version }, msg)
          })

          if (result.error) {
            log.warn({ groupId, artifactId, version, error: result.error }, 'POM extraction error')
            errors++
            return
          }

          // Step 3: upsert into `packages`
          // purl at package level has no version (package-level identifier)
          const packagePurl = `pkg:maven/${groupId}/${artifactId}`
          const packageId = await upsertPackage(qx, {
            purl: packagePurl,
            ecosystem: 'maven',
            namespace: groupId,
            name: artifactId,
            description: result.description,
            homepage: result.homepageUrl,
            declaredRepositoryUrl: result.scmUrl,
            licenses: result.licenses.length > 0 ? result.licenses : null,
            licensesRaw: result.licensesRaw,
            latestVersion: version,
            ingestionSource: 'pom_fetcher',
          })

          // Step 4: upsert maintainers (developers + contributors)
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

          processed++
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err)
          log.error({ groupId, artifactId, error: message }, 'Unexpected error processing package')
          errors++
        }
      }),
    )

    // done = packages processed so far (based on loop index, always accurate)
    const done = i + group.length
    const prevDone = i
    const crossedBoundary = Math.floor(done / PROGRESS_EVERY) > Math.floor(prevDone / PROGRESS_EVERY)
    if (crossedBoundary || done === packages.length) {
      log.info(
        { done, total: packages.length, processed, skipped, errors },
        `Progress: ${done}/${packages.length}`,
      )
    }
  }

  return { processed, skipped, errors }
}

// ─── Main loop ────────────────────────────────────────────────────────────────

/**
 * Loops indefinitely: pages through all Maven packages that need POM
 * enrichment, sleeps when the pass is complete, then restarts from offset 0.
 *
 * The caller is responsible for creating the DB connection and passing
 * `isShuttingDown` so the loop exits cleanly on SIGTERM/SIGINT.
 */
export async function runPomEnrichmentLoop(
  qx: QueryExecutor,
  config: ReturnType<typeof getPomFetcherConfig>,
  isShuttingDown: () => boolean,
): Promise<void> {
  let totalProcessed = 0
  let totalSkipped = 0
  let totalErrors = 0
  let passNumber = 0
  let passStartedAt = Date.now()

  while (!isShuttingDown()) {
    if (totalProcessed + totalSkipped + totalErrors === 0) {
      passNumber++
      passStartedAt = Date.now()
      log.info({ pass: passNumber }, 'Starting pass')
    }

    const result = await processBatch(qx, config)

    if (result.processed + result.skipped + result.errors === 0) {
      // Nothing left in this pass — log summary and sleep
      const durationMs = Date.now() - passStartedAt
      log.info(
        {
          totalProcessed,
          totalSkipped,
          totalErrors,
          durationMs,
          durationSec: Math.round(durationMs / 1000),
        },
        `Pass complete. Sleeping ${config.idleSleepSec}s before next pass.`,
      )
      await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
      totalProcessed = 0
      totalSkipped = 0
      totalErrors = 0
      continue
    }

    totalProcessed += result.processed
    totalSkipped += result.skipped
    totalErrors += result.errors

    log.info(
      { processed: result.processed, skipped: result.skipped, errors: result.errors, totalProcessed, totalSkipped, totalErrors },
      'Batch complete',
    )
  }
}
