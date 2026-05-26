import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getEnricherConfig } from '../config'

import { fetchLightRepo, parseGithubUrl } from './fetchLightRepo'
import { FetchError, LightRepoResult } from './types'
import { updateEnrichedRepos } from './updateEnrichedRepos'

const log = getServiceChildLogger('github-repos-enricher')

const MAX_RETRIES = 3

async function fetchWithRetries(url: string, token: string): Promise<LightRepoResult | null> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await fetchLightRepo(url, token)
    } catch (err) {
      if (!(err instanceof FetchError)) throw err

      if (['NOT_FOUND', 'AUTH', 'MALFORMED'].includes(err.kind)) {
        log.warn({ url, kind: err.kind }, err.message)
        return null
      }

      if (err.kind === 'RATE_LIMIT') throw err

      if (attempt < MAX_RETRIES) {
        const backoffMs = 1000 * 2 ** attempt
        log.warn({ url, attempt, backoffMs }, `Transient error, retrying: ${err.message}`)
        await new Promise((r) => setTimeout(r, backoffMs))
      } else {
        log.error({ url }, `Gave up after ${MAX_RETRIES} retries: ${err.message}`)
        return null
      }
    }
  }
  return null
}

async function fetchPage(
  qx: QueryExecutor,
  cursor: string | null,
  batchSize: number,
  updateIntervalHours: number,
): Promise<{ rows: Array<{ id: string; url: string }>; urls: string[] }> {
  const rows = await qx.select(
    `
    SELECT id, url
    FROM repos
    WHERE host = 'github'
      AND (last_synced_at IS NULL OR last_synced_at < NOW() - INTERVAL '$(updateIntervalHours) hours')
      AND ($(cursor) IS NULL OR id > $(cursor))
    ORDER BY id
    LIMIT $(batchSize)
    `,
    { cursor, batchSize, updateIntervalHours },
  )
  return {
    rows,
    urls: rows.map((r: { url: string }) => r.url),
  }
}

async function processPage(
  urls: string[],
  tokens: string[],
  parkedUntil: Map<string, number>,
  config: ReturnType<typeof getEnricherConfig>,
  qx: QueryExecutor,
): Promise<{ fetched: number; failed: number; flushed: number }> {
  const validUrls: string[] = []
  let skipped = 0
  for (const url of urls) {
    try {
      parseGithubUrl(url)
      validUrls.push(url)
    } catch {
      skipped++
    }
  }
  if (skipped > 0) log.warn(`Skipped ${skipped} non-GitHub URLs`)

  const buffer: LightRepoResult[] = []
  const failures: Array<{ url: string; reason: string }> = []
  let failed = 0
  let flushed = 0
  let nextIdx = 0

  await Promise.all(
    tokens.map(async (token, tokenIdx) => {
      // Respect any park set during a previous page of this run
      const initialPark = (parkedUntil.get(token) ?? 0) - Date.now()
      if (initialPark > 0) {
        log.warn(`token#${tokenIdx} still parked, waiting ${Math.round(initialPark / 1000)}s`)
        await new Promise((r) => setTimeout(r, initialPark))
      }

      while (true) {
        const idx = nextIdx++
        if (idx >= validUrls.length) break
        const url = validUrls[idx]

        try {
          const result = await fetchWithRetries(url, token)
          if (result) {
            buffer.push(result)
            if (buffer.length >= config.batchSize) {
              const batch = buffer.splice(0)
              await updateEnrichedRepos(qx, batch)
              flushed += batch.length
            }
          } else {
            failures.push({ url, reason: 'see warn log above' })
            failed++
          }
        } catch (err) {
          if (err instanceof FetchError && err.kind === 'RATE_LIMIT') {
            const resetAt = err.resetAt ?? Date.now() + 60_000
            const waitMs = Math.max(1_000, resetAt - Date.now())
            parkedUntil.set(token, resetAt)
            log.warn(
              { tokenIdx, parkedUntil: new Date(resetAt).toISOString() },
              `token#${tokenIdx} rate limited — parking for ${Math.round(waitMs / 1000)}s`,
            )
            await new Promise((r) => setTimeout(r, waitMs))
            failures.push({ url, reason: 'rate-limit' })
            failed++
          } else {
            log.error({ url, err }, 'Unexpected error')
            failures.push({ url, reason: (err as Error).message })
            failed++
          }
        }
      }
    }),
  )

  if (buffer.length > 0) {
    await updateEnrichedRepos(qx, buffer)
    flushed += buffer.length
  }

  if (failures.length > 0) {
    log.warn({ failures }, `${failures.length} repo(s) failed this page`)
  }

  return { fetched: validUrls.length - failed, failed, flushed }
}

export async function runEnrichmentLoop(
  qx: QueryExecutor,
  config: ReturnType<typeof getEnricherConfig>,
  isShuttingDown: () => boolean,
): Promise<void> {
  const parkedUntil = new Map<string, number>()
  let cursor: string | null = null
  let pageNum = 0

  while (!isShuttingDown()) {
    pageNum++

    const { rows, urls } = await fetchPage(qx, cursor, config.batchSize, config.updateIntervalHours)

    if (urls.length === 0) {
      log.info('No more repos to process, sleeping')
      await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
      cursor = null
      continue
    }

    const { fetched, failed, flushed } = await processPage(
      urls,
      config.tokens,
      parkedUntil,
      config,
      qx,
    )

    log.info(
      `Page ${pageNum}: read=${urls.length} fetched=${fetched} failed=${failed} flushed=${flushed}`,
    )

    if (rows.length > 0) {
      cursor = rows[rows.length - 1].id
    }
  }
}
