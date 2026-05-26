/**
 * sync-light-repos
 *
 * Fetches GitHub repo metadata via GraphQL and upserts into the `repos` table.
 * Runs one async worker per token — each worker claims URLs by index so no two
 * requests ever share a token concurrently.
 *
 * Success tracking: a successful fetch updates repos.last_synced_at to NOW().
 * Failed repos keep a stale/null last_synced_at and are picked up on the next run.
 * TODO: fetchPage will later filter by last_synced_at < NOW() - update_interval
 * so this script becomes a continuous sync with no extra failure tracking needed.
 *
 * Usage:
 *   pnpm run sync-light-repos -- [options]
 *
 * Options:
 *   --page-size <n>     Repos fetched from source per cursor page (default: 200)
 *   --batch-size <n>    Upsert batch size (default: 50)
 *   --max-retries <n>   Per-repo transient retry cap (default: 3)
 *   --start-after <id>  Resume from cursor id (printed after each page)
 *   --limit <n>         Stop after N repos total (for testing)
 *   --dry-run           Fetch but skip DB writes
 *
 * Environment:
 *   GITHUB_TOKENS                              Comma-separated GitHub PATs (required)
 *   CROWD_DB_WRITE_HOST/PORT/USERNAME/PASSWORD/DATABASE
 *   SERVICE
 */

import { WRITE_DB_CONFIG, getDbConnection } from '@crowd/data-access-layer/src/database'
import { pgpQx } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { fetchLightRepo, parseGithubUrl } from './fetchLightRepo'
import { FetchError, LightRepoResult } from './types'
import { upsertLightRepos } from './upsertLightRepos'

const log = getServiceChildLogger('sync-light-repos')

function parseArgs() {
  const args = process.argv.slice(2)
  const getArg = (flag: string) => {
    const idx = args.indexOf(flag)
    return idx !== -1 && idx + 1 < args.length ? args[idx + 1] : undefined
  }

  const pageSize = parseInt(getArg('--page-size') ?? '200', 10)
  const batchSize = parseInt(getArg('--batch-size') ?? '50', 10)
  const maxRetries = parseInt(getArg('--max-retries') ?? '3', 10)
  const startAfter = getArg('--start-after') ?? null
  const limitRaw = getArg('--limit')
  const limit = limitRaw !== undefined ? parseInt(limitRaw, 10) : null
  const dryRun = args.includes('--dry-run')

  if (isNaN(pageSize) || pageSize <= 0) { log.error('--page-size must be a positive integer'); process.exit(1) }
  if (isNaN(batchSize) || batchSize <= 0) { log.error('--batch-size must be a positive integer'); process.exit(1) }
  if (isNaN(maxRetries) || maxRetries < 0) { log.error('--max-retries must be a non-negative integer'); process.exit(1) }
  if (limit !== null && (isNaN(limit) || limit <= 0)) { log.error('--limit must be a positive integer'); process.exit(1) }

  return { pageSize, batchSize, maxRetries, startAfter, limit, dryRun }
}

// TODO: add LEFT JOIN repos r ON r.url = pr.url and filter
// WHERE (r.last_synced_at IS NULL OR r.last_synced_at < NOW() - INTERVAL '$(updateIntervalHours) hours')
// once the update interval logic is scoped in.
async function fetchPage(
  qx: ReturnType<typeof pgpQx>,
  cursor: string | null,
  pageSize: number,
): Promise<{ urls: string[]; nextCursor: string | null }> {
  const rows = await qx.select(
    `
    SELECT id, url
    FROM public.repositories
    WHERE url LIKE 'https://github.com/%'
      AND "deletedAt" IS NULL
      ${cursor ? 'AND id > $(cursor)' : ''}
    ORDER BY id
    LIMIT $(pageSize)
    `,
    { cursor, pageSize },
  )
  return {
    urls: rows.map((r: { url: string }) => r.url),
    nextCursor: rows.length > 0 ? (rows[rows.length - 1] as { id: string }).id : null,
  }
}

async function fetchWithRetries(
  url: string,
  token: string,
  maxRetries: number,
): Promise<LightRepoResult | null> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fetchLightRepo(url, token)
    } catch (err) {
      if (!(err instanceof FetchError)) throw err

      if (['NOT_FOUND', 'AUTH', 'MALFORMED'].includes(err.kind)) {
        log.warn({ url, kind: err.kind }, err.message)
        return null
      }

      if (err.kind === 'RATE_LIMIT') throw err

      if (attempt < maxRetries) {
        const backoffMs = 1000 * 2 ** attempt
        log.warn({ url, attempt, backoffMs }, `Transient error, retrying: ${err.message}`)
        await new Promise((r) => setTimeout(r, backoffMs))
      } else {
        log.error({ url }, `Gave up after ${maxRetries} retries: ${err.message}`)
        return null
      }
    }
  }
  return null
}

async function processPage(
  urls: string[],
  tokens: string[],
  parkedUntil: Map<string, number>,
  opts: ReturnType<typeof parseArgs>,
  qx: ReturnType<typeof pgpQx>,
): Promise<{ fetched: number; failed: number; flushed: number }> {
  const validUrls: string[] = []
  let skipped = 0
  for (const url of urls) {
    try { parseGithubUrl(url); validUrls.push(url) } catch { skipped++ }
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
          const result = await fetchWithRetries(url, token, opts.maxRetries)
          if (result) {
            buffer.push(result)
            if (!opts.dryRun && buffer.length >= opts.batchSize) {
              const batch = buffer.splice(0)
              await upsertLightRepos(qx, batch)
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

  if (!opts.dryRun && buffer.length > 0) {
    await upsertLightRepos(qx, buffer)
    flushed += buffer.length
  }

  if (failures.length > 0) {
    log.warn({ failures }, `${failures.length} repo(s) failed this page`)
  }

  return { fetched: validUrls.length - failed, failed, flushed }
}

async function main() {
  const opts = parseArgs()

  const tokens = (process.env.GITHUB_TOKENS ?? '')
    .split(',')
    .map((t) => t.trim())
    .filter(Boolean)

  if (tokens.length === 0) {
    log.error('GITHUB_TOKENS is required (comma-separated PATs)')
    process.exit(1)
  }

  // TODO: when connecting the real DB, replace with a connection pool and add keepalive /
  // reconnect-on-error handling. A single long-lived connection will be dropped by the server
  // during multi-hour runs (TCP timeout, idle reaper), crashing the script. Completed work
  // is safe via last_synced_at, but the run stops and must be manually resumed.
  const dbConnection = await getDbConnection(WRITE_DB_CONFIG())
  const qx = pgpQx(dbConnection)

  log.info('='.repeat(60))
  log.info('sync-light-repos')
  log.info(`tokens=${tokens.length} page-size=${opts.pageSize} batch-size=${opts.batchSize}`)
  log.info(`max-retries=${opts.maxRetries} dry-run=${opts.dryRun} limit=${opts.limit ?? 'none'}`)
  log.info(`start-after=${opts.startAfter ?? '(beginning)'}`)
  log.info('='.repeat(60))

  const parkedUntil = new Map<string, number>()
  let cursor = opts.startAfter
  let pageNum = 0
  let totalProcessed = 0
  let totalFailed = 0
  let totalFlushed = 0

  while (true) {
    pageNum++

    const remaining = opts.limit !== null ? opts.limit - totalProcessed : opts.pageSize
    if (remaining <= 0) break

    const { urls, nextCursor } = await fetchPage(qx, cursor, Math.min(opts.pageSize, remaining))

    if (urls.length === 0) {
      log.info('No more repos to process')
      break
    }

    const { fetched, failed, flushed } = await processPage(urls, tokens, parkedUntil, opts, qx)

    totalProcessed += urls.length
    totalFailed += failed
    totalFlushed += flushed

    log.info(
      `Page ${pageNum}: read=${urls.length} fetched=${fetched} failed=${failed}${opts.dryRun ? ' [dry-run]' : ` flushed=${flushed}`}`,
    )

    if (nextCursor) {
      log.info(`Resume with: --start-after ${nextCursor}`)
      cursor = nextCursor
    }

    if (urls.length < Math.min(opts.pageSize, remaining)) break
  }

  log.info('='.repeat(60))
  log.info(`Summary: pages=${pageNum} processed=${totalProcessed} failed=${totalFailed} flushed=${totalFlushed}`)
  log.info('='.repeat(60))

  process.exit(totalFailed > 0 ? 1 : 0)
}

main().catch((err) => {
  log.error({ err }, 'Unexpected error')
  process.exit(1)
})
