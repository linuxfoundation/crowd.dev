import { QueryExecutor } from '@crowd/data-access-layer/src/queryExecutor'
import { getServiceChildLogger } from '@crowd/logging'

import { getDockerhubConfig } from '../config'
import { parseGithubUrl } from '../enricher/fetchLightRepo'

import { buildCandidates } from './candidates'
import { detectDockerfile } from './detectDockerfile'
import { fetchDockerhub } from './fetchDockerhub'
import { DiscoveryRepoRow, DockerhubRepoResult, FetchError, RefreshImageRow } from './types'
import { markDockerChecked, touchRepoDocker, upsertRepoDocker } from './upsertRepoDocker'

const log = getServiceChildLogger('dockerhub-sync')

const MAX_RETRIES = 3

type DockerhubConfig = ReturnType<typeof getDockerhubConfig>

// Docker Hub's anonymous rate limit is per-IP, not per-token. hubParkedUntil
// is module state so the park survives across refresh and discovery pages, and
// hubChain serializes calls so the per-token GitHub fan-out below can't fire
// concurrent Hub requests against that single per-IP budget.
let hubParkedUntil = 0
let hubChain: Promise<unknown> = Promise.resolve()

function hubFetchWithRetries(
  baseUrl: string,
  imageName: string,
): Promise<DockerhubRepoResult | null> {
  const run = hubChain.then(() => hubFetchInner(baseUrl, imageName))
  hubChain = run.catch(() => undefined)
  return run
}

async function hubFetchInner(
  baseUrl: string,
  imageName: string,
): Promise<DockerhubRepoResult | null> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const wait = hubParkedUntil - Date.now()
    if (wait > 0) {
      log.warn({ waitSec: Math.round(wait / 1000) }, 'Docker Hub parked, waiting')
      await new Promise((r) => setTimeout(r, wait))
    }
    try {
      return await fetchDockerhub(baseUrl, imageName)
    } catch (err) {
      if (!(err instanceof FetchError)) throw err

      if (err.kind === 'NOT_FOUND') return null
      if (err.kind === 'AUTH') {
        // Systemic misconfig (wrong base URL, Hub now requires auth) — propagate
        // so the worker exits instead of silently marking every image gone.
        log.error({ imageName }, err.message)
        throw err
      }
      if (err.kind === 'MALFORMED') {
        log.warn({ imageName }, err.message)
        return null
      }
      if (err.kind === 'RATE_LIMIT') {
        hubParkedUntil = err.resetAt ?? Date.now() + 60_000
        continue
      }
      if (attempt < MAX_RETRIES) {
        const backoffMs = 1000 * 2 ** attempt
        log.warn({ imageName, attempt, backoffMs }, `Transient Hub error, retrying: ${err.message}`)
        await new Promise((r) => setTimeout(r, backoffMs))
      } else {
        log.error({ imageName }, `Gave up after ${MAX_RETRIES} retries: ${err.message}`)
        return null
      }
    }
  }
  return null
}

async function githubFetchWithRetries(
  owner: string,
  name: string,
  token: string,
): Promise<boolean | null> {
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    try {
      return await detectDockerfile(owner, name, token)
    } catch (err) {
      if (!(err instanceof FetchError)) throw err

      if (['NOT_FOUND', 'AUTH', 'MALFORMED'].includes(err.kind)) {
        log.warn({ owner, name, kind: err.kind }, err.message)
        return null
      }

      if (err.kind === 'RATE_LIMIT') throw err

      if (attempt < MAX_RETRIES) {
        const backoffMs = 1000 * 2 ** attempt
        log.warn({ owner, name, attempt, backoffMs }, `Transient error, retrying: ${err.message}`)
        await new Promise((r) => setTimeout(r, backoffMs))
      } else {
        log.error({ owner, name }, `Gave up after ${MAX_RETRIES} retries: ${err.message}`)
        return null
      }
    }
  }
  return null
}

async function fetchRefreshPage(
  qx: QueryExecutor,
  cursor: string | null,
  config: DockerhubConfig,
): Promise<RefreshImageRow[]> {
  return qx.select(
    `
    SELECT id, repo_id, image_name
    FROM repo_docker
    WHERE last_synced_at < NOW() - make_interval(hours => $(refreshIntervalHours))
      AND ($(cursor)::bigint IS NULL OR id > $(cursor)::bigint)
    ORDER BY id
    LIMIT $(batchSize)
    `,
    {
      cursor,
      batchSize: config.batchSize,
      refreshIntervalHours: config.refreshIntervalHours,
    },
  )
}

async function processRefreshPage(
  qx: QueryExecutor,
  rows: RefreshImageRow[],
  config: DockerhubConfig,
): Promise<{ ok: number; gone: number }> {
  let ok = 0
  let gone = 0

  for (const row of rows) {
    const result = await hubFetchWithRetries(config.hubBaseUrl, row.image_name)
    if (result) {
      await upsertRepoDocker(qx, row.repo_id, result)
      ok++
    } else {
      // Image deleted or unreachable — bump last_synced_at so we don't retry
      // it every page; the row (and its pulls_daily history) is kept.
      await touchRepoDocker(qx, row.image_name)
      gone++
    }
  }

  return { ok, gone }
}

async function fetchDiscoveryPage(
  qx: QueryExecutor,
  cursor: string | null,
  config: DockerhubConfig,
): Promise<DiscoveryRepoRow[]> {
  return qx.select(
    `
    SELECT id, url, owner, name
    FROM repos
    WHERE host = 'github'
      AND (docker_checked_at IS NULL
           OR docker_checked_at < NOW() - make_interval(days => $(discoveryIntervalDays)))
      AND ($(cursor)::bigint IS NULL OR id > $(cursor)::bigint)
    ORDER BY id
    LIMIT $(batchSize)
    `,
    {
      cursor,
      batchSize: config.batchSize,
      discoveryIntervalDays: config.discoveryIntervalDays,
    },
  )
}

function resolveOwnerName(row: DiscoveryRepoRow): { owner: string; name: string } | null {
  if (row.owner && row.name) return { owner: row.owner, name: row.name }
  try {
    return parseGithubUrl(row.url)
  } catch {
    return null
  }
}

async function discoverRepo(
  qx: QueryExecutor,
  row: DiscoveryRepoRow,
  token: string,
  config: DockerhubConfig,
): Promise<'hit' | 'miss' | 'skip'> {
  const parsed = resolveOwnerName(row)
  if (!parsed) {
    await markDockerChecked(qx, row.id)
    return 'skip'
  }

  const hasDockerfile = await githubFetchWithRetries(parsed.owner, parsed.name, token)
  if (hasDockerfile === null) {
    // GitHub lookup failed (404/auth/gave-up). Mark checked so the backlog
    // drains; the discoveryIntervalDays re-check will try again later.
    await markDockerChecked(qx, row.id)
    return 'skip'
  }
  if (!hasDockerfile) {
    await markDockerChecked(qx, row.id)
    return 'miss'
  }

  for (const candidate of buildCandidates(parsed.owner, parsed.name)) {
    const result = await hubFetchWithRetries(config.hubBaseUrl, candidate)
    if (result) {
      await upsertRepoDocker(qx, row.id, result)
      await markDockerChecked(qx, row.id)
      return 'hit'
    }
  }

  await markDockerChecked(qx, row.id)
  return 'miss'
}

async function processDiscoveryPage(
  qx: QueryExecutor,
  rows: DiscoveryRepoRow[],
  parkedUntil: Map<string, number>,
  config: DockerhubConfig,
): Promise<{ hits: number; misses: number; skipped: number; failed: number }> {
  let hits = 0
  let misses = 0
  let skipped = 0
  let failed = 0
  let nextIdx = 0

  await Promise.all(
    config.tokens.map(async (token, tokenIdx) => {
      const initialPark = (parkedUntil.get(token) ?? 0) - Date.now()
      if (initialPark > 0) {
        log.warn(`token#${tokenIdx} still parked, waiting ${Math.round(initialPark / 1000)}s`)
        await new Promise((r) => setTimeout(r, initialPark))
      }

      while (nextIdx < rows.length) {
        const idx = nextIdx++
        const row = rows[idx]

        // Retry the same row after a GitHub rate-limit park instead of abandoning
        // it. Without this the cursor advances past a row whose docker_checked_at
        // was never set, and it isn't picked up again until the cursor wraps to
        // null at end-of-backlog (potentially days on a 600k-row sweep).
        let done = false
        while (!done) {
          try {
            const outcome = await discoverRepo(qx, row, token, config)
            if (outcome === 'hit') hits++
            else if (outcome === 'miss') misses++
            else skipped++
            done = true
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
              // loop retries the same row
            } else {
              log.error({ url: row.url, err }, 'Unexpected discovery error')
              failed++
              done = true
            }
          }
        }
      }
    }),
  )

  return { hits, misses, skipped, failed }
}

export async function runDockerhubLoop(
  qx: QueryExecutor,
  config: DockerhubConfig,
  isShuttingDown: () => boolean,
): Promise<void> {
  const githubParkedUntil = new Map<string, number>()
  let refreshCursor: string | null = null
  let discoveryCursor: string | null = null
  let pageNum = 0

  while (!isShuttingDown()) {
    pageNum++

    const refreshRows = await fetchRefreshPage(qx, refreshCursor, config)
    if (refreshRows.length > 0) {
      const stats = await processRefreshPage(qx, refreshRows, config)
      log.info(
        `Refresh page ${pageNum}: read=${refreshRows.length} ok=${stats.ok} gone=${stats.gone}`,
      )
      refreshCursor = refreshRows[refreshRows.length - 1].id
    } else {
      refreshCursor = null
    }

    if (isShuttingDown()) break

    const discoveryRows = await fetchDiscoveryPage(qx, discoveryCursor, config)
    if (discoveryRows.length > 0) {
      const stats = await processDiscoveryPage(qx, discoveryRows, githubParkedUntil, config)
      log.info(
        `Discovery page ${pageNum}: read=${discoveryRows.length} hits=${stats.hits} ` +
          `misses=${stats.misses} skipped=${stats.skipped} failed=${stats.failed}`,
      )
      discoveryCursor = discoveryRows[discoveryRows.length - 1].id
    } else {
      discoveryCursor = null
    }

    if (refreshRows.length === 0 && discoveryRows.length === 0) {
      log.info('No work, sleeping')
      await new Promise((r) => setTimeout(r, config.idleSleepSec * 1000))
    }
  }
}
