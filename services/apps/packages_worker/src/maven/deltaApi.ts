/**
 * Client for our own Maven delta feed (deployed separately, e.g. on Railway).
 * It diffs the Maven Central index and exposes the artifacts that changed in a
 * time window, so we can enrich exactly the packages that moved instead of
 * polling the whole universe.
 *
 * Endpoint:
 *   GET {baseUrl}/api/changes?since=<ISO>&until=<ISO>&pageSize=<n>&includePrerelease=<bool>
 *
 * Response:
 *   {
 *     "window":  { "since": "...", "until": "..." },
 *     "changes": [
 *       { "purl", "groupId", "artifactId", "version", "publishedAt",
 *         "isPrerelease", "changeType" }, ...
 *     ],
 *     "nextCursor": "..."   // present while more pages remain
 *   }
 */
import axios from 'axios'

import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('maven-delta-api')

const REQUEST_TIMEOUT_MS = 15_000
// Hard stop so a misbehaving cursor can never spin forever.
const MAX_PAGES = 1_000
const MAX_RETRIES = 3
const RETRY_BASE_MS = 1_000

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms))
}

// Transient: no HTTP response at all (socket aborted/reset, timeout — the
// 'Error: aborted' we see when Railway drops the gzip stream mid-flight) or a
// retryable status (5xx / 429). Everything else (4xx, parse errors) is fatal.
function isTransientError(err: unknown): boolean {
  if (!axios.isAxiosError(err)) return false
  const status = err.response?.status
  if (status === undefined) return true
  return status >= 500 || status === 429
}

export interface MavenChange {
  purl: string
  groupId: string
  artifactId: string
  version: string
  publishedAt: string
  isPrerelease: boolean
  changeType: string
}

interface ChangesResponse {
  window: { since: string; until: string }
  changes: MavenChange[]
  nextCursor?: string
}

export interface FetchChangesOptions {
  baseUrl: string
  token?: string
  since: string // ISO timestamp
  until: string // ISO timestamp
  pageSize: number
  includePrerelease: boolean
}

/**
 * Fetches every change in [since, until), following `nextCursor` pagination.
 */
export async function fetchMavenChanges(opts: FetchChangesOptions): Promise<MavenChange[]> {
  const all: MavenChange[] = []
  let cursor: string | undefined
  let page = 0

  do {
    const params: Record<string, string | number | boolean> = {
      since: opts.since,
      until: opts.until,
      pageSize: opts.pageSize,
      includePrerelease: opts.includePrerelease,
    }
    if (cursor) params.cursor = cursor

    let res: { data: ChangesResponse } | undefined
    for (let attempt = 0; ; attempt++) {
      try {
        res = await axios.get<ChangesResponse>(`${opts.baseUrl}/api/changes`, {
          params,
          timeout: REQUEST_TIMEOUT_MS,
          headers: opts.token ? { Authorization: `Bearer ${opts.token}` } : undefined,
        })
        break
      } catch (err) {
        if (attempt < MAX_RETRIES && isTransientError(err)) {
          const delay = RETRY_BASE_MS * 2 ** attempt + Math.random() * 300
          log.warn(
            { page, attempt, error: err instanceof Error ? err.message : String(err) },
            'Delta page fetch failed — retrying',
          )
          await sleep(delay)
          continue
        }
        throw err
      }
    }

    const changes = res.data?.changes ?? []
    all.push(...changes)
    cursor = res.data?.nextCursor || undefined
    page++

    log.debug(
      { page, batch: changes.length, total: all.length, hasMore: Boolean(cursor) },
      'Fetched delta page',
    )

    if (page >= MAX_PAGES) {
      log.warn({ page, total: all.length }, 'Hit MAX_PAGES — stopping pagination early')
      break
    }
  } while (cursor)

  return all
}
