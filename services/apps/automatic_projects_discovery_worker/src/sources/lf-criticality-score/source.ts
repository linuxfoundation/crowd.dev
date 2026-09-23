import http from 'http'
import https from 'https'
import { Readable } from 'stream'

import { canonicalizeRepoUrl, parseEnvInt, timeout } from '@crowd/common'
import { deriveProjectIdentityFromRepoUrl } from '@crowd/data-access-layer'
import { IDiscoverySourceCursor } from '@crowd/data-access-layer/src/discovery/types'
import { getServiceLogger } from '@crowd/logging'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'

const log = getServiceLogger()

const DEFAULT_API_PORT = 443
const PAGE_SIZE = 100

// Requests per second sent to the LF Criticality Score API (throttle between pages).
const REQUESTS_PER_SECOND = parseEnvInt(
  process.env.LF_CRITICALITY_SCORE_REQUESTS_PER_SECOND,
  5,
  1,
  100,
)
// Max per-page attempts (initial + retries) on 429 or 5xx before giving up.
const MAX_ATTEMPTS = parseEnvInt(
  process.env.LF_CRITICALITY_SCORE_MAX_ATTEMPTS ?? process.env.LF_CRITICALITY_SCORE_MAX_RETRIES,
  7,
  1,
  20,
)

interface LfApiResponse {
  page: number
  pageSize: number
  total: number
  totalPages: number
  data: LfApiRow[]
}

interface LfApiRow {
  rundate: string
  repourl: string
  owner: string
  reponame: string
  contributors: number
  organizations: number
  sizesloc: number
  lastupdated: number
  age: number
  commitfreq: number
  score: number
}

function getApiBaseUrl(): string {
  if (process.env.LF_CRITICALITY_SCORE_API_URL) {
    return process.env.LF_CRITICALITY_SCORE_API_URL.replace(/\/$/, '')
  }
  const host = process.env.LF_CRITICALITY_SCORE_API_HOST?.trim().replace(/\/$/, '')
  if (!host) {
    throw new Error(
      'LF Criticality Score API host is not configured. Set LF_CRITICALITY_SCORE_API_URL or LF_CRITICALITY_SCORE_API_HOST.',
    )
  }
  const port = parseInt(process.env.LF_CRITICALITY_SCORE_API_PORT ?? String(DEFAULT_API_PORT), 10)
  const scheme = port === 443 ? 'https' : 'http'
  return `${scheme}://${host}:${port}`
}

function getApiKey(): string {
  const key = process.env.LF_CRITICALITY_SCORE_API_KEY?.trim()
  if (!key) {
    throw new Error(
      'LF Criticality Score API key is not configured. Set LF_CRITICALITY_SCORE_API_KEY.',
    )
  }
  return key
}

interface HttpGetResult {
  statusCode: number
  retryAfterMs: number | null
  body: string
}

function parseRetryAfterMs(header: string | string[] | undefined): number | null {
  const raw = Array.isArray(header) ? header[0] : header
  if (!raw) return null
  const secs = parseFloat(raw.trim())
  return Number.isFinite(secs) && secs > 0 ? secs * 1000 : null
}

// Bounds a single request so a hung connection doesn't block the activity's heartbeat
// for the full Temporal heartbeatTimeout (5 min) while the socket stays open.
const REQUEST_TIMEOUT_MS = 30_000

function httpGet(url: string, headers: Record<string, string>): Promise<HttpGetResult> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https://') ? https : http
    const req = client.get(url, { headers, timeout: REQUEST_TIMEOUT_MS }, (res) => {
      const statusCode = res.statusCode ?? 0
      const retryAfterMs = parseRetryAfterMs(res.headers['retry-after'])
      const chunks: Uint8Array[] = []
      res.on('data', (chunk: Uint8Array) => chunks.push(chunk))
      res.on('end', () =>
        resolve({ statusCode, retryAfterMs, body: Buffer.concat(chunks).toString('utf8') }),
      )
      res.on('error', reject)
    })
    req.on('timeout', () =>
      req.destroy(new Error(`Request timed out after ${REQUEST_TIMEOUT_MS}ms`)),
    )
    req.on('error', reject)
    req.end()
  })
}

async function fetchPage(
  baseUrl: string,
  apiKey: string,
  page: number,
  scoredAfter?: string,
): Promise<LfApiResponse> {
  const params = new URLSearchParams({ page: String(page), pageSize: String(PAGE_SIZE) })
  if (scoredAfter) params.set('scoredAfter', scoredAfter)
  const url = `${baseUrl}/projects?${params.toString()}`
  const headers = { Authorization: `Bearer ${apiKey}` }

  for (let attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
    let result: HttpGetResult | null = null

    try {
      result = await httpGet(url, headers)
    } catch (networkErr) {
      if (attempt === MAX_ATTEMPTS - 1) {
        throw new Error(`LF Criticality Score API network error for ${url}: ${networkErr}`)
      }
      const delayMs = Math.min(Math.pow(2, attempt) * 1000, 60_000)
      log.warn(
        { page, attempt: attempt + 1, maxAttempts: MAX_ATTEMPTS, delayMs, err: String(networkErr) },
        'LF Criticality Score: network error, retrying...',
      )
      await timeout(delayMs)
      continue
    }

    const { statusCode, retryAfterMs, body } = result

    if (statusCode === 200) {
      try {
        return JSON.parse(body) as LfApiResponse
      } catch (err) {
        throw new Error(`Failed to parse LF Criticality Score API response: ${err}`)
      }
    }

    if (statusCode === 401 || statusCode === 403) {
      throw new Error(
        `LF Criticality Score API returned status ${statusCode} for ${url}. Check LF_CRITICALITY_SCORE_API_KEY.`,
      )
    }

    const isRetryable = statusCode === 429 || statusCode >= 500
    if (!isRetryable || attempt === MAX_ATTEMPTS - 1) {
      throw new Error(`LF Criticality Score API returned status ${statusCode} for ${url}`)
    }

    const delayMs = retryAfterMs ?? Math.min(Math.pow(2, attempt) * 1000, 60_000)

    log.warn(
      { page, attempt: attempt + 1, maxAttempts: MAX_ATTEMPTS, statusCode, delayMs },
      'LF Criticality Score: rate limited or server error, retrying...',
    )
    await timeout(delayMs)
  }

  // Unreachable, but satisfies TypeScript.
  throw new Error(`LF Criticality Score API failed for ${url} after ${MAX_ATTEMPTS} attempts`)
}

export class LfCriticalityScoreSource implements IDiscoverySource {
  public readonly name = 'lf-criticality-score'
  public readonly format = 'json' as const

  async listAvailableDatasets(options?: {
    since?: string
    cursor?: IDiscoverySourceCursor
  }): Promise<IDatasetDescriptor[]> {
    const baseUrl = getApiBaseUrl()
    getApiKey()
    const today = new Date().toISOString().slice(0, 10)
    // `since` is the generic interface name; this API's query param is `scoredAfter`.
    const scoredAfter = options?.since

    const params = new URLSearchParams()
    if (scoredAfter) params.set('scoredAfter', scoredAfter)
    const qs = params.toString()

    return [
      {
        id: scoredAfter ? `${today}-since-${scoredAfter}` : today,
        date: today,
        url: `${baseUrl}/projects${qs ? `?${qs}` : ''}`,
        // Passed through as-is; fetchDatasetStream decides whether it's still resumable
        // once it learns the API's current rundate.
        cursor: options?.cursor,
      },
    ]
  }

  async fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable> {
    const baseUrl = getApiBaseUrl()
    const apiKey = getApiKey()
    const scoredAfter = new URL(dataset.url).searchParams.get('scoredAfter') ?? undefined
    const previousCursor = dataset.cursor

    log.info(
      {
        datasetId: dataset.id,
        baseUrl,
        scoredAfter: scoredAfter ?? 'none (full fetch)',
        previousCursor: previousCursor ?? 'none',
      },
      'LF Criticality Score: starting stream fetch.',
    )

    const throttleIntervalMs = Math.round(1000 / REQUESTS_PER_SECOND)

    async function* pages() {
      const firstPage = await fetchPage(baseUrl, apiKey, 1, scoredAfter)
      const { totalPages } = firstPage
      // The corpus is fully re-ranked ~monthly (all rows share one rundate between
      // reloads), so rundate is a version stamp for the whole ranking, not a per-row
      // timestamp: same rundate as last run -> resume paging; different -> the ranking
      // was recomputed, so page numbers from before no longer point at the same rows.
      const apiRundate = firstPage.data[0]?.rundate
      const resumable = apiRundate !== undefined && previousCursor?.rundate === apiRundate
      const startPage = resumable ? previousCursor.page + 1 : 1

      if (apiRundate !== undefined) {
        dataset.cursor = { rundate: apiRundate, page: resumable ? previousCursor.page : 0 }
      }

      log.info(
        {
          datasetId: dataset.id,
          total: firstPage.total,
          totalPages,
          pageSize: firstPage.pageSize,
          apiRundate,
          resumable,
          startPage,
        },
        'LF Criticality Score: first page received — total records available.',
      )

      if (startPage > totalPages) {
        log.info(
          { datasetId: dataset.id, startPage, totalPages },
          'LF Criticality Score: already caught up with this rundate, nothing to fetch.',
        )
        return
      }

      if (startPage <= 1) {
        for (const row of firstPage.data) {
          yield row
        }
        if (apiRundate !== undefined) {
          dataset.cursor = { rundate: apiRundate, page: 1 }
        }
      }

      for (let page = Math.max(startPage, 2); page <= totalPages; page++) {
        await timeout(throttleIntervalMs)

        log.info(
          { datasetId: dataset.id, page, totalPages },
          'LF Criticality Score: fetching page...',
        )
        const response = await fetchPage(baseUrl, apiKey, page, scoredAfter)

        for (const row of response.data) {
          yield row
        }

        if (apiRundate !== undefined) {
          dataset.cursor = { rundate: apiRundate, page }
        }

        log.info(
          { datasetId: dataset.id, page, totalPages, rowsInPage: response.data.length },
          'LF Criticality Score: page fetched.',
        )
      }

      log.info({ datasetId: dataset.id, totalPages }, 'LF Criticality Score: all pages fetched.')
    }

    return Readable.from(pages(), { objectMode: true })
  }

  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null {
    const rawRepoUrl = (rawRow['repourl'] ?? rawRow['repoUrl']) as string | undefined
    if (!rawRepoUrl) {
      return null
    }

    // Canonicalize the repoUrl itself; non-GitHub hosts are kept (not rejected) —
    // the evaluation pre-check needs them to skip deterministically.
    const canonical = canonicalizeRepoUrl(rawRepoUrl)
    if (!canonical) {
      return null
    }

    // repoName/projectSlug are derived from the original (case-preserving) URL:
    // they feed the LFX display name, and lowercasing would mangle names like "CMake".
    const identity = deriveProjectIdentityFromRepoUrl(rawRepoUrl)
    if (!identity) {
      return null
    }

    const score = rawRow['score']
    const lfCriticalityScore = typeof score === 'number' ? score : parseFloat(score as string)

    return {
      projectSlug: identity.projectSlug,
      repoName: identity.repoName,
      repoUrl: canonical.url,
      lfCriticalityScore: Number.isNaN(lfCriticalityScore) ? undefined : lfCriticalityScore,
    }
  }
}
