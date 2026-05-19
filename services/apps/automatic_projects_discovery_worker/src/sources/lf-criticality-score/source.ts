import http from 'http'
import https from 'https'
import { Readable } from 'stream'

import { timeout } from '@crowd/common'
import { getServiceLogger } from '@crowd/logging'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'

const log = getServiceLogger()

const DEFAULT_API_HOST = 'lf-criticality-score-api.example.com'
const DEFAULT_API_PORT = 443
const PAGE_SIZE = 100

// Requests per second sent to the LF Criticality Score API (throttle between pages).
const REQUESTS_PER_SECOND = parseInt(
  process.env.LF_CRITICALITY_SCORE_REQUESTS_PER_SECOND ?? '5',
  10,
)
// Max per-page retry attempts on 429 or 5xx before giving up and letting Temporal retry.
const MAX_RETRIES = parseInt(process.env.LF_CRITICALITY_SCORE_MAX_RETRIES ?? '7', 10)

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
  const host = (process.env.LF_CRITICALITY_SCORE_API_HOST ?? DEFAULT_API_HOST)
    .trim()
    .replace(/\/$/, '')
  const port = parseInt(process.env.LF_CRITICALITY_SCORE_API_PORT ?? String(DEFAULT_API_PORT), 10)
  const scheme = port === 443 ? 'https' : 'http'
  return `${scheme}://${host}:${port}`
}

function httpGet(url: string): Promise<{ statusCode: number; retryAfter: string | null; body: string }> {
  return new Promise((resolve, reject) => {
    const client = url.startsWith('https://') ? https : http
    const req = client.get(url, (res) => {
      const statusCode = res.statusCode ?? 0
      const retryAfter = res.headers['retry-after'] ?? null
      const chunks: Uint8Array[] = []
      res.on('data', (chunk: Uint8Array) => chunks.push(chunk))
      res.on('end', () => resolve({ statusCode, retryAfter: retryAfter as string | null, body: Buffer.concat(chunks).toString('utf8') }))
      res.on('error', reject)
    })
    req.on('error', reject)
    req.end()
  })
}

async function fetchPage(
  baseUrl: string,
  page: number,
  scoredAfter?: string,
): Promise<LfApiResponse> {
  const params = new URLSearchParams({ page: String(page), pageSize: String(PAGE_SIZE) })
  if (scoredAfter) params.set('scoredAfter', scoredAfter)
  const url = `${baseUrl}/projects?${params.toString()}`

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    const { statusCode, retryAfter, body } = await httpGet(url)

    if (statusCode === 200) {
      try {
        return JSON.parse(body) as LfApiResponse
      } catch (err) {
        throw new Error(`Failed to parse LF Criticality Score API response: ${err}`)
      }
    }

    const isRetryable = statusCode === 429 || statusCode >= 500
    if (!isRetryable || attempt === MAX_RETRIES) {
      throw new Error(`LF Criticality Score API returned status ${statusCode} for ${url}`)
    }

    let delayMs: number
    const retryAfterSecs = retryAfter ? parseFloat(retryAfter) : NaN
    if (!Number.isNaN(retryAfterSecs) && retryAfterSecs > 0) {
      delayMs = retryAfterSecs * 1000
    } else {
      delayMs = Math.min(Math.pow(2, attempt) * 1000, 60_000)
    }

    log.warn(
      { page, attempt: attempt + 1, maxRetries: MAX_RETRIES, statusCode, delayMs },
      'LF Criticality Score: rate limited or server error, retrying...',
    )
    await timeout(delayMs)
  }

  // Unreachable, but satisfies TypeScript.
  throw new Error(`LF Criticality Score API failed for ${url} after ${MAX_RETRIES} retries`)
}

export class LfCriticalityScoreSource implements IDiscoverySource {
  public readonly name = 'lf-criticality-score'
  public readonly format = 'json' as const

  async listAvailableDatasets(options?: { scoredAfter?: string }): Promise<IDatasetDescriptor[]> {
    const baseUrl = getApiBaseUrl()
    const today = new Date().toISOString().slice(0, 10)
    const { scoredAfter } = options ?? {}

    const params = new URLSearchParams()
    if (scoredAfter) params.set('scoredAfter', scoredAfter)
    const qs = params.toString()

    return [
      {
        id: scoredAfter ? `${today}-since-${scoredAfter}` : today,
        date: today,
        url: `${baseUrl}/projects${qs ? `?${qs}` : ''}`,
      },
    ]
  }

  async fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable> {
    const baseUrl = getApiBaseUrl()
    const scoredAfter = new URL(dataset.url).searchParams.get('scoredAfter') ?? undefined

    log.info(
      { datasetId: dataset.id, baseUrl, scoredAfter: scoredAfter ?? 'none (full fetch)' },
      'LF Criticality Score: starting stream fetch.',
    )

    const throttleIntervalMs = Math.round(1000 / REQUESTS_PER_SECOND)

    async function* pages() {
      let page = 1
      let totalPages = 1

      do {
        if (page > 1) {
          await timeout(throttleIntervalMs)
        }

        log.info(
          { datasetId: dataset.id, page, totalPages },
          'LF Criticality Score: fetching page...',
        )
        const response = await fetchPage(baseUrl, page, scoredAfter)
        totalPages = response.totalPages

        if (page === 1) {
          log.info(
            {
              datasetId: dataset.id,
              total: response.total,
              totalPages,
              pageSize: response.pageSize,
            },
            'LF Criticality Score: first page received — total records available.',
          )
        }

        for (const row of response.data) {
          yield row
        }

        log.info(
          { datasetId: dataset.id, page, totalPages, rowsInPage: response.data.length },
          'LF Criticality Score: page fetched.',
        )

        page++
      } while (page <= totalPages)

      log.info({ datasetId: dataset.id, totalPages }, 'LF Criticality Score: all pages fetched.')
    }

    return Readable.from(pages(), { objectMode: true })
  }

  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null {
    const repoUrl = (rawRow['repourl'] ?? rawRow['repoUrl']) as string | undefined
    if (!repoUrl) {
      return null
    }

    let repoName = ''
    let projectSlug = ''

    try {
      const urlPath = new URL(repoUrl).pathname.replace(/^\//, '').replace(/\/$/, '')
      projectSlug = urlPath
      repoName = urlPath.split('/').pop() || ''
    } catch {
      const parts = repoUrl.replace(/\/$/, '').split('/')
      projectSlug = parts.slice(-2).join('/')
      repoName = parts.pop() || ''
    }

    if (!projectSlug || !repoName) {
      return null
    }

    const score = rawRow['score']
    const lfCriticalityScore = typeof score === 'number' ? score : parseFloat(score as string)

    return {
      projectSlug,
      repoName,
      repoUrl,
      lfCriticalityScore: Number.isNaN(lfCriticalityScore) ? undefined : lfCriticalityScore,
    }
  }
}
