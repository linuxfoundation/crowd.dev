import http from 'http'
import https from 'https'
import { Readable } from 'stream'

import { getServiceLogger } from '@crowd/logging'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'

const log = getServiceLogger()

const DEFAULT_API_HOST = 'lf-criticality-score-api.example.com'
const DEFAULT_API_PORT = 443
const PAGE_SIZE = 100

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
  const host = (process.env.LF_CRITICALITY_SCORE_API_HOST ?? DEFAULT_API_HOST).trim().replace(/\/$/, '')
  const port = parseInt(process.env.LF_CRITICALITY_SCORE_API_PORT ?? String(DEFAULT_API_PORT), 10)
  const scheme = port === 443 ? 'https' : 'http'
  return `${scheme}://${host}:${port}`
}

async function fetchPage(
  baseUrl: string,
  page: number,
  scoredAfter?: string,
): Promise<LfApiResponse> {
  const params = new URLSearchParams({ page: String(page), pageSize: String(PAGE_SIZE) })
  if (scoredAfter) params.set('scoredAfter', scoredAfter)
  const url = `${baseUrl}/projects?${params.toString()}`

  return new Promise((resolve, reject) => {
    const client = url.startsWith('https://') ? https : http

    const req = client.get(url, (res) => {
      if (res.statusCode !== 200) {
        reject(new Error(`LF Criticality Score API returned status ${res.statusCode} for ${url}`))
        res.resume()
        return
      }

      const chunks: Uint8Array[] = []
      res.on('data', (chunk: Uint8Array) => chunks.push(chunk))
      res.on('end', () => {
        try {
          resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')) as LfApiResponse)
        } catch (err) {
          reject(new Error(`Failed to parse LF Criticality Score API response: ${err}`))
        }
      })
      res.on('error', reject)
    })

    req.on('error', reject)
    req.end()
  })
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

    async function* pages() {
      let page = 1
      let totalPages = 1

      do {
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
