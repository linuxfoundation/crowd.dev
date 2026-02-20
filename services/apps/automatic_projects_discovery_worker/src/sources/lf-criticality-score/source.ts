import http from 'http'
import https from 'https'
import { Readable } from 'stream'

import { getServiceLogger } from '@crowd/logging'

import { IDatasetDescriptor, IDiscoverySource, IDiscoverySourceRow } from '../types'

const log = getServiceLogger()

const DEFAULT_API_URL = 'https://hypervascular-nonduplicative-vern.ngrok-free.dev'
const PAGE_SIZE = 100

interface LfApiResponse {
  page: number
  pageSize: number
  total: number
  totalPages: number
  data: LfApiRow[]
}

interface LfApiRow {
  runDate: string
  repoUrl: string
  owner: string
  repoName: string
  contributors: number
  organizations: number
  sizeSloc: number
  lastUpdated: number
  age: number
  commitFreq: number
  score: number
}

function getApiBaseUrl(): string {
  return (process.env.LF_CRITICALITY_SCORE_API_URL ?? DEFAULT_API_URL).replace(/\/$/, '')
}

async function fetchPage(
  baseUrl: string,
  startDate: string,
  endDate: string,
  page: number,
): Promise<LfApiResponse> {
  const url = `${baseUrl}/projects/scores?startDate=${startDate}&endDate=${endDate}&page=${page}&pageSize=${PAGE_SIZE}`

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

/**
 * Generates the first day and last day of a given month.
 * monthOffset = 0 → current month, -1 → previous month, etc.
 */
function monthRange(monthOffset: number): { startDate: string; endDate: string } {
  const now = new Date()
  const year = now.getUTCFullYear()
  const month = now.getUTCMonth() + monthOffset // can be negative; Date handles rollover

  const first = new Date(Date.UTC(year, month, 1))
  const last = new Date(Date.UTC(year, month + 1, 0)) // last day of month

  const pad = (n: number) => String(n).padStart(2, '0')
  const fmt = (d: Date) =>
    `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`

  return { startDate: fmt(first), endDate: fmt(last) }
}

export class LfCriticalityScoreSource implements IDiscoverySource {
  public readonly name = 'lf-criticality-score'
  public readonly format = 'json' as const

  async listAvailableDatasets(): Promise<IDatasetDescriptor[]> {
    const baseUrl = getApiBaseUrl()

    // Return one dataset per month for the last 12 months (newest first)
    const datasets: IDatasetDescriptor[] = []

    for (let offset = 0; offset >= -11; offset--) {
      const { startDate, endDate } = monthRange(offset)
      const id = startDate.slice(0, 7) // e.g. "2026-02"

      datasets.push({
        id,
        date: startDate,
        url: `${baseUrl}/projects/scores?startDate=${startDate}&endDate=${endDate}`,
      })
    }

    return datasets
  }

  /**
   * Returns an object-mode Readable that fetches all pages from the API
   * and pushes each row as a plain object. Activities.ts iterates this
   * directly (no csv-parse) because format === 'json'.
   */
  async fetchDatasetStream(dataset: IDatasetDescriptor): Promise<Readable> {
    const baseUrl = getApiBaseUrl()

    // Extract startDate and endDate from the stored URL
    const parsed = new URL(dataset.url)
    const startDate = parsed.searchParams.get('startDate') ?? ''
    const endDate = parsed.searchParams.get('endDate') ?? ''

    const stream = new Readable({ objectMode: true, read() {} })

    // Fetch pages asynchronously and push rows into the stream
    ;(async () => {
      try {
        let page = 1
        let totalPages = 1

        do {
          const response = await fetchPage(baseUrl, startDate, endDate, page)
          totalPages = response.totalPages

          for (const row of response.data) {
            stream.push(row)
          }

          log.debug(
            { datasetId: dataset.id, page, totalPages, rowsInPage: response.data.length },
            'LF Criticality Score page fetched.',
          )

          page++
        } while (page <= totalPages)

        stream.push(null) // signal end of stream
      } catch (err) {
        stream.destroy(err instanceof Error ? err : new Error(String(err)))
      }
    })()

    return stream
  }

  parseRow(rawRow: Record<string, unknown>): IDiscoverySourceRow | null {
    const repoUrl = rawRow['repoUrl'] as string | undefined
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
