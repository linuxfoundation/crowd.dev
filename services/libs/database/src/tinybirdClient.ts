import axios from 'axios'
import https from 'https'

import { getServiceChildLogger } from '@crowd/logging'

const log = getServiceChildLogger('database.tinybirdClient')

export type QueryParams = Record<
  string,
  string | number | boolean | Date | (string | number | boolean)[] | undefined | null
>

export type PipeNames = 'activities_relations_filtered' | 'activities_daily_counts'

export type ActivityRelations = {
  activityId: string
  channel: string
  memberId: string
  organizationId: string
  platform: string
  segmentId: string
  sourceId: string
  sourceParentId: string
  timestamp: Date
  type: string
  attributes: string
  url: string
  body: string
  title: string
}

export type ActivityTimeseriesDatapoint = {
  date: string
  count: number
}

export type Counter = {
  count: number
}

export class TinybirdClient {
  private host: string
  private token: string
  private maxRetries = 3
  private retriableHttpCodes: number[] = [408, 429]

  private static httpsAgent = new https.Agent({
    keepAlive: false, // Disable keep-alive to avoid stale socket reuse
  })

  constructor(token?: string) {
    this.host = process.env.CROWD_TINYBIRD_BASE_URL ?? 'https://api.tinybird.co'
    this.token = token ?? process.env.CROWD_TINYBIRD_ACTIVITIES_TOKEN ?? ''
  }

  /**
   * Get common headers for Tinybird API requests
   * @param contentType - Optional Content-Type header value (default: undefined)
   * @returns Headers object for axios requests
   */
  private getHeaders(contentType?: string): Record<string, string> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
      Accept: 'application/json',
    }

    if (contentType) {
      headers['Content-Type'] = contentType
    }

    return headers
  }

  /**
   * Sleep for a specified number of milliseconds
   */
  private async sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms))
  }

  /**
   * Wait for a specific job to complete
   * @param jobId - The job ID to wait for
   * @param pollInterval - How often to check job status in ms (default: 5000)
   * @param maxWaitTime - Maximum time to wait in ms (default: 300000 = 5 minutes)
   */
  private async waitForJobCompletion(
    jobId: string,
    pollInterval = 5000,
    maxWaitTime = 300000,
  ): Promise<void> {
    const startTime = Date.now()

    log.info(`Waiting for job ${jobId} to complete...`)

    while (Date.now() - startTime < maxWaitTime) {
      try {
        const response = await axios.get(`${this.host}/v0/jobs/${jobId}`, {
          headers: this.getHeaders(),
          httpsAgent: TinybirdClient.httpsAgent,
        })

        const status = response.data.status
        const jobData = response.data

        if (status === 'done') {
          log.info(`Job ${jobId} completed successfully`)
          return
        } else if (status === 'error') {
          const errorMsg = jobData.error || 'Unknown error'
          log.error(`Job ${jobId} failed with error: ${errorMsg}`)
          throw new Error(`Tinybird job ${jobId} failed: ${errorMsg}`)
        }

        log.info(`Job ${jobId} status: ${status}. Waiting ${pollInterval}ms...`)
        await this.sleep(pollInterval)
      } catch (error) {
        // If it's our thrown error about job failure, re-throw it
        if (error.message?.includes('Tinybird job')) {
          throw error
        }
        log.warn(`Failed to check job ${jobId} status: ${error.message}`)
        await this.sleep(pollInterval)
      }
    }

    const errorMsg = `Timeout waiting for job ${jobId} to complete after ${maxWaitTime}ms`
    log.error(errorMsg)
    throw new Error(errorMsg)
  }

  /**
   * Wait for multiple jobs to complete in parallel
   * @param jobIds - Array of job IDs to wait for
   * @param pollInterval - How often to check job status in ms (default: 5000)
   * @param maxWaitTime - Maximum time to wait in ms (default: 300000 = 5 minutes)
   */
  async waitForJobs(jobIds: string[], pollInterval = 5000, maxWaitTime = 300000): Promise<void> {
    if (jobIds.length === 0) {
      return
    }

    log.info(`Waiting for ${jobIds.length} job(s) to complete...`)

    await Promise.all(
      jobIds.map((jobId) => this.waitForJobCompletion(jobId, pollInterval, maxWaitTime)),
    )

    log.info(`All ${jobIds.length} job(s) completed`)
  }

  /**
   * Wrapper to execute axios requests with retry logic for retriable HTTP errors
   * Respects the Retry-After header (in seconds) when present
   * @param fn - The function to execute
   * @param enableRetry - Whether to enable retry logic (default: true)
   * @param maxBackoffMs - Maximum backoff time in ms (default: 300000 = 5 minutes)
   */
  private async withRetry<T>(
    fn: () => Promise<T>,
    enableRetry = true,
    maxBackoffMs = 300000,
  ): Promise<T> {
    let lastError: Error | undefined
    const maxRetries = enableRetry ? this.maxRetries : 0

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        return await fn()
      } catch (error) {
        lastError = error

        const statusCode = error?.response?.status
        if (this.retriableHttpCodes.includes(statusCode) && attempt < maxRetries) {
          log.warn({
            statusCode,
            headers: error.response?.headers,
            responseBody: error.response?.data,
          })

          // Respect Retry-After header if present, otherwise use exponential backoff
          const retryAfter = error.response?.headers?.['retry-after']
          let waitTimeMs: number

          if (retryAfter) {
            waitTimeMs = parseInt(retryAfter, 10) * 1000
          } else {
            // Exponential backoff: 1s * 2^attempt, capped at maxBackoffMs
            const baseDelayMs = 1000
            waitTimeMs = Math.min(baseDelayMs * Math.pow(2, attempt), maxBackoffMs)
          }

          log.warn(
            `Tinybird request failed (${statusCode}). ${retryAfter ? `Retry-After: ${retryAfter}s.` : `Using exponential backoff.`} Waiting ${waitTimeMs}ms before retry ${attempt + 1}/${maxRetries}`,
          )

          await this.sleep(waitTimeMs)
          continue
        }

        // If it's not a retryable error or we've exhausted retries, throw the error
        if (statusCode) {
          log.error(
            { statusCode, responseBody: error.response?.data },
            'Tinybird non-retriable error',
          )
        }
        throw error
      }
    }

    throw lastError
  }

  /**
   * Call a Tinybird pipe in JSON format.
   * @param pipeName - The name of the pipe to call
   * @param params - Query parameters
   * @param withRetry - Enable retry logic for rate limits (default: true)
   */
  async pipe<T = unknown>(
    pipeName: PipeNames,
    params: QueryParams = {},
    withRetry = true,
  ): Promise<T> {
    const searchParams = new URLSearchParams()

    for (const [key, value] of Object.entries(params)) {
      if (value === undefined || value === null) continue

      if (Array.isArray(value)) {
        // join array values in comma-separated string
        searchParams.set(key, value.map((v) => String(v)).join(','))
      } else {
        searchParams.set(key, String(value))
      }
    }

    const url = `${this.host}/v0/pipes/${encodeURIComponent(pipeName)}.json${
      searchParams.toString() ? `?${searchParams}` : ''
    }`

    const result = await this.withRetry(
      async () =>
        axios.get<T>(url, {
          headers: this.getHeaders(),
          httpsAgent: TinybirdClient.httpsAgent,
        }),
      withRetry,
    )

    // TODO: check the response type
    return result.data
  }

  /**
   * POST to /v0/sql to avoid URL length limits and send typed parameters.
   * @param pipeName - The name of the pipe to call
   * @param params - Query parameters
   * @param withRetry - Enable retry logic for rate limits (default: true)
   */
  async pipeSql<T = unknown>(
    pipeName: PipeNames,
    params: QueryParams = {},
    withRetry = true,
  ): Promise<T> {
    // Guard against reserved keys
    const RESERVED_KEYS = new Set(['q', 'pipeline'])
    for (const k of Object.keys(params)) {
      if (RESERVED_KEYS.has(k)) {
        throw new Error(`Parameter "${k}" collides with a reserved Query API key`)
      }
    }

    const query = `% SELECT * FROM ${pipeName} FORMAT JSON`

    // Copy user params as-is, preserving arrays and primitives
    const bodyParams: Record<string, unknown> = { format: 'json' }
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null) continue

      // Sanity: Tinybird accepts arrays, booleans, numbers, strings
      if (Array.isArray(v)) {
        bodyParams[k] = v.map((x) => String(x))
      } else if (typeof v === 'string' || typeof v === 'number' || typeof v === 'boolean') {
        bodyParams[k] = v
      } else {
        // If you accidentally pass a Date here, throw to avoid silent mismatch
        throw new Error(
          `Unsupported param type for "${k}". Normalize to string/array/number/boolean.`,
        )
      }
    }

    return await this.executeSql<T>(query, bodyParams, withRetry)
  }

  /**
   * Execute raw SQL query on Tinybird
   * Useful for queries that don't go through named pipes (e.g., ALTER TABLE, direct SELECT)
   * @param query - SQL query to execute
   * @param bodyParams - Optional query parameters
   * @param withRetry - Enable retry logic for rate limits (default: true)
   */
  async executeSql<T = unknown>(
    query: string,
    bodyParams?: Record<string, unknown>,
    withRetry = true,
  ): Promise<T> {
    const url = `${this.host}/v0/sql`
    const body: Record<string, unknown> = { q: query, ...bodyParams }

    const result = await this.withRetry(
      async () =>
        axios.post<T>(url, body, {
          headers: this.getHeaders('application/json'),
          responseType: 'json',
          httpsAgent: TinybirdClient.httpsAgent,
        }),
      withRetry,
    )

    return result.data
  }

  /**
   * Delete data from a Tinybird datasource using the delete API
   * See: https://www.tinybird.co/docs/classic/get-data-in/data-operations/replace-and-delete-data#delete-data-selectively
   *
   * @param datasourceName - Name of the datasource to delete from
   * @param deleteCondition - SQL expression filter (e.g., "repoId = 'xxx'", "id IN ('a', 'b')")
   * @param withRetry - Enable retry logic for rate limits (default: true)
   * @returns Job response with job_id and job_url for tracking deletion progress
   */
  async deleteDatasource(
    datasourceName: string,
    deleteCondition: string,
    withRetry = true,
    waitForCompletion = false,
  ): Promise<{
    id: string
    job_id: string
    job_url: string
    status: string
    job: {
      kind: string
      id: string
      job_id: string
      status: string
      datasource: {
        id: string
        name: string
      }
      delete_condition: string
    }
  }> {
    const url = `${this.host}/v0/datasources/${encodeURIComponent(datasourceName)}/delete`

    // Tinybird expects URL-encoded form data, not JSON
    const payload = `delete_condition=${encodeURIComponent(deleteCondition)}`

    const result = await this.withRetry(
      async () =>
        axios.post(url, payload, {
          headers: this.getHeaders('application/x-www-form-urlencoded'),
          responseType: 'json',
          httpsAgent: TinybirdClient.httpsAgent,
        }),
      withRetry,
    )

    const jobData = result.data

    // Wait for the delete job to complete to avoid hitting the maximum concurrent jobs limit
    if (waitForCompletion && jobData.job_id) {
      await this.waitForJobCompletion(jobData.job_id)
    }

    return jobData
  }
}
