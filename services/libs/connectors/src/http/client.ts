import axios, { AxiosHeaders, AxiosRequestConfig, AxiosResponse } from 'axios'

import { timeout } from '@crowd/common'
import type { Logger } from '@crowd/logging'

import {
  ConnectorError,
  ProviderUnavailableError,
  RateLimitError,
  errorFromHttpStatus,
} from './errors'

export interface IPooledToken {
  id: string
  value: string
}

export interface HttpResponse {
  status: number
  headers: Record<string, string>
  data: unknown
}

export type TokenApplier = (config: AxiosRequestConfig, token: IPooledToken) => AxiosRequestConfig

export type ResponseInterpreter = (response: HttpResponse) => ConnectorError | null

export interface HttpClientDeps {
  acquireToken: () => Promise<IPooledToken>
  parkToken: (tokenId: string, resumeAt: Date) => Promise<void>
  invalidateToken: (tokenId: string) => Promise<void>
  log: Logger
  applyToken?: TokenApplier
  interpretResponse?: ResponseInterpreter
}

export interface ConnectorHttp {
  request<T>(config: AxiosRequestConfig): Promise<T>
  requestCount(): number
}

type CountingHttpClientDeps = HttpClientDeps & { countRequest: () => void }

const MAX_ATTEMPTS = 3
const BACKOFF_BASE_MS = 1000
const RATE_LIMIT_FALLBACK_MS = 60_000
const DEFAULT_TIMEOUT_MS = 60_000

export function createHttpClient(deps: HttpClientDeps): ConnectorHttp {
  let requests = 0
  const countingDeps: CountingHttpClientDeps = {
    ...deps,
    countRequest: () => {
      requests += 1
    },
  }
  return {
    request: <T>(config: AxiosRequestConfig) => requestWithRetry<T>(countingDeps, config),
    requestCount: () => requests,
  }
}

async function requestWithRetry<T>(
  deps: CountingHttpClientDeps,
  config: AxiosRequestConfig,
): Promise<T> {
  let lastError: ConnectorError = new ProviderUnavailableError()
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      return await attemptRequest<T>(deps, config, true)
    } catch (err) {
      if (!(err instanceof ConnectorError) || err.errorClass !== 'provider.unavailable') {
        throw err
      }
      lastError = err
      if (attempt < MAX_ATTEMPTS) {
        const delay = BACKOFF_BASE_MS * 2 ** (attempt - 1)
        deps.log.warn({ attempt, delay, reason: err.message }, 'provider unavailable, backing off')
        await timeout(delay)
      }
    }
  }
  throw lastError
}

async function attemptRequest<T>(
  deps: CountingHttpClientDeps,
  config: AxiosRequestConfig,
  allowTokenRotation: boolean,
): Promise<T> {
  const token = await deps.acquireToken()
  const response = await send<T>(deps, config, token)
  const headers = normalizeHeaders(response.headers)
  const error = classifyResponse(deps, response.status, headers, response.data)

  if (!error) {
    return response.data
  }

  if (error.errorClass === 'provider.rate_limit') {
    const resumeAt = error.options?.resumeAt ?? computeResumeAt(headers)
    await deps.parkToken(token.id, resumeAt)
    if (allowTokenRotation) {
      deps.log.info(
        { tokenId: token.id, resumeAt },
        'token rate limited, retrying with fresh token',
      )
      return attemptRequest<T>(deps, config, false)
    }
    throw new RateLimitError(error.message, { ...error.options, resumeAt })
  }

  if (error.errorClass === 'provider.auth') {
    await deps.invalidateToken(token.id)
    deps.log.warn(
      { tokenId: token.id, status: response.status },
      'token invalidated on auth failure',
    )
    if (allowTokenRotation) {
      return attemptRequest<T>(deps, config, false)
    }
  }

  throw error
}

async function send<T>(
  deps: CountingHttpClientDeps,
  config: AxiosRequestConfig,
  token: IPooledToken,
): Promise<AxiosResponse<T>> {
  const applyToken = deps.applyToken ?? applyBearerToken
  const authenticatedConfig = {
    timeout: DEFAULT_TIMEOUT_MS,
    ...applyToken(config, token),
    validateStatus: () => true,
  }
  deps.countRequest()
  try {
    return await axios.request<T>(authenticatedConfig)
  } catch (err) {
    throw new ProviderUnavailableError('no response from provider', { cause: err })
  }
}

function applyBearerToken(config: AxiosRequestConfig, token: IPooledToken): AxiosRequestConfig {
  const headers = AxiosHeaders.from(config.headers)
  headers.set('Authorization', `Bearer ${token.value}`)
  return { ...config, headers }
}

function classifyResponse(
  deps: HttpClientDeps,
  status: number,
  headers: Record<string, string>,
  data: unknown,
): ConnectorError | null {
  const custom = deps.interpretResponse?.({ status, headers, data })
  if (custom) {
    return custom
  }
  if (isRateLimited(status, headers)) {
    return new RateLimitError(`provider rate limited (status ${status})`, {
      status,
      resumeAt: computeResumeAt(headers),
    })
  }
  if (status >= 300) {
    return errorFromHttpStatus(status)
  }
  return null
}

function isRateLimited(status: number, headers: Record<string, string>): boolean {
  if (status === 429) {
    return true
  }
  return (
    status === 403 &&
    (headers['x-ratelimit-remaining'] === '0' || headers['retry-after'] !== undefined)
  )
}

function computeResumeAt(headers: Record<string, string>): Date {
  const retryAfter = headers['retry-after']
  const retryAfterSeconds = Number(retryAfter)
  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
    return new Date(Date.now() + retryAfterSeconds * 1000)
  }
  const retryAfterDateMs = Date.parse(retryAfter ?? '')
  if (Number.isFinite(retryAfterDateMs) && retryAfterDateMs > Date.now()) {
    return new Date(retryAfterDateMs)
  }
  // x-ratelimit-reset describes the primary bucket only. Honouring it on a
  // secondary/abuse block parks the token for the rest of the hour instead of
  // the minute or so the block actually lasts.
  if (headers['x-ratelimit-remaining'] === '0') {
    const resetEpochSeconds = Number(headers['x-ratelimit-reset'])
    if (Number.isFinite(resetEpochSeconds) && resetEpochSeconds * 1000 > Date.now()) {
      return new Date(resetEpochSeconds * 1000)
    }
  }
  return new Date(Date.now() + RATE_LIMIT_FALLBACK_MS)
}

function normalizeHeaders(raw: AxiosResponse['headers']): Record<string, string> {
  const headers: Record<string, string> = {}
  for (const [key, value] of Object.entries(raw)) {
    if (value !== undefined && value !== null) {
      headers[key.toLowerCase()] = Array.isArray(value) ? value.join(', ') : String(value)
    }
  }
  return headers
}
