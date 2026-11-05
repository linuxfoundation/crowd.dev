import axios, { AxiosRequestConfig, AxiosResponse } from 'axios'

import { timeout } from '@crowd/common'

import {
  ConnectorError,
  ProviderUnavailableError,
  RateLimitError,
  errorFromHttpStatus,
} from '../../http/errors'

export const OCTOLENS_API_BASE_URL = 'https://app.octolens.com/api/v2'
const OCTOLENS_REQUEST_TIMEOUT_MS = 30_000
// Only retries provider.unavailable (5xx/network); rate-limit and auth errors bubble up
// immediately so the dispatcher parks the whole sync_unit instead of retrying in-process.
const MAX_ATTEMPTS = 4
const BACKOFF_BASE_MS = 1_000
const BACKOFF_CAP_MS = 10_000

export function octolensRequestConfig(token: string): AxiosRequestConfig {
  return {
    headers: { Authorization: `Bearer ${token}` },
    timeout: OCTOLENS_REQUEST_TIMEOUT_MS,
    validateStatus: () => true,
  }
}

export async function withOctolensRetries<T>(attempt: () => Promise<T>): Promise<T> {
  let lastError: ConnectorError | undefined
  for (let i = 1; i <= MAX_ATTEMPTS; i++) {
    try {
      return await attempt()
    } catch (err) {
      if (!(err instanceof ConnectorError) || err.errorClass !== 'provider.unavailable') {
        throw err
      }
      lastError = err
      if (i < MAX_ATTEMPTS) {
        await timeout(Math.min(BACKOFF_BASE_MS * 2 ** (i - 1), BACKOFF_CAP_MS))
      }
    }
  }
  throw lastError
}

export async function sendOctolensRequest(
  send: () => Promise<AxiosResponse>,
  what: string,
): Promise<unknown> {
  // validateStatus covers only a received 4xx/5xx response — a connection failure
  // (refused/timeout/DNS) rejects as a plain Axios error unless wrapped here.
  let response: AxiosResponse
  try {
    response = await send()
  } catch (err) {
    throw new ProviderUnavailableError('no response from octolens', { cause: err })
  }

  if (response.status === 429) {
    throw new RateLimitError(`octolens rate limited (status ${response.status})`, {
      status: response.status,
      resumeAt: computeResumeAt(response.headers as Record<string, string>),
    })
  }
  if (response.status >= 300) {
    throw errorFromHttpStatus(response.status, `octolens ${what} request failed`)
  }
  return response.data
}

export async function octolensGet(token: string, path: string, what: string): Promise<unknown> {
  return withOctolensRetries(() =>
    sendOctolensRequest(
      () => axios.get(`${OCTOLENS_API_BASE_URL}${path}`, octolensRequestConfig(token)),
      what,
    ),
  )
}

// Octolens's hourly cap resets on a fixed UTC-hour bucket, not a rolling window, so
// Retry-After is the precise signal; X-RateLimit-Reset is the fallback.
function computeResumeAt(headers: Record<string, string>): Date {
  const retryAfterSeconds = Number(headers['retry-after'])
  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
    return new Date(Date.now() + retryAfterSeconds * 1000)
  }
  const resetEpochSeconds = Number(headers['x-ratelimit-reset'])
  if (Number.isFinite(resetEpochSeconds) && resetEpochSeconds * 1000 > Date.now()) {
    return new Date(resetEpochSeconds * 1000)
  }
  return new Date(Date.now() + 60_000)
}
