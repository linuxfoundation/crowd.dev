import axios from 'axios'

import { isSecondaryRateLimitMessage } from '@crowd/connectors/src/connectors/github/interpret'

import type { LifecycleSkipReason } from './githubLifecycle'

const GONE_STATUSES = new Set([404, 410, 451])

function responseHeader(err: unknown, name: string): string | undefined {
  if (!axios.isAxiosError(err)) {
    return undefined
  }
  const value = err.response?.headers?.[name]
  return value === undefined || value === null ? undefined : String(value)
}

function responseMessage(err: unknown): string | undefined {
  const data: unknown = axios.isAxiosError(err) ? err.response?.data : undefined
  return typeof data === 'object' &&
    data !== null &&
    'message' in data &&
    typeof data.message === 'string'
    ? data.message
    : undefined
}

function isRateLimited(err: unknown): boolean {
  if (!axios.isAxiosError(err)) {
    return false
  }
  const status = err.response?.status
  if (status === 429) {
    return true
  }
  return (
    status === 403 &&
    (responseHeader(err, 'x-ratelimit-remaining') === '0' ||
      responseHeader(err, 'retry-after') !== undefined ||
      isSecondaryRateLimitMessage(responseMessage(err)))
  )
}

export function rateLimitResetAt(err: unknown): Date | null {
  const retryAfter = Number(responseHeader(err, 'retry-after'))
  if (Number.isFinite(retryAfter) && responseHeader(err, 'retry-after') !== undefined) {
    return new Date(Date.now() + retryAfter * 1000)
  }
  const reset = Number(responseHeader(err, 'x-ratelimit-reset'))
  if (Number.isFinite(reset) && responseHeader(err, 'x-ratelimit-reset') !== undefined) {
    return new Date(reset * 1000)
  }
  return null
}

export function githubResolveSkipReason(
  err: unknown,
): 'repo_not_found' | 'rate_limited' | 'github_error' {
  if (isRateLimited(err)) {
    return 'rate_limited'
  }
  return axios.isAxiosError(err) && err.response?.status === 404 ? 'repo_not_found' : 'github_error'
}

export function lifecycleSkipReasonFor(
  stage: 'mint' | 'resolve',
  err: unknown,
): LifecycleSkipReason {
  return stage === 'mint' ? 'token_mint_failed' : githubResolveSkipReason(err)
}

export function renameProbeFailure(err: unknown): 'gone' | 'rate_limited' | 'unknown' {
  if (isRateLimited(err)) {
    return 'rate_limited'
  }
  const status = axios.isAxiosError(err) ? err.response?.status : undefined
  return status !== undefined && GONE_STATUSES.has(status) ? 'gone' : 'unknown'
}
