import axios from 'axios'

import { abortableSleep, acquireRubyGemsSlot, parseRetryAfterMs } from './rateLimiter'
import {
  RubyGemsFetchResult,
  RubyGemsGemResponse,
  RubyGemsOwner,
  RubyGemsVersionItem,
} from './types'

const MAX_RATE_LIMIT_RETRIES = 5

async function rubyGemsGet<T>(url: string, signal?: AbortSignal): Promise<RubyGemsFetchResult<T>> {
  for (let attempt = 0; ; attempt++) {
    await acquireRubyGemsSlot(signal)
    try {
      const resp = await axios.get<T>(url, { timeout: 15000, signal })
      return resp.data
    } catch (err) {
      signal?.throwIfAborted()
      if (axios.isAxiosError(err)) {
        const status = err.response?.status
        if (status === 404) return { kind: 'NOT_FOUND', status, message: err.message }
        if (status === 429) {
          if (attempt >= MAX_RATE_LIMIT_RETRIES) {
            return { kind: 'RATE_LIMIT', status, message: err.message }
          }
          await abortableSleep(parseRetryAfterMs(err.response?.headers['retry-after']), signal)
          continue
        }
      }
      throw err
    }
  }
}

export function fetchGem(
  name: string,
  signal?: AbortSignal,
): Promise<RubyGemsFetchResult<RubyGemsGemResponse>> {
  return rubyGemsGet<RubyGemsGemResponse>(
    `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}.json`,
    signal,
  )
}

export function fetchVersions(
  name: string,
  signal?: AbortSignal,
): Promise<RubyGemsFetchResult<RubyGemsVersionItem[]>> {
  return rubyGemsGet<RubyGemsVersionItem[]>(
    `https://rubygems.org/api/v1/versions/${encodeURIComponent(name)}.json`,
    signal,
  )
}

export function fetchOwners(
  name: string,
  signal?: AbortSignal,
): Promise<RubyGemsFetchResult<RubyGemsOwner[]>> {
  return rubyGemsGet<RubyGemsOwner[]>(
    `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}/owners.json`,
    signal,
  )
}
