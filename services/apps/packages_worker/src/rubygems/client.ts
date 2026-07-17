import axios from 'axios'

import { acquireRubyGemsSlot, parseRetryAfterMs } from './rateLimiter'
import {
  RubyGemsFetchResult,
  RubyGemsGemResponse,
  RubyGemsOwner,
  RubyGemsVersionItem,
} from './types'

const MAX_RATE_LIMIT_RETRIES = 5

async function rubyGemsGet<T>(url: string): Promise<RubyGemsFetchResult<T>> {
  for (let attempt = 0; ; attempt++) {
    await acquireRubyGemsSlot()
    try {
      const resp = await axios.get<T>(url, { timeout: 15000 })
      return resp.data
    } catch (err) {
      if (axios.isAxiosError(err)) {
        const status = err.response?.status
        if (status === 404) return { kind: 'NOT_FOUND', status, message: err.message }
        if (status === 429) {
          if (attempt >= MAX_RATE_LIMIT_RETRIES) {
            return { kind: 'RATE_LIMIT', status, message: err.message }
          }
          await new Promise((r) =>
            setTimeout(r, parseRetryAfterMs(err.response?.headers['retry-after'])),
          )
          continue
        }
      }
      throw err
    }
  }
}

export function fetchGem(name: string): Promise<RubyGemsFetchResult<RubyGemsGemResponse>> {
  return rubyGemsGet<RubyGemsGemResponse>(
    `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}.json`,
  )
}

export function fetchVersions(name: string): Promise<RubyGemsFetchResult<RubyGemsVersionItem[]>> {
  return rubyGemsGet<RubyGemsVersionItem[]>(
    `https://rubygems.org/api/v1/versions/${encodeURIComponent(name)}.json`,
  )
}

export function fetchOwners(name: string): Promise<RubyGemsFetchResult<RubyGemsOwner[]>> {
  return rubyGemsGet<RubyGemsOwner[]>(
    `https://rubygems.org/api/v1/gems/${encodeURIComponent(name)}/owners.json`,
  )
}
