import axios from 'axios'

import type { BudgetProbe } from '../../pool/tokenPool'

import { GITHUB_REQUEST_TIMEOUT_MS } from './appToken'

interface RateLimitResource {
  limit: number
  remaining: number
  reset: number
}

export const probeGithubBudget: BudgetProbe = async (_platform, token) => {
  try {
    const response = await axios.get('https://api.github.com/rate_limit', {
      headers: {
        Authorization: `Bearer ${token.value}`,
        Accept: 'application/vnd.github+json',
      },
      timeout: GITHUB_REQUEST_TIMEOUT_MS,
    })
    const graphql = response.data?.resources?.graphql as RateLimitResource | undefined
    if (!graphql) {
      return null
    }
    return {
      limit: graphql.limit,
      remaining: graphql.remaining,
      resetAt: new Date(graphql.reset * 1000),
    }
  } catch {
    return null
  }
}
