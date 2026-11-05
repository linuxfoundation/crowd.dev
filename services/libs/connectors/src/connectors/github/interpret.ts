import type { ResponseInterpreter } from '../../http/client'
import {
  ProviderAccessDeniedError,
  ProviderAuthError,
  ProviderContractError,
  RateLimitError,
} from '../../http/errors'

interface GithubErrorBody {
  errors?: { type?: string }[]
  message?: string
}

// Legacy prod parity (baseQuery.ts): GitHub's "temporarily blocked" 403 is retryable
const RETRYABLE_403_MESSAGES = [
  'secondary rate limit',
  'although you appear to have the correct authorization credentials',
]

const ORG_POLICY_REFUSAL_MESSAGES = ['ip allow list', 'saml enforcement']

export function isOrgPolicyRefusalMessage(message: string | undefined): boolean {
  const lower = message?.toLowerCase() ?? ''
  return ORG_POLICY_REFUSAL_MESSAGES.some((m) => lower.includes(m))
}

export function isSecondaryRateLimitMessage(message: string | undefined): boolean {
  return message?.toLowerCase().includes('secondary rate limit') ?? false
}

export function isRetryable403Message(message: string | undefined): boolean {
  const lower = message?.toLowerCase() ?? ''
  return RETRYABLE_403_MESSAGES.some((m) => lower.includes(m))
}

function hasRateLimitHeaders(headers: Record<string, string>): boolean {
  return headers['retry-after'] !== undefined || headers['x-ratelimit-remaining'] === '0'
}

export const interpretGithubResponse: ResponseInterpreter = (response) => {
  const body = response.data as GithubErrorBody | null
  if (response.status === 200) {
    // GitHub docs say RATE_LIMITED, but installation tokens return RATE_LIMIT
    if (body?.errors?.some((e) => e.type?.includes('RATE_LIMIT'))) {
      return new RateLimitError('github graphql rate limited')
    }
    return null
  }
  if (response.status === 401) {
    return new ProviderAuthError(`github unauthorized: ${body?.message ?? 'no message'}`, {
      status: 401,
    })
  }
  if (response.status === 403) {
    if (isOrgPolicyRefusalMessage(body?.message)) {
      return new ProviderAccessDeniedError(`github access denied: ${body?.message}`, {
        status: 403,
      })
    }
    if (isRetryable403Message(body?.message)) {
      return new RateLimitError(`github rate limited: ${body?.message}`)
    }
    if (hasRateLimitHeaders(response.headers)) {
      return null
    }
    return new ProviderContractError(`github forbidden: ${body?.message ?? 'no message'}`, {
      status: 403,
    })
  }
  return null
}
