import { describe, expect, it } from 'vitest'

import { ProviderAccessDeniedError, ProviderContractError, RateLimitError } from '../../http/errors'
import { interpretGithubResponse } from './interpret'

function forbidden(message: string) {
  return interpretGithubResponse({ status: 403, headers: {}, data: { message } })
}

describe('interpretGithubResponse 403', () => {
  it.each([
    'Although you appear to have the correct authorization credentials, the `maximhq` organization has an IP allow list enabled, and your IP address is not permitted to access this resource.',
    'Resource protected by organization SAML enforcement. You must grant your Personal Access token access to this organization.',
  ])('treats an org policy refusal as access denied: %s', (message) => {
    const err = forbidden(message)

    expect(err).toBeInstanceOf(ProviderAccessDeniedError)
    expect(err?.message).toContain(message)
    expect(err?.options?.status).toBe(403)
  })

  it('keeps a secondary rate limit as a rate limit', () => {
    expect(forbidden('You have exceeded a secondary rate limit')).toBeInstanceOf(RateLimitError)
  })

  it('keeps a missing app permission as a contract error', () => {
    expect(forbidden('Resource not accessible by integration')).toBeInstanceOf(
      ProviderContractError,
    )
  })
})
