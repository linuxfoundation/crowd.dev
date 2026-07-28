import { ApplicationFailure } from '@temporalio/workflow'
import { describe, expect, it } from 'vitest'

import { buildEcosystemNotSupportedFailure } from '../ecosystemSupport'

describe('buildEcosystemNotSupportedFailure', () => {
  it('builds a non-retryable ApplicationFailure tagged ECOSYSTEM_NOT_SUPPORTED', () => {
    const failure = buildEcosystemNotSupportedFailure('npm')
    expect(failure).toBeInstanceOf(ApplicationFailure)
    expect(failure.type).toBe('ECOSYSTEM_NOT_SUPPORTED')
    expect(failure.nonRetryable).toBe(true)
    expect(failure.message).toContain('npm')
  })

  it('falls back to "unknown" when ecosystem is null', () => {
    const failure = buildEcosystemNotSupportedFailure(null)
    expect(failure.message).toContain('unknown')
  })
})
