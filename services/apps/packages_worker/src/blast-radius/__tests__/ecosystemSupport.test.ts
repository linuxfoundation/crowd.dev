import { ApplicationFailure } from '@temporalio/workflow'
import { describe, expect, it } from 'vitest'

import { SUPPORTED_ECOSYSTEMS, buildEcosystemNotSupportedFailure } from '../ecosystemSupport'

describe('SUPPORTED_ECOSYSTEMS', () => {
  it('includes cargo alongside npm, go, and maven', () => {
    expect(SUPPORTED_ECOSYSTEMS).toEqual(['npm', 'go', 'maven', 'cargo'])
  })
})

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
